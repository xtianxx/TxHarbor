# 017 carrier store 分类器严格化（R1）：DriverStatus 结构化解析复验证据

- 对象：drill fixture 的 docker image store 分类器
  （`internal/recovery/targetwriter_drillbridge_linux_test.go` 的 `drillCarrierStoreClassify`），
  及其分类子例（`internal/recovery/targetwriter_drillbridge_identity_linux_test.go`）。
- 记录范围：R1 修订（**子串匹配 → 严格结构化解析**）在本机的复验证据归档。
  **本目录只读用，不含任何实现改动**；`logs/` 为原始运行产物，内容未改写
  （本 README 只在其外层包一层说明；SHA256SUMS 只记录哈希）。
- 运行基线：`HEAD = efcd9469323f60825d9b598bbef25316a401d37b` **加两个测试文件的未提交工作树修改**
  （git status 快照与文件 sha256 见 §4 与 `logs/env.txt`）。
- 本批结论：§3 的 6 组命令**全部 rc=0**；identity 单元组 4 顶层/48 子例、race 组同、real-docker 组 2 顶层；
  **无 fail、无 skip、无 race 报告**（逐项见 §2、§3）。

## 1. R1 修正说明：子串匹配 → 严格解析

### 1.1 修正前后行为

修正前（R1 之前工作树）：

```go
if strings.Contains(driverStatusJSON, "io.containerd.snapshotter.v1") {
    return drillCarrierStoreContainerd, nil
}
```

对**原始文本做子串匹配**：`io.containerd.snapshotter.v10`、`prefix-…-suffix` 包裹串、
任意键（如 `other`）承载同文本、裸字符串、截断碎片等都会被误收为 containerd store。

修正后（当前工作树，`targetwriter_drillbridge_linux_test.go`）：

- 解析：`var rows [][]json.RawMessage` + `json.Unmarshal`（整个 DriverStatus）；
- 结构校验：非空数组；每行**恰 2 元**；每个元素必须是 **JSON 字符串 token**
  （`bytes.TrimSpace` 后两端引号检查 + 解码进 `string`；number/null/object/array 一律拒绝）；
- 标记比较：**仅键精确等于 `"driver-type"`** 的行参与；值必须**精确等于**
  `drillCarrierStoreContainerdDriverType = "io.containerd.snapshotter.v1"`；
- 重复/冲突：`driver-type` 行数 > 1 一律拒绝（v1+v1 与 v1+v10 都拒绝）；
- classic 回退：无 `driver-type` 行且 `driver`（TrimSpace 后）**(精确)** 等于 `"overlay2"`；
- 其余一律拒绝（无标记的 overlayfs、vfs、空 driver、任何非 overlay2 名）。

关键点（与源码逐条对应）：

- `drillCarrierStatusToken`：token 的“是字符串”判定先于解码（外层引号检查），
  杜绝 number/null/对象被强转成字符串；
- `value != drillCarrierStoreContainerdDriverType`：**精确相等**，杜绝 v10 / 包裹 / 空串；
- 结构校验对**整个 status** 先执行完毕，再进入标记与 classic 层级：
  **`overlay2` 不能绕过结构校验**（driver=overlay2 但 status 畸形照样拒绝）。

### 1.2 决策次序（fail-closed）

| # | 层 | 检查 | 通过后 | 失败 |
| --- | --- | --- | --- | --- |
| 1 | 结构 | DriverStatus 可解码为 JSON **数组行** `[][]json.RawMessage`；`[]`、`null`、空串、裸串、裸对象、截断均拒绝 | 2 | err |
| 2 | 结构 | 每行恰好 **2 元**（1 元/3 元行拒绝） | 3 | err |
| 3 | 结构 | 每元是 **JSON 字符串 token**（number/null/object/array 拒绝） | 4 | err |
| 4 | 标记 | 每个 `driver-type` 行的值必须**精确等于** `io.containerd.snapshotter.v1`；`driver-type` 行 >1 **拒绝**（重复/冲突） | 5 | err |
| 5 | 标记 | 恰 1 个精确 `driver-type` 行 → `containerd-snapshotter`；**优先于 driver 名**（driver=overlay2 也判 containerd） | 结果 | — |
| 6 | classic | 无 `driver-type` 行且 driver（TrimSpace）精确 == `overlay2` → `classic-overlay2` | 结果 | 7 |
| 7 | 拒绝 | 其余：vfs / 无标记 overlayfs / 空 driver 等 | — | err |

### 1.3 反例归属（对照 `TestDrillCarrierStoreClassify` 的实际 24 个子例）

| 反例/子例 | 归属层 | 输入（driver / DriverStatus） | 期望 |
| --- | --- | --- | --- |
| 真实 runner status（classic 正例） | classic | `overlay2` / 016 制品 `runner-driver-status.json` | classic |
| 真实本机 status（containerd 正例） | 标记 | `overlayfs` / 016 制品 `local-driver-status.json` | containerd |
| **优先级组合**：标记压过 overlay2 | 标记 | `overlay2` / `[["driver-type","io.containerd.snapshotter.v1"],["Backing Filesystem","extfs"]]` | containerd |
| 普通元数据无 driver-type → classic | classic | `overlay2` / `[["Backing Filesystem","extfs"]]` | classic |
| **错误键**：非 driver-type 键承载同值文本 | 标记 | `overlay2` / `[["other","io.containerd.snapshotter.v1"]]` | classic（键不参与） |
| **v10**（vfs driver） | 标记 | `vfs` / `[["driver-type","io.containerd.snapshotter.v10"]]` | err |
| **v10**（overlayfs driver） | 标记 | `overlayfs` / `[["driver-type","io.containerd.snapshotter.v10"]]` | err |
| 错误键 + 未知 driver | 拒绝 | `vfs` / `[["other","io.containerd.snapshotter.v1"]]` | err |
| **包裹**串 | 标记 | `overlay2` / `[["driver-type","prefix-io.containerd.snapshotter.v1-suffix"]]` | err |
| 空标记值 | 标记 | `overlay2` / `[["driver-type",""]]` | err |
| **裸串** | 结构 | `overlay2` / `io.containerd.snapshotter.v1` | err |
| **截断** JSON | 结构 | `overlay2` / `[["driver-type",` | err |
| **1 元行** | 结构 | `overlay2` / `[["driver-type"]]` | err |
| **3 元行** | 结构 | `overlay2` / `[["driver-type","io.containerd.snapshotter.v1","extra"]]` | err |
| **number** token | 结构 | `overlay2` / `[["driver-type",123]]` | err |
| **null** token | 结构 | `overlay2` / `[["driver-type",null]]` | err |
| **对象行** | 结构 | `overlay2` / `[{"driver-type":"io.containerd.snapshotter.v1"}]` | err |
| `null` status | 结构 | `overlay2` / `null` | err |
| 空 status 串 | 结构 | `overlay2` / ``（空串） | err |
| 空数组 | 结构 | `overlay2` / `[]` | err |
| **重复** driver-type（v1+v1） | 标记 | `overlayfs` / 两行同值 | err |
| **冲突** driver-type（v1+v10） | 标记 | `overlayfs` / 两行不同版本 | err |
| 真实 classic status + 未知 driver | 拒绝 | `vfs` / 016 制品 `runner-driver-status.json` | err |
| 缺失 store 事实 | 结构 | `` / ``（零值） | err |

说明：表中“归属层”= 该子例首先被哪一层拒绝（或由哪一层命中）。上表即 §1.2 决策次序的
反例覆盖：结构层（1–3）覆盖裸串/截断/1元3元行/number/null/对象行/空数组/空串/null；
标记层（4–5）覆盖 v10/包裹/空值/错误键不参与/重复/冲突/优先级组合；classic（6）与
拒绝（7）覆盖 overlay2 精确回退与其余 driver 的拒绝。

## 2. 实际测试组/子例数（从 `identity-unit.go-test.json` 统计）

统计来源：`logs/identity-unit.go-test.json`（`go test -json` 原始事件流，仅 `Action`/`Test` 字段）。
统计命令：内联 Python（无外部依赖），程序原文与本次输出见 `logs/statistics.txt`，
在仓库根把该文件中的 heredoc 整段粘贴到 shell 即可复跑。

| 指标 | identity-unit | identity-unit-race | carrier-real-docker |
| --- | --- | --- | --- |
| 顶层测试组 | **4** | 4 | 2 |
| 子例合计 | **48** | 48 | 0 |
| └ `TestDrillCarrierStoreClassify`（分类） | **24** | 24 | — |
| └ `TestDrillCarrierIdentityStoreSemantics`（身份） | **18** | 18 | — |
| └ `TestDrillCarrierContainerImageConsistency`（容器） | **6** | 6 | — |
| └ `TestDrillCarrierContentChainMatchesCommittedPayloads` | 0（无子例） | 0 | — |
| 事件计数 pass/fail/skip | 53/0/0 | 53/0/0 | 3/0/0 |

- 顶层 4 组的实际名单（`logs/tests.list.txt` 同源）：
  `TestDrillCarrierContentChainMatchesCommittedPayloads`、
  `TestDrillCarrierStoreClassify`、`TestDrillCarrierIdentityStoreSemantics`、
  `TestDrillCarrierContainerImageConsistency`。
- identity-unit 与 race 两次运行的组/子例/结果计数完全一致（48 子例、52 测试判定 + 1 包级 pass = 53 事件 pass）。
- carrier-real-docker 组：`TestDrillProvisionNativePGToolsCarrierSeal`（3.71s）、
  `TestDrillArmRefusesReplacedELFAndPreStartTamper`（6.81s），包 10.614s，均 PASS。

## 3. 日志清单

时间窗口（UTC；文件内 `go test` 时间戳为 +08:00 本地表达，同一时刻）：

- 前置轮询：2026-10-07T01:55:29Z 首检 → 01:55:56Z 三条件满足 → 01:56:50Z 60s mtime 稳定门通过（READY）。
- 采集批次：2026-10-07T01:57:03Z → 01:57:55Z（`logs/env.txt` 的 batch start/end）。

| 文件 | 命令（原文见 `logs/commands.log`） | rc | 耗时/备注 |
| --- | --- | --- | --- |
| `logs/build.rc` | `go build ./...` | 0 | 7s，无输出（`build.out` 为空，见下注） |
| `logs/gofmt.rc` | `gofmt -l internal/recovery/` | 0 | 0s，无输出（无未格式化文件） |
| `logs/vet.rc` | `go vet -tags drill ./internal/recovery/` | 0 | 1s，无输出 |
| `logs/identity-unit.go-test.json` + `logs/identity-unit.rc` | `go test -tags drill -count=1 -json ./internal/recovery/ -run 'TestDrillCarrier'` | 0 | 4s |
| `logs/identity-unit-race.go-test.json` + `logs/identity-unit-race.rc` | `go test -race -tags drill -count=1 -json ./internal/recovery/ -run 'TestDrillCarrier'` | 0 | 21s，无 race 报告 |
| `logs/carrier-real-docker.go-test.json` + `logs/carrier-real-docker.rc` | `go test -tags drill -count=1 -json ./internal/recovery/ -run 'TestDrillProvisionNativePGToolsCarrierSeal\|TestDrillArmRefusesReplacedELFAndPreStartTamper'` | 0 | 14s，真实 Docker（testcontainers 连本机 29.6.1） |
| `logs/commands.log` | 逐命令记录：每条 CMD 行 + RET 行（rc/耗时/输出文件），另含 AUX 与运行前后 VERIFY（git status、测试文件 sha256） | — | 原始记录 |
| `logs/poll.log` | 前置轮询原始记录（15s 间隔、60s 稳定门、READY 行含两文件 mtime/size） | — | 由 `/tmp/017_poll.log` 原样复制 |
| `logs/env.txt` | 环境快照：date -u 起止、`git rev-parse HEAD`、`git status --porcelain=v2`、测试文件 sha256、go/docker/uname | — | 见下 |
| `logs/tests.list.txt` + `logs/tests.list.rc` | `go test -tags drill -list 'TestDrillCarrier' ./internal/recovery/` | 0 | 辅助：顶层 4 组名单 |
| `logs/source-files.sha256` + `logs/source-files.rc` | `sha256sum` 两个测试文件（独立留存） | 0 | 与 env.txt 一致 |
| `logs/statistics.txt` | 内联 Python 统计（§2 的派生文件，含可复跑命令原文） | — | 派生文件，非原始运行日志 |

> 注：`logs/build.out`、`logs/gofmt.out`、`logs/vet.out` 为 **0 字节空输出**，
> 因仓库 `.gitignore` 的 `*.out` 规则（仓库当前无任何已跟踪 `.out` 文件）未随仓；
> 其退出码见 `logs/build.rc` / `logs/gofmt.rc` / `logs/vet.rc`，命令与耗时见 `logs/commands.log`。

环境（`logs/env.txt` 原文）：Go `go1.26.5 linux/amd64`；Docker client/server `29.6.1 29.6.1`；
`docker info .Driver` = `overlayfs`；`.DriverStatus` = `[["driver-type","io.containerd.snapshotter.v1"]]`
（**本机即 containerd snapshotter store**）；内核 `6.18.33.2-microsoft-standard-WSL2`（WSL2/x86_64）。

**关于 rc**：本批 6 组 + 2 条辅助命令的 `.rc` 全部为 `0`（无任何非零项需标注）；
所有 rc 由命令退出码直接落盘（`rc=$?` 紧随命令，无管道/包装截取），详见 `logs/commands.log`。

**脱敏**：对 `logs/` 执行大小写不敏感扫描 `grep -riE 'token|auth|password|secret' logs/`：
`auth` / `password` / `secret` 零命中；`token` 仅命中 `identity-unit*.go-test.json` 中
**子测试名** `number_token_in_a_driver_status_row_is_rejected` 与
`null_token_in_a_driver_status_row_is_rejected`（指 JSON token 类型，非凭据），
属无害命中，无任何凭据类内容。

## 4. 指纹

- 运行基线：`HEAD = efcd9469323f60825d9b598bbef25316a401d37b`（含两个测试文件未提交修改的工作树快照；
  `git status --porcelain=v2` 原文见 `logs/env.txt`：两条 `1 .M` 为两个测试文件，`? …/017-…/` 为本目录）。
- 两个测试文件 sha256（本批运行时刻；与 `logs/env.txt`、`logs/source-files.sha256`
  及 `logs/commands.log` 的 post-run VERIFY 三方一致）：

```
03d38e4d015e104807cebb57babcfc894ece21ca625d683ce397beaadd39742e  internal/recovery/targetwriter_drillbridge_linux_test.go   （49751 字节）
174703df592b1c4b3afad5b27366b71d8dd1bf4ef4fe15637b21f2d5e06f0bf7  internal/recovery/targetwriter_drillbridge_identity_linux_test.go   （18064 字节）
```

- 本目录全部文件（含本 README 与 `logs/` 全部日志）逐文件 sha256 见 `SHA256SUMS`
  （生成命令：`find . -type f ! -name SHA256SUMS | sort | xargs sha256sum > SHA256SUMS`，相对路径）。
- 运行前后两次 `sha256sum` 一致（`logs/commands.log` 的 post-run VERIFY 与 env.txt 的 batch-start 值相同），
  即采集全程两测试文件未被再改动。

## 5. 边界与非声明

1. **旧日志未留存**：efcd946 基线上此前的定向 race/fixture 日志**未留存**。
   已检索 `.evidence/` 与 `/tmp`（模式含 `*identity-unit*`、`*carrier-real*`、`*go-test.json*`、
   `*race*` 等），**无本批相关旧日志**；未补造、未恢复、未改写。
   本目录 `logs/` = **本轮（R1 修订后）** 的复验日志，与任何旧轮次无关。
2. **classic 正例数据来源**：分类子例中的 classic 正例（`runner-driver-status.json`）来自
   **诊断 run 37493260522 的制品**（经 `docs/evidence/016-carrier-image-identity/` 归档并被测试运行时读取）；
   containerd 正例（`local-driver-status.json`）为本机（WSL2，containerd store）实测快照。
3. **真实托管全量演练仍未发生**：`diagnostic job ≠ 完整 drill`；本批全部为本机（WSL2）复验，
   与托管 runner 上的日志**分列**，互不替代。
4. **本批未推送**：未执行 `git commit`/`git push`/`gh` 写操作；工作区保持 §4 的未提交状态。
5. **本目录无实现改动**：仅含本 README、`logs/` 原始日志与派生统计、`SHA256SUMS`；
   未改写任何日志内容（rc 与输出均为原样落盘）。
6. 分类器语义的跨 store 依据（classic=config digest / containerd=index digest）见
   `docs/evidence/016-carrier-image-identity/`；本目录只覆盖**分类器严格性（R1）**的复验。

## 6. 复现命令与对应关系

在仓库根依次执行（每条的输出/rc 与本目录文件的对应关系如下）：

```sh
# 1) go build ./...                          → logs/build.rc（输出为空，未随仓）
# 2) gofmt -l internal/recovery/             → logs/gofmt.rc（输出为空，未随仓）
# 3) go vet -tags drill ./internal/recovery/ → logs/vet.rc（输出为空，未随仓）
# 4) go test -tags drill -count=1 -json ./internal/recovery/ -run 'TestDrillCarrier'
#                                            → logs/identity-unit.go-test.json / logs/identity-unit.rc
# 5) go test -race -tags drill -count=1 -json ./internal/recovery/ -run 'TestDrillCarrier'
#                                            → logs/identity-unit-race.go-test.json / logs/identity-unit-race.rc
# 6) go test -tags drill -count=1 -json ./internal/recovery/ \
#      -run 'TestDrillProvisionNativePGToolsCarrierSeal|TestDrillArmRefusesReplacedELFAndPreStartTamper'
#                                            → logs/carrier-real-docker.go-test.json / logs/carrier-real-docker.rc
# 辅助: go test -tags drill -list 'TestDrillCarrier' ./internal/recovery/  → logs/tests.list.txt / .rc
#       sha256sum <两个测试文件>                                             → logs/source-files.sha256 / .rc
```

对应关系说明：

- 每条命令的原始命令行、rc、耗时与输出文件名逐条记录在 `logs/commands.log`；
  `logs/*.rc` 为该命令真实退出码（无管道/包装截取，全部为 0）。
- 源码 ↔ 工作树：`logs/env.txt`（batch start）与 `logs/commands.log`（post-run VERIFY）中的
  两个测试文件 sha256 一致，即本批日志对应的源码 = §4 指纹的工作树状态；
  **该工作树指纹与提交后 blob 的一致性由主提交报告核实**（本目录不替代提交报告）。
- 前置轮询记录（`logs/poll.log`）证明采集启动时两文件已连续 60s 未被改动；
  `logs/env.txt` 的 batch start 亦含当时的 `git status` 快照。
