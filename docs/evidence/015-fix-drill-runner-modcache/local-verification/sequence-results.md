# 受控验证序列结果（本地，2026-10-06，WSL2 Docker）

本文件是受控序列运行 stdout 的整理稿（关键行逐字保留；逐步原始输出见同目录
`v0a-*.log`、`v0b-*.log`、`v2-*.log`、`v3-*.log`，V1 见 `v1-*.log`）。

披露与边界：

- V0/V2 使用 `scripts/drillcoverage/run-in-container.sh` 的副本，与受测文件的**唯一差异**
  是最后一行 `exec make test-drill` 被替换为有界命令（`go build ./...` +
  完整 drill 测试二进制编译 + 单个必验测试真实执行）；逐字 diff 见
  `harness-diff-bounded-vs-tracked.txt`（修复后）与
  `harness-diff-prefix-vs-origin-main.txt`（修复前 = `origin/main` blob）。
- V3 的末行替换为失败命令 `exec sh -ec "go build ./internal/definitely-missing-package"`
  （验证非零退出传播）；逐字 diff 见 `harness-diff-fail-vs-tracked.txt`。
- 完整 `make test-drill`（19 必验 + 3 子测试 + Kafka 事件层）本轮本地**未运行**；
  远程待验条件见 README §6。
- 缺模块状态构造：Go 抽取目录为只读权限（0555），删除前需 `chmod -R u+w`；
  删除抽取目录**与** `cache/download` 条目后以 `absent:` 逐项校验完全缺失。

## 序列（2026-10-06T12:04:58Z–12:07:01Z，退出码为 wrapper 退出码）

| 步骤 | 操作 | 结果 |
| --- | --- | --- |
| prune #1 | 完全删除 `testcontainers-go/modules/postgres@v0.44.0`（抽取 + download）| `absent:` 校验通过 |
| V0b 负例 | **修复前**脚本（`origin/main` blob）+ 缺 postgres 模块 | **exit 1**；`internal/recovery/borrowed-transport-phase1_linux_test.go:31:2: mkdir /host-gomodcache/cache/download/github.com/testcontainers/testcontainers-go/modules/postgres: read-only file system` + `FAIL github.com/xtianxx/txharbor/internal/recovery [setup failed]`（= 生产日志 471/474 行同签名）|
| prune #2 | 完全删除 `github.com/decred`（抽取 + download）| `absent:` 校验通过 |
| V0a 负例 | **修复前**脚本 + 缺 decred 模块 | **exit 1**；`/host-gomodcache/github.com/ethereum/go-ethereum@v1.17.5/crypto/signature_nocgo.go:28:2` 与 `:29:2`: `mkdir /host-gomodcache/cache/download/github.com/decred: read-only file system`（= 生产日志 372/375 行同签名）|
| V2 正例 | **修复后**脚本 + 同一缺失状态 | **exit 0**；宿主侧 `go mod download` priming 恢复缺失模块；容器内 `go build ./...` → 完整 drill 测试二进制编译 → 必验测试真实执行：`--- PASS: TestDrillTargetWitnessOldReconnectRejected (5.57s)`；`ok github.com/xtianxx/txharbor/internal/recovery 5.622s` |
| V3 负例 | 修复后脚本 + 容器内失败命令 | **exit 1**（失败传播保持；`stat /workspace/internal/definitely-missing-package: directory not found`）|
| post-state | 两模块抽取目录 + download 条目 | 均已恢复；缓存 716M |

重复性：更早一次同构运行（V2-first，2026-10-06T12:02Z 左右）在同一缓存上得
`--- PASS: TestDrillTargetWitnessOldReconnectRejected (12.85s)`、exit 0。

tree-validation（四组 wrapper 证据，`negB/negA/pos/fail`）：

```json
{"source_tree_stable":true,"fingerprint_before":"sha256:1d89cc9328376ec4c9e15a510cb67ae7d3effd2440f0a660513ee6808fdbc5de","fingerprint_after":"sha256:1d89cc9328376ec4c9e15a510cb67ae7d3effd2440f0a660513ee6808fdbc5de"}
```

V1（更早，冷缓存 → 真实 `--smoke`，见 `v1-cold-cache-wrapper-smoke.log`）：
空缓存 `0B → 716MB`（约 20s，11:58:32Z–11:58:52Z），两目标模块抽取 + download
齐备，wrapper_exit=0，metadata/tree-validation 正常写出。
