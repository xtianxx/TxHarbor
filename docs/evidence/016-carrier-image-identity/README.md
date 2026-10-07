# 016 carrier 镜像身份校验：按 image store 语义分层（classic=config digest / containerd=index digest）

- 对象：drill fixture 的 carrier 镜像身份校验
  （`internal/recovery/targetwriter_drillbridge_linux_test.go` 的 `drillCreatePristineCarrier`
  与相关常量）
- 记录范围：证据归档与语义定性；**本目录只读用，不含实现改动**
- 固定引用（不变）：`postgres@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280`
  （linux/amd64）

## 1. 结论/首因

fixture 曾把 **index digest 同时当作固定 ref 的一部分与 image `.Id` 的期望值**：
修正前的常量对是

```go
drillCarrierImage   = "postgres@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280"
drillCarrierImageID = "sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280"
```

`drillCreatePristineCarrier` 把 `docker image inspect --format '{{.Id}}'` 的结果与
`drillCarrierImageID`（即 index digest）做**单一相等断言**。但 Docker 的 image store
实现不同，`.Id` 的语义层级也不同：

| store | `.Id` 语义 | 本固定引用的期望层级 |
| --- | --- | --- |
| classic（graphdriver，如 overlay2） | image config digest（Moby v28 `img.ID()`） | `sha256:a6638641…b7878` |
| containerd（c8d snapshotter） | image target descriptor digest（Moby v28 `target.Digest`，本引用为 **index**） | `sha256:4ef4dbc9…2280` |

因此：

- **托管 runner（classic store）实测 `.Id` = config digest `sha256:a6638641…`**
  （诊断 run 37493260522，§3 表、§4 分类方法），与 fixture 期望的 index digest 不等
  → run **37481398499** 在 `targetwriter_drillbridge_linux_test.go:757` 的 `.Id`
  比较处**首失败**，错误串 `carrier image content digest does not match the pinned identity`。
  该断言失败后，同一函数后续的 `RepoDigests` 与容器 `.Image` 断言未执行。
- 本机 WSL2（containerd store）`.Id` **恰好**等于 index digest → 同一 fixture 在本机通过，
  故障只在 classic store 上暴露。这不是「环境不稳定」，而是断言把一个 store 相关的
  实现细节当成了跨 store 恒定事实。

修正方向（§6）：保留固定 ref、linux/amd64 平台与内容绑定不变；把「单一相等」改为
**按已取证的 store 语义分层的身份集合**（classic→config digest / containerd→index digest），
并把 `RepoDigests` 的**子串匹配**改为**数组元素精确 membership**（固定 ref 本身仍作
registry manifest 引用的来源证明）。

## 2. digest 链与自哈希（含双源一致）

三层对象的原始字节、长度与自哈希（payload 均为 registry 原始字节，未重排 JSON、未改换行）：

| 层级 | 文件 | 字节 | sha256 |
| --- | --- | --- | --- |
| OCI image index（顶层，固定 ref 指向） | `payloads/index-manifest.json` | 10229 | `4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280` |
| linux/amd64 image manifest（index 的 `manifests[]` 子项） | `payloads/amd64-platform-manifest.json` | 3439 | `7341002d2b8c7c5bdd7542a671a95b36196c0b5b888daf454ae4fc33ba5346d7` |
| image config（amd64 manifest 的 `config.digest`） | `payloads/image-config.json` | 10048 | `a6638641707cdf047e5d5c2781f437e2e809323cab22c70b280be8389fbb7878` |

链条：`index 10229B = 4ef4dbc9…` → `linux/amd64 manifest 3439B = 7341002d…` →
`config 10048B = a6638641…`。

**响应头佐证**（`inspect/registry-index-response-headers.txt`，含 CRLF 原样保留）：

```
content-type: application/vnd.oci.image.index.v1+json
docker-content-digest: sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280
```

`Content-Type` 说明该 body 是 **image index**（而非平台 manifest 或 config）；
`docker-content-digest` 与 body 自哈希**同值**，即 `4ef4dbc9…` 是 index 的内容 digest，
registry 的 digest 声明与原始字节自洽。平台层与 config 层同理由 raw body 自哈希与
descriptor 相等确认（平台 manifest 的 `config.digest` = `a6638641…`）。

**双源一致**：源 A（托管 runner 诊断 run 37493260522 的制品）与本地镜像站
`.evidence/drill-runner-env/carrier-forensics/local/registry/` 的原始 body **同哈希**：

| payload | 源 A | 本地镜像站同哈希原始 body（仅记哈希，未复制字节） |
| --- | --- | --- |
| index (`4ef4dbc9…`) | `manifest.body` | `mirror-manifest-pinned-get.body`、`mirror-blob-pinned-get-follow.body`、`mirror2-manifest-pinned-get-final.body` |
| amd64 manifest (`7341002d…`) | `amd64.body` | `mirror-manifest-amd64-get.body`、`mirror-manifest-amd64-get-follow.body` |
| config (`a6638641…`) | `config.body` | `mirror-blob-config-get-follow.body` |

即：**同一 registry 对象在两台主机、两条独立获取路径（托管 runner 直连
`registry-1.docker.io`／本机经 mirror）下得到逐字节相同的产物**——固定引用绑定的内容
没有因网络路径或主机而漂移。本目录只复制源 A 字节（§10 清单）；本地镜像站字节仅做
哈希比对，不落入本目录。

## 3. 身份对照表

| 主机 / store | Docker 版本 | `.Id` | `RepoDigests` | 容器 `.Image` |
| --- | --- | --- | --- | --- |
| 本机 WSL2 / containerd（overlayfs + `io.containerd.snapshotter.v1`） | 29.6.1 | `sha256:4ef4dbc9…2280`（**index** digest） | `["postgres@sha256:4ef4dbc9…2280"]` | `sha256:4ef4dbc9…2280` |
| 托管 runner（诊断 run 37493260522）/ classic（overlay2） | 28.0.4 | `sha256:a6638641…b7878`（**config** digest） | `["postgres@sha256:4ef4dbc9…2280"]` | `sha256:a6638641…b7878` |
| 原失败 run 37481398499 / classic（日志 `Storage Driver: overlay2`） | 28.0.4（Server Version） | **未观测** | 未观测（该 run 未执行到该断言） | 未观测（同上） |

要点：

- `RepoDigests` 在两台主机上**都是固定 `repo@index-digest` ref**——它是 registry manifest
  引用的来源证明，与 `.Id` 不是同一层级；classic store 完全可以在 `.Id`=config digest 的
  同时把 `RepoDigests` 记为 index digest 引用。
- 容器 `.Image` 继承创建容器的 image store 身份语义：containerd → index digest；
  classic → config digest。
- 关于第 3 行：旧 run 的代码**没有把 `imageID` 写进错误串**，日志与摘要也没有该 stdout，
  因此该值**保持未知**；表中 classic 身份的 `a6638641…` 来自**同类 runner 的诊断 run
  实测**（同 linux/amd64 引用、同 store 分类），**不是历史观测**，不得回填为旧 run 的
  `.Id`。
- 原始产物：`inspect/local-image-id.txt`、`inspect/runner-image-id.txt`、
  `inspect/local-repo-digests.json`、`inspect/runner-repo-digests.json`、
  `inspect/local-container-image.txt`、`inspect/runner-container-image.txt`。
- 旁证（同批原始字段，非身份判据）：本机 inspect 含 `Descriptor` =
  `{mediaType: application/vnd.oci.image.index.v1+json, digest: sha256:4ef4dbc9…, size: 10229}`，
  即 containerd store 返回的 target 正是该 index；runner 的 `inspect/runner-image-inspect.json`
  **没有** `Descriptor` 字段（classic store 不提供该键）。

## 4. store 分类方法与证据

分类只依据 store 自报字段，不依赖版本号或猜测：

```sh
docker info --format '{{.Driver}}'
docker info --format '{{json .DriverStatus}}'
```

| 主机 | `.Driver` | `.DriverStatus` | 判定 |
| --- | --- | --- | --- |
| 托管 runner（诊断 run） | `overlay2` | `[["Backing Filesystem","extfs"],["Supports d_type","true"],["Using metacopy","false"],["Native Overlay Diff","false"],["userxattr","false"]]` | **classic**：DriverStatus 是 overlay graphdriver 的键值对，**无** containerd 标记 |
| 本机 WSL2 | `overlayfs` | `[["driver-type","io.containerd.snapshotter.v1"]]` | **containerd**：`driver-type=io.containerd.snapshotter.v1` 即 containerd snapshotter |

证据文件：`inspect/runner-driver-status.json`、`inspect/local-driver-status.json`；
driver/版本/平台的合并视图见 `inspect/runner-docker-info.txt`
（`server_version=28.0.4 driver=overlay2 os=Ubuntu 24.04.5 LTS arch=x86_64`）。
另：原失败 run 37481398499 的 drill 日志亦打印 `Storage Driver: overlay2`（同属 classic），
但其 `.Id` 仍未观测（§3）。

**linux/amd64 平台绑定**：两主机均为 `x86_64` / inspect `.Os=.Architecture=` linux/amd64
（`inspect/runner-image-os-arch.txt` 为 `linux/amd64`；本机为 `inspect/local-image-os.txt`=`linux`
＋ `inspect/local-image-arch.txt`=`amd64`），因此两台主机解析到的是**同一个**平台子 manifest
（`7341002d…`）与同一个 config（`a6638641…`），身份差异只能来自 store 语义而非平台选择。

## 5. 运行指纹

| 项 | run 37481398499（失败基线） | run 37493260522（诊断） |
| --- | --- | --- |
| attempt | 1 | 1 |
| event | `workflow_dispatch` | `workflow_dispatch` |
| headBranch | `fix/drill-runner-modcache-prime` | `diag/carrier-identity-probe` |
| headSha | `08c96e849e25a9eb2e494ac6028ebbfc9bd5e959` | `e2d84cf7009c2b20073e2628fc92f1b2f2503c7a` |
| conclusion | `failure` | `success` |
| 时间 | 14:43:44Z → 14:54:23Z（2026-10-06） | 16:07:59Z → 16:08:29Z（2026-10-06） |
| URL | https://github.com/xtianxx/TxHarbor/actions/runs/37481398499 | https://github.com/xtianxx/TxHarbor/actions/runs/37493260522 |
| job | `full recovery drill (S1-S12 + F1-F7, Docker)` completed/**failure** | `carrier identity diagnostic (diagnostic only; never runs the drill)` completed/**success**；`full recovery drill (S1-S12 + F1-F7, Docker)` completed/**skipped** |

失败细节（37481398499）：job 摘要 `- FAIL: TestDrillArmRefusesReplacedELFAndPreStartTamper`；
该测试经 helper `provision-pg18-native-tools_linux_test.go:111` 上报
`provision sealed pinned PostgreSQL 18.6 tools: pristine carrier unavailable: carrier image content digest does not match the pinned identity`；
错误由 `targetwriter_drillbridge_linux_test.go:757` 的 `.Id` 断言产生（首失败点），
故后续的 `RepoDigests` / 容器 `.Image` 断言未执行。

诊断 run 的 checkout 自证（`artifact-37493260522/checkout-sha.txt`）：
`checkout=e2d84cf7009c2b20073e2628fc92f1b2f2503c7a`。

runner 环境（诊断 run，源 A 原始文件）：Ubuntu 24.04.5 LTS；
`Linux runnervm8df0l 6.17.0-1022-azure #22-Ubuntu SMP Mon Jul 27 17:24:03 UTC 2026 x86_64`；
Docker `client=28.0.4 server=28.0.4`。

## 6. fixture 修正后常量（完整）

```go
drillCarrierImage                  = "postgres@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280"
drillCarrierIndexDigest            = "sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280"
drillCarrierPlatformManifestDigest = "sha256:7341002d2b8c7c5bdd7542a671a95b36196c0b5b888daf454ae4fc33ba5346d7"
drillCarrierConfigDigest           = "sha256:a6638641707cdf047e5d5c2781f437e2e809323cab22c70b280be8389fbb7878"
```

- `drillCarrierImage`：不变的固定 ref（`repo@index-digest`），既是 pull/create 的引用，
  也是 `RepoDigests` 的来源证明（改为数组元素**精确 membership**，不再用子串包含）。
- **允许的 `.Id` / 容器 `.Image` 身份集合**：
  - classic store → `drillCarrierConfigDigest`（config digest）
  - containerd store → `drillCarrierIndexDigest`（index digest）
  
  实测两态各覆盖一支：classic → `a6638641…`（`inspect/runner-image-id.txt`）；
  containerd → `4ef4dbc9…`（`inspect/local-image-id.txt`）。store 由 §4 的
  `docker info` 字段判定，判定结果决定期望值，而不是反过来拿期望值筛主机。
- `drillCarrierPlatformManifestDigest`：**仅用于链核验**（确认 index 的 linux/amd64
  子项与平台绑定、config descriptor 指向 `drillCarrierConfigDigest`），
  **不作为身份允许值**——因为没有任何 store 实测以平台 manifest digest 表示 `.Id`/
  容器 `.Image`，把它放进允许集合会成为无证据的放宽。
- 平台保持 linux/amd64 固定；容器内原生工具（pg_restore/pg_dump/libpq）的**文件字节**
  校验（`drillCarrierRestoreSHA`/`DumpSHA`/`LibpqSHA`）属于另一层级，与本条无关、保持不变。
- 修正前基线（保留记录）：单一常量 `drillCarrierImageID = "sha256:4ef4dbc9…2280"`，
  在 `:757`（`.Id`）、`:779`（容器 `.Image`）处作相等断言，`:764` 处作 `RepoDigests` 子串匹配。

## 7. 边界/非声明

1. **诊断 job 成功 ≠ 19+3 完整演练通过**。run 37493260522 的 drill job 是
   `completed/skipped`：该 run 只跑探针，**没有执行**任何演练场景；其 `success` 只证明
   取证命令正常产出。
2. **本批修复尚未经托管 drill 验证**——远程复验待做；本目录不宣称 drill 已在远程通过，
   也不宣称失败基线已闭合。
3. **旧 run 37481398499 的实际 `.Id` 保持未知**：代码未输出该值，run 摘要/日志亦无该
   stdout；任何数值化描述都只能是条件性推导，不得写成历史观测。
4. **`a6638641…` 是同类 runner 实测（诊断 run 37493260522）而非历史观测**：它支持
   「classic store → config digest」的语义判定，但不能反推旧 run 当时打印了什么。
5. 本目录不含实现改动、不含测试运行结果；payload 为 registry 原始字节，未做任何
   JSON 重排/格式化——**对 payload 运行 jq/pretty-print 会破坏其自哈希意义**。
6. 本目录所有复制（`cp`，逐文件字节）均经源/目标 sha256 复核相等（§10、`SHA256SUMS`）。

## 8. Moby v28 源码依据

- classic store：https://github.com/moby/moby/blob/v28.0.4/daemon/images/image_inspect.go
  —— `ID` 取 `img.ID()`，即 **image config hash**（config digest）。
- containerd store：https://github.com/moby/moby/blob/v28.0.4/daemon/containerd/image_inspect.go
  —— `ID` 取 `target.Digest`（image target descriptor digest，本引用为 **index**）。

结论：`.Id` 的层级由 store 实现决定，**Docker 版本号本身不足以定性**；跨主机比较前必须
先采集 `docker info` 的 `Driver`/`DriverStatus`（§4）。

## 9. 复现命令

```sh
REF='postgres@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280'

# store 分类（两条）
docker info --format '{{.Driver}}'
docker info --format '{{json .DriverStatus}}'
# classic: DriverStatus 为 ["Backing Filesystem",...] 形态、无 containerd 标记
# containerd: DriverStatus 为 [["driver-type","io.containerd.snapshotter.v1"]]

# 身份字段（三条）
docker image inspect --format '{{.Id}}' "$REF"            # classic=config digest；containerd=index digest
docker image inspect --format '{{json .RepoDigests}}' "$REF"  # 恒为 ["postgres@sha256:4ef4dbc9…2280"]
docker image inspect --format '{{.Os}}/{{.Architecture}}' "$REF"  # 期望 linux/amd64

# 容器身份（可选，三条：create/inspect/rm）
cid=$(docker create "$REF" /bin/true)
docker inspect --format '{{.Image}}' "$cid"   # 继承同一 store 的 image 身份语义
docker rm -f "$cid" >/dev/null

# payload 原始字节复算（进入本目录后）
cd docs/evidence/016-carrier-image-identity/payloads
sha256sum index-manifest.json amd64-platform-manifest.json image-config.json
# 期望：4ef4dbc9…2280 / 7341002d…346d7 / a6638641…b7878
wc -c index-manifest.json amd64-platform-manifest.json image-config.json   # 10229 / 3439 / 10048
```

（标注说明：`4ef4dbc9…2280`、`7341002d…346d7`、`a6638641…b7878` 为 §2 表中全值的前缀缩写。）

## 10. 附件清单（18 个复制文件，逐文件 sha256 复核与源相等）

| 目标 | 源 | sha256(源)==sha256(目标) |
| --- | --- | --- |
| `payloads/index-manifest.json` | 源 A `manifest.body` | OK |
| `payloads/amd64-platform-manifest.json` | 源 A `amd64.body` | OK |
| `payloads/image-config.json` | 源 A `config.body` | OK |
| `inspect/registry-index-response-headers.txt` | 源 A `manifest.key-headers.txt` | OK |
| `inspect/runner-image-id.txt` | 源 A `image-id.txt` | OK |
| `inspect/runner-repo-digests.json` | 源 A `repo-digests.json` | OK |
| `inspect/runner-container-image.txt` | 源 A `container-image.txt` | OK |
| `inspect/runner-image-os-arch.txt` | 源 A `os-arch.txt` | OK |
| `inspect/runner-driver-status.json` | 源 A `driver-status.json` | OK |
| `inspect/runner-docker-info.txt` | 源 A `docker-info.txt` | OK |
| `inspect/runner-pull.log` | 源 A `pull.log` | OK |
| `inspect/runner-image-inspect.json` | 源 A `image-inspect.json` | OK |
| `inspect/local-image-id.txt` | 源 B `image/inspect-id.out` | OK |
| `inspect/local-repo-digests.json` | 源 B `image/inspect-repodigests.out` | OK |
| `inspect/local-container-image.txt` | 源 B `image/container-inspect-image.out` | OK |
| `inspect/local-image-os.txt` | 源 B `image/inspect-os.out` | OK |
| `inspect/local-image-arch.txt` | 源 B `image/inspect-arch.out` | OK |
| `inspect/local-driver-status.json` | 源 B `env/driver-status.out` | OK |

- 源 A = `.evidence/drill-runner-env/remote/diag/artifact-37493260522/`
- 源 B = `.evidence/drill-runner-env/carrier-forensics/local/`
- 敏感信息检查：复制前对 `pull.log` 与 `manifest.key-headers.txt` 执行
  `grep -i 'token\|auth\|password\|cookie'`，**均无命中**（exit 1），故两文件按原字节正常复制，
  无文件因敏感性被排除。
- 逐文件 sha256 与字节数（`SHA256SUMS` 覆盖除自身外全部文件）：

| 目标 | sha256 | 字节 |
| --- | --- | --- |
| `inspect/local-container-image.txt` | `f2d9b941bd8edebfc7f82a09a8959ebbb3f5013e44813ffcbea7b52cc687e290` | 72 |
| `inspect/local-driver-status.json` | `375efec34cde40c98b06ac3e9706b79cc21a8849fadda54740af1f3834fad8f4` | 49 |
| `inspect/local-image-arch.txt` | `d54d20eadbec9c9cc5ac6e0e371abc96d64a0d724d1cf45297fab6098026a69d` | 6 |
| `inspect/local-image-id.txt` | `f2d9b941bd8edebfc7f82a09a8959ebbb3f5013e44813ffcbea7b52cc687e290` | 72 |
| `inspect/local-image-os.txt` | `d745fba1cb70ab9dc02a80eeba8a1864a0f32b2941e008c0af389be7b56ba830` | 6 |
| `inspect/local-repo-digests.json` | `8d9867018df6e817b170911a9f16a5ac4bad444f5f5824d48f22b9fd27dcedb3` | 85 |
| `inspect/registry-index-response-headers.txt` | `ba41600b37b3e64ec6aa1c59c668cf7d60c825a94d99e26d8a1af2ccae63b672` | 151 |
| `inspect/runner-container-image.txt` | `19d3dc10767395a0108b57034e8f86642bf1d36e9b008a10a51a28b4c1008774` | 72 |
| `inspect/runner-docker-info.txt` | `a4ddd1b5c6e867a2f45c628e5d20594f13b0dd2d99ecc23e6a0fd4c45d15e2fd` | 72 |
| `inspect/runner-driver-status.json` | `b052d13d0b7452dc3741fcecd16af90a88266498be742cfa9b68720ec580b8ce` | 141 |
| `inspect/runner-image-id.txt` | `19d3dc10767395a0108b57034e8f86642bf1d36e9b008a10a51a28b4c1008774` | 72 |
| `inspect/runner-image-inspect.json` | `f72ad2dd0131f2067c48e171e5779eead2953cd9b836e3a6b4314ccc5be78e34` | 4640 |
| `inspect/runner-image-os-arch.txt` | `30a2860611f42f808b7012f033e39aaedf708c7ea934b69e9dd0b3fdcd756996` | 12 |
| `inspect/runner-pull.log` | `41bf10525e3af4342767f7c3135e5ee91f0d65b15d8ad16bcd4ce654f08e6fbc` | 2257 |
| `inspect/runner-repo-digests.json` | `8d9867018df6e817b170911a9f16a5ac4bad444f5f5824d48f22b9fd27dcedb3` | 85 |
| `payloads/amd64-platform-manifest.json` | `7341002d2b8c7c5bdd7542a671a95b36196c0b5b888daf454ae4fc33ba5346d7` | 3439 |
| `payloads/image-config.json` | `a6638641707cdf047e5d5c2781f437e2e809323cab22c70b280be8389fbb7878` | 10048 |
| `payloads/index-manifest.json` | `4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280` | 10229 |

产物来源命令（诊断 run 原始探针，逐条落盘；见 `inspect/runner-*`）：
`docker version --format 'client=… server=…'`、`docker info --format 'server_version=… driver=… os=… arch=…'`、
`docker info --format '{{json .DriverStatus}}'`、`docker pull "$REF"`、
`docker image inspect --format '{{.Id}}'|'{{json .RepoDigests}}'|'{{.Os}}/{{.Architecture}}'`、
`docker image inspect "$REF"`、`docker create` + `docker inspect --format '{{.Image}}'` + `docker rm -f`、
`uname -a`、`git rev-parse HEAD`，以及 registry 侧
`curl -D manifest.headers -o manifest.body`（index）＋ `sha256sum` 与
`grep -i 'content-type\|docker-content-digest' manifest.headers`（即
`inspect/registry-index-response-headers.txt` 的来源；该文件保留 CRLF 行尾原字节）。
