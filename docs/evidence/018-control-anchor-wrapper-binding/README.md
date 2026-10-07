# 018 control-anchor wrapper 绑定修复：本地双拓扑验证证据

- 仓库：`/home/dream/product_env/TxHarbor`
- 运行 HEAD：`fee2d904eb6e013ee046c4375f395df143957a9c`（分支 `fix/drill-runner-modcache-prime`），**工作区含本批未提交修改**：仅
  `internal/recovery/control-anchor-mini_linux_test.go`（`git status --porcelain` = ` M internal/recovery/control-anchor-mini_linux_test.go`）。
- 本批只改这一个测试文件（`+111 / -3`）。源码、Makefile、必验集合、预算均未改动；未 `git add/commit/push`。
- 工具链：Go `go1.26.5 linux/amd64`、Docker `29.6.1`（客户端/服务端同版本，API 1.55）、`testcontainers-go v0.44.0`、固定镜像
  `postgres@sha256:4ef4dbc939d61acea57712655ddb4b4ab27419c913f94cca0cd57cb3ea3c2280`、宿主 uid 1000。
- 宿主时区 CST（UTC+8）：**宿主侧日志打印本地时间**，**wrapper/容器侧日志打印 UTC**；README 所有时间窗一律换算标成 UTC。

结论摘要（细节见各节）：

| 运行 | 拓扑 | 修复 | 退出码 | 结果 |
|---|---|---|---|---|
| `logs/host-prefix/` | 宿主 | 修复前 | `0` | PASS（`host-prefix/run.log:29-31`） |
| `logs/wrapper-prefix/` | wrapper 容器 | 修复前 | `1` | FAIL：`control-anchor-mini_linux_test.go:163: anchor local tuple is not a loopback TCP endpoint`（`wrapper-prefix/run.log:25,30-33`） |
| `logs/postfix-host/` | 宿主 | 修复后 | `0` | PASS（`postfix-host/run.log:33,35`） |
| `logs/postfix-wrapper/` | wrapper 容器 | 修复后 | `0` | PASS（`postfix-wrapper/run.log:33,35`） |
| `logs/race/` | 宿主 + `-race` | 修复后 | `0` | PASS，无 race 报告（`race/run.log:33,35`；全文无 `DATA RACE`/`WARNING: DATA RACE`） |

---

## 1. 契约判定：`:163` 的 loopback 断言是**测试附加的宿主拓扑假设**，不是生产约束

修复前断言（`.evidence/drill-runner-env/topology-repro/MANIFEST.txt` §TEST UNDER REPRODUCTION 记 `control-anchor-mini_linux_test.go:162-163`）：

```go
if localIP == nil || remoteIP == nil || !localIP.IsLoopback() {
    t.Fatal("anchor local tuple is not a loopback TCP endpoint")
```

### 1.1 生产拨号路径对控制连接**零** loopback 约束

- `internal/recovery/targetlock.go:511-534`（`connectTargetLockSession`）：控制会话就是 `pgx.Connect(ctx, controlDSN)`（`:514`），仅当 drill-only socket wrap seam 被占用时才走 `config.DialFunc`（`:521-533`）——两条路径都直接把 DSN 交给 pgx/`net.Dialer`，**没有任何地址类别（loopback/接口）判断**。
- `internal/recovery/controlstore/store.go:1400-1414`（`ParseDSNTarget`）：只解析 `Host/Port/Database/Role`；`store.go:1419-1421`（`SameDatabase`）只做 host（大小写不敏感）+ port + database 相等，同样不看地址类别。
- 锚点读路径自身的形状约束也只有“非 nil / 端口>0 / 地址非 Unspecified”：`internal/recovery/targetwriter_controlanchor_drillbridge_linux_test.go:453-459`（`netConn.LocalAddr()/RemoteAddr()` → `local.IP.IsUnspecified()`、`remote.IP.IsUnspecified()` 才 `drillControlAnchorRefuse`）。
- `targetlock.go:90`（`controlstore.ParseDSNTarget(controlDSN)`）与 target observer 观察连接 `targetlock.go:398`（`pgx.Connect(ctx, targetDSN)`）同样是纯 DSN 信任，无地址类别约束。

### 1.2 全仓 `IsLoopback` 只出现在测试文件

`git grep -n IsLoopback -- '*.go'`（HEAD `fee2d90`，tracked 文件）共 4 处，全部是 `_test.go`：

```
internal/recovery/borrowed-replacement-bound-postcommit-native-start_linux_test.go:686
internal/recovery/origin_gate_proxy_linux_test.go:1176
internal/recovery/origin_gate_proxy_linux_test.go:1201
internal/recovery/targetwriter_drillbridge_linux_test.go:458   (DrillOpenOriginEndpoint，测试自建的 127.0.0.1 listener)
```

（唯一一处非 `_test.go` 命中是 git-excluded 的探针草稿 `.evidence/drill-runner-env/topology-repro/wrapper/scratch/probe-go/main.go:20`，属于一次性探针，不是仓库源码。）
本批修复后 `control-anchor-mini_linux_test.go` 内不再出现 `IsLoopback`。

### 1.3 wrapper 机制链（为什么同一测试在容器里必然违反该假设）

1. `scripts/drillcoverage/run-in-container.sh:87` 注入 `-e "TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal"`（`:88` 另有 `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock`）。
2. `testcontainers-go@v0.44.0/docker.go:1614-1621`（`daemonHostLocked`）：`host, exists := os.LookupEnv("TESTCONTAINERS_HOST_OVERRIDE")` → **原样返回该值**作为“容器可达的宿主 host”。
3. 于是 drill 的 control DSN host = `host.docker.internal`；`--add-host=host.docker.internal:host-gateway` 把它解析成 docker0 网关 `172.17.0.1`（实测：`wrapper-prefix/probe.log:118` `/etc/hosts`、`:121` `getent hosts`、`:241` S4.5；宿主侧 `docker0 = 172.17.0.1/16` 见 `probe.log:18,242`）。
4. 控制连接于是从**容器自己的 eth0 地址**发出（实测 `172.17.0.2`/`172.17.0.3`），客户端本地 tuple 永远不是 loopback → `:163` 必红。

### 1.4 去掉 override 的对照同样失败（override 不是唯一成因）

同一 wrapper 装配、只去掉 `TESTCONTAINERS_HOST_OVERRIDE`（保留 socket override）的对照运行，仍在同一行失败：见 `MANIFEST.txt` §“Control variant without TESTCONTAINERS_HOST_OVERRIDE”（源：`.evidence/drill-runner-env/topology-repro/wrapper/no-override/run.out`，`run.rc = 1`，窗口 `2026-10-07T03:56:49Z → 03:56:57Z`）。原因在
`docker.go:1636-1647`：`core.InAContainer()`（`internal/core/docker_host.go:320` 的 `DockerEnvFile = "/.dockerenv"`，判定见 `:324-333`）为真时，testcontainers 改为在容器内**推断默认网络网关 IP**：
`:1641` `p.getGatewayIP(ctx, defaultNetwork)`，失败则 `:1643` `core.DefaultGatewayIP()`；后者（`internal/core/docker_host.go:46-48`）是 `sh -c "ip route|awk '/default/ { print $3 }'"`，而该镜像里**没有 `ip`**（`probe.log:123` “ip command unavailable”），最终落到 `:1646-1647` 的 `ip = "localhost"`（容器内 localhost 仍是容器自身，控制连接仍从容器接口发出）。因此 loopback 假设的失效来自**容器化拓扑本身**，与 override 无关。

---

## 2. 修正与等价依据（只改 `internal/recovery/control-anchor-mini_linux_test.go`）

修正后绑定链（行号=修复后工作区文件 `sha256=e6477c9390bf38aba237a3cb23bf38766266ac0ec697411e0fea489ec908ab4e`）：

| 行 | 内容 |
|---|---|
| `:175-176` | 取代原 loopback 断言：只要求 `local/remote` 都是可解析的 TCP 端点对（`anchor tuple is not a TCP endpoint pair`） |
| `:177-182` | tuple 端口必须与 Diagnostics 投影一致，且 `remote port == controlTarget.Port`（控制端点） |
| `:193-201` | 从**同一活动连接对象**重导出真实 tuple：`lock.conn.(*pgx.Conn).PgConn().Conn()`（`:193,197`）→ `LocalAddr()/RemoteAddr().(*net.TCPAddr)`（`:198-199`） |
| `:203-206` | `drillControlAnchorSameTCP(liveLocal, captured) && drillControlAnchorSameTCP(liveRemote, captured)`：捕获的四元组必须等于活连接的 tuple |
| `:207-210` | `liveInode, ok := drillControlAnchorSocketInode(liveConn)`，要求 `ok && liveInode != 0 && liveInode == facts.SocketInode`：捕获 inode 必须是**活 socket fd** 的 inode |
| `:211-213` | `drillControlAnchorProcessOwnsSocketInode(os.Getpid(), facts.SocketInode)`：本进程 `/proc/<pid>/fd` 里确实持有 `socket:[<inode>]` |
| `:214` | 通过时打印 `anchor live binding accepted: local=… remote=… inode=…` |

辅助函数（同一文件，新增）：`drillControlAnchorSameTCP`（`internal/recovery/targetwriter_controlanchor_drillbridge_linux_test.go:580`，co-IP+port+zone 比较）、
`drillControlAnchorSocketInode`（同文件 `:597`，`syscall.Conn` → `Fstat` → `st.Ino`）、
`drillControlAnchorProcessOwnsSocketInode`（`control-anchor-mini_linux_test.go:264-293`，只扫**指定 pid 的** `/proc/<pid>/fd` 并匹配 `socket:[<inode>]`）。

### 2.1 为什么不复用已有 `hostOwnedSocketInode`

`hostOwnedSocketInode`（`internal/recovery/origin_gate_proxy_linux_test.go:2464`）不是“查一个 pid 的 fd 表”，而是**全进程属主普查**：先枚举全部 `/proc/<pid>`，逐个读 `/proc/<pid>/fd`（`hostSocketInodeOwners`，同文件 `:2387-2454`），任何一处不可读就 fail-closed 返回
`owner census incomplete: PID %d fd table unreadable: %w`（`:2440`，另见 `:2414/2426/2451/2454`）。

- 宿主实测（本批附加记录 `logs/proc-fd-permission-probe.txt`，`2026-10-07T04:09:54Z`，uid 1000）：
  `ls /proc/1/fd` → `rc=2  ls: cannot open directory '/proc/1/fd': Permission denied`；`/proc/2/fd` 同样 `Permission denied`（pid 1/2 属主 root）。
  即：在开发宿主机上，这个普查**必然**以 `census incomplete` 失败，而不是给出“持有/未持有”的裁决 → 该 helper 无法服务于需要在**宿主与 wrapper 双拓扑**都运行的断言。
- 同一探针在容器内（同一固定镜像、`--user 1000:1000`）显示 `/proc/1/fd` 可读（容器 pid 1 = docker-init，属容器 uid），**因此本条的失效条件是宿主侧特有的**；也就是说该 helper 是宿主侧装配（host `/proc/net/tcp` 行 + host pid fd 表 join），跨拓扑不成立。
- 该 helper 的契约本身也不同：它校验 **origin-gate 监听端 accepted socket** 与 peer/gate 端点的配对（`:2464` 定义；`:2502-2508` 调用属主普查并断言“唯一属主就是该 pid”），而本测试需要绑定的是**客户端侧**控制连接的活 tuple/inode。
- 因此本批新增了一个最小、只读**指定 pid** 的属性查询（`:264-293`）；它不会因无关进程不可读而失效，也不需要跨进程普查。

### 2.2 不弱于原断言（等价/更强的绑定）

原断言绑定的是“接口地址类别”（127.0.0.0/8），这是一种**夹具拓扑属性**。修正后绑定改为内核 socket 同一性，三重独立证据：

1. **活对象 tuple**：捕获的 local/remote 必须逐字段等于 `pgx.Conn` 上真实 conn 的 `LocalAddr()/RemoteAddr()`（`:193-206`）；
2. **活 fd inode**：捕获的 `SocketInode` 必须等于对同一活连接 `Fstat` 得到的 `st.Ino`（`:207-210`，`!=0` 强校验）；
3. **持有性**：本进程 fd 表中确实存在 `socket:[<inode>]`（`:211-213`）。

丢弃的只是“地址属于哪一类接口”，保留并加强的是“这个四元组+inode 就是那条唯一的活动控制连接”。宿主 loopback 路由与容器 bridge/host-gateway 路由在这一判据下是**同一件事**（内核 socket 事实），因此断言在两个拓扑下都成立且不放水。
负例（§3）进一步证明伪造/替换不能通过：他进程不持有该 inode、错误 inode 不被接受、到同一端点的**独立**连接 tuple 不同。

---

## 3. 正负例结果（全部为实测日志值）

宿主（`logs/postfix-host/run.log`）：

| 用例 | 结果 | 日志行 |
|---|---|---|
| 正向：活绑定接受 | `local=127.0.0.1:44726 remote=127.0.0.1:44687 inode=54487716` | `:25` |
| 负例：外来属主 | `foreign owner probe refused: process 383445 holds no descriptor for socket inode 54487716` | `:26` |
| 负例：错误 inode | `wrong socket inode probe refused: process 382631 holds no descriptor for socket inode 54487717` | `:27` |
| 负例：独立连接 tuple 不同 | `independent connection tuple differs: independent local=127.0.0.1:44734 vs captured local=127.0.0.1:44726` | `:28` |

wrapper 容器（`logs/postfix-wrapper/run.log`）：

| 用例 | 结果 | 日志行 |
|---|---|---|
| 正向：活绑定接受 | `local=172.17.0.3:42476 remote=172.17.0.1:44689 inode=54505159` | `:25` |
| 负例：外来属主 | `foreign owner probe refused: process 324 holds no descriptor for socket inode 54505159` | `:26` |
| 负例：错误 inode | `wrong socket inode probe refused: process 193 holds no descriptor for socket inode 54505160` | `:27` |
| 负例：独立连接 tuple 不同 | `independent connection tuple differs: independent local=172.17.0.3:42500 vs captured local=172.17.0.3:42476` | `:28` |

宿主 + `-race`（`logs/race/run.log`，本批任务 1）：

| 用例 | 结果 | 日志行 |
|---|---|---|
| 正向：活绑定接受 | `local=127.0.0.1:45960 remote=127.0.0.1:44691 inode=54532173` | `:25` |
| 负例：外来属主 | `foreign owner probe refused: process 388324 holds no descriptor for socket inode 54532173` | `:26` |
| 负例：错误 inode | `wrong socket inode probe refused: process 387523 holds no descriptor for socket inode 54532174` | `:27` |
| 负例：独立连接 tuple 不同 | `independent connection tuple differs: independent local=127.0.0.1:45980 vs captured local=127.0.0.1:45960` | `:28` |

注：wrapper 侧 local 是 `172.17.0.3`（该次容器拿到 .3），remote 是 `172.17.0.1:44689`（宿主网关 + 本次发布的控制端口）；宿主侧 tuple 全为 `127.0.0.1`。两侧 inode 均由活连接 `Fstat` 得到并与捕获值相等。

---

## 4. 拓扑事实（`logs/wrapper-prefix/probe.log`，wrapper 装配）

- **独立 netns**：容器内 `/proc/self/ns/net == /proc/1/ns/net == net:[4026532379]`，宿主 `/proc/self/ns/net == net:[4026531833]`（`probe.log:174-177`、`S4.4 :234-239`）；容器内 `/proc/self` 是 pid 17（`:176-177`），与宿主 docker top 里的 pid 不同 → 独立 PID 名空间。
- **地址**：容器 `/etc/hosts` 含 `172.17.0.1 host.docker.internal`、`172.17.0.2 <container-hostname>`（`:118-119`、`S4.5 :241`）；镜像无 `ip`（`:123`），由 `/proc/net/fib_trie` 可见 `172.17.0.2/32 host LOCAL` on eth0、`172.17.0.0/16 link`（`S4.5 :243-244`）；宿主 `docker0 172.17.0.1/16`（`:18,242`），`docker inspect` 容器 IP `172.17.0.2`（`:96-97`）。
- **探针实测拨号**：`/proc/net/tcp` 行 `020011AC:A9AA 010011AC:A112 st=01`（`:180-182`）→ local `172.17.0.2:43434`（容器接口，非 loopback）、remote `172.17.0.1:41234`（宿主网关），宿主镜像行 `010011AC:A112 020011AC:A9AA st=01` 与 `ss ESTAB 172.17.0.1:41234 <- 172.17.0.2:43434`（`:210-211`）；容器内 Go `net.Dial`（与锚点捕获相同的调用）`local=172.17.0.2:52952 local_ip_is_loopback=false remote=172.17.0.1:41234`（`:186-188`、`SECTION 3 :193-196`）。
- **真实控制连接**（wrapper 内，`S4.2 :215-224`，原始 `wrapper/tuple-watch/…`）`020011AC:9144 010011AC:AE81 01` / `020011AC:914A 010011AC:AE81 01` → local `172.17.0.2:37188/37194` remote `172.17.0.1:44673`；宿主同端口镜像 `172.17.0.1:44673 <- 172.17.0.2:37188/:37194`。
- **真实控制连接**（宿主，`S4.3 :225-233`，原始 `host/tuple-watch.out`）`0100007F:B758/B75A 0100007F:AE83 01` → local `127.0.0.1:46936/:46938` remote `127.0.0.1:44675`，fd 属主 `users:(("recovery.test",pid=344264,fd=8/fd=9))` → **宿主侧确实 loopback**（这正是 `:162` 想断言的东西，因此原断言只在宿主拓扑下偶然成立）。
- 段 4（`:198-244`）是唯一派生内容（hex→点分 IP 解码），已显式标注；段 1-3 为原始日志逐字拼接（`probe.log:1-9`）。

---

## 5. 命令、退出码、时间窗与环境

所有 rc 独立落盘（`run.rc`），`0`/`1` 均如实保留；rc 文件由 `echo $? > run.rc` 生成（2 字节，含换行），本批 race 的 `run.rc` 由 `printf '%s'` 生成（1 字节）。

| # | 运行 | 命令 | rc | 时间窗 (UTC) | 环境要点 | 证据 |
|---|---|---|---|---|---|---|
| 1 | 修复前宿主 | `go test -tags drill -count=1 -v -run '^TestControlAnchorCaptureMatchesActualControlOwner$' ./internal/recovery/` | `0` | `03:51:37Z → 03:51:51Z`（14s） | 宿主 Go/docker，无 `TESTCONTAINERS_*` | `logs/host-prefix/{run.log,run.rc,env.txt}`；窗口与 rc 见 `host-prefix/env.txt` 末尾字段 |
| 2 | 修复前 wrapper | `docker run --rm --init --user 1000:1000 --group-add 989 --add-host=host.docker.internal:host-gateway … --entrypoint /bin/sh postgres@sha256:4ef4dbc9… -ec 'unset PGDATA PG_MAJOR PG_VERSION; cd /workspace; go test -tags drill -count=1 -v -run "^TestControlAnchorCaptureMatchesActualControlOwner$" ./internal/recovery/'`（完整命令行逐字见 `logs/wrapper-prefix/cmd.txt`；装配与 `scripts/drillcoverage/run-in-container.sh` 同构，含 `run-in-container.sh:87` 的 `TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal`、`:88` socket override、`CGO_ENABLED=0`、`GOCACHE=/drill-scratch/gocache`、`TMPDIR`、`HOME`、`CI=true`、`TXHARBOR_REQUIRE_DOCKER=1`） | `1` | `03:52:58Z → 03:53:45Z`（47s） | 固定 postgres 镜像、宿主 docker.sock、workspace 只读挂载 | `logs/wrapper-prefix/{run.log,run.rc,cmd.txt,probe.log}`；窗口/rc 亦见 `MANIFEST.txt` |
| 3 | 修复后宿主 | 同 #1（源码/工作区：修复后测试文件 `e6477c93…`） | `0` | 日志时间戳 `12:06:31 → 12:06:41` CST = `04:06:31Z → 04:06:41Z`；单测 9.77s | 宿主 Go/docker | `logs/postfix-host/{run.log,run.rc}` |
| 4 | 修复后 wrapper | 同 #2（`run-postfix.sh` §wrapper：`docker run … -ec 'unset PGDATA PG_MAJOR PG_VERSION; cd /workspace; go test -tags drill -count=1 -v -run "^TestControlAnchorCaptureMatchesActualControlOwner$" ./internal/recovery/'`） | `0` | 日志时间戳 `04:06:49 → 04:06:53` UTC；单测 5.25s | 同 #2 | `logs/postfix-wrapper/{run.log,run.rc}` |
| 5 | 本批定向 race（宿主） | `go test -race -tags drill -count=1 -v -run '^TestControlAnchorCaptureMatchesActualControlOwner$' ./internal/recovery/` | `0` | `04:07:41Z → 04:08:09Z`（28s）；单测 8.02s | 宿主 Go/docker（无 `TESTCONTAINERS_*`），`GOMODCACHE=/home/dream/go/pkg/mod`、`GOCACHE=/home/dream/.cache/go-build`、uid 1000 | `logs/race/{run.log,run.rc,env.txt}` |

补充：#3/#4 的命令原文在 `.evidence/drill-runner-env/topology-repro/run-postfix.sh`（该目录 git-excluded、只读）。#3/#4 运行时刻测试文件 mtime = `2026-10-07 12:06:05 +0800`（= `04:06:05Z`），早于两次运行（`12:06:31` / `04:06:49`），即 postfix 日志对应的正是本批修复后的文件字节。

### 5.1 本批附加记录（非任务清单复制件）

- `logs/race/env.txt`：race 运行时刻环境（HEAD、`git status`、运行时刻 `sha256sum`、Go env、命令、窗口、rc）。
- `logs/proc-fd-permission-probe.txt`：`/proc/<pid>/fd` 可读性探针（宿主 uid 1000 对 root 进程 `Permission denied`；容器内 pid 1 可读），支撑 §2.1 的取舍说明。
- `logs/remote-run-37562840720-extract.txt`：远程 run 37562840720 的**只读**工件摘要（`gh run view` / `--log-failed` 摘录 + 下载的 `run.laVV8y/coverage.json` 字段与 22 项 `required_cases` 清单），支撑 §7；本批**未重跑**远程 drill。

---

## 6. 指纹与复制对应关系

### 6.1 代码指纹

- 运行 HEAD：`fee2d904eb6e013ee046c4375f395df143957a9c`（工作区另有本批未提交修改，见 `env.txt`）。
- `internal/recovery/control-anchor-mini_linux_test.go` **运行时刻** sha256 = `e6477c9390bf38aba237a3cb23bf38766266ac0ec697411e0fea489ec908ab4e`
  （与 `logs/race/env.txt` 的 `sha256sums_at_run` 一致；两次 postfix 运行使用同一未变字节，见 §5 补注）。
  **提交后的 git blob 由主报告按 commit 重新核实**（本证据目录不含 commit）。
- 修复前该文件 sha256 = `0a7fd0a140edabf92de070954adf783071bef37e8e59711b58030152d5dcbb61`（见 `logs/host-prefix/env.txt`、`MANIFEST.txt`）。
- 共享辅助文件（未改）：`internal/recovery/targetwriter_drillbridge_linux_test.go` = `03d38e4d015e104807cebb57babcfc894ece21ca625d683ce397beaadd39742e`。

### 6.2 日志/文件 sha256（完整清单见 `SHA256SUMS`）

| 文件 | sha256 | 对应源（`.evidence/drill-runner-env/topology-repro/`） |
|---|---|---|
| `MANIFEST.txt` | `fabc778b2fe4875ceece75abc90fc4208b4d446a11844bee18dd831c43913f15` | `MANIFEST.txt`（字节相同） |
| `logs/host-prefix/run.log` | `c697e45c34e313f6c8cbf7403462fe9a5afd764e55f16715c645d5fcd474bdb3` | `host/run.out` |
| `logs/host-prefix/run.rc` | `9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa` | `host/run.rc`（内容 `0`） |
| `logs/host-prefix/env.txt` | `f280dd916ef390388d570c4f838ba42d9336ea06dde117468d565e18b039c1ab` | `host/env.txt` |
| `logs/wrapper-prefix/run.log` | `754a999cffdead7857467c254c92e390c88f8457b72f644e806402ad6ba0ea9d` | `wrapper/run.out` |
| `logs/wrapper-prefix/run.rc` | `4355a46b19d348dc2f57c046f8ef63d4538ebb936000f3c9ee954a27460dd865` | `wrapper/run.rc`（内容 `1`） |
| `logs/wrapper-prefix/cmd.txt` | `b12f714d33f708ff8dbbe6751aca50c7adda3d91e812133fd97d4fb2f68c27b5` | `wrapper/cmd.txt` |
| `logs/wrapper-prefix/probe.log` | `b6cbb3a0cc1d3f84638af8bcbdc003a1d53747dcd205b714cb66668102445508` | `wrapper/probe.out` |
| `logs/postfix-host/run.log` | `4f02b5c59d731559368f88e3bb32f1fe5aa0accf4b44907dd5c119cadd9436ea` | `postfix-host/run.out` |
| `logs/postfix-host/run.rc` | `9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa` | `postfix-host/run.rc`（内容 `0`） |
| `logs/postfix-wrapper/run.log` | `5ea9a969aee3b5cc194c06a551740e572cc51e24c59f56c9d3bbd79103a8a810` | `postfix-wrapper/run.out` |
| `logs/postfix-wrapper/run.rc` | `9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa` | `postfix-wrapper/run.rc`（内容 `0`） |
| `logs/race/run.log` | `8380ad870aea3e3d64943bd4e75b7e6be3a0c5e476d051dae2ab8fa4134c9d82` | 本批新产出（任务 1） |
| `logs/race/run.rc` | `5feceb66ffc86f38d952786c6d696c79c2dbc239dd4e91b46729d73a27fb57e9` | 本批新产出（内容 `0`，1 字节） |
| `logs/race/env.txt` | `451e4ebd5bc37ce166f94e9d1a8bb29feb5f7a6f2111ad4aee80c8c81e272d23` | 本批新产出 |
| `logs/proc-fd-permission-probe.txt` | `a71976b23e47756938b8b086c533e3ec903ded9db04cb008a97201ca9e4076f2` | 本批新产出 |
| `logs/remote-run-37562840720-extract.txt` | `3e80cb7659839e777adf0f26e1b34e388a045ce1e667ac50f72e8977a24cd7ff` | 本批新产出（远程工件只读摘要） |
| `env.txt` | `5917eab859a3af7480f1bde0d59ee2742208ed4ec4ecb658688210f64d7ff10f` | 本批新产出 |

所有“对应源”项的复制均逐文件 `sha256` 复核为**字节相同**（上表两侧同值；复核命令见 §6.3）。`SHA256SUMS` 覆盖本目录除自身外的全部文件，`sha256sum -c SHA256SUMS` 通过（见 §6.3）。

> 注：证据副本扩展名由 `.out` 改为 `.log`（仓库 `.gitignore` 排除 `*.out`，017 轮起按此约定处理）；文件字节与 `sha256` 不变（见上表）；表中“对应源”列的 `.out` 名是 `.evidence` 暂存目录中的原始源文件。

### 6.3 复核命令

```bash
cd docs/evidence/018-control-anchor-wrapper-binding
find . -type f ! -name SHA256SUMS | sort | xargs sha256sum > SHA256SUMS
sha256sum -c SHA256SUMS
grep -riE 'token|auth|password|secret' logs/ env.txt   # 脱敏扫描
```

脱敏：`grep -riE 'token|auth|password|secret' logs/ env.txt README.md MANIFEST.txt` 的全部命中都是**非凭据**文本——测试名中的 `Auth`/`Authority`/`Unauthorized` 子串（`logs/remote-run-37562840720-extract.txt` 的 `TestBorrowedAuthEntryProbeFDChildProcess`、`TestT059F4…AuthorityWrites`、`TestT059F7Unauthorized…`）、§6.3 本行命令自身、以及 `MANIFEST.txt` 的“No secrets …”声明；**无任何凭据值**。PG 口令为仓库公开虚构值 `txharbor`（本目录日志中亦未出现），未引入其它凭据；日志中只有 loopback/RFC1918 地址、容器 id、镜像摘要、工具版本与测试输出。

---

## 7. 边界与非声明

- 本批是**本地双拓扑验证**：宿主（#1/#3/#5）与 wrapper 容器（#2/#4）两种拓扑的定向单测结果，不代表完整 drill。
- **未重跑远程 drill**。`run 37562840720`（`fix/drill-runner-modcache-prime`，HEAD `fee2d904…`）的失败保留：
  - `overall_result = FAIL`、`go_test_exit_status = 1`、`checker_result = PASS`（下载的 drill 证据 `run.laVV8y/coverage.json`；摘录见 `logs/remote-run-37562840720-extract.txt:13-15`）；
  - **必验集合 22/22 PASS**（同文件 `required_cases` 共 22 项、`status` 全 `PASS`；本测试 `TestControlAnchorCaptureMatchesActualControlOwner` **不在** `required_cases` 内 → 非必验；摘录与 22 项清单见 `logs/remote-run-37562840720-extract.txt:16-17,55-77`）；
  - 失败面收敛：`--log-failed` 中唯一失败包 = `internal/recovery`（`Output: FAIL\tgithub.com/xtianxx/txharbor/internal/recovery\t2954.206s`，`extract:28`），该包内唯一失败测试 = 本测试（`control-anchor-mini_linux_test.go:163: anchor local tuple is not a loopback TCP endpoint`，`extract:22-23`），传播为 `make: *** [Makefile:76: test-drill] Error 1`（`extract:51`）、`Process completed with exit code 2`；日志同时包含 `All required drill scenarios and restore-dependent subtests passed.`（`extract:52`）
  - 即：远程 FAIL 的成因是本批修复的这**一个非必验测试**在 wrapper 拓扑下的宿主 loopback 假设；本批只做该点局部修复，**不冒称完整 drill 通过**。
- 不改必验集合、不改预算、不改 `check.go`、不改任何源码（仅 `internal/recovery/control-anchor-mini_linux_test.go` 一个测试文件）；未修改 `.evidence/` 内任何源文件（本目录全部内容为复制件或本批新产出）。
- 未验证项：提交后的 git blob 指纹（由主报告按 commit 核实）；远程 CI 上的复跑结果（本批未触发）。
