# Tasks: Project Foundation

**Input**: Design documents from `specs/001-project-foundation/`

**Prerequisites**: plan.md, spec.md (US1–US5), research.md, data-model.md, contracts/cli.md, contracts/http.md, quickstart.md

**Tests**: FR-016 明确要求测试覆盖七类验收场景（+ /metrics + 并发迁移），故每故事含测试任务。测试先写并确认失败再实现。

**Organization**: 按用户故事分组，每故事独立可测；范围锁死 001（无业务表、无扫描/充值/提款/nonce/签名）。

## Format: `[ID] [P?] [Story] Description`

- **[P]**: 可并行（不同文件、无未完成依赖）
- **[Story]**: 所属用户故事（US1–US5，对应 spec.md）
- 描述含精确文件路径 + FR/SC 映射

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: 项目初始化：依赖、目录、本地编排、命令入口

- [ ] T001 在 `go.mod` 中引入 goose v3.28.0、pgx/v5 v5.11.0、go-ethereum v1.17.5、client_golang v1.24.1（生产）与 testcontainers-go v0.44.0（仅测试）并 `go mod tidy`（FR-005/009/019，research §1–5）
- [ ] T002 [P] 创建 `compose.yaml`：postgres:18.6-trixie（命名卷 `/var/lib/postgresql`，pg_isready TCP healthcheck）+ foundry:v1.8.1（automine，chain-id 31337，cast healthcheck），端口仅绑 127.0.0.1（FR-015，research §6–7）
- [ ] T003 [P] 创建 `.env.example`：全部 `TXHARBOR_*` 变量占位 + 默认值注释（FR-001/015，data-model §1）
- [ ] T004 [P] 创建目录骨架 `cmd/txharbor/main.go`、`internal/{config,db,eth,health,logx,metrics,app}/`、`migrations/`（plan §Project Structure）
- [ ] T005 [P] 创建 `Makefile`：`test`（`go test ./...`）、`test-integration`（`go test -tags integration ./...`）、`db-reset`（`docker compose down -v`）、`lint`（`gofmt -l` + `go vet ./...`）（research §5，quickstart §4–5）

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: 所有故事的前置阻塞项：配置、日志脱敏、DB/RPC 基础件

**⚠️ CRITICAL**: 本阶段完成前不得开始任何用户故事

- [ ] T006 在 `internal/config/config.go` 实现 env-only 加载与校验（缺失点名、格式/范围校验、超时间隔合法性 `interval+timeout<10s`），失败返回可诊断错误（FR-001/002/013，data-model §1）
- [ ] T007 [P] 在 `internal/config/config_test.go` 写配置矩阵单测（缺失项逐项、非法值逐项、默认值），先写先失败（FR-001/002，SC-002）
- [ ] T008 [P] 在 `internal/logx/redact.go` 实现凭据脱敏（连接串/口令/令牌→`[REDACTED]`，保留主机/端口/库名）（FR-003，research §8）
- [ ] T009 [P] 在 `internal/logx/redact_test.go` 写脱敏单测（含错误详情/启动回显路径），先写先失败（FR-003，SC-006 日志项）
- [ ] T010 在 `internal/db/pool.go` 实现 pgxpool 装配（显式 MaxConns/MinConns/Lifetime，创建后 `Ping` 验连通，每调用 ctx 超时）（FR-004，research §2）
- [ ] T011 [P] 在 `internal/eth/client.go` 实现 ethclient 封装（`DialContext` + 自带超时的 `ChainID` 校验 + 错误分类 transport/timeout/chain-mismatch/invalid-response，`Close` 释放）（FR-009/010，research §3）
- [ ] T012 [P] 在 `internal/eth/client_test.go` 写 httptest 假 JSON-RPC 单测（超时分支、错误分支、chain-id 比对），先写先失败（research §3）

**Checkpoint**: Foundation ready —— 配置/脱敏/DB 池/RPC 封装就绪，故事可并行开工

---

## Phase 3: User Story 1 - 本地正常启动并进入就绪 (Priority: P1) 🎯 MVP

**Goal**: `migrate up` 初始化空库后 `serve` 经完整启动链进入就绪，`/livez` 与 `/readyz` 均成功；重复迁移跳过；有 pending 时拒绝就绪

**Independent Test**: 空库 + 健康依赖下跑 `migrate up` → `serve`，30s 内 `/readyz` 200；再跑一次 `migrate up` 全跳过；跳过迁移直接 `serve` 被拒绝（SC-001/004/005）

### Tests for User Story 1 ⚠️

> **NOTE: 先写测试并确认失败，再实现**

- [ ] T013 [P] [US1] 在 `internal/db/migrate_test.go` 写迁移 embed 与编号排序单测（`go test -short` 可跑）（FR-005）
- [ ] T014 [P] [US1] 在 `internal/db/migrate_integration_test.go`（`//go:build integration`）写 testcontainers 真迁移测试：up/status、重复跳过、失败回滚后重试成功（FR-005/006/007，SC-005）

### Implementation for User Story 1

- [ ] T015 [US1] 在 `migrations/000001_baseline.sql` 创建底座基线迁移（goose Up/Down，单事务；001 仅底座自身对象、无业务表）（FR-005/018）
- [ ] T016 [US1] 在 `internal/db/migrate.go` 实现 goose Provider（embed FS + `WithSessionLocker` 会话锁）与 `up/status` 逻辑（FR-005/006，research §1）
- [ ] T017 [US1] 在 `cmd/txharbor/main.go` 实现子命令分发并接 `migrate up|status` 独立命令（contracts/cli.md，FR-005）
- [ ] T018 [US1] 在 `internal/app/serve.go` 实现 `serve` 启动链：配置→PG Ping→版本兼容（有 pending 则拒就绪提示先迁移）→RPC ChainID→监听并进入探针循环（FR-004/008/009/010，contracts/cli.md）
- [ ] T019 [US1] 在 `internal/health/server.go` 实现标准库 HTTP 服务与 `GET /livez`（进程存活恒 200）（FR-011，contracts/http.md）

**Checkpoint**: US1 独立可用 —— 迁移→启动→就绪全链路可演示

---

## Phase 4: User Story 2 - 配置错误快速失败且不泄露凭据 (Priority: P1)

**Goal**: 缺失/非法配置在启动早期点名报错并退出；全部日志无凭据明文

**Independent Test**: 逐项删 env / 填非法值启动，30s 内非零退出且错误定位到具体变量；归档日志 grep 凭据为 0（SC-002/006）

### Tests for User Story 2 ⚠️

- [ ] T020 [P] [US2] 在 `internal/app/serve_config_test.go` 写逐项缺失/非法启动失败单测（断言退出路径与错误含变量名）（FR-001/002，SC-002）

### Implementation for User Story 2

- [ ] T021 [US2] 在 `internal/app/serve.go` 接入启动早期配置门（加载→校验→脱敏摘要回显，失败即退、不监听端口）（FR-001/002/003，US2 验收 1–2）
- [ ] T022 [US2] 在 `internal/app/serve_integration_test.go`（`//go:build integration`）写配置错误端到端测试：缺失/非法/错链各一起，断言非零退出与可诊断输出（SC-002/004）

**Checkpoint**: US1 + US2 均独立工作 —— 正常启动与错误启动行为完整

---

## Phase 5: User Story 3 - 依赖中断导致未就绪、恢复后自动恢复 (Priority: P2)

**Goal**: 运行期 DB/RPC 中断→`/readyz` 10s 内 503 而 `/livez` 保持 200；恢复后 10s 内自动回 200；`/metrics` 暴露探测计数

**Independent Test**: 就绪后 `docker compose stop postgres`，10s 内 readyz 503 + livez 200；`start` 后 10s 内回 200；`/metrics` 含 `txharbor_probe_total`（SC-003，FR-012/019）

### Tests for User Story 3 ⚠️

- [ ] T023 [P] [US3] 在 `internal/health/aggregate_test.go` 写聚合逻辑单测（注入 fake Prober：单依赖失败→not-ready，恢复→ready）（FR-011/012）
- [ ] T024 [P] [US3] 在 `internal/metrics/metrics_test.go` 写指标注册与命名单测（ready gauge + probe CounterVec，无业务指标）（FR-019）

### Implementation for User Story 3

- [ ] T025 [US3] 在 `internal/health/prober.go` 实现 DB/RPC 探针（interval 2s、单次 5s 超时、错误分类计数）（FR-012，research §8）
- [ ] T026 [US3] 在 `internal/health/server.go` 实现 `GET /readyz`（200/503 JSON，`failed` 列未过项，凭据脱敏）（FR-011/012，contracts/http.md）
- [ ] T027 [US3] 在 `internal/metrics/metrics.go` 实现 `/metrics`（默认 collector + `txharbor_ready` GaugeFunc + `txharbor_probe_total{dep,result}`；禁业务指标/直方图）（FR-019，research §4）
- [ ] T028 [US3] 在 `internal/health/flip_integration_test.go`（`//go:build integration`）写翻转测试：停 PG→503/200 不变→启 PG→200；RPC 不可达同理（SC-003）

**Checkpoint**: US1–US3 独立可用 —— 降级与自愈可演示

---

## Phase 6: User Story 4 - 版本不兼容与错链拒绝就绪 (Priority: P2)

**Goal**: 错链 / 版本不兼容 / 迁移失败三态均拒绝就绪（或迁移非零退出）且原因可诊断；并发迁移串行无双写

**Independent Test**: 错 chain-id 启动被拒并同时报告期望/实际；高版本 DB 启动被拒；坏迁移重跑；双 `migrate up` 并发串行（SC-004/005）

### Tests for User Story 4 ⚠️

- [ ] T029 [P] [US4] 在 `internal/eth/chain_mismatch_test.go` 写错链单测（httptest 返回非期望 chain-id，断言拒绝就绪且错误含期望/实际）（FR-010，SC-004）

### Implementation for User Story 4

- [ ] T030 [US4] 在 `internal/app/serve.go` 补齐版本/链拒绝路径文案（期望/实际同时输出，pending 提示 `migrate up`）（FR-008/010，US4 验收 1–2）
- [ ] T031 [US4] 在 `internal/db/migrate_concurrent_integration_test.go`（`//go:build integration`）写并发迁移测试：双进程同库串行、不重复应用、锁等待超时非零退出（FR-006，SC-005，US4 验收 4）

**Checkpoint**: US1–US4 独立可用 —— 全部拒绝就绪态可诊断

---

## Phase 7: User Story 5 - 正常退出停止接收工作并释放资源 (Priority: P3)

**Goal**: SIGINT/SIGTERM 后停收新工作，15s 内释放（pool + client 关闭）退出；超时强制结束记错；外部调用永不无限阻塞

**Independent Test**: 就绪后 `kill -TERM`，15s 内进程退出且不再接受新连接；慢 RPC（httptest 延迟）触发调用超时而非挂起（SC-006，FR-013/014）

### Tests for User Story 5 ⚠️

- [ ] T032 [P] [US5] 在 `internal/app/shutdown_test.go` 写退出逻辑单测（signal→停收→关闭→退出；超时分支强制结束记错）（FR-014）

### Implementation for User Story 5

- [ ] T033 [US5] 在 `internal/app/serve.go` 实现信号处理与退出预算（`TXHARBOR_SHUTDOWN_TIMEOUT`，超时强制结束并记错）（FR-013/014，contracts/cli.md）
- [ ] T034 [US5] 枚举并补齐全部外部调用点的 `context.WithTimeout`：serve 启动链各步（PG Ping、版本查询、ChainID 校验各 5s，总和 < 30s 启动预算）、DB/RPC 运行期探针单次 5s、migrate 锁等待 `TXHARBOR_MIGRATE_LOCK_TIMEOUT`；取消 MUST 经 ctx 传播到 pgx/ethclient 调用（禁止无 ctx 裸调用）；验证：T035 慢 RPC 超时单测 + T012 httptest 超时分支 + 复核无裸调用（FR-013，research §2–3）
- [ ] T035 [US5] 在 `internal/eth/slow_rpc_test.go` 写慢 RPC 超时单测（httptest 延迟 > probe timeout，断言调用失败而非挂起）（FR-013）

**Checkpoint**: 全部用户故事独立可用 —— 退出可控可计时

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: 全量验证与文档收尾（不新增功能）

- [ ] T036 [P] 跑 `gofmt -l` + `go vet ./...` 全仓通过并修复（plan Quality Gates）
- [ ] T037 [P] 跑 `go test -race` 于并发敏感包（health/db）通过（constitution XI）
- [ ] T038 按 `quickstart.md` 全量演练一遍（8 行矩阵）并修复文档漂移（FR-015/016）
- [ ] T039 全量门禁一次过：`go test ./...` + `go test -tags integration ./...` + 七类验收×（SC-001…006）对照 spec 回检（FR-016/017/018 范围复核：无业务表、无扫描/充值/提款/nonce/签名）
- [ ] T040 按 `go.mod` 实际引用的 go-ethereum 子包逐一核对许可证：查验所用版本的包路径（设计为 `ethclient`、`rpc` 等）是否均位于上游 `cmd/` 之外并适用 `COPYING.LESSER`（LGPL-3.0），确认未引用 `cmd/` 下 GPL-3.0 包；将引用清单、许可证结论与二进制分发策略记录到 `THIRD_PARTY.md`（若任一引用为 GPL-3.0 则阻塞分发并升级决策）（research §3）

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: 无依赖，可立即开始
- **Foundational (Phase 2)**: 依赖 Setup —— 阻塞全部用户故事
- **User Stories (Phase 3–7)**: 依赖 Foundational；按 P1→P2→P3 顺序，可并行（人力允许）
- **Polish (Phase 8)**: 依赖全部期望故事完成

### User Story Dependencies

- **US1 (P1)**: Foundational 后可开始；无故事间依赖（MVP）
- **US2 (P1)**: Foundational 后可开始；复用 US1 的 serve 骨架但独立可测（坏 env 即可验证）
- **US3 (P2)**: Foundational 后可开始；探针循环挂载于 US1 的 server，需 US1 的 serve 存在 —— 实现顺序在 US1 之后
- **US4 (P2)**: Foundational 后可开始；拒绝路径文本依赖 US1 的 serve 骨架 —— 实现顺序在 US1 之后
- **US5 (P3)**: Foundational 后可开始；信号退出挂载于 US1 的 serve —— 实现顺序在 US1 之后

### Within Each User Story

- 测试先写并确认失败 → 实现 → 故事内 checkpoint 独立验证
- US3/US4/US5 的实现任务依赖 US1 的 `serve`/`server.go` 骨架（T018/T019）先合入

### Parallel Opportunities

- [P] 任务（不同文件）可并行：T002/T003/T004/T005、T007/T008/T009/T011/T012、T013/T014、T020、T023/T024、T029、T032、T036/T037
- Foundational 完成后 US1/US2 可并行；US1 骨架合入后 US3/US4/US5 可并行
- 全部 integration 测试（T014/T022/T028/T031）共享 testcontainers 模式，可同批执行

---

## Parallel Example: User Story 1

```bash
# US1 测试并行（T013 单测 + T014 集成，互不依赖）：
Task: "迁移 embed 与编号排序单测 in internal/db/migrate_test.go"
Task: "testcontainers 真迁移测试 in internal/db/migrate_integration_test.go"
```

## Parallel Example: User Story 3

```bash
# US3 测试并行（T023 聚合单测 + T024 指标单测，互不依赖）：
Task: "聚合逻辑单测 in internal/health/aggregate_test.go"
Task: "指标注册命名单测 in internal/metrics/metrics_test.go"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Phase 1 Setup + Phase 2 Foundational
2. Phase 3 US1（迁移→启动→就绪）
3. **STOP and VALIDATE**: 空库 `migrate up` → `serve` → 30s 内 ready；`quickstart.md` §2
4. 可演示

### Incremental Delivery

1. Setup + Foundational → 地基就绪
2. + US1 → MVP（可启动可迁移）
3. + US2 → 配置错误行为完整
4. + US3 → 降级自愈 + /metrics
5. + US4 → 拒绝就绪全诊断 + 并发迁移
6. + US5 → 可控退出
7. Phase 8 全量门禁

---

## Notes

- 范围红线：任务中出现业务表/扫描/充值/提款/nonce/签名即越界，另立规范
- 环境前提（C1）：integration 测试（T014/T022/T028/T031）要求 Docker daemon 可用；无 daemon 时 unit 可先行，集成层必须在有 Docker 的本地/CI 上跑。不得以 mock 替代真实依赖验收；skip 计为未执行，不得视为通过（quickstart 前置有记录）
- 每个任务完成后 commit；checkpoint 处停下独立验证
- 集成测试需 Docker；无 Docker 环境用 `testcontainers.SkipIfProviderIsNotHealthy` 跳过（research §5）
