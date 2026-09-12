# Acceptance Record: 002-chain-indexer

**Branch**: `002-chain-indexer` | **Baseline**: `9e060bf` | **Date**: 2026-09-13

**提交**：`9e060bf..HEAD` 共 4 个新提交（`9e060bf` 为基线本身）：
`aa0c6f1`、`e7a4dc8`、`e9f3e50`、`96c7739`（另有本次验收修复待提交，见下）。

**任务**：T001–T018 共 18 项，无删除（此前“16”为未勾选数误读）。

## 运行证据（Docker 恢复后全量执行，全部通过）

前置阻塞解除：daemon 本就运行，原因是用户不在 `docker` 组；权限处理后
`docker info` 成功。本机执行：

- `make build` → 通过
- `make lint`（gofmt + 双 tag vet）→ 干净
- `make test` → 8 包全过（含 config 37、eth 15）
- `go test -race -tags integration -run TestCoordination ./internal/indexer/` → 6 passed
  （T014–T017 串行，真实 PG；另含 lease 接管用例）
- `make test-integration` → **8 包全过**：
  app / config / db / eth / health / indexer / logx / metrics，
  含 001 回归、002 全部 13 场景、T1–T4、迁移版本断言（applied=2/current_version=2）

## 验收中发现并修复的问题（未跳过、未弱化断言）

1. `migrate_concurrent_test.go` CLI env 缺 `TXHARBOR_START_HEIGHT`（T002 新增必需变量）→ 已补。
2. 同文件 `total applied == 1` 为 001 单迁移时代过期断言 → 改为与嵌入迁移数解耦
   （总数 == 迁移文件数，且每版本恰一行），意图不变、覆盖更强。
3. 自写场景测试三处越过目标断言竞态（首轮 Run 越过观察点）→ 改为 checkpoint 锚定 +
   `seedScanTo` 确定性播种；实现代码无问题。

## 逐任务状态（T001–T018）

| 任务 | 实现 | 验证 | 证据 |
|------|------|------|------|
| T001 迁移 | ✅ | ✅ 已勾选 | MigrationFiles/embed 单元 + 真库 applied=2 集成绿 |
| T002 配置+脱敏 | ✅ | ✅ 已勾选 | 37 单元绿 |
| T003 取块+分类 | ✅ | ✅ 已勾选 | 15 单元绿 |
| T004 lease | ✅ | ✅ 已勾选 | 11 单元 + race + 接管集成绿 |
| T005 scanner | ✅ | ✅ 已勾选 | 4 单元 + 场景集成绿 |
| T006 接线+指标 | ✅ | ✅ 已勾选 | 契约单元 + serve 集成绿，readyz 未被污染 |
| T007 场景1,5+S变更+创世 | ✅ | ✅ 已勾选 | 集成绿（含拒绝启动零写入、创世零父） |
| T008 场景2,3,6,7 | ✅ | ✅ 已勾选 | 集成绿（等待零写入、故障零推进、自动恢复） |
| T009 场景8,13 | ✅ | ✅ 已勾选 | 集成绿（DB 为准继续、安全退出零残留） |
| T010 场景4 | ✅ | ✅ 已勾选 | 集成绿（重投恒 1） |
| T011 场景9 | ✅ | ✅ 已勾选 | 集成绿（单 canonical、单调连续） |
| T012 场景10,11,12 | ✅ | ✅ 已勾选 | 集成绿（三类暂停、重启仍拒、错链零写） |
| T013 契约 | ✅ | ✅ 已勾选 | 集成绿（四态/暂停行/零凭据） |
| T014–T017 T1–T4 | ✅ | ✅ 已勾选 | -race 串行绿，同步点+有界等待 |
| T018 总体验收 | ✅ | ✅ 已勾选 | 本文件 + 上述全绿 |

## 13 场景 × 不变量核对

13 场景全部在真库真链（stub 链 + Anvil 模式）下变绿；硬断言
（不倒退、不跳高、连续一致、无重复、暂停后零推进）随集成测试通过。
`Summary` RPC URL 脱敏有单元覆盖；全量日志凭据零出现由 T013 断言。

## 结论

002 实现与验证完成：18/18 任务勾选，阻塞解除，证据如上。未 push、未合并、不进入 003。
