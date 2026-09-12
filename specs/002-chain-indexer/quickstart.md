# Quickstart: 002-chain-indexer 验证指南

**Branch**: `002-chain-indexer` | **Date**: 2026-09-12

本文件只给可运行的验证路径，不含实现代码。测试分层沿用仓库约定：
单元（无 tag）+ 集成（`//go:build integration`，testcontainers 真 PG + Anvil/可控假 RPC，
`make test-integration`，并发项加 `-race`）。

前置：`docker info` 可用；`TXHARBOR_PG_DSN` 指测试库；Anvil 容器 `chain-id 31337`
（helper 见既有 `serve_integration_test` 模式）。

## 场景映射（spec Acceptance Matrix 1–13）

1. **首次扫描**：空库 + Anvil 预生 N 块，`START_HEIGHT=S` 启动；断言首行即 S、顺序连续、
   `checkpoint=(S+k, hash)` 且外键行存在。含 S=0 创世变体：首块 parent 全零保存、
   checkpoint 建行、`start_height=0` 冻结，S+1 父子衔接正常。
2. **S 高于链头**：S=H+5 启动；断言零 block 行、checkpoint 无行、`state=1`、无 error 日志；
   再产块至 S，断言首块事务一次建成。
3. **追上链头**：追平后停产块；断言 `state=1`、checkpoint 不动、readyz 仍 200；复产后继续。
4. **重复扫描**：重投同块 3 次；断言有效记录恒 1、checkpoint 不变。
5. **重启恢复**：推进至 N 后 SIGTERM 重启；断言从 N+1 继续、无缺口。
6. **RPC 中断后恢复**：Anvil 停服 30s 再恢复；断言期间 checkpoint 推进 0、`rpc_total{kind,result=error}`
   增长、恢复后自动继续。
7. **DB 写入失败**：故障注入（kill 连接/只读事务级错误）；断言 checkpoint 不动、
   无"checkpoint 已进但块缺失"（外键即证）。
8. **提交结果不确定**：代理层在 COMMIT 回包时掐断；重启后断言以 DB 为准继续，不跳不重。
9. **双实例竞争**：两实例同链同 S 并跑 60s；断言同高度 canonical 有效记录恒 1、
   checkpoint 单调且连续（见硬断言）。
10. **父哈希不一致**：假 RPC 在 N+1 返回错 parent；断言停推、`indexer_pause` 行 kind=parent_mismatch、
    历史零改写。
11. **停机期间 checkpoint 变化**：Anvil 重启换链（同 chain_id 不同哈希）后恢复扫描；
    断言核验失败 → kind=checkpoint_changed 暂停，不推进。
12. **chain_id 错误**：RPC 返回错误 chain_id；断言零写入、启动/运行门禁报错含期望与实际值。
13. **安全退出**：提交临界点发 SIGTERM；断言无部分进度（checkpoint 行与其 block 行成对），
    退出码 0 且在 ShutdownTimeout 内。

## 硬断言（9/8/4/10 必过门禁，SQL/指标级）

- **不倒退**：全程 `checkpoint.height` 单调非递减（轮询采样 + 结束值）。
- **不跳高**：结束时 `checkpoint.height - min连续缺口检查`：`chain_blocks` 在
  `[S, checkpoint.height]` 无缺号（`generate_series` 左连断言 0 缺口）。
- **连续一致**：`parent_hash` 逐块等于前块 `hash`（创世/首块边界除外，首块按 FR-05 豁免）。
- **无重复有效记录**：`(chain_id, number)` 分组计数恒 1。
- **暂停后零推进**：pause 行存在后，任一实例的 checkpoint 写次数为 0（指标 + DB 双重采样）。

## 确定性并发测试（已设计、尚未运行；tasks/实现阶段执行）

以下四项直接对应 data-model 并发正确性论证的三种情形 + 暂停硬门禁，全部使用真实
PostgreSQL + 锁原语，禁止以"静止后采样"替代断言：

- **T1 暂停-推进互斥**：测试事务 A 取协调锁后写入 pause 行但暂不提交；另一会话 B 发起
  推进协议——断言 B 阻塞于协调锁（未完成、未写入，而非超时通过）；A 提交后 B 继续，
  断言 B 拒绝写入且 checkpoint 行（height+hash）与暂停前完全一致。
- **T2 旧 token 拒绝**：A 持 token=5；B 执行接管（token→6，owner=B）并提交；
  A 用 token=5 发起写事务——断言步骤 4 裁决失败、零写入、checkpoint 不变（含暂停事务同理）。
- **T3 双首写互斥**：空进度下两实例经起跑屏障并发执行首块协议（同 S 同哈希）；
  断言终态恰好一行 checkpoint、一行区块 S、无缺口；败者已转向 S+1 路径（其 S 推进被拒）。
- **T4 暂停后零推进（硬门禁）**：pause 行提交后，多实例发起 N 次推进尝试；
  断言 checkpoint 行值与暂停前逐字段一致（非采样：提交前后精确比对），且
  `txharbor_indexer_pause_total` 仅计数首暂停。

## 日志/脱敏抽查

全量用例跑完后归档日志 `grep` 凭据模式出现次数必须为 0；`config.Summary` 含 RPCURL 脱敏后的输出。
