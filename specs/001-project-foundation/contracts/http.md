# Contracts: HTTP

基地址：`TXHARBOR_HTTP_ADDR`（默认 `127.0.0.1:8080`）。全部端点只读、无认证（本地底座；对外暴露由后续规范决定）。

## `GET /livez`

- 语义：进程存活（未死锁）。依赖中断时**仍 200**。
- 成功：`200 {"status":"alive"}`
- 失败（仅进程级故障）：`500 {"status":"dead","reason":"..."}`

## `GET /readyz`

- 语义：`ready = db_ok && rpc_ok && version_ok && chain_ok`。启动完成前、依赖中断、版本不兼容、错链时均为失败。
- 成功：`200 {"status":"ready","checks":{"db":"ok","rpc":"ok","version":"ok","chain":"ok"}}`
- 失败：`503 {"status":"not-ready","failed":["db"],"detail":"..."}`（`failed` 列出未过项，`detail` 含期望/实际差异等诊断信息，**凭据脱敏**）

## `GET /metrics`

- 格式：Prometheus exposition（`client_golang`），消费者为 curl/排障（不部署 Prometheus/Grafana）。
- 内容：`process_*`、`go_*`（默认 collector）+ `txharbor_ready`（GaugeFunc，1/0）+ `txharbor_probe_total{dep="db|rpc",result="success|failure"}`。
- 禁止：业务指标、延迟直方图、backlog 预留。
