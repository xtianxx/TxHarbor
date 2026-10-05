# 015-followup-deposit-unknown / archive 清单

- 仓库: `/home/dream/product_env/TxHarbor`
- 分支: `015-followup-deposit-unknown-commit`
- HEAD: `6440b2ffa5d51d69246d1f6c6c03e2d72f37bdee`
- 归档目录: `docs/evidence/015-followup-deposit-unknown/archive/`
- 归档日期: 2026-10-05
- 边界: 仅新建本目录及其中文件；未编辑其他任何文件；未 `git add`/`git commit`；未重跑任何测试（仅只读检索、哈希、解压校验）

## 1. 文件清单

| 归档件 | 来源路径 | 原文件 sha256 | 归档件 sha256 | 原字节数 | 归档字节数 | 复制命令 |
|---|---|---|---|---|---|---|
| `pgfix3.log` | `/tmp/r2lab/pgfix3.log` | `a5e1c3e681914f10a513dd91a1ce21d905cc4da43291798c6968508fe7e5fd92` | `a5e1c3e681914f10a513dd91a1ce21d905cc4da43291798c6968508fe7e5fd92` | 65 | 65 | `cp -p /tmp/r2lab/pgfix3.log docs/evidence/015-followup-deposit-unknown/archive/pgfix3.log` |
| `pgfix3b.log` | `/tmp/r2lab/pgfix3b.log` | `4a1f1b042d9c499790e45c1a3e5701d59500865f2cbb43ccf0aa0d7e41ea26c8` | `4a1f1b042d9c499790e45c1a3e5701d59500865f2cbb43ccf0aa0d7e41ea26c8` | 373 | 373 | `cp -p /tmp/r2lab/pgfix3b.log docs/evidence/015-followup-deposit-unknown/archive/pgfix3b.log` |
| `indexer.jsonl.gz` | `/tmp/r2lab/run_pgfix3b/indexer.jsonl` | `e9e7f6de9f54998ecd3ee1db24e570138143332713c3c045a3e985319ad76208` | `eaf05aa617cb11502d3b4e600785cc1ef286c58bad76a88d927783f8ffdeca94` | 1179592 | 70411 | `gzip -9 -c /tmp/r2lab/run_pgfix3b/indexer.jsonl > docs/evidence/015-followup-deposit-unknown/archive/indexer.jsonl.gz` |

备注:
- `pgfix3.log`: 3 行；`pgfix3b.log`: 12 行；`indexer.jsonl`: 4158 行（解压后），源文件属主 `root:root`（容器内生成）。
- 两个 `.log` 为逐字节原样复制（`cp -p`），归档件与原件哈希必然一致（已实测复核）。

## 2. gzip 解压校验

命令与结果（在 `archive/` 内执行）:

```
gunzip -c indexer.jsonl.gz | sha256sum
  → e9e7f6de9f54998ecd3ee1db24e570138143332713c3c045a3e985319ad76208  -
  == 原始 /tmp/r2lab/run_pgfix3b/indexer.jsonl 的 sha256（逐字节一致）

gunzip -t indexer.jsonl.gz
  → 无输出，退出码 0（gzip 完整性 OK）

gzip -l indexer.jsonl.gz
  → compressed 70411 / uncompressed 1179592 / ratio 94.0% / uncompressed_name indexer.jsonl
```

说明: `indexer.jsonl.gz` 自身的 sha256（`eaf05aa6…ca94`）是归档产物本身的哈希；gzip 头内嵌原文件名 `indexer.jsonl`，压缩字节不作为内容同一性凭据，内容同一性以「解压流哈希 == 原文件 sha256」为准（已满足）。

## 3. 统计重算

口径:
- 数据源 = 解压流（等价于原始 `indexer.jsonl`）
- 逐行 `json.loads`；终态记录 = `Action ∈ {pass, fail, skip}` 且 `Test` 字段非空
- `Test` 不含 `/` → 顶层测试；含 `/` → 子测试
- 包级终态（`Action ∈ {pass, fail, skip}` 且无 `Test` 字段）单独列出，**不计入** 674

命令 1（主口径，逐行 JSON 解析）:

```bash
gunzip -c indexer.jsonl.gz | python3 -c '
import sys, json
from collections import Counter
acts = Counter(); term = Counter(); pkg = Counter()
tests = Counter(); pkgs = set(); tgt = []
for i, line in enumerate(sys.stdin, 1):
    line = line.strip()
    if not line: continue
    r = json.loads(line)
    a = r.get("Action"); t = r.get("Test", ""); p = r.get("Package", "")
    pkgs.add(p); acts[a] += 1
    if a in ("pass", "fail", "skip"):
        if t:
            kind = "sub" if "/" in t else "top"
            term[(a, kind)] += 1
            if a == "pass":
                tests[t] += 1
                if t == "TestDepositCommitUnknownOutcomeRereadsDB":
                    tgt.append((i, p))
        else:
            pkg[a] += 1
print("packages:", sorted(pkgs))
print("action_counts:", dict(acts))
print("package_level_terminal:", dict(pkg))
print("terminal_with_test:", dict(term))
print("total_terminal_with_test:", sum(term.values()))
print("pass_total:", term[("pass","top")] + term[("pass","sub")])
print("distinct_pass_test_names:", len(tests))
print("target_pass_lines:", tgt)
'
```

输出（实测）:

```
packages: ['github.com/xtianxx/txharbor/internal/indexer']
action_counts: {'start': 1, 'output': 2808, 'run': 674, 'pass': 675}
package_level_terminal: {'pass': 1}
terminal_with_test: {('pass', 'top'): 312, ('pass', 'sub'): 362}
total_terminal_with_test: 674
pass_total: 674
distinct_pass_test_names: 674
target_pass_lines: [(2493, 'github.com/xtianxx/txharbor/internal/indexer')]
```

命令 2（独立 grep 交叉核对，全部为实测输出）:

```bash
# A: 带 Test 字段的 pass 总数
gunzip -c indexer.jsonl.gz | grep -c '"Action":"pass","Package":"[^"]*","Test":"'
#   → 674

# B: 子测试 pass（Test 含 /）
gunzip -c indexer.jsonl.gz | grep -c '"Action":"pass","Package":"[^"]*","Test":"[^"]*/'
#   → 362

# C: 带 Test 字段的 fail|skip
gunzip -c indexer.jsonl.gz | grep -Ec '"Action":"(fail|skip)","Package":"[^"]*","Test":"'
#   → 0（grep -c 计数为 0，退出码 1）

# D: 包级 pass（无 Test 字段）
gunzip -c indexer.jsonl.gz | grep '"Action":"pass"' | grep -vc '"Test":"'
#   → 1（第 4158 行，末行）

# E: 目标测试全部记录行号
gunzip -c indexer.jsonl.gz | grep -n '"Test":"TestDepositCommitUnknownOutcomeRereadsDB"'
#   → 2490 run / 2491 output("=== RUN") / 2492 output("--- PASS … (0.17s)") / 2493 pass
```

重算结果:
- **pass = 674**（顶层 312 + 子测试 362）— 与预期 674（312+362）一致
- **fail = 0，skip = 0** — 全流 `action_counts` 中不存在 fail/skip action
- 顶层 + 子测试合计 = 674；distinct pass 测试名 = 674（无重名、无重复终态）
- 全流仅 1 个包: `github.com/xtianxx/txharbor/internal/indexer`；action 总览 `start=1, run=674, output=2808, pass=675`
- **包级 pass（无 Test）: 1 条，第 4158 行（末行）**，`Elapsed:147.702`，不计入 674；对应其上方 output 行 `ok  github.com/xtianxx/txharbor/internal/indexer  147.701s`
- **目标测试 `TestDepositCommitUnknownOutcomeRereadsDB`: pass 记录 = 第 2493 行**（run=2490，output=2491/2492），`--- PASS … (0.17s)`，无子测试；该测试无 fail/skip 记录

## 4. git 运行对应章节

### 4.1 pgfix3 = 运行器前置失败（测试未启动）

- `/tmp/r2lab/pgfix3.log` 全部 3 行: `/bin/sh: 4: git: not found`、`tree_head=`（空值）、`/bin/sh: 5: git: not found`
- 测试未启动的旁证: `/tmp/r2lab/run_pgfix3/scratch/` 仅含空目录 `home/`、`tmp/`（无 `indexer.jsonl`）
- 无 `go_test_exit` 行

### 4.2 pgfix3b = 元数据命令遇 dubious-ownership 后以 safe.directory 补取且测试成功

- `/tmp/r2lab/pgfix3b.log` 第 1–4 行与第 6–9 行: 两次 `fatal: detected dubious ownership in repository at '/workspace'`（附 safe.directory 提示），即元数据命令遇阻
- 第 5 行 `tree_head=`（空）、第 10 行 `dirty_tracks=0`；第 12 行 `go_test_exit=0`
- 测试成功: `indexer.jsonl` 全包 **674 pass / 0 fail / 0 skip**（见第 3 节）

### 4.3 运行树绑定方式（事后核对）

**运行当时精确树快照缺失**（见第 5 节）。两份日志中 `tree_head=` 均为空；全 /tmp 文本扫描无 `tree_head=2fca0b05` 或任何非空 `tree_head=` 留存。本绑定属**事后核对**，证据链为:
- (a) 事后以 safe.directory 补取运行树哈希 —— **补取输出未持久化为文件，仅存于会话记录，无法归档为"当时输出"**；
- (b) 事后 diff blob 匹配 `7194719..e19dbd8` —— 无留存文件（全 /tmp 文本检索 `7194719`、`e19dbd8` 均 0 命中）。

旁证（本轮范围外、只读引用，未归档）:
- `/tmp/wpa-rst/SUMMARY.md` 第 3 行: `Branch: 015-followup-deposit-unknown-commit @ 2fca0b05 (no branch switch, no repo edits)`
- `/tmp/015pr/commitmsg.txt` 第 35 行: `run tree = 2fca0b05 + this fix (read-only mount)`
- `/tmp/015pr/pr39-merge.md` 第 3 行: 合并 SHA `2fca0b05b1f21944388ef32d1e1ec71ed98c203b`

## 5. 缺失清单 / safe.directory 复查

**复查关键词与范围**: `safe.directory`、`2604`、`bash-original`、`tree_head=2fca0b05`、`detected dubious`；范围 `/tmp/r2lab`、`/tmp/015pr`、`/tmp/wpa-rst`，并做全 `/tmp` 文本级扫描（排除 git-2.47.0 源码树与 gocache 二进制缓存）。

**结论: 未找到 safe.directory 补取输出的任何留存。**

- `tree_head=2fca0b05`: **0 命中**（`2fca0b05` 仅作为合并/运行树标识出现在 `/tmp/wpa-rst/SUMMARY.md`、`ci_job.log`、`/tmp/015pr/commitmsg.txt`、`pr39-merge.md`）
- `bash-original`: **0 命中**
- `2604`: 仅 hash 片段、时间戳等噪音命中，无证据文件
- `7194719`、`e19dbd8`: **0 命中**
- 含 `safe.directory` 的文本文件全列（均为命令来源或无关对照，**无一为补取输出**）:
  - `/tmp/r2lab/run_suite_pg3.sh`（第 32 行）、`/tmp/r2lab/run_drill4b.sh`（第 34 行）: 探测脚本源内含 `git config --global --add safe.directory /workspace`，是**命令本身**而非其输出
  - `/tmp/r2lab/pgfix3b.log`: 已归档；仅含 dubious fatal 消息（无补取输出）
  - `/tmp/r2lab/pgfull3_outer.log`（164 B）: **另一运行**（pgfull3）的同类 dubious fatal 消息，无补取输出
  - `/tmp/wpa-rst/ci_job.log`、`/tmp/015pr/mainci_pg_full.log`、`/tmp/pr38_pgjob.log`: GitHub Actions checkout 日志（runner 路径 `/home/runner/work/…`、`set-safe-directory: true`），与 `/workspace` 补取无关
  - `/tmp/r2lab/gittest/git-2.47.0/**`: git 源码树自带内容，无关
- **状态产物（非输出）**: 全 `/tmp` 仅 1 个 `.gitconfig` —— `/tmp/r2lab/run_pgfull3b/scratch/home/.gitconfig`，内容为 `[safe] directory = /workspace`；属 **run_pgfull3b** 的 scratch，**不是** pgfix3b 运行目录。pgfix3b scratch 仅含 `gocache/` 与 `home/.config/go/telemetry`（无 `.gitconfig`）。

其余缺失项:
- **当时树快照**: 缺失（见 4.3）
- **phase 探针源码**: `/tmp/wpa` 下无 `*phase*` 文件；仅有另一探针 `exit_window` 的取证副本 `zz_wpa_probe_exit_window_test.go.forensic-copy` 及配套 `.json`/`.tsv`（非 phase 探针源码）
- **wpa run1/run2 原始日志**: 留存于 `/tmp/wpa-rst/run.log`（64640 B）、`run2.log`（64754 B），本轮范围外，**未归档**
- 意外发现: `/tmp/r2lab` 顶层无任何 `*pgfix*.sh` 运行器脚本（仅 `run_suite.sh`、`run_suite_pg3.sh`、`run_drill4b.sh`）；pgfix3/pgfix3b 的调用脚本亦未留存

## 6. 边界声明

- 本目录（`docs/evidence/015-followup-deposit-unknown/archive/`）之外零文件改动；未执行 `git add`/`git commit`；未重跑任何测试或运行命令（除只读检索、哈希、gzip 解压校验）
- 本文所有哈希、行号、计数、字节数均为本轮实测输出，未做任何重建或推断性填充
