# PROVENANCE —— 来源、复制规则与哈希对照

## 1. 来源（本地 ignored 原始树）

- 源根（仓库相对）：`.evidence/portfolio-demo/run04w-20261011130031/`
  （该目录树经 `.git/info/exclude` 忽略，**未**纳入版本控制；原始文件**未被修改或删除**）。
- 源 public 根：`.evidence/portfolio-demo/run04w-20261011130031/public/`。
- 运行身份：runID `run04w-20261011130031`；project `txharbor-demo-run04w-20261011130031`；
  run_token `ab9f084dc44409fbf7c2d19556b7af55`；执行时 HEAD
  `11d698c75edf14e28e7075300ca082fe6dc9a308`（`git_dirty_files=0`）。
- 时间/退出：UTC `2026-10-11T13:01:02Z → 13:03:14Z`；`exit_code=0`、`cleanup_rc=0`。

## 2. 复制规则（白名单）

只复制源 `public/` 下四类文件：`chain/*.json`、`chain/*.txt`、`http/*.json`、`sql/*.out`，
保持子目录结构与**原始字节**（复制后逐文件 sha256 核对相等，22/22）。未复制任何
`private/` 内容、运行 stderr、凭据/DSN/curl 文件、Anvil 日志或整个 `.evidence/` 树。

## 3. 逐文件对照（dest = 本包内路径；sha256 两侧相等）

| dest | source（`public/` 之下） | sha256 |
| --- | --- | --- |
| `chain/deposit-receipt.json` | `chain/deposit-receipt.json` | `487439c2afc1504192ec4b90c21c00c7a36264315ddde884678f946235be2170` |
| `chain/signing-receipt.json` | `chain/signing-receipt.json` | `f536185343a9336786bf29ae5bc8643e87b271084e06bbca899dce3914ab2963` |
| `chain/transfer-log-query.json` | `chain/transfer-log-query.json` | `2a5c18770924fc2e016f52b24718f0285a322d99dbd723bee96cee82716ad897` |
| `chain/transfer-log-query.filter.txt` | `chain/transfer-log-query.filter.txt` | `579b7288c7e9f517e201a52800259776c42793362ce40577738375838293ddc3` |
| `chain/final-state.txt` | `chain/final-state.txt` | `b3621dd432277eee6c49a7bf7041f2f654ccf233a04ffce2cc498364ec3616d7` |
| `http/create-201.json` | `http/create-201.json` | `b8cb5f0c0c9b5514dfece01c754d595e3e6d950725755fd8bdf29805b469bc93` |
| `http/create-200-replay.json` | `http/create-200-replay.json` | `b8cb5f0c0c9b5514dfece01c754d595e3e6d950725755fd8bdf29805b469bc93` |
| `http/create-409.json` | `http/create-409.json` | `0a5d6ce42dd345d3249f27037295dc4134e86c3f989330599f8f548b2d0f4240` |
| `http/admit-201.json` | `http/admit-201.json` | `fad47addb0d1713f617b4dcd63a85eff6d6344b3c0fa88bd88b619b9449b8663` |
| `http/admit-200-unfinished.json` | `http/admit-200-unfinished.json` | `fad47addb0d1713f617b4dcd63a85eff6d6344b3c0fa88bd88b619b9449b8663` |
| `http/admit-200-replay.json` | `http/admit-200-replay.json` | `940ffbe40f579e00728dcffc9a3f11600c3252ac5d2aa7d5b17d5c4c602edecf` |
| `http/admit-401.json` | `http/admit-401.json` | `7da7d3addac11e29f7d02959253f850220d95410ef52ca7ae0e29a5bfca098d5` |
| `http/admit-403.json` | `http/admit-403.json` | `8b1ce93864bae8b0bff275038b85d74cb20c1c0640e7e93aeb529c7ad720f3b5` |
| `sql/deposit-pending.out` | `sql/deposit-pending.out` | `195a333c7f811d2e31372493c469973df1ec282b0c4cfe4aa957291fb1bf0954` |
| `sql/deposit-depth-n1.out` | `sql/deposit-depth-n1.out` | `195a333c7f811d2e31372493c469973df1ec282b0c4cfe4aa957291fb1bf0954` |
| `sql/deposit-confirmed.out` | `sql/deposit-confirmed.out` | `c848fc40637ce40f6d60cd0904891b9f5ba4aa1f3b2bbc2915e0dcd7a78f42d3` |
| `sql/b-counts-before-replay.out` | `sql/b-counts-before-replay.out` | `fa33d90feaaaae75a4c554b8de73eb41cea6b6127dcc9dd7fd32f4f544696554` |
| `sql/b-counts-after-replay.out` | `sql/b-counts-after-replay.out` | `fa33d90feaaaae75a4c554b8de73eb41cea6b6127dcc9dd7fd32f4f544696554` |
| `sql/b-window-before.out` | `sql/b-window-before.out` | `36eff90164b291f7f040732e13dc20ef9e41a1ad7df352484c01276eecfc6ac4` |
| `sql/b-window-after.out` | `sql/b-window-after.out` | `36eff90164b291f7f040732e13dc20ef9e41a1ad7df352484c01276eecfc6ac4` |
| `sql/b-window-after-stop.out` | `sql/b-window-after-stop.out` | `36eff90164b291f7f040732e13dc20ef9e41a1ad7df352484c01276eecfc6ac4` |
| `sql/b-outbox-aggregate-versions.out` | `sql/b-outbox-aggregate-versions.out` | `1b067c1b3df9e22155806348709c64ed317f04d6b136f650d72382f3913aa5f0` |

## 4. 有意未复制的源 public 文件（仅本地追溯）

本包只收 `chain/*.json`、`chain/*.txt`、`http/*.json`、`sql/*.out` 四类白名单文件；以下 4 份
源 public 文件**按白名单未复制**，各自原因不同（均已逐一核对）：

| source（`public/` 之下） | sha256 | 未复制原因 |
| --- | --- | --- |
| `summary.txt` | `187684c4032cfa0719a0781a1cf204014dbfedd5130ce810bd9b999a73100bb6` | 含**证据根主机绝对路径** |
| `resources.txt` | `1c109415bb10e6343535ca57d2aebea6335575c8cb891c5b797c2382d16d747d` | 含**证据根主机绝对路径** |
| `run-metadata.txt` | `f23fbddcda4a0e7d3bc7d0b041122e59b031cd911d7c6d6f02f2e61c4e298f74` | 含宿主本地工具目录绝对路径（`/tmp/…`）；其脚本/fixture 哈希已摘入 `README.md` |
| `run-exit.txt` | `959d4087d3cc85b69abaf392c4677f9206697b8d97739c644016cf90618d19c1` | **不含任何绝对路径**；纯按白名单未收（退出/清理码已摘入 `README.md`） |

如需复核这 4 份内容，请回到本机 ignored 源根（上述哈希可先验完整性）。

## 5. 本目录内的派生（新增）文件

| 文件 | 性质 |
| --- | --- |
| `README.md` | 本包说明与读法（派生文本，不冒充原始记录） |
| `PROVENANCE.md` | 本文件（来源/规则/哈希对照，派生文本） |
| `SHA256SUMS` | 覆盖 22 个 raw + `README.md` + `PROVENANCE.md`，**不含自身**（避免自引用循环） |

除上述 3 个派生文件外，其余 22 个文件均为**未改动的原始字节复制**（sha256 已逐一核对相等）。
