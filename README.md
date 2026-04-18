# Covered Call 筛选器

这个项目把“适合长期持有并持续卖 covered call 的标的”变成了一套可重复运行的量化流程：

1. 刷新候选池的市场数据。
2. 用硬性阈值过滤掉不符合条件的标的。
3. 对通过筛选的标的做加权打分和排序。

## 统一运行环境

这个项目现在要求使用项目内自己的 `.venv`，不要混用系统 Python、conda 的 `python3`、或者别的外部虚拟环境。
原因很简单：如果解释器不统一，就会出现“这边有 `yfinance`，那边没有”“这边能导入，那里不能导入”的随机问题。

推荐只用下面这套命令：

```bash
cd /Users/patrick/Projects/covered_call
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

以后运行测试和界面，也都用这个环境里的解释器，不要再混 `python3`：

```bash
. .venv/bin/activate
python -m unittest discover -s tests -p 'test_*.py'
python screening_dashboard.py --port 8765
```

如果你想少敲一点命令，也可以直接用 `Makefile`：

```bash
make venv
make test
make dashboard
```

## Render 部署

这套项目现在支持部署到 Render 的免费 Web Service，推荐的形态是：

- 本地先用 IB 跑数据
- 把最新的 `out/*.csv` 和需要保留的 `out/workflow_runs/<最近一次运行>/...` 一起推上去
- Render 作为“云端安全模式界面”，不在云端直接连你的本地 IB

原因是 Render 上拿不到你本机的 IB Gateway，而且免费 Web 更适合做轻量筛选和结果展示，不适合长期在线跑 IB 抓数任务。

仓库根目录已经带了 [render.yaml](/Users/patrick/Projects/covered_call/render.yaml)，默认配置是：

- `plan: free`
- `runtime: python`
- `buildCommand: pip install -r requirements.txt`
- `startCommand: python screening_dashboard.py`
- `COVERED_CALL_READ_ONLY=true`

这意味着部署后的站点会：

- 自动绑定 Render 提供的 `0.0.0.0:$PORT`
- 默认进入云端安全模式
- 允许你切配置、看 Step 结果、看排序和历史产物
- 允许在云端运行 `Step 1`
- 允许在云端运行使用 `Yahoo` 数据源的 `Step 2`
- 允许在云端运行 `Step 3`
- 不允许在云端使用 `IB / hybrid` 跑 `Step 2`
- 不允许在云端保存 YAML

如果你要更新云端内容，正确流程是：

1. 本地用 IB 跑完你需要保留的 `Step 2/3` 结果
2. 把新的 `out/*.csv` 和对应 workflow 产物提交并推送
3. 让 Render 自动重部署
4. 云端如果只是想重做轻量候选池，或者用 Yahoo 重抓小池子的 `Step 2`，可以直接在线操作

## 这套筛选会计算什么

对每个代码，脚本会抓取并计算：

- `dividend_yield`：过去 12 个月分红总额除以最新收盘价。
- `beta_1y`：相对基准指数的 1 年 beta，默认基准是 `SPY`。
- `realized_vol_1y`：过去 1 年日收益率的年化已实现波动率。
- `total_return_3y`：过去 3 年价格总回报近似值。
- `max_drawdown_3y`：过去 3 年最大回撤。
- `selected option`：在目标 DTE 和目标 OTM 比例附近选出的 call 合约。
- `annualized_call_yield`：期权权利金除以现价后，再按到期天数年化。
- `selected_call_open_interest`、`selected_call_volume`、`selected_call_spread_pct`：用于判断期权流动性的简单指标。

## 默认量化筛选逻辑

默认配置对应的是一套偏“长期持有底仓 + 稳定卖 call”的思路：

- 股息率至少 `3%`
- 年化 call 收益率至少 `6%`
- 选中的 call 合约满足：
  `open interest >= 500`，或者当日 `volume >= 10`
- 期权点差不高于中间价的 `8%`
- beta 上限 `1.20`，下限给一个较宽的软边界 `-0.25`
- 已实现波动率在 `15%` 到 `45%` 之间
- 3 年总回报不低于 `-5%`
- 3 年最大回撤不高于 `45%`

通过硬过滤后，再按下面的权重打分：

- `25%`：股息率
- `30%`：年化 call 收益率
- `20%`：期权流动性
- `15%`：质量分，主要看 3 年回报和最大回撤
- `10%`：风险匹配度，主要看 beta 和波动率是否落在目标区间内

## 使用方法

现在正式保留两套配置：

- `etf_broad_config.yaml`：广覆盖 ETF 候选池
- `sp500_config.yaml`：S&P 500 成分股候选池

ETF 版本只重筛：

```bash
python3 covered_call_screener.py screen --config etf_broad_config.yaml
```

ETF 版本刷新并筛选：

```bash
python3 covered_call_screener.py run --config etf_broad_config.yaml
```

S&P 500 版本只重筛：

```bash
python3 covered_call_screener.py screen --config sp500_config.yaml
```

S&P 500 版本刷新并筛选：

```bash
python3 covered_call_screener.py run --config sp500_config.yaml
```

如果你会频繁改参数、反复筛选，可以直接开本地调参界面：

```bash
./.venv/bin/python screening_dashboard.py --port 8765
```

然后在浏览器打开 `http://127.0.0.1:8765`。这个界面支持：

- 下拉里只显示这两套正式配置
- 按 `候选池规则 -> 硬过滤规则 -> 排序规则` 三步走，这是现在唯一正式主路径
- 直接修改筛选阈值、打分权重、候选池路径和输出文件名
- Step 1 只用轻量基础数据缩候选池，Step 2 才对候选池抓更深的数据
- 实时查看进度日志、当前 ticker、错误数量和 ETA
- 把当前页面里的参数另存为新的 YAML 配置，方便做多套策略对比
- 每一步把中间结果保存到 `out/workflow_runs/...`
- Step 1 里直接显示 `CSV 文件说明`，方便区分 source universe / metrics / screen / workflow 过程文件
- Step 1 / 2 / 3 跑完后，结果直接显示在页面里的 `流程结果` 区，不需要再去外部打开 CSV
- 顶部不再提供一步式“全量刷新并筛选”按钮，避免一开始就对大池子抓最重的数据

如果你想先看规则说明，再去界面里调参数，可以直接读：

- [`SCREENING_RULES.md`](/Users/patrick/Projects/covered_call/SCREENING_RULES.md)

依赖说明：

- 如果你只是用已有的 `metrics.csv` 做 `只重筛`，不需要 `yfinance`
- 只有在执行 `refresh` 或 `刷新并筛选` 时，才需要 `yfinance`

## CSV 文件说明

界面里 Step 1 的 `CSV 文件说明` 会直接显示当前选中文件的用途，下面这张表是项目里最常见的几类：

- `finance_etfs.csv`：ETF 主数据库快照，广覆盖扩池时的原始源文件。
- `etf_broad_universe.csv`：ETF 正式候选池。
- `sp500_constituents.csv`：S&P 500 成分股快照，用来生成个股候选池。
- `out/etf_broad_metrics.csv`、`out/sp500_metrics.csv`：两套正式配置对应的计算结果文件。
- `out/etf_broad_screen.csv`、`out/sp500_screen.csv`：两套正式配置对应的筛选结果文件。
- `out/workflow_runs/.../step1_candidate_review.csv`：Step 1 候选池逐行审查结果。
- `out/workflow_runs/.../step1_candidate_universe.csv`：Step 1 保留下来的候选池。
- `out/workflow_runs/.../step2_candidate_metrics.csv`：Step 2 只对 Step 1 候选池抓取后的计算结果，含价格、分红、波动率和期权链。
- `out/workflow_runs/.../step2_hard_filter.csv`：Step 2 抓完候选池数据后做出的全量结果，含 `PASS / FAIL / ERROR`。
- `out/workflow_runs/.../step2_hard_filter_pass.csv`：Step 2 仅保留 PASS 的样本。
- `out/workflow_runs/.../step3_ranked.csv`：Step 3 最终排序榜单。

一个实用原则：

- `universe.csv` 负责“研究范围”
- `metrics.csv` 负责“每标的一行计算结果”
- `screen.csv` 负责“筛选结果”
- `workflow step*.csv` 负责“三步流程的中间过程”

在统一好的 `.venv` 里，把界面交给人手动点之前，建议先跑一遍自动化测试：

```bash
python3 -m unittest discover -s tests -p 'test_*.py'
```

当前这组测试至少会覆盖两条关键链路：

- 没有安装 `PyYAML` 时，项目自带的 YAML 回退解析器仍然能读写配置
- dashboard 在不访问外网的情况下，能读取配置、读取现有 artifacts，并触发一次本地 `screen` 任务

ETF 候选池来自 `finance_etfs.csv -> etf_broad_universe.csv` 这条扩池链：

```bash
python3 build_etf_universe.py
python3 covered_call_screener.py run --config etf_broad_config.yaml
```

## 怎么调整策略

修改 [`etf_broad_config.yaml`](/Users/patrick/Projects/covered_call/etf_broad_config.yaml) 或 [`sp500_config.yaml`](/Users/patrick/Projects/covered_call/sp500_config.yaml)：

- 提高 `min_dividend_yield`：更偏纯收息。
- 调整 `min_otm_pct / max_otm_pct / preferred_otm_pct`：控制 covered call 的 OTM 区间与偏好中心。
- 提高 `min_call_open_interest`、提高 `min_call_volume`、或者降低 `max_option_spread_pct`：更强调执行质量。
- 收紧 beta 或波动率区间：更偏稳健、防守型标的。
- 设置 `allowed_asset_types`：可以强制只保留 `ETF`，避免候选池数据源里混进少量非 ETF 代码。

当前正式保留的两套配置：

- [`etf_broad_config.yaml`](/Users/patrick/Projects/covered_call/etf_broad_config.yaml)：广覆盖 ETF 候选池
- [`sp500_config.yaml`](/Users/patrick/Projects/covered_call/sp500_config.yaml)：S&P 500 成分股候选池

## 输出文件

脚本会把结果写到 `out/` 目录，正式主输出是：

- `out/etf_broad_metrics.csv` / `out/etf_broad_screen.csv`
- `out/sp500_metrics.csv` / `out/sp500_screen.csv`

这样做的好处是：你改完阈值后，可以直接重新跑 `screen`，不需要每次都重新下载全部市场数据。

## 全池刷新建议

- `build_etf_universe.py`：负责先把 ETF 扩池，并用价格、交易天数、平均成交额做一层基础预筛。
- `covered_call_screener.py refresh`：负责抓每个代码的一行计算结果。现在默认支持“同日续跑”，会优先复用当天已经成功抓到的数据，只补抓缺失或报错的 ticker。
- `covered_call_screener.py screen`：只负责套筛选条件和打分，不会重新下载市场数据。

筛选结果现在会分成三类：

- `PASS`：数据完整，并且满足当前规则。
- `FAIL`：数据完整，但不满足当前规则。
- `ERROR`：这轮数据没有抓全，通常是限流或接口异常，适合下一次 `refresh` 继续补抓。
