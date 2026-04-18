# 三层筛选规则

这套 covered call 筛选分成三层，每一层只做一件事：

1. 候选池规则：定义研究范围
2. 硬过滤规则：剔除明显不合格
3. 排序规则：在合格样本里找最优

这样做的原因很简单：

- 候选池如果一直变，后面的分数没有可比性
- 硬过滤如果和排序混在一起，会把“必须剔除”和“只是分数靠后”混成一团
- 排序只应该在已经合格的样本里比较谁更好，不应该再改变研究范围

当前正式保留两套运行配置：

- `etf_broad_config.yaml`
- `sp500_config.yaml`

## v1 规则

这份 v1 先按当前项目已经支持并已经接进界面的版本来定义。

### 第 1 层：候选池规则

目标：先定义值得研究的范围，不讨论优劣。

建议的默认口径：

- 市场范围：`股票市场 ETF + 商品 ETF`
- 交易货币：`USD`
- 上市范围：`美国主交易所`
- 资产类型：`ETF`
- 基础体量：`size_metric >= 全池中位数`
- 基础流动性：`avg_dollar_volume >= 全池中位数`
- 明确成交额门槛：`avg_dollar_volume >= 1亿美元/日`

这一步的典型问题是：

- 它不是最终答案
- 它只是先把“太冷、太小、范围不对”的样本排掉

### 第 2 层：硬过滤规则

目标：剔除明显不适合长期持有并卖 covered call 的品种。

当前项目里的默认硬过滤字段：

- `size_metric >= min_size_metric`
- `dividend_yield >= min_dividend_yield`
- `annualized_call_yield >= min_call_annualized_yield`
- `selected_call_spread_pct <= max_option_spread_pct`
- `selected_call_open_interest >= min_call_open_interest`
  或者 `selected_call_volume >= min_call_volume`
- `beta_1y` 落在 `[min_beta_1y, max_beta_1y]`
- `realized_vol_1y` 落在 `[min_realized_vol_1y, max_realized_vol_1y]`
- `total_return_3y >= min_total_return_3y`
- `max_drawdown_3y <= max_drawdown_3y`
- `asset_type` 落在 `allowed_asset_types`

第 2 层的原则是：

- 不过线就剔除
- 不在这里讨论“谁更好”
- 它是资格审查，不是打分排位

### 第 3 层：排序规则

目标：只在第 2 层已经合格的样本里，找更优的品种。

当前项目里的排序维度：

- `dividend_yield`
- `annualized_call_yield`
- `liquidity`
- `quality`
- `risk_fit`

默认权重：

- `25%`：`dividend_yield`
- `30%`：`call_yield`
- `20%`：`liquidity`
- `15%`：`quality`
- `10%`：`risk_fit`

第 3 层的原则是：

- 不改变候选池范围
- 不再做资格审查
- 只回答“合格样本里谁更优”

## 界面流程

现在 dashboard 已经按这三层拆成向导流程：

1. 先运行 `Step 1 候选池规则`
2. 只有 Step 1 完成后，`Step 2 硬过滤规则` 才开放
3. 只有 Step 2 完成后，`Step 3 排序规则` 才开放

## 每一步会保存什么

每次从第 1 步开始，都会创建一个新的工作目录：

- `out/workflow_runs/<timestamp>-<config_name>/`

### Step 1 产物

- `step1_candidate_review.csv`
  说明：全体候选的审查表，包含 `candidate_status` 和 `candidate_fail_reasons`
- `step1_candidate_universe.csv`
  说明：保留下来的候选池
- `step1_candidate_tickers.txt`
  说明：候选池 ticker 列表
- `step1_candidate_settings.json`
  说明：第 1 步实际使用的候选池规则和中位数

### Step 2 产物

- `step2_candidate_metrics.csv`
  说明：只对 Step 1 候选池抓下来的市场数据与期权链结果
- `step2_hard_filter.csv`
  说明：第 2 步完整结果，包含 `PASS / FAIL / ERROR`
- `step2_hard_filter_pass.csv`
  说明：第 2 步通过的样本
- `step2_hard_filter_config.json`
  说明：第 2 步实际使用的硬过滤参数

### Step 3 产物

- `step3_ranked.csv`
  说明：第 2 步通过样本的最终排序结果
- `step3_scoring_config.json`
  说明：第 3 步实际使用的打分参数

### 全流程状态

- `workflow_state.json`
  说明：三步流程当前的汇总状态，dashboard 也用它来回显每一步的结果
