# A股全量历史数据库

个人研究/回测用的 A 股历史行情库。数据来自免费接口(AKShare),落地到本机 PostgreSQL 后即为自有数据资产,自用合规。

## 设计要点

| 决策 | 说明 |
| --- | --- |
| 存原始价 + 后复权因子 | `daily_price` 只存**不复权**价,`adj_factor` 单独存后复权因子。前/后复权价通过视图动态算,**绝不落地前复权价**(除权后会失效)。 |
| 日线按年分区 | `daily_price` 按 `trade_date` 年度 RANGE 分区(1990~2030),主键 `(stock_code, trade_date)`,另有 `(trade_date, stock_code)` 反向索引支持截面查询。 |
| 代码带后缀 | `000001.SZ` / `600000.SH` / `830799.BJ`。价格用 `NUMERIC` 不用 `FLOAT`。 |
| 周线/月线派生 | 不单独拉,从后复权日线用**物化视图**聚合(单一事实来源)。 |
| 退市股保留 | `stock_basic` 含 `delist_date`,防幸存者偏差(注意免费源退市股覆盖不全)。 |
| 断点续传 | `etl_progress` 记录每只完成情况,中断重跑自动跳过。 |

> 成交量单位为**股**——三市场统一(2026-07-09 起;东财/腾讯 A 股源返回"手",入库层 ×100 换算)。分钟线存于 `minute_price`(1 分钟粒度,按月分区,通达信源,ETL 待建)。

## 目录

| 文件 | 作用 |
| --- | --- |
| `01_schema.sql` | 建表 + 分区 + 复权视图 + 周线/月线物化视图 |
| `common.py` | 数据库连接、AKShare 拉取、列名映射、upsert、进度 |
| `02_init_load.py` | 全量历史初始化(断点续传,首次约 2~4 小时) |
| `03_daily_update.py` | 每日增量 + 自动补漏 |
| `04_schema_hk_us.sql` | 港股/美股建表(方案 B 分表,前缀 hk_/us_) |
| `05_init_load_intl.py` | 港/美全量初始化(`--market hk|us`) |
| `06_daily_update_intl.py` | 港/美每日增量 + 补漏 |
| `07_minute_update.py` | A股 1 分钟线回填/增量(通达信源,`--recon` 对账) |
| `08_schema_fundamental.sql` | 基本面四层表建表 + 视图(报表/指标/股本/估值) |
| `09_init_fundamental.py` | 基本面全量初始化 + 分阶段回填 |
| `10_fundamental_update.py` | 基本面增量更新(公告日 ann_date 驱动) |
| `requirements.txt` | Python 依赖 |

## 快速开始(在你本机执行)

这些脚本连的是**本机** PostgreSQL,需要在你自己的电脑上跑(云端连不到你的 5432)。

```bash
# 1) 拉取本分支
git fetch origin claude/astock-database-setup-x2w73b
git checkout claude/astock-database-setup-x2w73b

# 2) 装依赖
pip install -r requirements.txt

# 3) 建库 + 建表
createdb astock
psql -d astock -f 01_schema.sql

# 4) 配置数据库连接(推荐用环境变量)
export ASTOCK_DB_PASSWORD='你的密码'
# 如需改 host/port/user/dbname,见 common.py 顶部 DB_CONFIG,或用同名环境变量

# 5) 先小规模试跑(前 50 只),确认接口正常
ASTOCK_DB_USER=zhu .venv/bin/python 02_init_load.py --limit 50

# 6) 全量初始化(可 Ctrl-C 中断,重跑自动续)
ASTOCK_DB_USER=zhu .venv/bin/python 02_init_load.py

# 7) 之后每日增量(收盘后)
ASTOCK_DB_USER=zhu .venv/bin/python 03_daily_update.py
```

### 每日定时(cron 示例)

```cron
# 工作日 18:00 收盘后增量更新
0 18 * * 1-5  cd /path/to/my_stocks && ASTOCK_DB_USER=zhu ASTOCK_DB_PASSWORD='xxx' .venv/bin/python 03_daily_update.py >> update.log 2>&1
```

## 常用查询

```sql
-- 某只股票的后复权日线
SELECT * FROM daily_price_hfq WHERE stock_code = '000001.SZ' ORDER BY trade_date;

-- 某只股票的前复权日线
SELECT * FROM daily_price_qfq WHERE stock_code = '600000.SH' ORDER BY trade_date;

-- 某一天全市场截面(走反向索引)
SELECT stock_code, close, pct_chg FROM daily_price WHERE trade_date = '2026-07-08';

-- 全市场统计:某日成交总额 / 涨跌家数 / 涨停数
SELECT trade_date,
       sum(amount)                            AS total_amount,
       count(*) FILTER (WHERE pct_chg > 0)    AS up_cnt,
       count(*) FILTER (WHERE pct_chg < 0)    AS down_cnt,
       count(*) FILTER (WHERE pct_chg >= 9.9) AS limit_up_cnt
FROM daily_price WHERE trade_date = '2026-07-08' GROUP BY trade_date;

-- 后复权周线
SELECT * FROM weekly_price_hfq WHERE stock_code = '000001.SZ' ORDER BY period_start;
```

## 排错

- **接口列名报错**:AKShare 会随数据源改列名。先 `pip install -U akshare`,仍不行就改 `common.py` 里的 `RENAME_HIST` / `RENAME_INDEX` 映射。
- **后复权因子拿不到**:`fetch_hfq_factor` 会自动退化为「后复权价 ÷ 原始价」现算,一般无需干预。
- **限流/超时**:所有接口调用已带指数退避重试(2s→4s→8s→16s)。大规模跑建议错峰。
- **东财单 IP 风控**:东财对单个 IP 有累计流量限制,约 5 并发 × 2 小时后会触发连接级封禁(RemoteDisconnected 异常),冷却期可能超过 30 分钟。全量初始化建议使用 ≤3 并发、分市场错峰拉取;若被风控封禁,建议等待解封后继续跑,断点续传机制保证零数据浪费。

## 港股 / 美股

方案 B 分市场独立表(设计:`docs/superpowers/specs/2026-07-09-hk-us-daily-db-design.md`):

- 范围:港股全列表 ~2,700 只;美股 = 标普500 + 纳指100 + 中概精选,约 550 只(清单来自 GitHub 数据集 + Wikipedia + 内置精选)(快照式,只增不删)。
- **成交量单位为股**(三市场统一);货币按表隐含:`hk_*`=HKD,`us_*`=USD。
- 交易日历从指数日线派生(恒指 / 标普500);港股日历派生自新浪恒指数据,仅覆盖 2013-08 之后;增量补漏只依赖近期日历,不受影响。复权因子来自新浪(港股 hfq 因子;美股 qfq 因子直乘,重锚定到最早日=1,三市场锚点统一);东财现算路径保留为备用(ASTOCK_INTL_SOURCE=em)。

### 数据源与已知限制

- 港/美日线原始价来自腾讯 K 线(ASTOCK_INTL_SOURCE 开关,默认 tx;em=东财备用);A 股默认东财,ASTOCK_ASHARE_SOURCE=tx 可切腾讯应急(成交额/换手率会缺失)。
- 港股清单来自 HKEX 官方证券列表(仅 Equity)。
- **hk_/us_ 表的 amount(成交额)与 turnover(换手率)恒为 NULL**(腾讯源不提供),周/月线的 sum(amount) 也为 NULL。
- 美股历史深度因股而异(部分 1984 起,多数 2007 起;CBOE.US 无数据——腾讯无 BZX 源)。
- pct_chg 口径:A 股为东财除权口径;港/美为裸收盘环比(拆股日会出现大幅值,复权分析请用 hfq/qfq 视图价格自行计算)。
- 港股当日数据通常次日增量运行时补齐(日历派生自新浪指数,发布有延迟)。

```bash
psql -d astock -f 04_schema_hk_us.sql
ASTOCK_DB_USER=zhu .venv/bin/python 05_init_load_intl.py --market hk --limit 20   # 试跑
ASTOCK_DB_USER=zhu .venv/bin/python 05_init_load_intl.py --market hk --workers 3  # 港股全量 ~1.5h
ASTOCK_DB_USER=zhu .venv/bin/python 05_init_load_intl.py --market us --workers 3  # 美股 ~20min
```

每日定时(北京时间;美股收盘为北京次日凌晨,早上拉前一交易日):

```cron
0 18 * * 1-5  cd /path/to/my_stocks && ASTOCK_DB_USER=zhu ASTOCK_DB_PASSWORD='xxx' .venv/bin/python 06_daily_update_intl.py --market hk >> update_hk.log 2>&1
0 9  * * 2-6  cd /path/to/my_stocks && ASTOCK_DB_USER=zhu ASTOCK_DB_PASSWORD='xxx' .venv/bin/python 06_daily_update_intl.py --market us >> update_us.log 2>&1
# 港股当日 K 线一般次日补齐,属正常
```

## 分钟线(A股)

数据源为通达信行情服务器(pytdx):免费、可回溯约 90-140 个交易日的 1 分钟线,
成交量原生为股。节点 IP 会漂移,脚本启动时自动探测可用节点、断线换节点重连。

```bash
ASTOCK_DB_USER=zhu .venv/bin/python 07_minute_update.py --limit 5    # 试跑
ASTOCK_DB_USER=zhu .venv/bin/python 07_minute_update.py --workers 3  # 全市场(首次 ~1-2h)
ASTOCK_DB_USER=zhu .venv/bin/python 07_minute_update.py --recon 20   # 抽样对账
```

- 增量幂等:按库内 max(trade_time) 续拉,可随时中断重跑;建议 cron 收盘后每日一跑(1 分钟历史只保留约 3 个月,断档不可补)。
- 北交所暂不支持(通达信标准行情接口无 BJ);指数分钟线未做。
- 已知单位陷阱:**腾讯行情对科创板(688/689)成交量返回股、主板/创业板返回手**,`_fetch_daily_tx` 已按板块区分换算——接新数据源时务必先做"分钟/日线对账"验证单位。

## 基本面数据(二期)

### 四层表速查

| 层级 | 表名 | 用途 | 数据量 | 特点 |
| --- | --- | --- | --- | --- |
| **报表层** | `fin_statement` | 资产负债表/利润表/现金流量表(资产/负债/权益/收入/利润等 30+ 科目) | 60.1 万行 / 5,530 只 | 季/年报,公告日 `ann_date` 回溯 |
| **指标层** | `fin_indicator` | 财务比率指标(ROE/ROA/负债率/流动比等 40+ 指标) | 33 万行 | 公告日 100% 覆盖,无调整历史 |
| **股本层** | `share_capital` | 股本结构(总股本/流通股/A/B/H 股等)时间序列 | 16.1 万行 | 全历史翻页版(每次配股/转增/送股后重新披露) |
| **估值层** | `daily_valuation` | 日级估值指标(PE/PB/PS/价格/市值等) | 920 万行(2018 起) | 交易日级,源起点所限无 2018 前数据 |

### 初始化(分阶段)

```bash
# 1) 建表
psql -d astock -f 08_schema_fundamental.sql

# 2) 四阶段顺序全跑(默认等价 --phase all,断点续传,首次 ~1-2 小时)
#    阶段1=截面骨干(fin_indicator) 阶段2=全科目报表(fin_statement)
#    阶段3=股本+估值(share_capital/daily_valuation) 阶段4=派生列+ann_date 回填(纯本地)
ASTOCK_DB_USER=zhu .venv/bin/python 09_init_fundamental.py

# 3) 也可分阶段跑(--phase 1|2|3|4|all;阶段 2/3 支持 --workers 并发、--limit 试跑)
ASTOCK_DB_USER=zhu .venv/bin/python 09_init_fundamental.py --phase 2 --workers 3
ASTOCK_DB_USER=zhu .venv/bin/python 09_init_fundamental.py --phase 3 --workers 3
```

### fin_asof / fin_asof_all 用法

财务数据是**报告期维度**,需用 `fin_asof` / `fin_asof_all` 函数将其对齐到任意交易日。`ann_date`(公告日期) ≥ 报告期后才能使用(防止未来函数);用法示例:

```sql
-- 单只股票:查 2026-05-20 时已公告的最新一期指标(通常为 2026Q1)
SELECT stock_code, report_date, ann_date, net_profit, roe
  FROM fin_asof('600519.SH', '2026-05-20'::date);

-- 截面:查 2026-06-30 全市场每只股票当时可见的最新一期,JOIN stock_basic 只留在市 A 股
SELECT f.stock_code, b.name, f.report_date, f.ann_date, f.total_assets
  FROM fin_asof_all('2026-06-30'::date) AS f
  JOIN stock_basic AS b USING (stock_code)
 WHERE b.is_active            -- 或 b.delist_date IS NULL
 ORDER BY f.total_assets DESC NULLS LAST
 LIMIT 10;
```

### 已知限制

1. **无修订历史**:表中数据对应公告时刻的披露版本,后续如被修正/重述不回溯更新。对标基金年报/企业年报,公告日确定后不再改。
2. **`ann_date` 防提前看,不防事后修正**:`fin_asof` 按公告日 ≥ 查询日来筛选,防止提前获知;但若披露方后续修正数据,仓库不会回溯更新。
3. **`ann_date IS NULL` 的行在 as-of 查询里不可见**:某些历史较久的 A 股或停牌期间的数据公告日缺失时,该行被排除;业务决策时应理解这是源数据限制,非库表设计缺陷。
4. **估值层 `dv_ratio`(股息率)与 `ps_ttm` 恒为 NULL**:东财 `stock_value_em` 接口不提供这两项指标,需从其他因子库或财务引擎另行计算。
5. **指标/报表表含约 6,100 只退市股与新三板/老三板主体、78 只 B 股**:东财截面接口天然包含全部曾披露主体(这是防幸存者偏差的特性);筛选在市 A 股时 `JOIN stock_basic b USING (stock_code) WHERE b.is_active`(或 `delist_date IS NULL`),回测时按 `daily_price` 股票域自然过滤。
6. **2026Q2(报告期 20260630)三张截面尚未披露完毕**:属正常,披露后跑 `09_init_fundamental.py --phase 1` 自动补。

### 每日定时(cron 示例)

```cron
40 18 * * 1-5  cd /Users/zhu/own/my_stocks && ASTOCK_DB_USER=zhu ASTOCK_DB_PASSWORD='xxx' .venv/bin/python 10_fundamental_update.py >> update_fund.log 2>&1
```

(cron 的实际安装由控制者在验收时执行,README 只记录。)

## 后续(第二期)

- 数据量大后可加 TimescaleDB 扩展 / 迁移到独立主机(当前无损可迁)。
