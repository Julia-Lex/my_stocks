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

> 成交量单位为**股**——三市场统一(2026-07-09 起;东财/腾讯 A 股源返回"手",入库层 ×100 换算)。分钟线无法从日线反推,需另行获取——本项目暂只做日线。

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

- 范围:港股全列表 ~2,700 只;美股市值前 600(主要中概股如 BABA/PDD 已在其中)(快照式,只增不删)。
- **成交量单位为股**(三市场统一);货币按表隐含:`hk_*`=HKD,`us_*`=USD。
- 交易日历从指数日线派生(恒指 / 标普500);港股日历派生自新浪恒指数据,仅覆盖 2013-08 之后;增量补漏只依赖近期日历,不受影响。复权因子 = 东财 hfq 收盘 ÷ 原始收盘。

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
```

## 后续(第二期)

- 股本变动表、财务指标表(**必须含公告日 `ann_date`**,防未来函数)。
- 分钟线:从现在起每日增量归档;历史分钟数据为付费稀缺资源,确需再购。
- 数据量大后可加 TimescaleDB 扩展 / 迁移到独立主机(当前无损可迁)。
