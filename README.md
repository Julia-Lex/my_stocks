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

## 定时任务总表(生效快照 2026-07-11)

唯一事实源是系统 crontab(`crontab -l`);本表为其快照,换机重装照抄即可。

```cron
# 美股日线(腾讯):周二~六 09:00,拉前一交易日;指数行(新浪)周六常晚半天,自愈
0 9 * * 2-6   cd /Users/zhu/own/my_stocks && ASTOCK_DB_USER=zhu ASTOCK_INTL_SOURCE=tx .venv/bin/python 06_daily_update_intl.py --market us >> update_us.log 2>&1
# A股日线(东财):工作日 18:00
0 18 * * 1-5  cd /Users/zhu/own/my_stocks && ASTOCK_DB_USER=zhu .venv/bin/python 03_daily_update.py >> update.log 2>&1
# 港股日线(腾讯):18:05,当晚到位(日历腾讯代理探测)
5 18 * * 1-5  cd /Users/zhu/own/my_stocks && ASTOCK_DB_USER=zhu ASTOCK_INTL_SOURCE=tx .venv/bin/python 06_daily_update_intl.py --market hk >> update_hk.log 2>&1
# A股分钟线(通达信):18:30 ⚠️ 历史仅约3个月,长期断档不可补
30 18 * * 1-5 cd /Users/zhu/own/my_stocks && ASTOCK_DB_USER=zhu .venv/bin/python 07_minute_update.py >> update_minute.log 2>&1
# A股基本面:18:40(估值日更+披露季核查)
40 18 * * 1-5 cd /Users/zhu/own/my_stocks && ASTOCK_DB_USER=zhu .venv/bin/python 10_fundamental_update.py >> update_fund.log 2>&1
# 富途四连链:18:50 港基本面→美基本面→指数成分diff→港美板块diff ⚠️ 依赖 FutuOpenD 常驻
50 18 * * 1-5 cd /Users/zhu/own/my_stocks && ASTOCK_DB_USER=zhu .venv/bin/python 13_fundamental_update_intl.py --market hk >> update_fund_hk.log 2>&1 && ASTOCK_DB_USER=zhu .venv/bin/python 13_fundamental_update_intl.py --market us >> update_fund_us.log 2>&1 && ASTOCK_DB_USER=zhu .venv/bin/python 17_index_member_intl.py >> update_idxmember.log 2>&1 && ASTOCK_DB_USER=zhu .venv/bin/python 19_board_intl.py >> update_board_intl.log 2>&1
# 事件数据:19:00(龙虎榜近5日+披露季预告/快报;北向已终结默认跳过)
0 19 * * 1-5 cd /Users/zhu/own/my_stocks && ASTOCK_DB_USER=zhu .venv/bin/python 15_events_update.py >> update_events.log 2>&1
# 周全量备份:周日 03:00(本地3份轮换 + rsync NAS,脚本带未挂载守卫)
0 3 * * 0 /Users/zhu/backups/astock/backup_astock.sh >> /Users/zhu/backups/astock/backup.log 2>&1
```

注:A股板块日更(21_board_update.py)由板块会话负责,截至本快照未挂 cron。各章节内的 cron 示例以本表为准。

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
- 港股当日数据**当晚 18:05 到位**(日历带腾讯代理探测,2026-07-11 起;此前受新浪指数发布延迟拖到次日甚至周一)。

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
5. **指标表(`fin_indicator`)含约 6,100 只退市/新三板主体与 78 只 B 股;报表表(`fin_statement`)仅覆盖在市 5,530 只**:东财截面接口天然包含全部曾披露主体(这是防幸存者偏差的特性),而 `fin_statement` 逐股拉自新浪三大报表、只覆盖阶段2处理过的在市 A 股清单,两表股票域不完全一致。筛选在市 A 股时 `JOIN stock_basic b USING (stock_code) WHERE b.is_active`(或 `delist_date IS NULL`),回测时按 `daily_price` 股票域自然过滤。
6. **2026Q2(报告期 20260630)三张截面尚未披露完毕**:属正常,披露后跑 `09_init_fundamental.py --phase 1` 自动补。

### 每日定时(cron 示例)

```cron
40 18 * * 1-5  cd /Users/zhu/own/my_stocks && ASTOCK_DB_USER=zhu ASTOCK_DB_PASSWORD='xxx' .venv/bin/python 10_fundamental_update.py >> update_fund.log 2>&1
```

(cron 的实际安装由控制者在验收时执行,README 只记录。)

## 港股 / 美股基本面(三期)

设计:`docs/superpowers/specs/2026-07-10-hkus-fundamental-design.md`。**富途 OpenAPI 主源**
(`get_financials_statements`,历史深达港股 2001/美股 1979)+ 东财备源与美股公告日源。
本期裁定只做两层(报表 + 指标),无股本/估值层(免费源短板,备档待启)。

| 表 | 内容 | 覆盖 |
| --- | --- | --- |
| `hk_/us_fin_statement` | 三大报表全科目 JSONB(`{科目名: 金额}`)+ `currency` | 近 10 年,累计/年度口径 |
| `hk_/us_fin_indicator` | EPS/BPS/ROE/毛利率/负债率等 14 指标宽表 + `ann_date` | 同上 |
| `hk_/us_fin_asof(股, 日期)` / `*_fin_asof_all(日期)` | 防未来函数取数入口 | 只认非 NULL `ann_date` |

```sql
-- 美股 as-of:2026-06-20 时点可见的 ADBE 最新报告期(实测返回 2026-05-28 期,公告日 06-15)
SELECT report_date, ann_date, eps, roe FROM us_fin_asof('ADBE.US', '2026-06-20');

-- 港股指标直查(注意:港股无 as-of,见下方限制 1)
SELECT report_date, currency, eps, roe, debt_ratio
FROM hk_fin_indicator WHERE stock_code = '00001.HK' ORDER BY report_date DESC LIMIT 3;
```

### FutuOpenD 运维(硬依赖)

初始化与增量都要求 **FutuOpenD 网关常驻并已登录**(127.0.0.1:11111)。网关不可达时脚本
明确报错退出(提示"请启动 FutuOpenD"),不静默降级;重启网关后重跑即断点续传。富途限频
30 次/30 秒,代码内置全局 1.05 秒节流(`ASTOCK_FUTU_MIN_INTERVAL` 可调)。

```bash
python 12_init_fundamental_intl.py --market us --workers 2   # 美股全量 ~1.2h
python 12_init_fundamental_intl.py --market hk --workers 2   # 港股全量 ~6h(过夜)
python 13_fundamental_update_intl.py --market hk             # 增量(7 天门控,平日秒退)
```

### 已知限制

1. **港股 `ann_date` 不可得 → 港股 as-of 防未来函数不可用**:东财/富途的免费口径都不提供
   港股财报披露日(三级探测全部落空,见 spec)。`hk_fin_asof(_all)` 对港股永远返回空;
   回测请勿直接使用港股基本面因子,或自行按披露惯例(年报 3 月底/中报 8 月底)加保守滞后。
2. **美股 `ann_date` 护栏收紧至 200 天,历史覆盖率因此下降(宁缺勿假)**:东财"累计季报"/
   "年报"接口的 NOTICE_DATE 对老报告期存在系统性"被下一次同类披露覆盖"缺陷 —— 最终审查
   在全量入库数据上复核发现,旧的 `>400 天滞后` 护栏几乎不设防:已入库 `ann_date` 非空行里
   78% 恰好落在 380-401 天这个窄带,而美股 10-K/10-Q 法定披露滞后上限仅 ≤90 天,该窄带
   与真实披露窗口完全不重叠,确证是假滞后而非真实晚披露。护栏已收紧为 `>200 天` 剔除
   (仍偏保守,不保证拦住 200-380 天之间可能存在的更隐蔽假值);存量污染数据已清洗
   (`ann_date - report_date > 200` 的历史行全部置回 NULL,再用新护栏全量重跑阶段B 回填);
   US 两表清洗+重跑前后 ann_date 非空行数:`us_fin_indicator` 6582 → 1427,
   `us_fin_statement` 19678 → 4244。净效果:US as-of 历史截面现在只含可证真实的公告日,
   覆盖率显著低于清洗前
   ——这是主动取舍而非退步,清洗前的"高覆盖率"里含有大量假公告日,若拿来做 as-of 防未来
   过滤,反而会让本应不可见的旧报告期提前"可见",构成真实的未来函数风险。财季末日期与
   东财差 ±1 天,按(年,月)容差匹配(ADBE 等非日历财年已实测)。美股 Q1 单季公告日为
   已知覆盖缺口(NULL)。
3. 美股指标中 `bps`/`ocf_ps` 恒 NULL(富途 type4 与东财美股接口均无此科目);其余
   12 列由富途 + 东财互补填充。
4. `currency` 如实存不换算:部分港股财报以 CNY 计价(如腾讯),跨币种比较自行处理。

```cron
# 单进程串行链式(&&):hk/us 共享同一富途节流是进程级(ASTOCK_FUTU_MIN_INTERVAL 全局
# 变量作用于单个 Python 进程内),两个独立 cron 触发点各自起进程互不知晓对方节流状态,
# 理论上可并发抢占富途连接触发限频;链式合并为一行后 hk 跑完(平日秒退)才起 us 进程,
# 天然共享节流不会重叠请求。
50 18 * * 1-5  cd /Users/zhu/own/my_stocks && ASTOCK_DB_USER=zhu ASTOCK_DB_PASSWORD='xxx' .venv/bin/python 13_fundamental_update_intl.py --market hk >> update_fund_hk.log 2>&1 && ASTOCK_DB_USER=zhu ASTOCK_DB_PASSWORD='xxx' .venv/bin/python 13_fundamental_update_intl.py --market us >> update_fund_us.log 2>&1
```

## 事件数据(C 补全包)

设计:`docs/superpowers/specs/2026-07-11-events-pack-design.md`。四表 + 改码治理:

| 表 | 内容 | 防未来 |
| --- | --- | --- |
| `fin_forecast` | 业绩预告(预测指标/变动幅度/原因,2015Q4 起) | `ann_date`,**比正式财报早数周**(实测中际旭创早 59 天) |
| `fin_express` | 业绩快报(营收/净利/EPS 等,2015Q4 起) | `ann_date` |
| `lhb_detail` | 龙虎榜明细(2016 起,同日同股可多原因上榜) | 榜单本身即当日事件 |
| `nb_hold` | 北向个股持股序列(数量/市值/占比)——**历史数据集:2017~2024-08**(港交所披露改制后序列终结) | 日频(已终结) |
| `stock_alias` | 改码股映射(如 BGNE→ONC):**新码拉数、旧码入库**保历史连续 | — |

```sql
-- 预告领先性:同一报告期,预告公告日比正式财报公告日早多少天(实测可跑)
SELECT f.ann_date AS forecast_ann, i.ann_date AS report_ann,
       (i.ann_date - f.ann_date) AS lead_days
FROM fin_forecast f JOIN fin_indicator i USING (stock_code, report_date)
WHERE f.stock_code = '300308.SZ' AND f.ann_date IS NOT NULL AND i.ann_date IS NOT NULL
ORDER BY f.report_date DESC LIMIT 3;
```

```bash
psql -d astock -f 14_schema_events.sql
ASTOCK_DB_USER=zhu .venv/bin/python 14_init_events.py --part all   # 预告/快报分钟级;lhb ~1h;nb ~1.5h
ASTOCK_DB_USER=zhu .venv/bin/python 15_events_update.py            # 每日:lhb 近5日 + 披露季核查
```

已知限制:预告数值为区间/定性混合(`forecast_value` 可 NULL,完整语义看 `change_desc`);北向为历史持股序列,**止于 2024-08-16**(披露改制,序列终结;15 默认跳过,--with-nb 可探针);龙虎榜披露规则 2017 有修订,跨期统计注意口径;`stock_alias` 人工维护,15 脚本周报提示疑似改码股。

```cron
0 19 * * 1-5  cd /Users/zhu/own/my_stocks && ASTOCK_DB_USER=zhu ASTOCK_DB_PASSWORD='xxx' .venv/bin/python 15_events_update.py >> update_events.log 2>&1
```

## 港美指数成分(区间表)

`index_member`:HSI(93)/HSTECH(30)/HSCEI(50)富途快照 + 每日 diff;**SPX 含 1996 起真实历史区间**(1,255 段,TSLA 2020-12-21 纳入实证);NDX 现势(Wikipedia,源偶发超时次日自愈)。

```sql
-- 防偷看:任意历史日的指数成分
SELECT stock_code FROM index_member
WHERE index_code='SPX' AND in_date <= '2019-06-01' AND (out_date IS NULL OR out_date > '2019-06-01');
```

限制:恒指族历史成分免费不可得——`note='snapshot-open'` 行的 in_date 是**建档日**非真实纳入日,从建档起 diff 累积;A股成分见板块数据层(另建)。

## 港美板块(富途源,每市场分表)

`hk_board`/`us_board`(231/262 个,行业+概念)+ `hk_board_member`/`us_board_member`(成分区间,5,066/8,362 条在册)。A 股板块见上一章(东财体系,独立建设)。

```sql
-- 个股反查(实测:腾讯 → 数码解决方案服务/腾讯概念/人工智能...)
SELECT b.board_name, b.board_type FROM hk_board_member m
JOIN hk_board b USING (board_code)
WHERE m.stock_code = '00700.HK' AND m.out_date IS NULL;
```

限制:富途无板块历史成分——`note='snapshot-open'` 的 in_date 是建档日(2026-07-11),此后每日 diff 累积真实变更;`get_plate_stock` 有独立限频(10 次/30 秒),脚本已按 3.2s 节流。

## 板块数据(行业/概念,支持板块轮动)

设计:`docs/superpowers/specs/2026-07-10-board-rotation-design.md`。
**双源**(`ASTOCK_BOARD_SOURCE=em|futu`,默认 em,`board.source` 列区分,同库共存):

| 源 | 体系 | 日线起点 | 资金流 | 依赖/风险 |
|---|---|---|---|---|
| `futu`(当前主用,2026-07-11 全量已入库) | 行业 131 + 概念 792(代码 SH.LISTxxxx) | 2018-01-02 | ❌ 无 | 本地 OpenD,零封禁;历史K线耗月度额度(~918 标的/月),低频接口 10 次/30s |
| `em`(东财,解封后由守候脚本补) | 行业 86 + 概念 ~450(代码 BKxxxx) | ~2006 | ✅ 五档全历史 | 行情族限流/封禁(见"排错") |

### 四表速查

| 表 | 内容 | 主键 |
|---|---|---|
| `board` | 板块字典(代码/名称/类型/源/是否在市) | board_code |
| `board_member` | 成分**区间表**(valid_from/valid_to) | (board_code, stock_code, valid_from) |
| `board_daily` | 板块指数日线(volume 单位股) | (board_code, trade_date) |
| `board_fund_flow` | 板块资金流(主力/超大/大/中/小单净额与占比,元;仅 em 源) | (board_code, trade_date) |

### 初始化与增量

```bash
psql -U zhu -d astock -f 11_schema_board.sql
ASTOCK_BOARD_SOURCE=futu ASTOCK_DB_USER=zhu .venv/bin/python 12_init_board.py --workers 1  # 富途全量(限频串行,~55min)
ASTOCK_BOARD_SOURCE=futu ASTOCK_DB_USER=zhu .venv/bin/python 13_board_update.py            # 每日增量
# 东财口径(解封后):不带 ASTOCK_BOARD_SOURCE 或 =em,可用 --workers 3
```

### 成分区间表用法

```sql
-- 某日 d 某板块的成分(as-of)
SELECT stock_code FROM board_member
WHERE board_code = 'BK0475' AND valid_from <= :d AND (valid_to IS NULL OR valid_to > :d);

-- 某股当前所属全部板块
SELECT b.board_type, b.board_name FROM board_member m JOIN board b USING (board_code)
WHERE m.stock_code = '600519.SH' AND m.valid_to IS NULL;
```

### 已知限制

1. **成分历史 = 观测历史**:两源都只提供当前成分快照,`valid_from` 是本库首次观测到
   纳入的日期(首次建库日 2026-07-11 的存量成分尤其如此),不是真实纳入日;停跑期间
   的变动归到下一次观测日。**as-of 查询在观测起点之前的日期返回空是正确行为**。
2. **概念板块会生灭**:从源列表消失的板块 `is_active=false`,历史数据保留;富途列表
   还存在少量"僵尸残留"板块(列表有、行情系统不认,无指数无成分),已标退场。
3. **资金流历史深度依源**(em 源专属):`lmt=0` 拉全东财可用历史,各板块起点不一。
4. 板块指数无复权概念,`board_daily` 即原始点位;板块内**无权重数据**,需要加权口径
   时用成分股流通市值自行近似(交叉校验显示富途板块指数即市值加权,偏差 ≤0.03pp)。
5. **富途接口坑位备忘**:板块代码 `request_history_kline` 的 `end=None` 返回 0 行
   (必须显式传日期);`get_plate_stock` 与历史K线同为 10 次/30s 低频限频(各配独立
   3.1s 节流时钟);OpenQuoteContext 有非守护线程,脚本收尾必须 `close_futu()`。

### 每日定时(cron 示例,合并主分支后安装)

```cron
# 19:30 起跑:避开港美股板块链的 18:50-19:20 富途限频窗口(账号级共享,跨会话约定)
30 19 * * 1-5  cd /Users/zhu/own/my_stocks && ASTOCK_BOARD_SOURCE=futu ASTOCK_DB_USER=zhu .venv/bin/python 13_board_update.py >> update_board.log 2>&1
```

## 后续(第二期)

- 数据量大后可加 TimescaleDB 扩展 / 迁移到独立主机(当前无损可迁)。
