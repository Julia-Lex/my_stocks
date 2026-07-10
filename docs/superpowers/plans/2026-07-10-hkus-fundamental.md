# 港股/美股基本面数据层 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 astock 库新增港股/美股基本面两层(JSONB 报表 + 指标宽表,含 ann_date 与 as-of 函数),富途 OpenAPI 主源、东财备源/公告日源,近 10 年 + 每日门控增量。

**Architecture:** 富途 `get_financials_statements`(types 1/2/3 → JSONB;type 4 关键指标 → 指标宽表)经全局 1.05s 节流的常驻网关连接拉取;东财指标接口专职回填 ann_date(美股 NOTICE_DATE 已确认,港股三级探测);镜像二期 `fin_asof` 语义与 ETL 基建(run_stock_todo/etl_progress/熔断)。

**Tech Stack:** Python 3.12(`.venv`)、futu-api 10.08.6808(FutuOpenD 网关 127.0.0.1:11111)、AKShare(东财备源)、psycopg2、PostgreSQL 14。

**Spec:** `docs/superpowers/specs/2026-07-10-hkus-fundamental-design.md`(富途主源修订版)

## Global Constraints

- 数据库 `ASTOCK_DB_USER=zhu` 免密;Python 用 `.venv/bin/python`。
- 历史范围 `report_date >= '2015-12-31'`(`common.FUND_START`)。
- **富途限频 30 次/30 秒**:全局节流锁,任意两次富途请求间隔 ≥1.05 秒(env `ASTOCK_FUTU_MIN_INTERVAL` 可覆盖);网关不可达时**明确报错退出**(不静默降级),错误信息含"请启动 FutuOpenD 并登录"。
- 源开关 `ASTOCK_INTL_FUND_SOURCE`(默认 `futu`;`em`=东财备源)。
- ann_date 宁缺勿假:拿不到就 NULL,as-of 函数只认非 NULL;绝不用报告期估算。
- 币种如实存(`currency` 列),不换算。
- 熔断 `max_consecutive_errors=15` 必传;东财调用走 `with_retry`。
- 全量执行避开 18:00-18:50 cron 窗口;港股全量为 ~6 小时过夜任务。
- 代码映射:`00700.HK` ↔ 富途 `HK.00700`,`AAPL.US` ↔ `US.AAPL`。
- **已实探事实**(2026-07-10,直接信任):`get_financials_statements(code, statement_type, financial_type, currency_code, next_key, num≤50)`;statement_type 整数 1利润/2资产负债/3现金流/4关键指标(1/2/4 已实证,3 首跑确认);响应 `{next_key, structure_list, report_list:[{date_time_str, fiscal_year, financial_type, period_text, currency_code, accounting_standards, item_list:[{field_id, display_name, data, yoy, qoq}]}]}`;深度港股 2001/美股 1979;type 4 的 item_list 含**节标题行**(如"每股指标",data=None)需跳过。

## File Structure

| 文件 | 动作 | 职责 |
| --- | --- | --- |
| `11_schema_fundamental_intl.sql` | Create | hk_/us_ 两表两函数 ×2 市场 |
| `common.py` | Modify | 富途连接/节流/报表拉取层 + 东财 ann_date 提供者 + em 备源 |
| `12_init_fundamental_intl.py` | Create | 全量初始化(报表+指标+ann_date 回填三阶段) |
| `13_fundamental_update_intl.py` | Create | 每日门控增量 |
| `README.md` | Modify | 三期章节 + FutuOpenD 运维说明 + cron |
| `requirements.txt` | Modify | + futu-api |

---

### Task 1: 港/美基本面 Schema

**Files:**
- Create: `11_schema_fundamental_intl.sql`

**Interfaces:**
- Produces: `hk_fin_statement` / `hk_fin_indicator` / `hk_fin_asof(p_stock,p_date)` / `hk_fin_asof_all(p_date)`,`us_` 同构四对象。

- [ ] **Step 1: 写 schema(两市场段完整写出,不得省略)**

港股段如下,美股段将 `hk_` 全替换为 `us_`(stock_code 列宽 VARCHAR(16),与 us_daily_price 一致):

```sql
-- =============================================================================
-- 港股/美股基本面(三期)· 富途主源。设计: docs/superpowers/specs/2026-07-10-hkus-fundamental-design.md
-- 两层架构(本期裁定):JSONB 报表 + 指标宽表;无股本/估值层。
-- ann_date 宁缺勿假:NULL 行在 as-of 中不可见。currency 如实存不换算。
-- 用法: psql -d astock -f 11_schema_fundamental_intl.sql
-- =============================================================================
CREATE TABLE IF NOT EXISTS hk_fin_statement (
    stock_code  VARCHAR(12) NOT NULL,
    report_date DATE        NOT NULL,
    stmt_type   VARCHAR(8)  NOT NULL,          -- income / balance / cashflow
    ann_date    DATE,
    currency    VARCHAR(8),                    -- 富途 currency_code(HKD/CNY/USD...)
    data        JSONB       NOT NULL,          -- {display_name: data},科目名保留源中文
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stock_code, report_date, stmt_type)
);
CREATE INDEX IF NOT EXISTS idx_hk_fin_statement_period ON hk_fin_statement (report_date, stock_code);

CREATE TABLE IF NOT EXISTS hk_fin_indicator (
    stock_code      VARCHAR(12)  NOT NULL,
    report_date     DATE         NOT NULL,
    ann_date        DATE,
    currency        VARCHAR(8),
    eps             NUMERIC(12,4),
    eps_diluted     NUMERIC(12,4),
    bps             NUMERIC(12,4),
    ocf_ps          NUMERIC(12,4),
    roe             NUMERIC(10,4),
    roa             NUMERIC(10,4),
    gross_margin    NUMERIC(10,4),
    net_margin      NUMERIC(10,4),
    debt_ratio      NUMERIC(10,4),
    current_ratio   NUMERIC(10,4),
    revenue         NUMERIC(20,2),
    revenue_yoy     NUMERIC(10,4),
    net_profit      NUMERIC(20,2),
    net_profit_yoy  NUMERIC(10,4),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stock_code, report_date)
);
CREATE INDEX IF NOT EXISTS idx_hk_fin_indicator_period ON hk_fin_indicator (report_date, stock_code);
CREATE INDEX IF NOT EXISTS idx_hk_fin_indicator_ann ON hk_fin_indicator (stock_code, ann_date);

CREATE OR REPLACE FUNCTION hk_fin_asof(p_stock VARCHAR, p_date DATE)
RETURNS SETOF hk_fin_indicator AS $$
    SELECT * FROM hk_fin_indicator
    WHERE stock_code = p_stock AND ann_date IS NOT NULL AND ann_date <= p_date
    ORDER BY report_date DESC LIMIT 1;
$$ LANGUAGE sql STABLE;

CREATE OR REPLACE FUNCTION hk_fin_asof_all(p_date DATE)
RETURNS SETOF hk_fin_indicator AS $$
    SELECT DISTINCT ON (stock_code) * FROM hk_fin_indicator
    WHERE ann_date IS NOT NULL AND ann_date <= p_date
    ORDER BY stock_code, report_date DESC;
$$ LANGUAGE sql STABLE;
```

- [ ] **Step 2: 应用并验证**

Run:
```bash
psql -d astock -f 11_schema_fundamental_intl.sql
psql -d astock -tAc "select proname from pg_proc where proname in ('hk_fin_asof','hk_fin_asof_all','us_fin_asof','us_fin_asof_all') order by 1"
```
Expected: 无 ERROR;四个函数名。

- [ ] **Step 3: as-of 边界单测(假数据三断言,同二期 F1 模式:公告前一日不可见/当日可见/NULL 不可见,测完删)**

- [ ] **Step 4: Commit** `git add 11_schema_fundamental_intl.sql && git commit -m "feat: add HK/US fundamental schema (statements + indicators + asof)"`

---

### Task 2: common.py 富途层 + 东财 ann_date 提供者

**Files:**
- Modify: `common.py`(末尾追加"基本面·港美(三期)"区)
- Modify: `requirements.txt`(+ `futu-api`)

**Interfaces:**
- Consumes: 既有 `with_retry/log/FUND_START/to_full_code`。
- Produces(签名固定,供 Task 3/4):
  - `INTL_FUND_SOURCE = os.getenv("ASTOCK_INTL_FUND_SOURCE", "futu")`
  - `futu_code(stock_code: str) -> str`(纯函数:`'00700.HK'→'HK.00700'`,`'AAPL.US'→'US.AAPL'`)
  - `fetch_intl_fund_statements(stock_code, stmt_type: str) -> pd.DataFrame`——stmt_type ∈ `income|balance|cashflow`;返回列 `report_date, currency, period_kind, data(dict)`;**只含 report_date ≥ FUND_START 且累计/年度口径的行**(单季行剔除,见 Step 1 探测);futu 主路径 + `_em` 备路径按开关分发
  - `fetch_intl_fund_indicator(stock_code) -> pd.DataFrame`——富途 type 4,列 = 指标宽表数值列 + `report_date, currency`;节标题行(data=None)跳过;`revenue_yoy/net_profit_yoy` 取对应 item 的 `yoy` 字段
  - `fetch_intl_ann_dates(market: str, symbol: str) -> pd.DataFrame`——列 `report_date, ann_date`;us=东财 `stock_financial_us_analysis_indicator_em` 的 NOTICE_DATE(年报+累计两种 indicator 取并集);hk=三级探测结果(见 Step 2)
  - `close_futu()`(进程收尾关连接;脚本 finally 调)

- [ ] **Step 1: 富途枚举与口径探测(网关已就绪,~10 个请求)**

写探测脚本确认并记入报告:
a) `statement_type=3` 返回现金流量表(科目名验证);
b) `financial_type` 字段的取值语义——对 00700 收集全部 report_list 的 `(period_text, financial_type)` 对,确认哪些值代表**累计/年度**(如 FY/H1/Q1累计)、哪些代表**单季**;同一 report_date 出现 FY 与 Q4 两行时的区分方式。**入库规则:只保留累计/年度口径**(与 A 股惯例一致),规则写成代码常量 + 注释;
c) type 4 关键指标的完整 display_name 清单(港股 00700 + 美股 AAPL 各一份)→ 据此写两市场的指标映射字典 `_FUTU_MAININDEX_MAP_HK/_US`(display_name → 指标列名;缺的列置 NULL 并记录);
d) 港股 ann_date 三级探测:①`stock_financial_hk_analysis_indicator_em("00700","报告期")` 打印**全部列名**找 NOTICE_DATE 类字段;②报表接口 `STD_REPORT_DATE` 与 REPORT_DATE 是否有滞后差(=疑似公告日);③若都落空,记录"港股 ann_date 不可得",Task 3 按 NULL 处理并触发 README 声明义务。

- [ ] **Step 2: 写代码**

富途连接管理(模块级):

```python
_futu_ctx = None
_futu_lock = threading.Lock()
_futu_last_req = [0.0]
FUTU_MIN_INTERVAL = float(os.getenv("ASTOCK_FUTU_MIN_INTERVAL", "1.05"))

def _futu_context():
    """懒建常驻富途连接;网关不可达时给出可操作报错。"""
    global _futu_ctx
    with _futu_lock:
        if _futu_ctx is None:
            try:
                from futu import OpenQuoteContext
                _futu_ctx = OpenQuoteContext(host="127.0.0.1", port=11111)
            except Exception as exc:
                raise ConnectionError(
                    "无法连接 FutuOpenD 网关(127.0.0.1:11111)。"
                    "请启动 FutuOpenD 并登录后重试。原始错误: %s" % exc) from exc
        return _futu_ctx

def _futu_call(fn_name, *args, **kwargs):
    """全局节流的富途调用:任意两次请求间隔 >= FUTU_MIN_INTERVAL;ret!=0 抛异常。"""
    ctx = _futu_context()
    with _futu_lock:
        wait = FUTU_MIN_INTERVAL - (time.monotonic() - _futu_last_req[0])
        if wait > 0:
            time.sleep(wait)
        _futu_last_req[0] = time.monotonic()
    ret, data = getattr(ctx, fn_name)(*args, **kwargs)
    if ret != 0:
        raise RuntimeError(f"futu {fn_name} ret={ret}: {data}")
    return data
```

分页拉取(报表与指标共用):

```python
_FUTU_STMT_TYPE = {"income": 1, "balance": 2, "cashflow": 3}

def _futu_fetch_reports(code: str, stype: int) -> list[dict]:
    """分页拉 report_list,翻到整页 report_date 都早于 FUND_START 即停。"""
    out, nk = [], None
    while True:
        d = _futu_call("get_financials_statements", code,
                       statement_type=stype, num=50, next_key=nk)
        rl = d.get("report_list", [])
        out.extend(rl)
        nk = d.get("next_key")
        if not nk or not rl:
            break
        oldest = min(r["date_time_str"] for r in rl)
        if oldest < FUND_START.isoformat():
            break
    return out
```

其余按 Interfaces 落全:report→DataFrame 转换(date_time_str→report_date date 型、item_list→{display_name: data} dict、按 Step 1 的累计口径规则过滤、FUND_START 过滤)、`fetch_intl_fund_indicator` 的映射与节标题跳过、`fetch_intl_ann_dates` 两市场实现、em 备源(`_fetch_intl_fund_statements_em`:东财长表 pivot,同 spec"原东财实探"节)、`close_futu()`。

- [ ] **Step 3: 验证(轻量:港/美各 1 股全链路)**

```bash
ASTOCK_DB_USER=zhu .venv/bin/python - <<'EOF'
import common as c
s = c.fetch_intl_fund_statements("00700.HK", "balance")
assert not s.empty and s["report_date"].min().year >= 2015 and isinstance(s.iloc[0]["data"], dict)
assert "资产合计" in s.iloc[0]["data"], list(s.iloc[0]["data"])[:5]
i = c.fetch_intl_fund_indicator("00700.HK")
assert not i.empty and {"report_date","eps","roe","currency"} <= set(i.columns)
u = c.fetch_intl_fund_statements("AAPL.US", "income")
assert not u.empty
a = c.fetch_intl_ann_dates("us", "AAPL")
assert not a.empty and a["ann_date"].notna().any()
h = c.fetch_intl_ann_dates("hk", "00700")   # 若三级探测落空,允许全 NULL,但函数不得抛异常
c.close_futu()
print("INTL_FUND_COMMON_OK, hk_ann 非空率:", h["ann_date"].notna().mean() if not h.empty else "N/A")
EOF
```
Expected: `INTL_FUND_COMMON_OK`;港股 ann 非空率如实记录。

- [ ] **Step 4: Commit** `git add common.py requirements.txt && git commit -m "feat: add Futu-primary intl fundamental fetch layer"`

---

### Task 3: 12_init_fundamental_intl.py 全量初始化

**Files:**
- Create: `12_init_fundamental_intl.py`

**Interfaces:**
- Consumes: Task 1 表/函数、Task 2 全部、既有 `run_stock_todo(…, max_consecutive_errors=15)/get_done_codes/mark_progress/upsert/get_conn`。
- Produces: CLI `--market hk|us [--workers N] [--limit N] [--reset] [--skip-ann]`;task 名 `init_fund_hk` / `init_fund_us`;ann_date 回填阶段 task `init_fundann_hk` / `_us`。

- [ ] **Step 1: 写脚本**

结构镜像 `09_init_fundamental.py`(读它作模板),两阶段:

```python
"""
12_init_fundamental_intl.py — 港/美基本面全量初始化(富途主源)。

阶段A 逐股(run_stock_todo):3 张报表 + 1 份关键指标 → {p}fin_statement / {p}fin_indicator
      每股 4 次富途分页调用(全局 1.05s 节流 ⇒ workers>1 仅重叠 DB 写入,建议 --workers 2)
阶段B ann_date 回填(东财,逐股 1 请求):fetch_intl_ann_dates → UPDATE 两表 ann_date
      (LEAST 合并语义同二期;港股若探测落空则整阶段跳过并 log.warning)
用法:
  python 12_init_fundamental_intl.py --market hk --workers 2      # 全量(港股 ~6h 过夜)
  python 12_init_fundamental_intl.py --market us --limit 10       # 试跑
"""
```

阶段A load_one:`fetch_intl_fund_statements` ×3 → upsert(`{p}fin_statement`,update_cols=[data,currency]);`fetch_intl_fund_indicator` → upsert 指标行;mark_progress(message=f"stmt={n1},ind={n2}")。
阶段B:对 done 股票逐个 `fetch_intl_ann_dates` → 参数化 UPDATE 两表(`ann_date = LEAST(coalesce(ann_date,新值), 新值)` 语义:非空不后移)。
脚本 finally: `c.close_futu()`。

- [ ] **Step 2: 试跑 港/美各 10 只**

```bash
ASTOCK_DB_USER=zhu .venv/bin/python 12_init_fundamental_intl.py --market hk --limit 10 --workers 2
ASTOCK_DB_USER=zhu .venv/bin/python 12_init_fundamental_intl.py --market us --limit 10 --workers 2
```
Expected(SQL 核对):hk_fin_statement ≈ 10 股 × 3 表 × 10-20 期;指标行有值;美股 ann_date 非空率 >80%;00700.HK(若在前 10)`data->>'资产合计'` 非空。

- [ ] **Step 3: 幂等复跑 + as-of 真数据验收(美股必做:取 AAPL 某期 ann_date 前后各查一次)**

- [ ] **Step 4: Commit** `git add 12_init_fundamental_intl.py && git commit -m "feat: add HK/US fundamental init loader (Futu primary)"`

---

### Task 4: 13_fundamental_update_intl.py 每日门控增量

**Files:**
- Create: `13_fundamental_update_intl.py`

**Interfaces:**
- Consumes: Task 2/3;二期 `10_fundamental_update.py` 的 `_due_for_check` 门控模式(读它作模板;经 importlib 或复制小函数,报告说明选择)。
- Produces: CLI `--market hk|us [--workers N] [--limit N] [--force]`;哨兵 task=`daily_fund_{market}`(stock_code=`_check`);per-stock task=`daily_fund_{market}_stk`。

- [ ] **Step 1: 写脚本**

逻辑:7 天门控(或 --force)→ 对全部股票重拉**最近 2 个报告期窗口**——富途指标(type 4 首页即近期,num=8 足够)与三表首页(num=8),upsert 覆盖;东财 ann_date 同步回填;港美披露季自适应不需要(半年/季度节奏低频,7 天门控即可)。熔断 15;finally close_futu()。

- [ ] **Step 2: 验证** `--market hk --limit 10 --force` 跑通 + 幂等复跑行数不变(除真实新披露)。

- [ ] **Step 3: Commit** `git add 13_fundamental_update_intl.py && git commit -m "feat: add HK/US fundamental daily updater (gated weekly)"`

---

### Task 5: README + 运维

**Files:**
- Modify: `README.md`

- [ ] **Step 1:** 三期章节:两层表速查(含"本期无股本/估值层"裁定与原因)、hk_fin_asof 用法示例(**实测后粘贴**,吸取二期 F5 教训:所有示例必须 psql 跑过)、FutuOpenD 运维说明(必须常驻+登录;网关挂了增量会报错退出,重启网关重跑即补)、已知限制(港股 ann_date 探测结果如实写;币种不换算;美股口径 US GAAP 等)、cron 行:
```cron
50 18 * * 1-5  cd /Users/zhu/own/my_stocks && ASTOCK_DB_USER=zhu ASTOCK_DB_PASSWORD='xxx' .venv/bin/python 13_fundamental_update_intl.py --market hk >> update_fund_hk.log 2>&1
55 18 * * 1-5  cd /Users/zhu/own/my_stocks && ASTOCK_DB_USER=zhu ASTOCK_DB_PASSWORD='xxx' .venv/bin/python 13_fundamental_update_intl.py --market us >> update_fund_us.log 2>&1
```
- [ ] **Step 2: Commit** `git add README.md && git commit -m "docs: add HK/US fundamental section (Futu ops notes)"`

---

### Task 6: 全量执行与验收

前置:Task 1-5 完成;FutuOpenD 在线;避开 18:00-18:55。

- [ ] **Step 1: 美股全量先行**(~550 股 × 4 调用 ≈ 45-75 分钟)`--market us --workers 2`
- [ ] **Step 2: 港股全量过夜**(~2,809 × 4 ≈ 6h)`--market hk --workers 2`(后台;熔断触发则冷却续跑)
- [ ] **Step 3: 失败重跑一轮 + ann_date 回填阶段确认**
- [ ] **Step 4: 验收查询**:两表行数/覆盖(港股报表预期 ≥15 万报告期行、美股 ≥3 万;指标各 ~3 万/~1 万);币种分布统计(CNY 计价港股占比);00700.HK ROE/毛利率对照腾讯公开财报(误差 <2%);AAPL 对照公开数据;美股 as-of 边界;幂等。
- [ ] **Step 5: 装 cron(需用户在场确认)+ 账本关账 + 合并推送。**

---

## Self-Review 结果

- **Spec 覆盖**:两层 schema+asof(T1)、富途主源/节流/网关报错(T2)、em 备源与开关(T2)、ann_date 分工与港股三级探测(T2 Step1d)、累计口径去重规则(T2 Step1b——spec 未细化、计划补上的关键数据语义)、初始化两阶段(T3)、门控增量(T4)、README 运维+示例实测义务(T5)、验收含币种统计(T6)。
- **占位符**:T2 Step2 末段与 T3/T4 主体为"按模板落全"式规格——模板文件(09/10)真实存在且模式已双审;探测依赖项(financial_type 语义、MainIndex 映射、港股 ann_date)均有明确探测步与落空处理路径,不构成留白。
- **类型一致性**:`fetch_intl_fund_statements(stock_code, stmt_type)->df[report_date,currency,period_kind,data]` T2 定义 T3/T4 消费;task 命名 `init_fund_{market}`/`daily_fund_{market}*` 与二期不冲突(二期用 `init_fund_stmt` 等);`futu_code`/`close_futu` 一致。
