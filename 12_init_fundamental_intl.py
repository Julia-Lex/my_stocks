"""
12_init_fundamental_intl.py — 港/美基本面全量初始化(富途主源)。

阶段A 逐股(run_stock_todo):3 张报表 + 1 份关键指标 → {p}fin_statement / {p}fin_indicator
      每股 4 次富途分页调用(全局 1.05s 节流 ⇒ workers>1 仅重叠 DB 写入,建议 --workers 2)
阶段B ann_date 回填(东财,逐股 1-2 请求):fetch_intl_ann_dates 语义 → UPDATE 两表 ann_date
      (LEAST 合并语义同二期;港股 ann_date 三级探测已知全部落空 —— 见 task-2-report,整阶段
      跳过并 log.warning,不做无意义的逐股请求)。

美股专属三条裁定(H2 审查发现,brief 之外由控制者拍板,见 task-3-report):
  1. ann_date 护栏:东财"累计季报"/"年报"接口的 NOTICE_DATE 对老报告期存在系统性"被最新一次
     同类披露覆盖"的缺陷(实测 AAPL 历史行大量出现 ~390-800 天的假滞后,仅最新一期 NOTICE_DATE
     可信)。回填前丢弃 (ann_date - report_date) > 400 天的行,丢弃计数打 log(方向保守,不保证
     拦住全部,见下方 _ANN_GUARD_DAYS 注释与 task-3-report 疑虑)。
  2. 容差 join:美股财年/财季期末日,富途 report_date 与东财 REPORT_DATE 存在 ±1~2 天系统偏差
     (如 AAPL 09-27 vs 09-28),按 (年,月) 而非精确日期匹配 report_date 回填 ann_date(美股财季末
     聚集月末,同月同一报表类型下唯一)。港股 ann_date 恒 NULL,精确日期 join 即可(反正不会命中)。
  3. 指标增补:美股富途 type4 关键指标只有 5 个 TTM 比率列(gross_margin/net_margin/roe/roa/
     current_ratio),阶段B 顺手从同一份东财指标响应里提取可映射的 EPS/BPS/ROE/营收/净利/同比等
     列(探测得到的实际列名见 _EM_US_IND_MAP),UPDATE 进 us_fin_indicator 对应 NULL 列 ——
     只填当前为 NULL 的列,绝不覆盖富途已写入的值。

用法:
  python 12_init_fundamental_intl.py --market hk --workers 2      # 全量(港股 ~6h 过夜)
  python 12_init_fundamental_intl.py --market us --limit 10       # 试跑
  python 12_init_fundamental_intl.py --market us --skip-ann       # 只跑阶段A
  python 12_init_fundamental_intl.py --market hk --reset          # 清空该市场进度重来
"""

from __future__ import annotations

import argparse
import json
import sys
import threading

import pandas as pd
import psycopg2.extras

import common as c

# 各列可表示上限,与 11_schema_fundamental_intl.sql 的 NUMERIC 精度一一对应
# (同 09_init_fundamental.py 的越界防护套路:超界值置 NULL 而非让整批 upsert 报
# numeric field overflow 全军覆没)。
_NUM_LIMIT = {
    "eps": 1e8, "eps_diluted": 1e8, "bps": 1e8, "ocf_ps": 1e8,             # NUMERIC(12,4)
    "roe": 1e6, "roa": 1e6, "gross_margin": 1e6, "net_margin": 1e6,
    "debt_ratio": 1e6, "current_ratio": 1e6,
    "revenue_yoy": 1e6, "net_profit_yoy": 1e6,                            # NUMERIC(10,4)
    "revenue": 1e18, "net_profit": 1e18,                                  # NUMERIC(20,2)
}


def _clean_num(v, col: str):
    """None/NaN → None;越界 → None(调用方负责累计计数打 log)。返回 (value, was_clipped)。"""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None, False
    v = float(v)
    if abs(v) >= _NUM_LIMIT.get(col, 1e18):
        return None, True
    return v, False


def _to_jsonb(data: dict) -> str:
    """report dict → JSON 字符串;NaN 置 None,非常规类型(Decimal 等)兜底 str()。"""
    clean = {k: (None if (isinstance(v, float) and pd.isna(v)) else v) for k, v in data.items()}
    return json.dumps(clean, ensure_ascii=False, default=str)


def _upsert_indicator_keep_supplement(conn, table: str, cols: list[str], rows: list[tuple]) -> int:
    """{p}fin_indicator 专用 upsert:值列用 COALESCE(EXCLUDED.col, {table}.col) 而非直接覆盖。

    动机(幂等复跑实测发现的真实 bug,2026-07-10):阶段A 若在阶段B 之后重跑(--reset 全量
    重来 / 单股熔断重试等场景都会触发),富途主源对 US 的 eps/revenue/net_profit/debt_ratio
    等列本就恒为 NULL(见 task-2-report——US type4 无这些科目),若用普通 `col = EXCLUDED.col`
    覆盖式 upsert,阶段A 重跑会把阶段B 用东财补齐的这些列重新写回 NULL,悄悄丢弃已完成的
    指标增补(裁定3)。COALESCE(新值, 旧值) 语义:富途给出非 NULL 新值时用新值(主源优先、
    支持源端更正),给不出时保留数据库里已有的值(不管那是富途早先写的还是东财补的)——
    在 ON CONFLICT DO UPDATE 语境下 `{table}.col` 指向冲突前的已有行,是合法引用。
    """
    if not rows:
        return 0
    value_cols = [col for col in cols if col not in ("stock_code", "report_date")]
    set_parts = [f"{col} = COALESCE(EXCLUDED.{col}, {table}.{col})" for col in value_cols]
    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
        f"ON CONFLICT (stock_code, report_date) DO UPDATE SET {', '.join(set_parts)}"
    )
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
    conn.commit()
    return len(rows)


# ===========================================================================
# 阶段A:逐股报表(3 张,JSONB)+ 关键指标(富途)
# ===========================================================================
def make_loader_a(market: str, task: str):
    p = c.MARKETS[market]["prefix"]

    def load_one(conn, r):
        n_stmt = 0
        for st in ("income", "balance", "cashflow"):
            df = c.fetch_intl_fund_statements(r.stock_code, st)
            if df.empty:
                continue
            rows = [
                (r.stock_code, row.report_date, st, row.currency, _to_jsonb(row.data))
                for row in df.itertuples(index=False)
            ]
            n_stmt += c.upsert(
                conn, f"{p}fin_statement",
                ["stock_code", "report_date", "stmt_type", "currency", "data"],
                rows, ["stock_code", "report_date", "stmt_type"],
                update_cols=["data", "currency"],
            )

        n_ind = 0
        ind = c.fetch_intl_fund_indicator(r.stock_code)
        if not ind.empty:
            cols = ["stock_code", "report_date", "currency"] + c._FUND_INDICATOR_COLS
            rows = []
            clipped = 0
            for row in ind.itertuples(index=False):
                vals = [r.stock_code, row.report_date, row.currency]
                for col in c._FUND_INDICATOR_COLS:
                    v, was_clipped = _clean_num(getattr(row, col), col)
                    if was_clipped:
                        clipped += 1
                    vals.append(v)
                rows.append(tuple(vals))
            if clipped:
                c.log.warning("  %s 指标越界值置 NULL: %d 个(超出列精度,视为脏数据)",
                              r.stock_code, clipped)
            n_ind = _upsert_indicator_keep_supplement(conn, f"{p}fin_indicator", cols, rows)

        c.mark_progress(conn, task, r.stock_code, None, "done", f"stmt={n_stmt},ind={n_ind}")
        c.log.info("  %s: 报表 %d 行 / 指标 %d 行", r.stock_code, n_stmt, n_ind)

    return load_one


# ===========================================================================
# 阶段B:ann_date 回填(东财)。仅 us 实现;hk 已知三级探测全部落空,整阶段跳过。
# ===========================================================================
# 探测结论(2026-07-10,AAPL "年报"+"累计季报" 全历史 78 行实测,见 task-3-report):
# 东财 NOTICE_DATE 对同一 REPORT_TYPE 的老报告期存在系统性"被下一次同类披露覆盖"的
# 缺陷 —— 每个 report_date 只有"当前最新一期"的 NOTICE_DATE 是真实近端披露日(约
# 30-40 天滞后),更老的报告期的 NOTICE_DATE 全部指向"下一次同类报表披露"的日期
# (实测滞后集中在 ~390-410 天(累计季报)与 ~760-810 天(年报),个别更极端到 850+
# 天)。400 天阈值能拦住全部年报族异常行(除了刚好 398 天的次新一期)和约半数累计
# 季报族异常行(该族多数滞后 389-410 天,阈值附近有漏网,见 task-3-report 疑虑)。
_ANN_GUARD_DAYS = 400

# 探测结论(stock_financial_us_analysis_indicator_em 实际列名,2026-07-10,AAPL "年报"
# 响应 49 列):无 BPS/OCF_PS 科目(与富途 US type4 同样缺失,双源皆无从填充,保留 NULL);
# 可映射列如下(仅填充 us_fin_indicator 当前为 NULL 的列,不覆盖富途已有值)。
_EM_US_IND_MAP = {
    "OPERATE_INCOME": "revenue",
    "OPERATE_INCOME_YOY": "revenue_yoy",
    "PARENT_HOLDER_NETPROFIT": "net_profit",
    "PARENT_HOLDER_NETPROFIT_YOY": "net_profit_yoy",
    "BASIC_EPS": "eps",
    "DILUTED_EPS": "eps_diluted",
    "ROE_AVG": "roe",
    "ROA": "roa",
    "GROSS_PROFIT_RATIO": "gross_margin",
    "NET_PROFIT_RATIO": "net_margin",
    "DEBT_ASSET_RATIO": "debt_ratio",
    "CURRENT_RATIO": "current_ratio",
}


def _fetch_us_ann_and_indicators(symbol: str) -> tuple[pd.DataFrame, int]:
    # 东财美股符号用下划线表示点号股类别(BRK.B → BRK_B,2026-07-11 实测:
    # BRK.B/BRK-B/BRKB 均无数据,BRK_B 返回 22 行),入口处归一化。
    symbol = symbol.replace(".", "_")
    """直接调东财美股指标接口(绕过 common.fetch_intl_ann_dates 的窄列裁剪:那个函数
    只返回 report_date/ann_date 两列),取 ann_date 之外顺手带出可映射的指标值。

    返回 (df[report_date, ann_date, revenue, revenue_yoy, ..., current_ratio], 护栏丢弃行数)。
    "年报"(DATE_TYPE_CODE=001)+ "累计季报"(002/004,H1/9M)取并集,与 common.py
    fetch_intl_ann_dates 的 us 分支同一策略;"单季报"(含 Q1)不纳入,同为已知覆盖缺口。
    """
    import akshare as ak

    def _safe_call(sym: str, indicator: str) -> pd.DataFrame:
        # 实测某些标的(如 JPM,银行类报表结构差异)该接口对特定 indicator 枚举无数据时
        # akshare 内部 `data_json["result"]["data"]` 对 None 取下标,抛 TypeError——这是
        # "该股此报表类型无数据"的确定性结果而非网络抖动,不应进 with_retry 的指数退避
        # (否则每次都白等 ~30s 且永远记 error,同 fetch_valuation 的 _value_em 处理套路)。
        try:
            return ak.stock_financial_us_analysis_indicator_em(symbol=sym, indicator=indicator)
        except TypeError as exc:
            c.log.warning("  %s 美股指标接口(%s)无数据(疑似报表结构差异,跳过): %s",
                          sym, indicator, exc)
            return pd.DataFrame()

    frames = []
    for ind in ("年报", "累计季报"):
        df = c.with_retry(_safe_call, symbol, ind)
        if df is not None and not df.empty and {"REPORT_DATE", "NOTICE_DATE"} <= set(df.columns):
            frames.append(df)

    empty_cols = ["report_date", "ann_date"] + list(_EM_US_IND_MAP.values())
    if not frames:
        return pd.DataFrame(columns=empty_cols), 0

    df = pd.concat(frames, ignore_index=True)
    df["report_date"] = pd.to_datetime(df["REPORT_DATE"], errors="coerce").dt.date
    df["ann_date"] = pd.to_datetime(df["NOTICE_DATE"], errors="coerce").dt.date
    df = df.dropna(subset=["report_date", "ann_date"])
    if df.empty:
        return pd.DataFrame(columns=empty_cols), 0

    # 裁定1:ann_date 护栏 —— 丢弃老周期被最新披露覆盖的假滞后行
    lag_days = (pd.to_datetime(df["ann_date"]) - pd.to_datetime(df["report_date"])).dt.days
    bad = lag_days > _ANN_GUARD_DAYS
    dropped = int(bad.sum())
    df = df[~bad]
    if df.empty:
        return pd.DataFrame(columns=empty_cols), dropped

    # 裁定3:顺手提取可映射指标列(探测得到的实际列名,见 _EM_US_IND_MAP)
    for src, dst in _EM_US_IND_MAP.items():
        df[dst] = pd.to_numeric(df[src], errors="coerce") if src in df.columns else pd.NA

    # 同 report_date 若"年报"/"累计季报"两路重叠(理论上不会,不同报表类型报告期不重叠),
    # 取 ann_date 较早者,与 common.fetch_intl_ann_dates 的 us 分支去重语义一致
    out = (df[empty_cols].sort_values("ann_date")
                          .drop_duplicates(subset=["report_date"], keep="first")
                          .sort_values("report_date").reset_index(drop=True))
    return out, dropped


def _apply_ann_us(conn, stock_code: str, df: pd.DataFrame) -> tuple[int, int]:
    """裁定2:按 (年,月) 容差 join 回填 ann_date(两表)+ 指标 NULL 列补全(仅 us_fin_indicator)。
    ann_date 用 LEAST(现有值, 新值) 合并(Postgres LEAST 对 NULL 自动忽略);指标列用
    COALESCE(现有值, 新值),只填 NULL、不覆盖富途已有值。返回 (ann_date 更新行数, 指标补全行数)。
    """
    n_ann = 0
    n_ind = 0
    with conn.cursor() as cur:
        for row in df.itertuples(index=False):
            yr, mo = row.report_date.year, row.report_date.month
            ann = row.ann_date

            cur.execute(
                "UPDATE us_fin_statement SET ann_date = LEAST(ann_date, %s) "
                "WHERE stock_code = %s AND EXTRACT(YEAR FROM report_date) = %s "
                "AND EXTRACT(MONTH FROM report_date) = %s",
                (ann, stock_code, yr, mo),
            )
            n_ann += cur.rowcount

            set_parts = ["ann_date = LEAST(ann_date, %s)"]
            params: list = [ann]
            for col in _EM_US_IND_MAP.values():
                v, was_clipped = _clean_num(getattr(row, col), col)
                if v is None:
                    continue
                set_parts.append(f"{col} = COALESCE({col}, %s)")
                params.append(v)
            params.extend([stock_code, yr, mo])
            cur.execute(
                f"UPDATE us_fin_indicator SET {', '.join(set_parts)} "
                "WHERE stock_code = %s AND EXTRACT(YEAR FROM report_date) = %s "
                "AND EXTRACT(MONTH FROM report_date) = %s",
                params,
            )
            n_ind += cur.rowcount
    conn.commit()
    return n_ann, n_ind


def phase_b_ann(conn, market: str, stocks: pd.DataFrame, workers: int, skip_ann: bool) -> None:
    task = f"init_fundann_{market}"

    if skip_ann:
        c.log.info("[%s] --skip-ann 指定,跳过阶段B ann_date 回填", market)
        return

    if market == "hk":
        # 三级探测已在 Task2 全部落空(see task-2-report Step1d):指标接口无 NOTICE_DATE
        # 语义字段、报表接口 STD_REPORT_DATE 与 REPORT_DATE 逐行恒等、无独立公告日接口。
        # 逐股请求换不来任何数据,直接跳过整阶段并显著告警。
        c.log.warning(
            "[%s] 港股 ann_date 三级探测已知全部落空(NOTICE_DATE 字段不存在/无滞后差可用/"
            "无独立公告日接口,见 task-2-report),阶段B 整体跳过 —— hk_fin_indicator/"
            "hk_fin_statement.ann_date 将保持全 NULL,hk_fin_asof(_all) 对港股永远返回空结果,"
            "回测请勿使用港股基本面因子或自行确认披露滞后。",
            market,
        )
        return

    done = c.get_done_codes(conn, task)
    todo = [r for r in stocks.itertuples(index=False) if r.stock_code not in done]
    c.log.info("[%s] 阶段B ann_date 回填:待处理 %d 只(已完成 %d 只,并发 %d)",
               market, len(todo), len(done), workers)

    guard_lock = threading.Lock()
    guard_stats = {"dropped": 0, "rows": 0}

    def load(conn2, r):
        df, dropped = _fetch_us_ann_and_indicators(r.symbol)
        with guard_lock:
            guard_stats["dropped"] += dropped
            guard_stats["rows"] += len(df)
        if df.empty:
            c.mark_progress(conn2, task, r.stock_code, None, "done", "ann=0,ind=0")
            return
        n_ann, n_ind = _apply_ann_us(conn2, r.stock_code, df)
        c.mark_progress(conn2, task, r.stock_code, None, "done", f"ann={n_ann},ind={n_ind}")
        c.log.info("  %s: ann_date 回填 %d 行 / 指标补全 %d 行(护栏丢弃 %d)",
                   r.stock_code, n_ann, n_ind, dropped)

    c.run_stock_todo(todo, task, load, workers, max_consecutive_errors=15)
    c.log.info("[%s] 阶段B 完成:ann_date 护栏累计丢弃 %d 行(>%d 天滞后,%d 行通过护栏进入回填)",
               market, guard_stats["dropped"], _ANN_GUARD_DAYS, guard_stats["rows"])


# ===========================================================================
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", required=True, choices=("hk", "us"))
    ap.add_argument("--workers", type=int, default=1,
                    help="阶段A 并发线程数(富途全局节流,>1 仅重叠 DB 写入,建议 2;阶段B 复用同一并发数)")
    ap.add_argument("--limit", type=int, default=None, help="只处理前 N 只(试跑)")
    ap.add_argument("--reset", action="store_true", help="清空该市场基本面初始化进度重来")
    ap.add_argument("--skip-ann", action="store_true", help="跳过阶段B ann_date 回填(仅报表+指标入库)")
    args = ap.parse_args()

    market = args.market
    task_a = f"init_fund_{market}"
    task_b = f"init_fundann_{market}"

    conn = c.get_conn()
    try:
        if args.reset:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM etl_progress WHERE task = ANY(%s)", ([task_a, task_b],))
            conn.commit()
            c.log.info("[%s] 已清空基本面初始化进度 %s / %s", market, task_a, task_b)

        # 股票域直接读库({p}stock_basic,一期已灌好)——不碰 HKEX/Wikipedia/GitHub 等
        # 外部清单源(一期最终审查 M-4 指出其结构脆弱;实测 Wikipedia 超时会让本脚本
        # 在拉任何数据前就崩)。新股由 06 日线增量维护 stock_basic 后自然进入本域。
        c.log.info("[%s] 从库加载股票域 ...", market)
        p = c.MARKETS[market]["prefix"]
        stocks = pd.read_sql(
            f"SELECT stock_code, symbol FROM {p}stock_basic ORDER BY stock_code",
            conn)
        conn.commit()  # 结束只读事务,避免 idle-in-transaction 长期持锁(一期教训)
        if args.limit:
            stocks = stocks.head(args.limit)
        c.log.info("[%s] 股票域 %d 只", market, len(stocks))

        c.log.info("=== [%s] 阶段A: 报表 + 关键指标(富途) ===", market)
        done_a = c.get_done_codes(conn, task_a)
        todo_a = [r for r in stocks.itertuples(index=False) if r.stock_code not in done_a]
        c.log.info("[%s] 阶段A 待处理 %d 只(已完成 %d 只,并发 %d)",
                   market, len(todo_a), len(done_a), args.workers)
        c.run_stock_todo(todo_a, task_a, make_loader_a(market, task_a), args.workers,
                         max_consecutive_errors=15)

        c.log.info("=== [%s] 阶段B: ann_date 回填(东财) ===", market)
        phase_b_ann(conn, market, stocks, args.workers, args.skip_ann)

        c.log.info("[%s] 港/美基本面初始化完成 ✅", market)
        return 0
    finally:
        c.close_futu()
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
