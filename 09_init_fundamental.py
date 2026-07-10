"""
09_init_fundamental.py — A股基本面全量初始化(近10年,四阶段,断点续传)。

阶段1 截面:40+ 个报告期 × 4 东财截面接口 → fin_indicator 骨干(含 ann_date)。
        幂等:按 (report_date, kind) 记录于 etl_progress(task='init_fund_cross',
        stock_code 字段借存 'YYYYMMDD:kind'),重跑跳过已完成期。
阶段2 全科目:逐股新浪三大报表 → fin_statement(JSONB),run_stock_todo 并行。
阶段3 股本+估值:逐股东财 → share_capital / daily_valuation,run_stock_todo 并行。
阶段4 派生+回填(纯 SQL/本地计算,无网络):
   a) fin_statement.ann_date ← fin_indicator.ann_date(同 stock+report_date);
   b) 派生列 UPDATE:net_margin/roa/debt_ratio/ocf_to_profit(用指标表已有列),
      current_ratio(从 balance JSONB 取 流动资产合计/流动负债合计)。

用法:
  python 09_init_fundamental.py                # 四阶段顺序全跑
  python 09_init_fundamental.py --phase 2 --workers 3 --limit 20
  python 09_init_fundamental.py --reset        # 清空本脚本相关进度
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

import pandas as pd
import psycopg2.extras

import common as c

FUND_TASKS = ("init_fund_cross", "init_fund_stmt", "init_fund_misc")


# ===========================================================================
# 阶段1:截面骨干(40+ 报告期 × 4 接口)
# ===========================================================================
# 各列可表示上限(与 08_schema_fundamental.sql 的 NUMERIC 精度一一对应):
#   NUMERIC(12,4) → abs < 10^8;NUMERIC(10,4) → abs < 10^6;NUMERIC(20,2) → abs < 10^18
_NUM_LIMIT = {
    "eps": 1e8, "bps": 1e8, "ocf_ps": 1e8,                     # NUMERIC(12,4)
    "roe": 1e6, "gross_margin": 1e6,
    "revenue_yoy": 1e6, "net_profit_yoy": 1e6,                 # NUMERIC(10,4)
    "revenue": 1e18, "net_profit": 1e18,
    "total_assets": 1e18, "total_liab": 1e18,
    "total_equity": 1e18, "ocf": 1e18,                         # NUMERIC(20,2)
}


def _upsert_indicator_from_cross(conn, kind: str, report_date: date, df: pd.DataFrame) -> int:
    """按 kind 写 fin_indicator 对应列子集。

    ann_date 统一用 LEAST(现有值, 新值) 更新(Postgres LEAST 对 NULL 自动忽略,
    两者都非空取更早者、任一为空取非空者、都为空则仍为空)——四个 kind 无论
    先后处理顺序,最终收敛到该报告期"最早已知公告日",不会被后处理的 kind
    用空值或更晚日期覆盖已确定的更早公告日。

    lrb 的 net_profit 是兜底列(yjbb 是主源,已提供 net_profit):只在
    fin_indicator.net_profit 当前为空时才用 lrb 的值填(COALESCE 保留已有值,
    不覆盖 yjbb 已写入的数)。

    数值越界防护(阶段1实跑发现,2026-07-10):yoy 类百分比列偶有荒谬极值
    (基数接近 0 时同比可达数亿 %),超出 NUMERIC(10,4) 可表示上限(abs<10^6)
    会让整期 5,000 行的 upsert 报 numeric field overflow 全军覆没。按 schema
    各列精度设越界界限,越界值置 NULL 并计数打日志——极值本身无分析意义,
    丢单值优于丢整期。
    """
    if df.empty:
        return 0

    if kind == "yjbb":
        value_cols = ["eps", "bps", "ocf_ps", "roe", "gross_margin", "revenue",
                      "revenue_yoy", "net_profit", "net_profit_yoy", "industry"]
        fallback_cols: set[str] = set()
    elif kind == "zcfz":
        value_cols = ["total_assets", "total_liab", "total_equity"]
        fallback_cols = set()
    elif kind == "xjll":
        value_cols = ["ocf"]
        fallback_cols = set()
    elif kind == "lrb":
        value_cols = ["net_profit"]
        fallback_cols = {"net_profit"}
    else:
        raise ValueError(f"unknown kind: {kind}")

    value_cols = [vc for vc in value_cols if vc in df.columns]
    if not value_cols:
        return 0

    cols = ["stock_code", "report_date", "ann_date"] + value_cols
    rows = []
    clipped = 0
    for r in df.itertuples(index=False):
        row = [r.stock_code, report_date, getattr(r, "ann_date", None)]
        for vc in value_cols:
            v = getattr(r, vc)
            if vc == "industry":
                row.append(None if pd.isna(v) else str(v)[:32])
            elif pd.isna(v):
                row.append(None)
            else:
                v = float(v)
                if abs(v) >= _NUM_LIMIT.get(vc, 1e18):
                    clipped += 1
                    row.append(None)
                else:
                    row.append(v)
        rows.append(tuple(row))
    if clipped:
        c.log.warning("  截面 %s %s: %d 个越界值置 NULL(超出列精度,视为脏数据)",
                      report_date, kind, clipped)

    set_parts = ["ann_date = LEAST(fin_indicator.ann_date, EXCLUDED.ann_date)"]
    for vc in value_cols:
        if vc in fallback_cols:
            set_parts.append(f"{vc} = COALESCE(fin_indicator.{vc}, EXCLUDED.{vc})")
        else:
            set_parts.append(f"{vc} = EXCLUDED.{vc}")

    sql = (
        f"INSERT INTO fin_indicator ({', '.join(cols)}) VALUES %s "
        f"ON CONFLICT (stock_code, report_date) DO UPDATE SET {', '.join(set_parts)}"
    )
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    return len(rows)


def phase1_cross(conn) -> None:
    periods = c.quarter_ends(c.FUND_START, date.today())
    done = c.get_done_codes(conn, "init_fund_cross")
    c.log.info("阶段1 截面:%d 个报告期 × 4 接口(已完成 %d 项)", len(periods), len(done))
    for p in periods:
        ps = p.strftime("%Y%m%d")
        for kind in ("yjbb", "lrb", "zcfz", "xjll"):
            key = f"{ps}:{kind}"
            if key in done:
                continue
            try:
                df = c.fetch_fin_cross(kind, ps)
                n = _upsert_indicator_from_cross(conn, kind, p, df)
                c.mark_progress(conn, "init_fund_cross", key, p, "done", f"rows={n}")
                c.log.info("  截面 %s %s: %d 行", ps, kind, n)
            except Exception as exc:  # noqa: BLE001 — 单个 (期,kind) 失败不影响其余
                conn.rollback()
                c.mark_progress(conn, "init_fund_cross", key, p, "error", str(exc))
                c.log.error("  截面 %s %s 失败: %s", ps, kind, exc)


# ===========================================================================
# 阶段2:全科目报表(逐股新浪三大报表 → fin_statement JSONB)
# ===========================================================================
def phase2_statements(conn, workers: int, limit: int | None) -> None:
    stocks = c.fetch_stock_list()
    if limit:
        stocks = stocks.head(limit)
    done = c.get_done_codes(conn, "init_fund_stmt")
    todo = [r for r in stocks.itertuples(index=False) if r.stock_code not in done]
    c.log.info("阶段2 全科目:待处理 %d 只(已完成 %d 只,并发 %d)", len(todo), len(done), workers)

    def load(conn2, r):
        total = 0
        for st in ("income", "balance", "cashflow"):
            total += c.upsert_jsonb_statement(conn2, r.stock_code, st,
                                              c.fetch_fin_report_sina(r.symbol, st))
        c.mark_progress(conn2, "init_fund_stmt", r.stock_code, None, "done", f"rows={total}")
        c.log.info("  %s: 报表 %d 行", r.stock_code, total)

    c.run_stock_todo(todo, "init_fund_stmt", load, workers, max_consecutive_errors=15)


# ===========================================================================
# 阶段3:股本 + 估值(逐股东财)
# ===========================================================================
_VALUATION_START = date(2016, 1, 1)
_SHARE_SANITY_MAX = 5 * 10 ** 11  # 总股本量级哨兵(见 Task2 报告"疑虑2"):超过此值疑似单位错误
# 阈值取 5e11(原 1e11 偏紧,工商银行实际总股本约 3.564e11 股,是全市场最大,
# 1e11 会把这只真实存在的大盘股误判为"单位错误"而告警)。


def _f(v):
    return None if pd.isna(v) else float(v)


def _i(v):
    v = _f(v)
    return int(v) if v is not None else None


def phase3_misc(conn, workers: int, limit: int | None) -> None:
    stocks = c.fetch_stock_list()
    if limit:
        stocks = stocks.head(limit)
    done = c.get_done_codes(conn, "init_fund_misc")
    todo = [r for r in stocks.itertuples(index=False) if r.stock_code not in done]
    c.log.info("阶段3 股本+估值:待处理 %d 只(已完成 %d 只,并发 %d)", len(todo), len(done), workers)

    def load(conn2, r):
        share = c.fetch_share_structure(r.symbol)
        if not share.empty:
            bad = share[share["total_shares"] > _SHARE_SANITY_MAX]
            if not bad.empty:
                c.log.warning("  %s share_capital.total_shares 疑似单位异常(>%.0e): %s",
                              r.stock_code, _SHARE_SANITY_MAX, bad["total_shares"].tolist())
        n_share = c.upsert(
            conn2, "share_capital",
            ["stock_code", "change_date", "total_shares", "float_shares", "reason"],
            [(r.stock_code, row.change_date, _i(row.total_shares), _i(row.float_shares),
              None if pd.isna(getattr(row, "reason", None)) else str(row.reason))
             for row in share.itertuples(index=False)],
            ["stock_code", "change_date"],
        )

        val = c.fetch_valuation(r.symbol)
        if not val.empty:
            val = val[val["trade_date"] >= _VALUATION_START]
        n_val = c.upsert(
            conn2, "daily_valuation",
            ["stock_code", "trade_date", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "total_mv"],
            [(r.stock_code, row.trade_date, _f(row.pe), _f(row.pe_ttm), _f(row.pb),
              _f(row.ps), _f(row.ps_ttm), _f(row.dv_ratio), _f(row.total_mv))
             for row in val.itertuples(index=False)],
            ["stock_code", "trade_date"],
        )

        c.mark_progress(conn2, "init_fund_misc", r.stock_code, None, "done",
                        f"share={n_share},val={n_val}")
        c.log.info("  %s: 股本 %d / 估值 %d", r.stock_code, n_share, n_val)

    c.run_stock_todo(todo, "init_fund_misc", load, workers, max_consecutive_errors=15)


# ===========================================================================
# 阶段4:派生 + 回填(纯 SQL,无网络)
# ===========================================================================
# 派生比率列都是 NUMERIC(10,4)(abs < 10^6):分母接近 0 时比率可轻松越界
# (实跑触发 numeric field overflow,与阶段1同类问题),越界置 NULL——
# _clip 生成 "算得出且在界内才写,否则 NULL" 的 SQL 片段。
def _clip(expr: str, cond: str, limit: str = "1e6") -> str:
    return f"CASE WHEN {cond} AND abs({expr}) < {limit} THEN {expr} END"


def phase4_derive(conn, stock_codes: list[str] | None = None) -> None:
    """阶段4:派生列 + ann_date 回填(纯 SQL,无网络)。

    stock_codes=None(默认,本脚本全量初始化路径):三条 UPDATE 均不加过滤,全表重算。
    传入股票代码列表时(10_fundamental_update.py 每日增量复用本函数的路径):
    三条 UPDATE 均加 stock_code = ANY(%s) 范围过滤,只重算这些股票——每日增量
    只有"本次 ann_date 有变化"的一小撮股票需要重算,不应对几十万行 fin_indicator/
    fin_statement 做无谓全表扫描。空列表([])会被过滤条件正确匹配为零行(不同于
    None 的"不过滤"),调用方仍应在自己一侧先判断"无变化则不必调用本函数"以省一次
    空转的数据库往返。
    """
    scope_fs = "AND fs.stock_code = ANY(%s)" if stock_codes is not None else ""
    scope_fi = "WHERE stock_code = ANY(%s)" if stock_codes is not None else ""
    scope_join = "AND fi.stock_code = ANY(%s)" if stock_codes is not None else ""
    p = (stock_codes,) if stock_codes is not None else ()

    with conn.cursor() as cur:
        c.log.info("阶段4a: 回填 fin_statement.ann_date ...")
        cur.execute(f"""
            UPDATE fin_statement fs SET ann_date = fi.ann_date
            FROM fin_indicator fi
            WHERE fs.stock_code = fi.stock_code AND fs.report_date = fi.report_date
              AND fs.ann_date IS NULL AND fi.ann_date IS NOT NULL
              {scope_fs}
        """, p)
        c.log.info("  ann_date 回填 %d 行", cur.rowcount)
        conn.commit()

        c.log.info("阶段4b: 派生列(net_margin/roa/debt_ratio/ocf_to_profit) ...")
        cur.execute(f"""
            UPDATE fin_indicator SET
              net_margin    = {_clip("net_profit / revenue * 100", "revenue <> 0")},
              roa           = {_clip("net_profit / total_assets * 100", "total_assets <> 0")},
              debt_ratio    = {_clip("total_liab / total_assets * 100", "total_assets <> 0")},
              ocf_to_profit = {_clip("ocf / net_profit", "net_profit <> 0")}
            {scope_fi}
        """, p)
        c.log.info("  派生列更新 %d 行", cur.rowcount)
        conn.commit()

        c.log.info("阶段4c: current_ratio(取自 balance JSONB) ...")
        cur.execute(f"""
            UPDATE fin_indicator fi SET current_ratio =
              {_clip("(fs.data->>'流动资产合计')::numeric"
                     " / NULLIF((fs.data->>'流动负债合计')::numeric, 0)",
                     "(fs.data->>'流动负债合计')::numeric <> 0")}
            FROM fin_statement fs
            WHERE fs.stmt_type = 'balance'
              AND fs.stock_code = fi.stock_code AND fs.report_date = fi.report_date
              AND (fs.data->>'流动资产合计') ~ '^[0-9.eE+-]+$'
              AND (fs.data->>'流动负债合计') ~ '^[0-9.eE+-]+$'
              {scope_join}
        """, p)
        c.log.info("  current_ratio 更新 %d 行", cur.rowcount)
        conn.commit()


# ===========================================================================
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=1,
                    help="并发拉取线程数(默认 1;免费源限流,建议不超过 4)")
    ap.add_argument("--limit", type=int, default=None, help="阶段2/3 只处理前 N 只(试跑)")
    ap.add_argument("--reset", action="store_true", help="清空本脚本相关进度重来")
    ap.add_argument("--phase", default="all", choices=("1", "2", "3", "4", "all"),
                    help="只跑指定阶段(默认 all:四阶段顺序全跑)")
    args = ap.parse_args()

    conn = c.get_conn()
    try:
        if args.reset:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM etl_progress WHERE task = ANY(%s)", (list(FUND_TASKS),)
                )
            conn.commit()
            c.log.info("已清空基本面初始化进度 %s", FUND_TASKS)

        phases = ("1", "2", "3", "4") if args.phase == "all" else (args.phase,)

        if "1" in phases:
            c.log.info("=== 阶段1: 截面骨干 ===")
            phase1_cross(conn)
        if "2" in phases:
            c.log.info("=== 阶段2: 全科目报表 ===")
            phase2_statements(conn, args.workers, args.limit)
        if "3" in phases:
            c.log.info("=== 阶段3: 股本 + 估值 ===")
            phase3_misc(conn, args.workers, args.limit)
        if "4" in phases:
            c.log.info("=== 阶段4: 派生 + 回填 ===")
            phase4_derive(conn)

        c.log.info("基本面初始化完成 ✅")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
