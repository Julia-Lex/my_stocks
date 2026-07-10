"""
10_fundamental_update.py — A股基本面每日增量。

1) 估值:对全部股票增量拉 daily_valuation(从库内该股 max(trade_date) 起),
   run_stock_todo 并行 + 熔断 15。
2) 财报核查(自适应):当月 ∈ {1,2,4,7,8,10} (披露季)或 --force-cross 或距上次
   核查 ≥7 天(etl_progress task='daily_fund' stock_code='_cross_check' 的
   last_date 记录)时:对「最近 2 个报告期」重拉 4 个截面接口 → upsert
   fin_indicator;随后对「本次 ann_date 有变化的股票」重拉新浪三大报表 →
   fin_statement,并对这些股票重算阶段4派生列 + ann_date 回填。
3) 股本:每周核查一次(同 _cross_check 机制,stock_code='_share_check'),
   变化则整段重取覆盖。
cron(README 同步):40 18 * * 1-5(在分钟线 18:30 之后)

用法:
  python 10_fundamental_update.py                          # 日常:估值增量 + 自适应财报/股本核查
  python 10_fundamental_update.py --limit 20 --workers 2    # 试跑前 20 只
  python 10_fundamental_update.py --force-cross             # 强制财报截面核查(忽略披露季/7天间隔判断)

已知限制(与 09_init_fundamental.py 共享,详见其 docstring/task-3-report):
  * 披露季展开初期(如季报刚过截止日),部分截面接口对最新报告期可能暂无数据
    (akshare 内部异常),按单元(period, kind)捕获、跳过,不阻塞其余单元与
    本次核查完成标记。
  * fetch_valuation/fetch_share_structure 均为"全历史单请求"接口,不支持增量
    参数——增量靠拉回全量后本地过滤 trade_date/新旧对比,由 upsert 幂等覆盖。
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from importlib import import_module

import pandas as pd

import common as c

# 09_init_fundamental.py 文件名以数字开头,不是合法标识符,不能用 `import 09_...`
# 语句,用 importlib 按字符串加载。复用其 _upsert_indicator_from_cross(截面
# upsert 逻辑,含 ann_date LEAST 合并与数值越界防护)与 phase4_derive(派生列
# +ann_date 回填 SQL,已改造为可选 stock_codes 范围过滤,见该函数改动)、
# _VALUATION_START/_SHARE_SANITY_MAX/_f/_i 等阶段3小工具,避免复制。
init_fund = import_module("09_init_fundamental")

TASK = "daily_fund"
_CROSS_MONTHS = {1, 2, 4, 7, 8, 10}   # 披露季:每月都核查;其余月份靠 7 天间隔兜底
_CHECK_INTERVAL_DAYS = 7


# ===========================================================================
# 核查节奏:etl_progress task='daily_fund' 借 stock_code='_cross_check' /
# '_share_check' 存"上次核查日"(与 09 的 'YYYYMMDD:kind' 借用同一约定)。
# ===========================================================================
def _last_check_date(conn, marker: str) -> date | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_date FROM etl_progress WHERE task = %s AND stock_code = %s",
            (TASK, marker),
        )
        row = cur.fetchone()
        return row[0] if row else None


def _due_for_check(conn, marker: str, force: bool = False, seasonal: bool = False) -> bool:
    if force or seasonal:
        return True
    last = _last_check_date(conn, marker)
    if last is None:
        return True  # 从未核查过:立即核查一次
    return (date.today() - last).days >= _CHECK_INTERVAL_DAYS


# ===========================================================================
# 1) 估值日增量(逐股,run_stock_todo 并行 + 熔断)
# ===========================================================================
def update_valuation(conn, workers: int, limit: int | None) -> None:
    stocks = c.fetch_stock_list()
    if limit:
        stocks = stocks.head(limit)
    todo = list(stocks.itertuples(index=False))
    c.log.info("估值增量:%d 只(并发 %d)", len(todo), workers)

    def load(conn2, r):
        max_d = c.get_max_trade_date(conn2, r.stock_code, table="daily_valuation")
        val = c.fetch_valuation(r.symbol)
        if val.empty:
            c.mark_progress(conn2, TASK, r.stock_code, max_d, "done", "val=+0")
            return
        if max_d is not None:
            val = val[val["trade_date"] > max_d]
        else:
            # 库内该股从未有估值行(新股/断点续传遗漏):按 09 阶段3 同源起点全量补齐
            val = val[val["trade_date"] >= init_fund._VALUATION_START]
        n = c.upsert(
            conn2, "daily_valuation",
            ["stock_code", "trade_date", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "total_mv"],
            [(r.stock_code, row.trade_date, init_fund._f(row.pe), init_fund._f(row.pe_ttm),
              init_fund._f(row.pb), init_fund._f(row.ps), init_fund._f(row.ps_ttm),
              init_fund._f(row.dv_ratio), init_fund._f(row.total_mv))
             for row in val.itertuples(index=False)],
            ["stock_code", "trade_date"],
        )
        last = val["trade_date"].max() if not val.empty else max_d
        c.mark_progress(conn2, TASK, r.stock_code, last, "done", f"val=+{n}")
        if n:
            c.log.info("  %s: 估值 +%d", r.stock_code, n)

    c.run_stock_todo(todo, TASK, load, workers, max_consecutive_errors=15)


# ===========================================================================
# 2) 财报核查(自适应节奏):最近 2 个报告期 × 4 个截面接口 → fin_indicator
#    返回本次 ann_date 有变化的 stock_code 集合。
# ===========================================================================
def _recent_periods(n: int = 2) -> list[date]:
    periods = c.quarter_ends(c.FUND_START, date.today())
    return periods[-n:]


def cross_check(conn, force: bool) -> set[str]:
    marker = "_cross_check"
    seasonal = date.today().month in _CROSS_MONTHS
    if not _due_for_check(conn, marker, force=force, seasonal=seasonal):
        c.log.info("财报核查:未到期(非披露季、距上次核查 <%d 天),跳过", _CHECK_INTERVAL_DAYS)
        return set()

    periods = _recent_periods(2)
    c.log.info("财报核查:最近 %d 个报告期 %s × 4 接口(触发原因: force=%s / 披露季=%s)",
               len(periods), [p.isoformat() for p in periods], force, seasonal)

    changed: set[str] = set()
    for p in periods:
        ps = p.strftime("%Y%m%d")
        with conn.cursor() as cur:
            cur.execute("SELECT stock_code, ann_date FROM fin_indicator WHERE report_date = %s", (p,))
            before = dict(cur.fetchall())

        for kind in ("yjbb", "lrb", "zcfz", "xjll"):
            try:
                df = c.fetch_fin_cross(kind, ps)
                n = init_fund._upsert_indicator_from_cross(conn, kind, p, df)
                c.log.info("  核查 %s %s: %d 行", ps, kind, n)
            except Exception as exc:  # noqa: BLE001 — 单元失败(常见于最新报告期尚未披露)不阻塞其余
                conn.rollback()
                c.log.warning("  核查 %s %s 暂无数据/失败(视为正常,不阻塞): %s", ps, kind, exc)

        with conn.cursor() as cur:
            cur.execute("SELECT stock_code, ann_date FROM fin_indicator WHERE report_date = %s", (p,))
            after = dict(cur.fetchall())
        for code, ann in after.items():
            if before.get(code) != ann:
                changed.add(code)

    c.mark_progress(conn, TASK, marker, date.today(), "done",
                    f"periods={[p.isoformat() for p in periods]},changed={len(changed)}")
    c.log.info("财报核查完成:%d 只股票 ann_date 有变化", len(changed))
    return changed


def refresh_changed_statements(conn, changed: set[str], workers: int, limit: int | None) -> None:
    """对 ann_date 有变化的股票重拉新浪三大报表,并重算这些股票的阶段4派生列。"""
    if not changed:
        c.log.info("无 ann_date 变化股票,跳过报表重拉与派生重算")
        return

    stocks = c.fetch_stock_list()
    if limit:
        stocks = stocks.head(limit)
    stocks = stocks[stocks["stock_code"].isin(changed)]
    todo = list(stocks.itertuples(index=False))
    c.log.info("ann_date 变化 %d 只(--limit 范围内 %d 只):重拉新浪三大报表", len(changed), len(todo))

    def load(conn2, r):
        total = 0
        for st in ("income", "balance", "cashflow"):
            total += c.upsert_jsonb_statement(conn2, r.stock_code, st,
                                              c.fetch_fin_report_sina(r.symbol, st))
        c.mark_progress(conn2, TASK, r.stock_code, date.today(), "done", f"stmt=+{total}")
        c.log.info("  %s: 报表 %d 行", r.stock_code, total)

    c.run_stock_todo(todo, TASK, load, workers, max_consecutive_errors=15)

    codes = sorted(r.stock_code for r in todo)
    if codes:
        c.log.info("重算阶段4派生列(限本次重拉的 %d 只)", len(codes))
        init_fund.phase4_derive(conn, stock_codes=codes)


# ===========================================================================
# 3) 股本每周核查:同 _cross_check 机制,变化则整段重取覆盖(逐股 upsert 本身
#    就是"覆盖":fetch_share_structure 每次都返回全量,ON CONFLICT DO UPDATE
#    对有变化的行写入新值,对无变化的行幂等重写同值)。
# ===========================================================================
def share_check(conn, workers: int, limit: int | None) -> None:
    marker = "_share_check"
    if not _due_for_check(conn, marker):
        c.log.info("股本核查:距上次核查 <%d 天,跳过", _CHECK_INTERVAL_DAYS)
        return

    stocks = c.fetch_stock_list()
    if limit:
        stocks = stocks.head(limit)
    todo = list(stocks.itertuples(index=False))
    c.log.info("股本核查:%d 只(并发 %d)", len(todo), workers)

    def load(conn2, r):
        share = c.fetch_share_structure(r.symbol)
        if share.empty:
            c.mark_progress(conn2, TASK, r.stock_code, None, "done", "share=+0")
            return
        bad = share[share["total_shares"] > init_fund._SHARE_SANITY_MAX]
        if not bad.empty:
            c.log.warning("  %s share_capital.total_shares 疑似单位异常(>%.0e): %s",
                          r.stock_code, init_fund._SHARE_SANITY_MAX, bad["total_shares"].tolist())
        n = c.upsert(
            conn2, "share_capital",
            ["stock_code", "change_date", "total_shares", "float_shares", "reason"],
            [(r.stock_code, row.change_date, init_fund._i(row.total_shares), init_fund._i(row.float_shares),
              None if pd.isna(getattr(row, "reason", None)) else str(row.reason))
             for row in share.itertuples(index=False)],
            ["stock_code", "change_date"],
        )
        c.mark_progress(conn2, TASK, r.stock_code, date.today(), "done", f"share=+{n}")

    c.run_stock_todo(todo, TASK, load, workers, max_consecutive_errors=15)

    c.mark_progress(conn, TASK, marker, date.today(), "done", f"stocks={len(todo)}")
    c.log.info("股本核查完成")


# ===========================================================================
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=1,
                    help="并发拉取线程数(默认 1;免费源限流,建议不超过 4)")
    ap.add_argument("--limit", type=int, default=None,
                    help="只处理前 N 只(试跑;同时限制估值增量、变化股票报表重拉与股本核查范围)")
    ap.add_argument("--force-cross", action="store_true",
                    help="强制执行财报截面核查(忽略披露季/7天间隔判断)")
    args = ap.parse_args()

    conn = c.get_conn()
    try:
        c.log.info("=== 1) 估值日增量 ===")
        update_valuation(conn, args.workers, args.limit)

        c.log.info("=== 2) 财报核查(自适应) ===")
        changed = cross_check(conn, args.force_cross)
        refresh_changed_statements(conn, changed, args.workers, args.limit)

        c.log.info("=== 3) 股本周核查 ===")
        share_check(conn, args.workers, args.limit)

        c.log.info("基本面每日增量完成 ✅")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
