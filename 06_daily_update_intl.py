"""
06_daily_update_intl.py — 港股/美股每日增量更新(带自动补漏)。

同 03_daily_update.py:先刷参考数据(列表/指数/日历),再按缺口增量。
新股(库内无记录)直接全量拉取,不走日历缺口检测——因为港/美交易日历由
指数日线派生,覆盖有限(港股 2013-08 起,HSI 派生;美股 2004-01 起,
.DJI/.INX/.IXIC 派生),缺口检测会漏掉更早历史(见 update_one)。

cron 建议(北京时间):
  港股: 0 18 * * 1-5  ... python 06_daily_update_intl.py --market hk
  美股: 0 9  * * 2-6  ... python 06_daily_update_intl.py --market us   # 拉前一交易日
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta

import common as c


def expected_open_dates(conn, market: str, since: date) -> list[date]:
    p = c.MARKETS[market]["prefix"]
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT trade_date FROM {p}trade_calendar "
            f"WHERE is_open AND trade_date >= %s AND trade_date <= %s ORDER BY trade_date",
            (since, date.today()),
        )
        return [r[0] for r in cur.fetchall()]


def existing_dates(conn, market: str, stock_code: str, since: date) -> set[date]:
    p = c.MARKETS[market]["prefix"]
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT trade_date FROM {p}daily_price "
            f"WHERE stock_code = %s AND trade_date >= %s",
            (stock_code, since),
        )
        return {r[0] for r in cur.fetchall()}


def update_reference(conn, market: str):
    cfg = c.MARKETS[market]
    p = cfg["prefix"]
    if market == "hk":
        stocks = c.fetch_hk_stock_list()
        cols = ["stock_code", "symbol", "name", "exchange"]
    else:
        stocks = c.fetch_us_stock_list()
        cols = ["stock_code", "symbol", "name", "exchange", "em_symbol"]
    c.upsert(conn, f"{p}stock_basic", cols,
             [tuple(getattr(r, x) for x in cols) for r in stocks.itertuples(index=False)],
             ["stock_code"], update_cols=["name", "exchange"])
    for idx in cfg["indexes"]:
        try:
            c.upsert_index(conn, idx, c.fetch_intl_index(market, idx),
                           table=f"{p}index_daily")
        except Exception as exc:  # noqa: BLE001
            c.log.warning("指数 %s 更新失败: %s", idx, exc)
    c.rebuild_intl_calendar(conn, market)
    return stocks


def make_updater(market: str, task: str, lookback_days: int):
    p = c.MARKETS[market]["prefix"]

    def update_one(conn, r):
        fetch_symbol = getattr(r, "em_symbol", None) or r.symbol
        max_d = c.get_max_trade_date(conn, r.stock_code, table=f"{p}daily_price")

        if max_d is None:
            # 新股:直接全量(日历覆盖有限——港 2013-08+/美 2004-01+,
            # 缺口检测会漏掉更早历史)
            daily = c.fetch_intl_daily(market, fetch_symbol)
            n = c.upsert_daily(conn, r.stock_code, daily, table=f"{p}daily_price")
            # raw= 仅 em 回退路径生效(省一次请求);tx 生产路径因子取自新浪,忽略之
            adj = c.fetch_intl_hfq_factor(market, fetch_symbol, raw=daily)
            c.upsert_adj_factor(conn, r.stock_code, adj, table=f"{p}adj_factor")
            last = daily["trade_date"].max() if not daily.empty else None
            c.mark_progress(conn, task, r.stock_code, last, status="done", message=f"init+{n}")
            return

        start = max_d - timedelta(days=lookback_days)
        need = set(expected_open_dates(conn, market, start))
        if not need:
            return
        have = existing_dates(conn, market, r.stock_code, start)
        missing = need - have
        if not missing:
            return

        daily = c.fetch_intl_daily(market, fetch_symbol,
                                   start=min(missing).strftime("%Y%m%d"),
                                   end=max(missing).strftime("%Y%m%d"))
        n = c.upsert_daily(conn, r.stock_code, daily, table=f"{p}daily_price")
        adj = c.fetch_intl_hfq_factor(market, fetch_symbol)   # 因子整段重取覆盖
        c.upsert_adj_factor(conn, r.stock_code, adj, table=f"{p}adj_factor")
        last = daily["trade_date"].max() if not daily.empty else max_d
        c.mark_progress(conn, task, r.stock_code, last, status="done", message=f"+{n}")

    return update_one


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", required=True, choices=("hk", "us"))
    ap.add_argument("--days", type=int, default=5, help="回看天数(补漏安全边界)")
    ap.add_argument("--limit", type=int, default=None, help="只处理前 N 只(调试)")
    ap.add_argument("--no-matview", action="store_true", help="跳过物化视图刷新")
    ap.add_argument("--workers", type=int, default=1, help="并发线程数")
    args = ap.parse_args()

    task = f"daily_{args.market}"
    conn = c.get_conn()
    try:
        c.log.info("[%s] 更新参考数据(列表/指数/日历) ...", args.market)
        stocks = update_reference(conn, args.market)
        if args.limit:
            stocks = stocks.head(args.limit)

        rows = list(stocks.itertuples(index=False))
        c.log.info("[%s] 增量更新 %d 只 ...", args.market, len(rows))
        c.run_stock_todo(rows, task, make_updater(args.market, task, args.days),
                         args.workers, max_consecutive_errors=15)

        if not args.no_matview:
            c.log.info("[%s] 刷新周线/月线物化视图 ...", args.market)
            c.refresh_matviews(conn, c.MARKETS[args.market]["mviews"])
        c.log.info("[%s] 增量更新完成 ✅ (%s)", args.market,
                   datetime.now().strftime("%Y-%m-%d %H:%M"))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
