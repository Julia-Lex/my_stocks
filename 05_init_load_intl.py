"""
05_init_load_intl.py — 港股/美股全量历史初始化(带断点续传)。

流程同 02_init_load.py:参考数据(列表/指数/派生日历)→ 逐只日线+因子 → 物化视图。
用法:
  python 05_init_load_intl.py --market hk               # 港股全量
  python 05_init_load_intl.py --market us --workers 3   # 美股 3 并发
  python 05_init_load_intl.py --market hk --limit 20    # 试跑
  python 05_init_load_intl.py --market hk --reset       # 清空进度重来
"""

from __future__ import annotations

import argparse
import sys

import common as c


def load_reference_data(conn, market: str):
    """股票列表 + 指数日线 + 派生交易日历。返回股票 DataFrame。"""
    cfg = c.MARKETS[market]
    p = cfg["prefix"]

    c.log.info("[%s] 加载股票列表 ...", market)
    if market == "hk":
        stocks = c.fetch_hk_stock_list()
        cols = ["stock_code", "symbol", "name", "exchange"]
    else:
        stocks = c.fetch_us_stock_list()
        cols = ["stock_code", "symbol", "name", "exchange", "em_symbol"]
    c.upsert(conn, f"{p}stock_basic", cols,
             [tuple(getattr(r, x) for x in cols) for r in stocks.itertuples(index=False)],
             ["stock_code"], update_cols=["name", "exchange"])
    c.log.info("[%s] 股票列表 %d 只", market, len(stocks))

    c.log.info("[%s] 加载指数日线 ...", market)
    for idx in cfg["indexes"]:
        try:
            n = c.upsert_index(conn, idx, c.fetch_intl_index(market, idx),
                               table=f"{p}index_daily")
            c.log.info("  指数 %s: %d 行", idx, n)
        except Exception as exc:  # noqa: BLE001
            c.log.warning("  指数 %s 失败: %s", idx, exc)
    c.rebuild_intl_calendar(conn, market)
    return stocks


def make_loader(market: str, task: str):
    """返回 load_fn(conn, row) 供 run_stock_todo 调用。"""
    p = c.MARKETS[market]["prefix"]

    def load_one(conn, r):
        fetch_symbol = getattr(r, "em_symbol", None) or r.symbol
        daily = c.fetch_intl_daily(market, fetch_symbol)
        n_daily = c.upsert_daily(conn, r.stock_code, daily, table=f"{p}daily_price")
        adj = c.fetch_intl_hfq_factor(market, fetch_symbol, raw=daily)
        n_adj = c.upsert_adj_factor(conn, r.stock_code, adj, table=f"{p}adj_factor")
        last = daily["trade_date"].max() if not daily.empty else None
        c.mark_progress(conn, task, r.stock_code, last, status="done",
                        message=f"daily={n_daily},adj={n_adj}")
        c.log.info("  %s: 日线 %d / 因子 %d", r.stock_code, n_daily, n_adj)

    return load_one


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", required=True, choices=("hk", "us"))
    ap.add_argument("--limit", type=int, default=None, help="只处理前 N 只(试跑)")
    ap.add_argument("--reset", action="store_true", help="清空该市场 init 进度重来")
    ap.add_argument("--workers", type=int, default=1,
                    help="并发拉取线程数(默认 1;免费源限流,建议不超过 4)")
    args = ap.parse_args()

    task = f"init_{args.market}"
    conn = c.get_conn()
    try:
        if args.reset:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM etl_progress WHERE task = %s", (task,))
            conn.commit()
            c.log.info("已清空 %s 进度", task)

        stocks = load_reference_data(conn, args.market)
        if args.limit:
            stocks = stocks.head(args.limit)

        done = c.get_done_codes(conn, task)
        todo = [r for r in stocks.itertuples(index=False) if r.stock_code not in done]
        c.log.info("[%s] 待处理 %d 只(已完成 %d 只,并发 %d)",
                   args.market, len(todo), len(done), args.workers)

        c.run_stock_todo(todo, task, make_loader(args.market, task), args.workers,
                        max_consecutive_errors=15)

        c.log.info("[%s] 刷新周线/月线物化视图 ...", args.market)
        c.refresh_matviews(conn, c.MARKETS[args.market]["mviews"])
        c.log.info("[%s] 全量初始化完成 ✅", args.market)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
