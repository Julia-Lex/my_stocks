"""
02_init_load.py — 全量历史初始化(带断点续传)。

流程:
  1. 建立/刷新交易日历、股票列表、指数日线。
  2. 逐只股票拉取:不复权日线 + 后复权因子,写入库。
  3. 每只完成后写 etl_progress;中断后重跑会自动跳过已完成的股票。
  4. 全部完成后刷新周线/月线物化视图。

预计首次全量 2~4 小时(取决于网络与免费源限流)。可随时 Ctrl-C 中断,
重跑会从断点继续。

用法:
  python 02_init_load.py                 # 全量(串行)
  python 02_init_load.py --workers 4     # 4 个并发拉取(注意免费源限流)
  python 02_init_load.py --limit 50      # 只跑前 50 只(试跑)
  python 02_init_load.py --reset         # 清空进度重来
"""

from __future__ import annotations

import argparse
import sys

import common as c

TASK = "init_daily"


def load_reference_data(conn) -> None:
    """交易日历 + 股票列表 + 指数。"""
    c.log.info("加载交易日历 ...")
    cal = c.fetch_calendar()
    n = c.upsert(conn, "trade_calendar", ["trade_date", "is_open"],
                 [(r.trade_date, bool(r.is_open)) for r in cal.itertuples(index=False)],
                 ["trade_date"])
    c.log.info("交易日历 %d 行", n)

    c.log.info("加载股票列表 ...")
    stocks = c.fetch_stock_list()
    c.upsert(conn, "stock_basic",
             ["stock_code", "symbol", "name", "exchange"],
             [(r.stock_code, r.symbol, r.name, r.exchange) for r in stocks.itertuples(index=False)],
             ["stock_code"],
             update_cols=["name", "exchange"])
    c.log.info("股票列表 %d 只", len(stocks))

    c.log.info("加载指数日线 ...")
    for idx in c.INDEX_LIST:
        try:
            df = c.fetch_index(idx)
            n = c.upsert_index(conn, idx, df)
            c.log.info("  指数 %s: %d 行", idx, n)
        except Exception as exc:  # noqa: BLE001
            c.log.warning("  指数 %s 失败: %s", idx, exc)


def load_one_stock(conn, stock_code: str, symbol: str) -> None:
    """单只股票:日线 + 后复权因子。"""
    daily = c.fetch_daily(symbol)
    n_daily = c.upsert_daily(conn, stock_code, daily)

    adj = c.fetch_hfq_factor(symbol)
    n_adj = c.upsert_adj_factor(conn, stock_code, adj)

    # 进度里的 last_date 必须是「真正写入」的日期:盘中运行时当天的
    # bar 会被 drop_unclosed_bars 拦掉,不能记成已完成
    last = min(daily["trade_date"].max(), c.safe_cutoff_date()) if not daily.empty else None
    c.mark_progress(conn, TASK, stock_code, last, status="done",
                    message=f"src={c.ASHARE_SOURCE},daily={n_daily},adj={n_adj}")
    c.log.info("  %s: 日线 %d / 因子 %d", stock_code, n_daily, n_adj)




def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="只处理前 N 只(试跑)")
    ap.add_argument("--reset", action="store_true", help="清空 init 进度重来")
    ap.add_argument("--workers", type=int, default=1,
                    help="并发拉取线程数(默认 1;免费源限流,建议不超过 4)")
    args = ap.parse_args()

    conn = c.get_conn()
    try:
        if args.reset:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM etl_progress WHERE task = %s", (TASK,))
            conn.commit()
            c.log.info("已清空 %s 进度", TASK)

        load_reference_data(conn)

        stocks = c.fetch_stock_list()
        if args.limit:
            stocks = stocks.head(args.limit)

        done = c.get_done_codes(conn, TASK)
        todo = [r for r in stocks.itertuples(index=False) if r.stock_code not in done]
        c.log.info("待处理 %d 只(已完成 %d 只,并发 %d)", len(todo), len(done), args.workers)

        conn.commit()  # 结束只读事务,避免长时间 idle-in-transaction 阻塞 DDL/vacuum

        def _load(conn2, r):
            load_one_stock(conn2, r.stock_code, r.symbol)

        c.run_stock_todo(todo, TASK, _load, args.workers, max_consecutive_errors=15)

        c.log.info("刷新周线/月线物化视图 ...")
        c.refresh_matviews(conn)
        c.log.info("全量初始化完成 ✅")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
