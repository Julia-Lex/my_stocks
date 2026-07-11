"""
22_hk_amount_backfill.py — 港股日线 成交额/换手率 历史回填(一次性)。

背景:hk_daily_price 主源为腾讯(无成交额/换手率两列);东财港股日线有。
本脚本逐股拉东财全历史,仅 UPDATE amount/turnover 两列(不碰腾讯的 OHLCV,
两源价格口径一致但以主源为准)。断点续传 task='hk_amount_fill';熔断 15。
日常增量由 06 的当日快照补列负责(common.fetch_hk_spot_amount)。

用法: python 22_hk_amount_backfill.py [--workers 2] [--limit N] [--reset]
注意:东财行情族(push2his)封禁时不可跑,解封后续传。
"""

from __future__ import annotations

import argparse
import sys

import pandas as pd

import common as c

TASK = "hk_amount_fill"


def load_one(conn, r):
    df = c._fetch_intl_daily_em("hk", r.symbol)   # 东财港股:含 amount/turnover
    n = 0
    if not df.empty and "amount" in df.columns:
        with conn.cursor() as cur:
            for row in df.itertuples(index=False):
                amount = getattr(row, "amount", None)
                turnover = getattr(row, "turnover", None)
                if amount is None and turnover is None:
                    continue
                cur.execute(
                    "UPDATE hk_daily_price SET "
                    "amount = COALESCE(%s, amount), turnover = COALESCE(%s, turnover) "
                    "WHERE stock_code = %s AND trade_date = %s",
                    (None if pd.isna(amount) else float(amount),
                     None if pd.isna(turnover) else float(turnover),
                     r.stock_code, row.trade_date))
                n += cur.rowcount
    conn.commit()
    c.mark_progress(conn, TASK, r.stock_code, None, "done", f"filled={n}")
    c.log.info("  %s: 补列 %d 行", r.stock_code, n)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--reset", action="store_true")
    args = ap.parse_args()

    conn = c.get_conn()
    try:
        if args.reset:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM etl_progress WHERE task=%s", (TASK,))
            conn.commit()
        stocks = pd.read_sql(
            "SELECT stock_code, symbol FROM hk_stock_basic ORDER BY stock_code", conn)
        conn.commit()
        if args.limit:
            stocks = stocks.head(args.limit)
        done = c.get_done_codes(conn, TASK)
        todo = [r for r in stocks.itertuples(index=False) if r.stock_code not in done]
        c.log.info("港股成交额/换手率回填:待处理 %d 只(已完成 %d,并发 %d)",
                   len(todo), len(done), args.workers)
        c.run_stock_todo(todo, TASK, load_one, args.workers, max_consecutive_errors=15)
        c.log.info("回填完成 ✅")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
