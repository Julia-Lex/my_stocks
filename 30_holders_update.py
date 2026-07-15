"""
30_holders_update.py — 股权结构层增量/回填(东财 f10,断点续传)。见 29_schema_holders.sql。

移交清单 #8:补十大股东(控盘度)/十大流通股东(机构占比,含股东性质)/股东户数(散户结构)。
东财 datacenter 族,不在行情族封禁范围。

每股:
  - shareholder_count:单请求拉全历史户数(1 call);
  - top10_holder:最近 N 个"已披露"季末(默认 1=最新),各拉 total+float(2 call/期)。

用法:
  ASTOCK_DB_USER=zhu .venv/bin/python 30_holders_update.py --workers 3            # 全市场最新期
  ASTOCK_DB_USER=zhu .venv/bin/python 30_holders_update.py --periods 4 --workers 3  # 回填近4期
  ASTOCK_DB_USER=zhu .venv/bin/python 30_holders_update.py --limit 20             # 试跑
  ASTOCK_DB_USER=zhu .venv/bin/python 30_holders_update.py --reset
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta

import common as c

TASK = "init_holders"


def _disclosed_periods(n: int) -> list[date]:
    """最近 n 个'很可能已披露'的季末:季末日 + 45 天 <= 今天(避开当季在披露中)。"""
    today = c.beijing_now().date()
    qs = [q for q in c.quarter_ends(date(today.year - 3, 1, 1), today)
          if q + timedelta(days=45) <= today]
    return sorted(qs, reverse=True)[:n]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--periods", type=int, default=1, help="top10 回填最近 N 个已披露季末(默认 1)")
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--reset", action="store_true")
    args = ap.parse_args()

    periods = _disclosed_periods(args.periods)
    conn = c.get_conn()
    try:
        if args.reset:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM etl_progress WHERE task = %s", (TASK,))
            conn.commit()
            c.log.info("已清空 %s 进度", TASK)

        stocks = c.fetch_stock_list()
        if args.limit:
            stocks = stocks.head(args.limit)
        done = c.get_done_codes(conn, TASK)
        todo = [r for r in stocks.itertuples(index=False) if r.stock_code not in done]
        c.log.info("股权结构:待处理 %d 只(已完成 %d,并发 %d);top10 期 %s",
                   len(todo), len(done), args.workers, [str(p) for p in periods])
        conn.commit()

        def load(conn2, r):
            n_gdhs = c.upsert_shareholder_count(conn2, r.stock_code,
                                                c.fetch_shareholder_count(r.symbol))
            n_h = 0
            for p in periods:
                pstr = p.strftime("%Y%m%d")
                for kind in ("total", "float"):
                    n_h += c.upsert_top10_holders(conn2, r.stock_code, p, kind,
                                                  c.fetch_top10_holders(r.symbol, pstr, kind))
            c.mark_progress(conn2, TASK, r.stock_code, None, "done", f"gdhs={n_gdhs},top10={n_h}")

        c.run_stock_todo(todo, TASK, load, args.workers, max_consecutive_errors=15)
        c.log.info("股权结构层完成 ✅")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
