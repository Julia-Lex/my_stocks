"""
26_amount_backfill.py — 回填 daily_price.amount 缺口(新浪源,断点续传)。

背景:走过腾讯兜底(ASTOCK_ASHARE_SOURCE=tx)的行 amount 为空(腾讯 K 线不带成交额)。
本脚本按"有缺口的股票"逐只取新浪成交额,只 UPDATE amount IS NULL 的行(不碰
volume/价格)。仅回填,不新增行。

选用新浪而非东财:新浪 stock_zh_a_daily 直接给成交额,口径与东财到元一致
(2026-07-13 实测),覆盖老历史与北交所,且不受东财行情族封禁影响——可随时运行。

用法:
  ASTOCK_DB_USER=zhu .venv/bin/python 26_amount_backfill.py            # 全量缺口(断点续传)
  ASTOCK_DB_USER=zhu .venv/bin/python 26_amount_backfill.py --start 2023-01-01  # 仅该日期起
  ASTOCK_DB_USER=zhu .venv/bin/python 26_amount_backfill.py --limit 20 # 试跑
  ASTOCK_DB_USER=zhu .venv/bin/python 26_amount_backfill.py --reset
"""

from __future__ import annotations

import argparse
import sys
import time

import common as c

TASK = "amount_backfill"
_SINA_INTERVAL = 0.35   # 新浪 WAF 保守节流


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2015-01-01", help="只回填该日期起的缺口(默认 2015)")
    ap.add_argument("--limit", type=int, default=None, help="只处理前 N 只(试跑)")
    ap.add_argument("--reset", action="store_true")
    args = ap.parse_args()

    conn = c.get_conn()
    try:
        if args.reset:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM etl_progress WHERE task = %s", (TASK,))
            conn.commit()
            c.log.info("已清空 %s 进度", TASK)

        # 有 amount 缺口的股票 + 其缺口日期区间(缩小每只的取数窗口)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT stock_code, min(trade_date), max(trade_date), count(*) "
                "FROM daily_price WHERE amount IS NULL AND trade_date >= %s "
                "GROUP BY stock_code ORDER BY stock_code", (args.start,))
            gaps = cur.fetchall()
        done = c.get_done_codes(conn, TASK)
        todo = [g for g in gaps if g[0] not in done]
        if args.limit:
            todo = todo[: args.limit]
        total_missing = sum(g[3] for g in todo)
        c.log.info("amount 回填(新浪源):%d 只待处理(缺口 %d 行;已完成 %d 只)",
                   len(todo), total_missing, len(done))

        n_ok = n_upd = n_err = 0
        consecutive_errors = 0
        for i, (code, gmin, gmax, cnt) in enumerate(todo, 1):
            symbol = code.split(".")[0]
            try:
                time.sleep(_SINA_INTERVAL)
                df = c.fetch_amount_sina(symbol, start=gmin.strftime("%Y%m%d"),
                                         end=gmax.strftime("%Y%m%d"))
                consecutive_errors = 0
                if df.empty:
                    c.mark_progress(conn, TASK, code, gmax, "done", "src_empty")
                    continue
                rows = [(r.trade_date, float(r.amount)) for r in df.itertuples(index=False)]
                with conn.cursor() as cur:
                    cur.executemany(
                        "UPDATE daily_price SET amount = %s "
                        "WHERE stock_code = %s AND trade_date = %s AND amount IS NULL",
                        [(amt, code, td) for td, amt in rows])
                    upd = cur.rowcount
                conn.commit()
                n_ok += 1
                n_upd += upd
                c.mark_progress(conn, TASK, code, gmax, "done", f"filled={upd}")
                if i % 100 == 0:
                    c.log.info("进度 %d/%d(成功 %d,已补 %d 行)", i, len(todo), n_ok, n_upd)
            except Exception as exc:  # noqa: BLE001
                conn.rollback()
                n_err += 1
                consecutive_errors += 1
                c.mark_progress(conn, TASK, code, None, "error", str(exc)[:200])
                c.log.error("  %s 失败: %s", code, str(exc)[:120])
                if consecutive_errors >= 20:
                    c.log.critical("连续 %d 只失败,疑似新浪限流,提前终止(已补 %d 行,断点续传补齐)",
                                   consecutive_errors, n_upd)
                    break
        c.log.info("完成:成功 %d / 失败 %d,共补 amount %d 行", n_ok, n_err, n_upd)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
