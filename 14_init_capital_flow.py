"""
14_init_capital_flow.py — 个股资金流入库(富途,A股全市场,断点续传)。

背景见 13_schema_capital_flow.sql:板块资金流由个股聚合,富途历史仅滚动一年。
富途节流是进程级(common._futu_call),单进程顺序拉取即可,勿开多进程。

用法:
  ASTOCK_DB_USER=zhu .venv/bin/python 14_init_capital_flow.py            # 全量(断点续传)
  ASTOCK_DB_USER=zhu .venv/bin/python 14_init_capital_flow.py --limit 10 # 试跑
  ASTOCK_DB_USER=zhu .venv/bin/python 14_init_capital_flow.py --codes 300308.SZ,000001.SZ
  ASTOCK_DB_USER=zhu .venv/bin/python 14_init_capital_flow.py --update   # 日增量(cron 用,
                                                              # 忽略断点全量重拉,upsert 幂等)
  ASTOCK_DB_USER=zhu .venv/bin/python 14_init_capital_flow.py --reset
"""

from __future__ import annotations

import argparse
import sys

import common as c

TASK = "init_capital_flow"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--codes", type=str, default=None, help="逗号分隔的指定代码")
    ap.add_argument("--update", action="store_true", help="日增量:忽略断点,全部重拉")
    ap.add_argument("--reset", action="store_true")
    args = ap.parse_args()

    conn = c.get_conn()
    try:
        if args.reset:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM etl_progress WHERE task = %s", (TASK,))
            conn.commit()
            c.log.info("已清空 %s 进度", TASK)
        with conn.cursor() as cur:
            cur.execute("SELECT stock_code FROM stock_basic ORDER BY stock_code")
            codes = [r[0] for r in cur.fetchall()]
        if args.codes:
            codes = [x.strip() for x in args.codes.split(",") if x.strip()]
        if args.limit:
            codes = codes[: args.limit]
        done = set() if (args.update or args.codes) else c.get_done_codes(conn, TASK)
        todo = [x for x in codes if x not in done]
        c.log.info("资金流:待处理 %d / 共 %d(已完成 %d)", len(todo), len(codes), len(done))

        n_ok = n_empty = n_err = 0
        for i, code in enumerate(todo, 1):
            try:
                df = c.fetch_capital_flow(code)
                n = c.upsert_capital_flow(conn, code, df)
                if n:
                    n_ok += 1
                else:
                    n_empty += 1
                if not (args.update or args.codes):
                    c.mark_progress(conn, TASK, code, None, "done", f"rows={n}")
            except Exception as exc:  # noqa: BLE001 — 北交所等无行情代码,非致命
                n_err += 1
                c.log.warning("%s 资金流失败(跳过): %s", code, str(exc)[:120])
                if not (args.update or args.codes):
                    c.mark_progress(conn, TASK, code, None, "error", str(exc)[:200])
            if i % 200 == 0:
                c.log.info("进度 %d/%d(成功 %d 空 %d 失败 %d)", i, len(todo), n_ok, n_empty, n_err)
        c.log.info("完成:成功 %d / 空 %d / 失败 %d", n_ok, n_empty, n_err)
    finally:
        c.close_futu()
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
