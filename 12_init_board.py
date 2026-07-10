"""
12_init_board.py — 板块数据层全量初始化(行业+概念,断点续传)。

流程:
  1. 板块列表 → board(幂等 upsert);
  2. 逐板块:日K全历史 → board_daily(收盘防护),资金流全历史 → board_fund_flow,
     当前成分 → board_member 开区间(valid_from=今天,观测起点语义见 schema 注释);
  3. 断点续传:etl_progress task='init_board',stock_code 字段借存 board_code。

用法:
  ASTOCK_DB_USER=zhu .venv/bin/python 12_init_board.py --workers 3
  ASTOCK_DB_USER=zhu .venv/bin/python 12_init_board.py --limit 5   # 试跑
  ASTOCK_DB_USER=zhu .venv/bin/python 12_init_board.py --reset

请求量 ≈ 板块数×3 ≈ 1600 次,3 并发约 15-20 分钟。全部为东财行情族接口,
限流特征见 memory eastmoney-rate-limit;遇熔断等冷却后重跑即续传。
"""

from __future__ import annotations

import argparse
import sys
from collections import namedtuple

import common as c

TASK = "init_board"
BoardRow = namedtuple("BoardRow", "stock_code board_name board_type")  # stock_code=board_code


def upsert_board_list(conn):
    boards = c.fetch_board_list()
    n = c.upsert(conn, "board",
                 ["board_code", "board_name", "board_type"],
                 [(r.board_code, r.board_name, r.board_type)
                  for r in boards.itertuples(index=False)],
                 ["board_code"], update_cols=["board_name", "board_type"])
    c.log.info("板块列表 %d 个(行业 %d / 概念 %d)", n,
               (boards.board_type == "industry").sum(), (boards.board_type == "concept").sum())
    return boards


def load_one_board(conn, r: BoardRow) -> None:
    """单板块全量:日K + 资金流 + 当前成分。r.stock_code 即 board_code。"""
    code = r.stock_code
    n_d = c.upsert_board_daily(conn, code, c.fetch_board_daily(r.board_name, r.board_type))
    n_f = c.upsert_board_fund_flow(conn, code, c.fetch_board_fund_flow(r.board_name, r.board_type))
    cons = c.fetch_board_cons(code, r.board_type)
    today = c.beijing_now().date()
    n_o, n_c = c.sync_board_members(conn, code, cons, today) if cons else (0, 0)
    if not cons:
        c.log.warning("  %s %s: 成分为空,跳过成分同步", code, r.board_name)
    c.mark_progress(conn, TASK, code, None, "done", f"daily={n_d},flow={n_f},cons=+{n_o}/-{n_c}")
    c.log.info("  %s %s: 日线 %d / 资金流 %d / 成分 %d", code, r.board_name, n_d, n_f, n_o)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--limit", type=int, default=None, help="只处理前 N 个板块(试跑)")
    ap.add_argument("--reset", action="store_true", help="清空 init_board 进度重来")
    args = ap.parse_args()

    conn = c.get_conn()
    try:
        if args.reset:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM etl_progress WHERE task = %s", (TASK,))
            conn.commit()
            c.log.info("已清空 %s 进度", TASK)

        boards = upsert_board_list(conn)
        if args.limit:
            boards = boards.head(args.limit)
        done = c.get_done_codes(conn, TASK)
        todo = [BoardRow(r.board_code, r.board_name, r.board_type)
                for r in boards.itertuples(index=False) if r.board_code not in done]
        c.log.info("待处理 %d 个板块(已完成 %d,并发 %d)", len(todo), len(done), args.workers)
        conn.commit()

        c.run_stock_todo(todo, TASK, load_one_board, args.workers, max_consecutive_errors=15)
        c.log.info("板块全量初始化完成 ✅")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
