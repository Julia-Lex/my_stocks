"""
13_board_update.py — 板块每日增量(cron 18:10,排在 03 日线 18:00 之后)。

流程:
  1. 刷新板块列表:新板块 → 插入并补拉全历史(复用 12 的 load_one_board);
     改名 → upsert 覆盖 board_name;从列表消失 → is_active=false(数据保留);
     列表数量异常(行业<50 或 概念<200)→ 判定源故障,直接退出不做任何 diff。
  2. 逐 active 板块:日K自 max(trade_date)+1 增量;资金流全拉幂等覆盖;
     成分 diff(接口失败/空返回则跳过该板块成分,宁可不更新不误判全员移出)。
  3. 进度:etl_progress task='daily_board',按板块记 done/error。
"""

from __future__ import annotations

import sys
from datetime import timedelta
from importlib import import_module

import common as c

init_board = import_module("12_init_board")
TASK = "daily_board"


def refresh_board_list(conn) -> list:
    boards = c.fetch_board_list()
    n_ind = (boards.board_type == "industry").sum()
    n_con = (boards.board_type == "concept").sum()
    if n_ind < 50 or n_con < 200:
        c.log.critical("板块列表数量异常(行业 %d / 概念 %d),疑似源故障,本次退出", n_ind, n_con)
        return []
    with conn.cursor() as cur:
        cur.execute("SELECT board_code FROM board")
        known = {r[0] for r in cur.fetchall()}
        listed = set(boards.board_code)
        gone = sorted(known - listed)
        if gone:
            cur.execute("UPDATE board SET is_active = FALSE, updated_at = now() "
                        "WHERE board_code = ANY(%s)", (gone,))
            c.log.info("板块退场 %d 个: %s", len(gone), gone[:10])
        cur.execute("UPDATE board SET is_active = TRUE, updated_at = now() "
                    "WHERE board_code = ANY(%s) AND NOT is_active", (sorted(listed),))
    conn.commit()
    init_board.upsert_board_list(conn)   # 幂等:改名覆盖 + 新板块插入
    new = sorted(listed - known)
    rows = [init_board.BoardRow(r.board_code, r.board_name, r.board_type)
            for r in boards.itertuples(index=False)]
    if new:
        c.log.info("新板块 %d 个,补拉全历史: %s", len(new), new)
        for r in [x for x in rows if x.stock_code in new]:
            init_board.load_one_board(conn, r)
    return [x for x in rows if x.stock_code not in new]


def update_one_board(conn, r) -> None:
    code = r.stock_code
    with conn.cursor() as cur:
        cur.execute("SELECT max(trade_date) FROM board_daily WHERE board_code = %s", (code,))
        max_d = cur.fetchone()[0]
    start = (max_d + timedelta(days=1)).strftime("%Y%m%d") if max_d else "19900101"
    n_d = c.upsert_board_daily(conn, code, c.fetch_board_daily(r.board_name, r.board_type, start=start))
    n_f = c.upsert_board_fund_flow(conn, code, c.fetch_board_fund_flow(r.board_name, r.board_type))
    cons = c.fetch_board_cons(code, r.board_type)
    if cons:
        n_o, n_c = c.sync_board_members(conn, code, cons, c.beijing_now().date())
    else:
        n_o = n_c = 0
        c.log.warning("  %s: 成分为空,跳过成分 diff", code)
    last = max_d if n_d == 0 else None   # 简化:成功即记 done,last_date 仅参考
    c.mark_progress(conn, TASK, code, last, "done", f"daily=+{n_d},flow={n_f},cons=+{n_o}/-{n_c}")
    if n_d or n_o or n_c:
        c.log.info("  %s %s: 日线 +%d / 成分 +%d/-%d", code, r.board_name, n_d, n_o, n_c)


def main() -> int:
    conn = c.get_conn()
    try:
        rows = refresh_board_list(conn)
        if not rows:
            return 1
        with conn.cursor() as cur:   # 只更新 active 板块
            cur.execute("SELECT board_code FROM board WHERE is_active")
            active = {r[0] for r in cur.fetchall()}
        todo = [r for r in rows if r.stock_code in active]
        c.log.info("板块增量:%d 个(并发 3)", len(todo))
        conn.commit()
        c.run_stock_todo(todo, TASK, update_one_board, 3, max_consecutive_errors=15)
        c.log.info("板块增量完成 ✅")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
