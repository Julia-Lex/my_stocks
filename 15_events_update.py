"""
15_events_update.py — 事件类数据每日增量(龙虎榜/业绩预告/快报/北向)。

设计: docs/superpowers/specs/2026-07-11-events-pack-design.md
  * 龙虎榜:回看最近 5 个交易日,补 etl_progress 无记录或标 error 的日子(幂等)。
  * 预告/快报:披露季(1/2/4/7/8/10 月)每日核查最近 2 个报告期;平季 7 天门控
    (哨兵 task='daily_events', stock_code='_check')。
  * 北向:全域逐股增量(从库内该股 max(trade_date) 起;非陆股通标的空返回秒过);
    量大,与预告/快报共用披露季/门控节奏(北向本质日频,7 天粒度可接受——
    每次核查整段补齐,无数据损失)。
  * alias 周报:etl_progress 中连续 error 的股票汇总告警(人工处置,不自动改域)。

cron(README 同步): 0 19 * * 1-5
用法: python 15_events_update.py [--force] [--limit N] [--workers N]
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime

import pandas as pd

import common as c
import importlib

_m14 = importlib.import_module("14_init_events")

TASK = "daily_events"
_SEASON_MONTHS = {1, 2, 4, 7, 8, 10}


def _due(conn, force: bool) -> bool:
    if force or date.today().month in _SEASON_MONTHS:
        return True
    with conn.cursor() as cur:
        cur.execute("SELECT last_date FROM etl_progress WHERE task=%s AND stock_code='_check'",
                    (TASK,))
        row = cur.fetchone()
    conn.commit()
    if row is None or row[0] is None:
        return True
    return (date.today() - row[0]).days >= 7


def update_lhb(conn) -> None:
    """回看最近 5 个交易日,重放 init 的按日逻辑(进度键幂等,error 日自动重试)。"""
    with conn.cursor() as cur:
        cur.execute("SELECT trade_date FROM trade_calendar WHERE is_open AND trade_date <= %s "
                    "ORDER BY trade_date DESC LIMIT 5", (date.today(),))
        days = [r[0] for r in cur.fetchall()]
        # 清掉这 5 天的 done 标记强制重拉(收盘后榜单可能补充修订;upsert 幂等)
        keys = [f"{d.strftime('%Y%m%d')}:lhb" for d in days]
        cur.execute("DELETE FROM etl_progress WHERE task=%s AND stock_code = ANY(%s)",
                    (_m14.TASK_CROSS, keys))
    conn.commit()
    _m14.LHB_START  # noqa: B018 — 仅提示复用来源
    # 借用 init 的循环:临时把起点视角交给进度表(done 已清,只会拉这 5 天)
    with conn.cursor() as cur:
        cur.execute("SELECT trade_date FROM trade_calendar WHERE is_open AND trade_date = ANY(%s)",
                    (days,))
    _init_lhb_days(conn, days)


def _init_lhb_days(conn, days: list[date]) -> None:
    done = c.get_done_codes(conn, _m14.TASK_CROSS)
    todo = [d for d in sorted(days) if f"{d.strftime('%Y%m%d')}:lhb" not in done]
    for d in todo:
        ds = d.strftime("%Y%m%d")
        key = f"{ds}:lhb"
        try:
            df = c.fetch_lhb(ds, ds)
            rows = [(r.stock_code, d, _m14._s(getattr(r, "reason", None), 128) or "未知",
                     _m14._f(getattr(r, "close", None)), _m14._f(getattr(r, "pct_chg", None)),
                     _m14._f(getattr(r, "net_buy", None)), _m14._f(getattr(r, "buy_amount", None)),
                     _m14._f(getattr(r, "sell_amount", None)),
                     _m14._s(getattr(r, "interpret", None), 256))
                    for r in df.itertuples(index=False)]
            seen, dedup = set(), []
            for row in rows:
                k = (row[0], row[2])
                if k not in seen:
                    seen.add(k)
                    dedup.append(row)
            n = c.upsert(conn, "lhb_detail",
                         ["stock_code", "trade_date", "reason", "close", "pct_chg",
                          "net_buy", "buy_amount", "sell_amount", "interpret"],
                         dedup, ["stock_code", "trade_date", "reason"]) if dedup else 0
            c.mark_progress(conn, _m14.TASK_CROSS, key, d, "done", f"rows={n}")
            c.log.info("  龙虎榜 %s: %d 行", ds, n)
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            c.mark_progress(conn, _m14.TASK_CROSS, key, d, "error", str(exc))
            c.log.error("  龙虎榜 %s 失败: %s", ds, exc)


def update_cross_recent(conn) -> None:
    """预告/快报:重拉最近 2 个报告期(清 done 标记强制刷新,upsert 幂等)。"""
    periods = c.quarter_ends(c.FUND_START, date.today())[-2:]
    keys = [f"{p.strftime('%Y%m%d')}:{k}" for p in periods for k in ("yjyg", "yjkb")]
    with conn.cursor() as cur:
        cur.execute("DELETE FROM etl_progress WHERE task=%s AND stock_code = ANY(%s)",
                    (_m14.TASK_CROSS, keys))
    conn.commit()
    _m14.init_cross(conn)  # done 已清的期会重拉,其余秒过


def update_nb(conn, workers: int, limit: int | None) -> None:
    """北向:清 done 全域重查(fetch 为全序列拉取,upsert 幂等;非标的空返回秒过)。"""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM etl_progress WHERE task=%s", (_m14.TASK_NB,))
    conn.commit()
    _m14.init_nb(conn, workers, limit)


def alias_report(conn) -> None:
    """连续 error 股票周报(改码嫌疑,人工处置)。"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT task, stock_code, left(message, 60) FROM etl_progress "
            "WHERE status='error' AND task LIKE 'init_fund%%' "
            "AND stock_code NOT LIKE '%%:%%' "   # 排除 'YYYYMMDD:kind' 复合键(截面期误报)
            "ORDER BY task, stock_code LIMIT 20")
        rows = cur.fetchall()
    conn.commit()
    if rows:
        c.log.warning("疑似改码/退市股票(基本面任务持续 error,%d 只,人工核查 stock_alias):", len(rows))
        for t, sc, msg in rows:
            c.log.warning("  %s | %s | %s", t, sc, msg)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--workers", type=int, default=3)
    args = ap.parse_args()

    conn = c.get_conn()
    try:
        c.log.info("=== 龙虎榜增量(近 5 交易日) ===")
        update_lhb(conn)

        if _due(conn, args.force):
            c.log.info("=== 预告/快报核查(最近 2 期) ===")
            update_cross_recent(conn)
            c.log.info("=== 北向持股全域刷新 ===")
            update_nb(conn, args.workers, args.limit)
            alias_report(conn)
            c.mark_progress(conn, TASK, "_check", date.today(), "done", "full check")
        else:
            c.log.info("平季且距上次核查不足 7 天,预告/快报/北向跳过(--force 强制)")

        c.log.info("事件增量完成 ✅ (%s)", datetime.now().strftime("%Y-%m-%d %H:%M"))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
