"""
14_init_events.py — 事件类数据全量初始化(业绩预告/快报、龙虎榜、北向持股)。

设计: docs/superpowers/specs/2026-07-11-events-pack-design.md
  * 预告/快报:按报告期截面循环(2015Q4~今,~43 期 × 2 接口),etl_progress 借存
    'YYYYMMDD:yjyg' / 'YYYYMMDD:yjkb'(task='init_events_cross'),同二期阶段1模式。
  * 龙虎榜:按交易日循环 2016-01-01~今(查 trade_calendar,~2,550 天),借存
    'YYYYMMDD:lhb';串行 + with_retry 自带退避,批间 0.3s 轻节流。
  * 北向:逐股序列(沪深港通标的才有数据,非标的空返回),task='init_events_nb',
    run_stock_todo 并行 + 熔断;历史深度按源所及(实测 ~7 年)。

用法:
  python 14_init_events.py --part all                # 顺序全跑
  python 14_init_events.py --part lhb                # 单块
  python 14_init_events.py --part nb --workers 3 --limit 20
  python 14_init_events.py --part cross --reset      # 清进度重来
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date

import pandas as pd

import common as c

TASK_CROSS = "init_events_cross"
TASK_NB = "init_events_nb"
LHB_START = date(2016, 1, 1)


def _f(v):
    if v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _s(v, maxlen: int | None = None):
    """安全字符串:None/NaN→None;剔除 NUL(0x00,源数据实测存在,psycopg2 拒收);截断。"""
    if v is None or pd.isna(v):
        return None
    s = str(v).replace("\x00", "").strip()
    if not s or s == "nan":
        return None
    return s[:maxlen] if maxlen else s


def init_cross(conn) -> None:
    """业绩预告 + 快报:按报告期截面。"""
    periods = c.quarter_ends(c.FUND_START, date.today())
    done = c.get_done_codes(conn, TASK_CROSS)
    for p in periods:
        ps = p.strftime("%Y%m%d")
        for kind, fetch in (("yjyg", c.fetch_yjyg), ("yjkb", c.fetch_yjkb)):
            key = f"{ps}:{kind}"
            if key in done:
                continue
            try:
                df = fetch(ps)
            except Exception as exc:  # noqa: BLE001
                c.mark_progress(conn, TASK_CROSS, key, p, "error", str(exc))
                c.log.error("  截面 %s 失败: %s", key, exc)
                continue
            if kind == "yjyg":
                rows = [(r.stock_code, p, _s(getattr(r, "forecast_type", None), 64) or "未知",
                         getattr(r, "ann_date", None), _s(getattr(r, "change_desc", None)),
                         _f(getattr(r, "forecast_value", None)), _f(getattr(r, "change_pct", None)),
                         _s(getattr(r, "reason", None)))
                        for r in df.itertuples(index=False)]
                # 同期同股同指标去重(源偶有重复行,保留首行)
                seen, dedup = set(), []
                for row in rows:
                    k = (row[0], row[2])
                    if k not in seen:
                        seen.add(k)
                        dedup.append(row)
                n = c.upsert(conn, "fin_forecast",
                             ["stock_code", "report_date", "forecast_type", "ann_date",
                              "change_desc", "forecast_value", "change_pct", "reason"],
                             dedup, ["stock_code", "report_date", "forecast_type"])
            else:
                rows = [(r.stock_code, p, getattr(r, "ann_date", None),
                         _f(getattr(r, "eps", None)), _f(getattr(r, "revenue", None)),
                         _f(getattr(r, "revenue_yoy", None)), _f(getattr(r, "net_profit", None)),
                         _f(getattr(r, "net_profit_yoy", None)), _f(getattr(r, "bps", None)),
                         _f(getattr(r, "roe", None)))
                        for r in df.itertuples(index=False)]
                seen, dedup = set(), []
                for row in rows:
                    if row[0] not in seen:
                        seen.add(row[0])
                        dedup.append(row)
                n = c.upsert(conn, "fin_express",
                             ["stock_code", "report_date", "ann_date", "eps", "revenue",
                              "revenue_yoy", "net_profit", "net_profit_yoy", "bps", "roe"],
                             dedup, ["stock_code", "report_date"])
            c.mark_progress(conn, TASK_CROSS, key, p, "done", f"rows={n}")
            c.log.info("  截面 %s: %d 行", key, n)


def init_lhb(conn) -> None:
    """龙虎榜:按交易日循环(缺口日=进度表无记录的开市日)。"""
    with conn.cursor() as cur:
        cur.execute("SELECT trade_date FROM trade_calendar "
                    "WHERE is_open AND trade_date >= %s AND trade_date <= %s ORDER BY trade_date",
                    (LHB_START, date.today()))
        days = [r[0] for r in cur.fetchall()]
    conn.commit()
    done = c.get_done_codes(conn, TASK_CROSS)
    todo = [d for d in days if f"{d.strftime('%Y%m%d')}:lhb" not in done]
    c.log.info("龙虎榜:待处理 %d 个交易日(共 %d)", len(todo), len(days))
    for i, d in enumerate(todo, 1):
        ds = d.strftime("%Y%m%d")
        key = f"{ds}:lhb"
        try:
            df = c.fetch_lhb(ds, ds)
            rows = [(r.stock_code, d, _s(getattr(r, "reason", None), 128) or "未知",
                     _f(getattr(r, "close", None)), _f(getattr(r, "pct_chg", None)),
                     _f(getattr(r, "net_buy", None)), _f(getattr(r, "buy_amount", None)),
                     _f(getattr(r, "sell_amount", None)),
                     _s(getattr(r, "interpret", None), 256))
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
            c.mark_progress(conn, TASK_CROSS, key, d, "done", f"rows={n}")
            if i % 100 == 0:
                c.log.info("  龙虎榜进度 %d / %d(%s)", i, len(todo), ds)
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            c.mark_progress(conn, TASK_CROSS, key, d, "error", str(exc))
            c.log.error("  龙虎榜 %s 失败: %s", ds, exc)
        time.sleep(0.3)


def init_nb(conn, workers: int, limit: int | None) -> None:
    """北向持股:逐股序列(A 股全域,非陆股通标的空返回秒过)。"""
    stocks = pd.read_sql("SELECT stock_code, symbol FROM stock_basic ORDER BY stock_code", conn)
    conn.commit()
    if limit:
        stocks = stocks.head(limit)
    done = c.get_done_codes(conn, TASK_NB)
    todo = [r for r in stocks.itertuples(index=False) if r.stock_code not in done]
    c.log.info("北向持股:待处理 %d 只(已完成 %d,并发 %d)", len(todo), len(done), workers)

    def load(conn2, r):
        df = c.fetch_nb_hold(r.symbol)
        n = 0
        if not df.empty:
            rows = [(r.stock_code, row.trade_date,
                     int(row.hold_shares) if not pd.isna(row.hold_shares) else None,
                     _f(getattr(row, "hold_value", None)), _f(getattr(row, "hold_ratio", None)))
                    for row in df.itertuples(index=False)]
            n = c.upsert(conn2, "nb_hold",
                         ["stock_code", "trade_date", "hold_shares", "hold_value", "hold_ratio"],
                         rows, ["stock_code", "trade_date"])
        c.mark_progress(conn2, TASK_NB, r.stock_code, None, "done", f"nb={n}")

    c.run_stock_todo(todo, TASK_NB, load, workers, max_consecutive_errors=15)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--part", default="all", choices=("cross", "lhb", "nb", "all"))
    ap.add_argument("--workers", type=int, default=2, help="仅 nb 部分使用")
    ap.add_argument("--limit", type=int, default=None, help="仅 nb 部分使用(试跑)")
    ap.add_argument("--reset", action="store_true")
    args = ap.parse_args()

    conn = c.get_conn()
    try:
        if args.reset:
            # cross 与 lhb 共用 task 名但键后缀不同(:yjyg/:yjkb vs :lhb),按后缀限定
            with conn.cursor() as cur:
                if args.part in ("cross", "all"):
                    cur.execute("DELETE FROM etl_progress WHERE task=%s AND "
                                "(stock_code LIKE '%%:yjyg' OR stock_code LIKE '%%:yjkb')",
                                (TASK_CROSS,))
                if args.part in ("lhb", "all"):
                    cur.execute("DELETE FROM etl_progress WHERE task=%s AND stock_code LIKE '%%:lhb'",
                                (TASK_CROSS,))
                if args.part in ("nb", "all"):
                    cur.execute("DELETE FROM etl_progress WHERE task=%s", (TASK_NB,))
            conn.commit()
            c.log.info("已清空 %s 进度(按键前缀限定)", args.part)

        if args.part in ("cross", "all"):
            c.log.info("=== 业绩预告/快报截面 ===")
            init_cross(conn)
        if args.part in ("lhb", "all"):
            c.log.info("=== 龙虎榜 ===")
            init_lhb(conn)
        if args.part in ("nb", "all"):
            c.log.info("=== 北向持股 ===")
            init_nb(conn, args.workers, args.limit)
        c.log.info("事件数据初始化完成 ✅")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
