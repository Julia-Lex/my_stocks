"""
13_fundamental_update_intl.py — 港股/美股基本面每日门控增量(富途主源)。

设计(镜像 10_fundamental_update.py 的门控机制,港美披露为半年/季度节奏,
7 天门控等效周检,cron 可每日挂但平日秒退):
  * 哨兵 (task=f"daily_fund_{market}", stock_code="_check") 的 last_date 距今 <7 天
    且未 --force 时直接退出。
  * 到期核查:对全部股票重拉「最近窗口」——富途三表 + 关键指标各取首页(num=8,
    覆盖最近 ~2 年报告期),upsert 覆盖(报表 data 覆盖、指标经 COALESCE 保增补,
    复用 12 的写库函数);美股随后重拉东财 ann_date + 指标增补(护栏/容差 join
    同 12 阶段B);港股 ann_date 已知不可得,跳过。
  * per-stock 进度 task=f"daily_fund_{market}_stk"(阶段隔离教训,见二期 F4)。

用法:
  python 13_fundamental_update_intl.py --market hk [--workers 2] [--limit N] [--force]
cron(北京时间,港美收盘节奏不敏感,挂在基本面 18:40 之后):
  50 18 * * 1-5  --market hk    ;    55 18 * * 1-5  --market us
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime

import pandas as pd

import common as c

# 复用 12 的写库/回填函数(文件名数字开头,走 importlib;12 顶层无副作用)
import importlib

_m12 = importlib.import_module("12_init_fundamental_intl")

_RECENT_NUM = 8  # 富途首页条数:覆盖最近 ~2 年报告期(FY/H1/Q 累计混合)


def _due(conn, task: str, force: bool) -> bool:
    """7 天门控:读哨兵 (task, '_check') 的 last_date。"""
    if force:
        return True
    with conn.cursor() as cur:
        cur.execute("SELECT last_date FROM etl_progress WHERE task=%s AND stock_code='_check'",
                    (task,))
        row = cur.fetchone()
    conn.commit()
    if row is None or row[0] is None:
        return True
    return (date.today() - row[0]).days >= 7


def _fetch_recent_reports(code: str, stype: int) -> list[dict]:
    """只取富途首页(num=_RECENT_NUM),不分页——增量只关心最近披露。"""
    d = c._futu_call("get_financials_statements", code,
                     statement_type=stype, num=_RECENT_NUM)
    return d.get("report_list", []) or []


def make_updater(market: str, task_stk: str):
    p = c.MARKETS[market]["prefix"]

    def update_one(conn, r):
        code = c.futu_code(r.stock_code)
        n_stmt = 0
        for st, stype in c._FUTU_STMT_TYPE.items():
            df = c._futu_reports_to_df(_fetch_recent_reports(code, stype))
            if df.empty:
                continue
            rows = [(r.stock_code, row.report_date, st, row.currency, _m12._to_jsonb(row.data))
                    for row in df.itertuples(index=False)]
            n_stmt += c.upsert(conn, f"{p}fin_statement",
                               ["stock_code", "report_date", "stmt_type", "currency", "data"],
                               rows, ["stock_code", "report_date", "stmt_type"],
                               update_cols=["data", "currency"])

        n_ind = 0
        ind_reports = _fetch_recent_reports(code, c._FUTU_INDICATOR_TYPE)
        # 复用 common._futu_indicator_reports_to_df(2026-07-11 最终审查 M3 抽出的模块级
        # 转换):直接喂首页 reports,省下每股每周 1 次全量分页的指标请求(不必再退化到
        # fetch_intl_fund_indicator 的全量翻页路径)。
        ind = c._futu_indicator_reports_to_df(r.stock_code, ind_reports)
        if not ind.empty:
            cols = ["stock_code", "report_date", "currency"] + c._FUND_INDICATOR_COLS
            rows = []
            for row in ind.itertuples(index=False):
                vals = [r.stock_code, row.report_date, row.currency]
                for col in c._FUND_INDICATOR_COLS:
                    v, _clipped = _m12._clean_num(getattr(row, col), col)
                    vals.append(v)
                rows.append(tuple(vals))
            n_ind = _m12._upsert_indicator_keep_supplement(conn, f"{p}fin_indicator", cols, rows)

        # 美股:ann_date + 指标增补(护栏/容差 join 复用 12 阶段B 实现)
        n_ann = 0
        if market == "us":
            df_ann, _dropped = _m12._fetch_us_ann_and_indicators(r.symbol)
            if not df_ann.empty:
                n_ann, _ = _m12._apply_ann_us(conn, r.stock_code, df_ann)

        c.mark_progress(conn, task_stk, r.stock_code, None, "done",
                        f"stmt={n_stmt},ind={n_ind},ann={n_ann}")

    return update_one


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", required=True, choices=("hk", "us"))
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--force", action="store_true", help="忽略 7 天门控立即核查")
    args = ap.parse_args()

    market = args.market
    task = f"daily_fund_{market}"
    task_stk = f"daily_fund_{market}_stk"
    p = c.MARKETS[market]["prefix"]

    conn = c.get_conn()
    try:
        if not _due(conn, task, args.force):
            c.log.info("[%s] 距上次基本面核查不足 7 天,本次跳过(--force 可强制)", market)
            return 0

        stocks = pd.read_sql(
            f"SELECT stock_code, symbol FROM {p}stock_basic ORDER BY stock_code", conn)
        conn.commit()
        if args.limit:
            stocks = stocks.head(args.limit)

        rows = list(stocks.itertuples(index=False))
        # 断点续传(2026-07-11 最终审查 M3):不再"跑前先清空" per-stock 进度 —— 那样一旦
        # 跑到一半崩溃(富途网关断线/东财熔断等),本轮已完成的股票也会在下次重试时被当
        # 成没跑过,白白重复请求。改为 get_done_codes 过滤 todo,只处理本轮尚未标记 done
        # 的股票;全部跑完后才清空 task_stk 进度,好让下一次到期核查(7 天后)重新全量核查
        # (而不是被上一轮的 done 记录挡住,永远查不到新披露)。
        done = c.get_done_codes(conn, task_stk)
        todo = [r for r in rows if r.stock_code not in done]
        c.log.info("[%s] 基本面增量核查 %d 只(最近 %d 期窗口,待处理 %d/已完成 %d,并发 %d)...",
                   market, len(rows), _RECENT_NUM, len(todo), len(done), args.workers)

        c.run_stock_todo(todo, task_stk, make_updater(market, task_stk), args.workers,
                         max_consecutive_errors=15)

        # 本轮全部完成:清空 per-stock 进度供下一到期周期重新全量核查
        with conn.cursor() as cur:
            cur.execute("DELETE FROM etl_progress WHERE task=%s", (task_stk,))
        conn.commit()

        c.mark_progress(conn, task, "_check", date.today(), "done",
                        f"checked={len(rows)}")
        c.log.info("[%s] 基本面增量完成 ✅ (%s)", market,
                   datetime.now().strftime("%Y-%m-%d %H:%M"))
        return 0
    finally:
        c.close_futu()
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
