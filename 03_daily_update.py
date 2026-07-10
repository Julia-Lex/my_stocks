"""
03_daily_update.py — 每日增量更新(带自动补漏)。

设计:
  * 从库中每只股票的 max(trade_date) 之后开始增量拉取(含后复权因子)。
  * 自动补漏:对比 trade_calendar 的应有交易日与库内已有交易日,
    若发现缺口(比如某天定时任务没跑成),自动回补。
  * 更新股票列表(捕捉新上市/更名),刷新指数与物化视图。

建议收盘后(如 18:00)用 cron/计划任务每天跑一次:
  0 18 * * 1-5  cd /path/to/my_stocks && python 03_daily_update.py >> update.log 2>&1

用法:
  python 03_daily_update.py                 # 增量 + 自动补漏
  python 03_daily_update.py --days 10       # 强制回看最近 10 个自然日
  python 03_daily_update.py --no-matview    # 跳过物化视图刷新(加速)
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta

import common as c

TASK = "daily_update"


def expected_open_dates(conn, since: date) -> list[date]:
    """交易日历中 since 之后(含)、且已定盘可写入的开市日。

    上界用 safe_cutoff_date() 而非本机 date.today():盘中运行时
    今天尚未定盘,不应算作缺口,否则会白拉一遍全市场再被防护丢弃。
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT trade_date FROM trade_calendar "
            "WHERE is_open AND trade_date >= %s AND trade_date <= %s "
            "ORDER BY trade_date",
            (since, c.safe_cutoff_date()),
        )
        return [r[0] for r in cur.fetchall()]


def existing_dates(conn, stock_code: str, since: date) -> set[date]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT trade_date FROM daily_price "
            "WHERE stock_code = %s AND trade_date >= %s",
            (stock_code, since),
        )
        return {r[0] for r in cur.fetchall()}


def update_reference(conn) -> None:
    """刷新交易日历、股票列表、指数。"""
    cal = c.fetch_calendar()
    c.upsert(conn, "trade_calendar", ["trade_date", "is_open"],
             [(r.trade_date, bool(r.is_open)) for r in cal.itertuples(index=False)],
             ["trade_date"])

    stocks = c.fetch_stock_list()
    c.upsert(conn, "stock_basic",
             ["stock_code", "symbol", "name", "exchange"],
             [(r.stock_code, r.symbol, r.name, r.exchange) for r in stocks.itertuples(index=False)],
             ["stock_code"], update_cols=["name", "exchange"])

    for idx in c.INDEX_LIST:
        try:
            c.upsert_index(conn, idx, c.fetch_index(idx))
        except Exception as exc:  # noqa: BLE001
            c.log.warning("指数 %s 更新失败: %s", idx, exc)
    return stocks


def update_one(conn, stock_code: str, symbol: str, lookback_days: int) -> int:
    """
    单只增量 + 补漏。返回写入的日线行数。
    起点 = min(库内最新日, today - lookback) 之后;再和日历比对补缺。
    """
    max_d = c.get_max_trade_date(conn, stock_code)
    if max_d is None:
        start = date(1990, 1, 1)          # 新股:全量
    else:
        start = max_d - timedelta(days=lookback_days)

    # 缺口检测:应开市日 - 已有日
    need = set(expected_open_dates(conn, start))
    if not need:
        return 0
    have = existing_dates(conn, stock_code, start)
    missing = need - have
    if not missing:
        return 0

    fetch_start = min(missing).strftime("%Y%m%d")
    fetch_end = max(missing).strftime("%Y%m%d")
    daily = c.fetch_daily(symbol, start=fetch_start, end=fetch_end)
    n = c.upsert_daily(conn, stock_code, daily)

    # 后复权因子:整段重取覆盖(因子会随最近除权变化)
    adj = c.fetch_hfq_factor(symbol)
    c.upsert_adj_factor(conn, stock_code, adj)

    last = daily["trade_date"].max() if not daily.empty else max_d
    c.mark_progress(conn, TASK, stock_code, last, status="done", message=f"+{n}")
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=5, help="回看天数(补漏安全边界)")
    ap.add_argument("--limit", type=int, default=None, help="只处理前 N 只(调试)")
    ap.add_argument("--no-matview", action="store_true", help="跳过物化视图刷新")
    args = ap.parse_args()

    conn = c.get_conn()
    try:
        c.log.info("更新参考数据(日历/列表/指数) ...")
        stocks = update_reference(conn)
        if args.limit:
            stocks = stocks.head(args.limit)

        total = 0
        active = [r for r in stocks.itertuples(index=False)]
        c.log.info("增量更新 %d 只 ...", len(active))
        # 熔断:与 run_stock_todo 同口径。2026-07-10 实案:东财封禁期 cron 启动本脚本,
        # 无熔断的串行循环以 ~35s/只 逐股撞墙,5500 只要撞 ~45 小时且不断续期封禁。
        # 增量按库内 max(trade_date) 补漏,提前退出零损失,下次运行自动接着补。
        consecutive_errors = 0
        for i, r in enumerate(active, 1):
            try:
                total += update_one(conn, r.stock_code, r.symbol, args.days)
                consecutive_errors = 0
            except Exception as exc:  # noqa: BLE001
                conn.rollback()
                c.mark_progress(conn, TASK, r.stock_code, None, status="error", message=str(exc))
                c.log.error("  %s 失败: %s", r.stock_code, exc)
                consecutive_errors += 1
                if consecutive_errors >= 15:
                    c.log.critical(
                        "连续 %d 只失败,疑似数据源被封禁(WAF/限流),提前终止本次增量"
                        "(已处理 %d / %d)。冷却后重跑或等明日 cron,补漏机制自动补齐。",
                        consecutive_errors, i, len(active))
                    break
            if i % 200 == 0:
                c.log.info("进度 %d / %d,累计写入 %d 行", i, len(active), total)

        if not args.no_matview:
            c.log.info("刷新周线/月线物化视图 ...")
            c.refresh_matviews(conn)

        c.log.info("增量更新完成 ✅ 共写入 %d 行(%s)", total,
                   datetime.now().strftime("%Y-%m-%d %H:%M"))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
