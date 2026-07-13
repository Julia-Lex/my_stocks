"""
28_announcement_update.py — A股公告流增量/回填(东财公告 API,发布时间到秒)。

见 27_schema_announcement.sql。与 15_events(预告/快报/龙虎榜)性质不同:本表是原始
公告披露流(一条一行,含精确发布时间与全部披露类型),补库内缺失的"公告时间"与
减持/回购/重组等一大类事件。源为 datacenter 族,不在东财行情族封禁范围。

用法:
  ASTOCK_DB_USER=zhu .venv/bin/python 28_announcement_update.py                # 日更(近2天,幂等)
  ASTOCK_DB_USER=zhu .venv/bin/python 28_announcement_update.py --backfill-days 90  # 首次回填近90天
  ASTOCK_DB_USER=zhu .venv/bin/python 28_announcement_update.py --days 5       # 自定回看窗口

日更折进事件 cron(18:30 + 23:00 两轮):晚间披露高峰的公告,23:00 轮即可入库,
发布时间随之更新。按天翻页,单日 upsert 幂等(art_code 去重),重跑零副作用。
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta

import common as c


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=2, help="回看天数(日更默认近 2 天,补晚到的公告)")
    ap.add_argument("--backfill-days", type=int, default=None, help="首次回填:近 N 天(逐日翻页)")
    args = ap.parse_args()

    span = args.backfill_days if args.backfill_days else args.days
    today = c.beijing_now().date()
    start = today - timedelta(days=span - 1)

    conn = c.get_conn()
    try:
        total = 0
        d = today
        n_days = 0
        while d >= start:
            ds = d.strftime("%Y-%m-%d")
            try:
                df = c.fetch_announcements(ds, ds)
                n = c.upsert_announcements(conn, df)
                total += n
                if n:
                    c.log.info("公告 %s: %d 条", ds, n)
            except Exception as exc:  # noqa: BLE001
                c.log.error("公告 %s 失败: %s", ds, str(exc)[:120])
            n_days += 1
            if n_days % 20 == 0:
                c.log.info("回填进度:已处理 %d 天,累计 %d 条", n_days, total)
            d -= timedelta(days=1)
        c.log.info("公告增量完成 ✅ 共 upsert %d 条(窗口 %s ~ %s)", total, start, today)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
