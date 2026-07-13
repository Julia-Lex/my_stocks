"""
24_spot_refill.py — 用腾讯批量实时快照补全当日 A 股日线(东财不可用时的兜底件)。

补两类腾讯 K 线源(ASTOCK_ASHARE_SOURCE=tx)的天然缺口:
  1. 北交所整日 bar(腾讯 K 线不覆盖 920 段,快照覆盖);
  2. 沪深行的 amount(成交额)/turnover(换手率)(腾讯 K 线不提供,快照提供)。

窗口:收盘定盘(15:30)后至下一交易日开盘前,快照即当日定盘值(时间戳字段校验,
非当日时间戳的行=停牌,跳过)。成交量单位自适应(科创板股/其余手,按 均价≈现价 判定)。

用法(通常由 25_daily_ashare_chain.sh 在东财失败时自动调用):
  ASTOCK_DB_USER=zhu .venv/bin/python 24_spot_refill.py
"""

from __future__ import annotations

import sys
import time

import pandas as pd
import requests

import common as c


def main() -> int:
    conn = c.get_conn()
    try:
        target = c.safe_cutoff_date()   # 收盘防护口径下"允许写入的最晚交易日"
        with conn.cursor() as cur:
            cur.execute("SELECT is_open FROM trade_calendar WHERE trade_date = %s", (target,))
            row = cur.fetchone()
        if not row or not row[0]:
            c.log.info("cutoff 日 %s 非开市日,无需补全", target)
            return 0
        ts_prefix = target.strftime("%Y%m%d")

        with conn.cursor() as cur:
            cur.execute("SELECT stock_code FROM stock_basic WHERE is_active ORDER BY stock_code")
            codes = [r[0] for r in cur.fetchall()]
        c.log.info("快照补全 %s:%d 只", target, len(codes))

        def tx_code(sc: str) -> str:
            sym, _, ex = sc.partition(".")
            return ex.lower() + sym

        rows, skipped = [], 0
        for i in range(0, len(codes), 60):
            batch = codes[i:i + 60]
            url = "https://qt.gtimg.cn/q=" + ",".join(tx_code(s) for s in batch)
            r = c.with_retry(requests.get, url, timeout=10)
            for line in r.text.strip().split(";"):
                line = line.strip()
                if "=" not in line or line.count("~") < 45:
                    continue
                f = line.partition("=")[2].strip('"').split("~")
                try:
                    ts, close, opn = f[30], float(f[3]), float(f[5])
                    high, low = float(f[33]), float(f[34])
                    vol_raw, amt_wan = float(f[6]), float(f[37])
                    pct = float(f[32]) if f[32] else None
                    turn = float(f[38]) if f[38] else None
                except (ValueError, IndexError):
                    continue
                if not ts.startswith(ts_prefix):     # 停牌/无当日成交
                    skipped += 1
                    continue
                if vol_raw <= 0 or close <= 0 or amt_wan <= 0:
                    continue
                amount = amt_wan * 1e4
                # 单位自适应:成交额/量 的隐含均价应与现价同量级(科创板返回股,其余手)
                px_shou, px_gu = amount / (vol_raw * 100), amount / vol_raw
                volume = int(vol_raw * 100) if abs(px_shou - close) <= abs(px_gu - close) else int(vol_raw)
                rows.append((c.to_full_code(f[2]), target, opn, high, low, close,
                             volume, amount, pct, turn))
            time.sleep(0.3)

        n = c.upsert(conn, "daily_price",
                     ["stock_code", "trade_date", "open", "high", "low", "close",
                      "volume", "amount", "pct_chg", "turnover"],
                     rows, ["stock_code", "trade_date"])
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FILTER (WHERE amount IS NULL), count(*) "
                        "FROM daily_price WHERE trade_date = %s", (target,))
            null_amt, total = cur.fetchone()
        c.log.info("快照补全完成:upsert %d 行(跳过停牌 %d);%s 共 %d 行,amount 空 %d",
                   n, skipped, target, total, null_amt)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
