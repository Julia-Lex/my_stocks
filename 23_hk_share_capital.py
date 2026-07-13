"""
23_hk_share_capital.py — 港股股本表(富途快照 + 每日 diff)+ 市值派生基础。

背景(分析会话移交任务,2026-07-13):恒科指候选筛选需要市值口径,库内无港股股本。
  * 源:富途 get_market_snapshot(批量 400 只/次,全港股 8 次调用)的
    issued_shares/outstanding_shares。akshare 无港股股本接口(已侦察)。
  * 表:hk_share_capital(stock_code, effective_date, issued/outstanding_shares)
    ——首日快照建档(note='snapshot-open'),此后每日 diff:股本变化(回购/增发/
    合股)当日落新行。as-of 语义:取 effective_date <= d 的最新一行。
  * 已知限制:建档日之前无历史股本,历史市值派生用建档股本近似(README 声明)。

用法: python 23_hk_share_capital.py [--init]     # 每日模式挂 18:50 富途链尾
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

import pandas as pd

import common as c

_BATCH = 400  # 富途快照单次上限


def fetch_snapshots() -> pd.DataFrame:
    """全港股股本快照。列: stock_code, issued_shares, outstanding_shares。"""
    conn = c.get_conn()
    codes = pd.read_sql("SELECT stock_code FROM hk_stock_basic ORDER BY stock_code",
                        conn)["stock_code"].tolist()
    conn.commit()
    conn.close()
    futu_codes = [c.futu_code(sc) for sc in codes]
    frames = []
    for i in range(0, len(futu_codes), _BATCH):
        batch = futu_codes[i:i + _BATCH]
        try:
            df = c._futu_call("get_market_snapshot", batch)
            frames.append(df[["code", "issued_shares", "outstanding_shares"]])
        except Exception as exc:  # noqa: BLE001
            c.log.warning("快照批次 %d 失败(跳过,次日自愈): %s", i // _BATCH, str(exc)[:60])
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    mk_sym = df["code"].str.split(".", n=1)
    df["stock_code"] = mk_sym.str[1].str.zfill(5) + "." + mk_sym.str[0]
    for col in ("issued_shares", "outstanding_shares"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["issued_shares"])[
        ["stock_code", "issued_shares", "outstanding_shares"]]


def sync(conn, init: bool) -> None:
    snap = fetch_snapshots()
    if snap.empty:
        c.log.warning("快照为空,本次跳过")
        return
    today = date.today()
    note = "snapshot-open" if init else "diff"
    # 库内每股最新股本
    cur_map = {}
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT ON (stock_code) stock_code, issued_shares, outstanding_shares "
                    "FROM hk_share_capital ORDER BY stock_code, effective_date DESC")
        for sc, ish, osh in cur.fetchall():
            cur_map[sc] = (int(ish) if ish else None, int(osh) if osh else None)
    conn.commit()

    n_new = n_chg = 0
    with conn.cursor() as cur:
        for r in snap.itertuples(index=False):
            ish = int(r.issued_shares)
            osh = int(r.outstanding_shares) if not pd.isna(r.outstanding_shares) else None
            prev = cur_map.get(r.stock_code)
            if prev == (ish, osh):
                continue
            cur.execute(
                "INSERT INTO hk_share_capital "
                "(stock_code, effective_date, issued_shares, outstanding_shares, note) "
                "VALUES (%s,%s,%s,%s,%s) "
                "ON CONFLICT (stock_code, effective_date) DO UPDATE "
                "SET issued_shares=EXCLUDED.issued_shares, "
                "outstanding_shares=EXCLUDED.outstanding_shares",
                (r.stock_code, today, ish, osh, note))
            if prev is None:
                n_new += 1
            else:
                n_chg += 1
    conn.commit()
    c.log.info("股本同步:建档 %d / 变动 %d(快照 %d 只)", n_new, n_chg, len(snap))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--init", action="store_true")
    args = ap.parse_args()
    conn = c.get_conn()
    try:
        sync(conn, args.init)
        return 0
    finally:
        c.close_futu()
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
