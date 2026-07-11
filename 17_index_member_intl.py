"""
17_index_member_intl.py — 港美指数成分区间表(init + daily diff)。

设计: docs/superpowers/specs/2026-07-11-index-member-intl-design.md
  * 港股(HSI/HSTECH/HSCEI):富途 get_plate_stock 快照;历史变更免费无 →
    首日建开区间(note='snapshot-open',in_date=启用日≠真实纳入日),此后每日
    diff 累积(新增开区间/消失闭区间,note='diff')。
  * SP500:GitHub fja05680/sp500 历史成分(1996 起)重建全区间(note='history');
    NDX:Wikipedia 现势快照 + 每日 diff(同港股模式)。
  * 幂等:同快照重跑零变化;区间重建整表按 index_code 重写(DELETE+INSERT,原子事务)。

用法:
  python 17_index_member_intl.py --init          # 首次:快照 + SP500 历史回填
  python 17_index_member_intl.py                 # 每日 diff(挂 18:50 富途链尾)
"""

from __future__ import annotations

import argparse
import io
import sys
from datetime import date

import pandas as pd
import requests

import common as c

# 富途板块码(2026-07-11 实测:HSI 93 只 / HSTECH 30 只;HSCEI 待验证,失败则跳过并告警)
FUTU_INDEXES = {"HSI": "HK.800000", "HSTECH": "HK.800700", "HSCEI": "HK.800100"}
SP500_HIST_URL = ("https://raw.githubusercontent.com/fja05680/sp500/master/"
                  "S%26P%20500%20Historical%20Components%20%26%20Changes%20(Updated).csv")
NDX_WIKI_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"


def _hk_snapshot(index_code: str) -> set[str]:
    """富途板块成分快照 → {'00700.HK', ...}。"""
    d = c._futu_call("get_plate_stock", FUTU_INDEXES[index_code])
    codes = set()
    for code in d["code"] if isinstance(d, dict) else d["code"].tolist():
        # futu 'HK.00700' → '00700.HK'
        mk, sym = code.split(".", 1)
        codes.add(f"{sym.zfill(5)}.{mk}")
    return codes


def _sp500_history() -> pd.DataFrame:
    """GitHub 历史成分 CSV(每行=日期+当日全成分 ticker 列表)→ 区间表 DataFrame。"""
    r = requests.get(SP500_HIST_URL, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(io.BytesIO(r.content))
    df.columns = [str(col).strip().lower() for col in df.columns]
    date_col, tick_col = df.columns[0], df.columns[1]
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    df = df.dropna(subset=[date_col]).sort_values(date_col)

    intervals: dict[str, list] = {}   # sym -> [ [in,out|None], ... ]
    prev: set[str] = set()
    for row in df.itertuples(index=False):
        d, ticks = row[0], {t.strip() for t in str(row[1]).split(",") if t.strip()}
        for sym in ticks - prev:                       # 新纳入
            intervals.setdefault(sym, []).append([d, None])
        for sym in prev - ticks:                       # 剔除
            spans = intervals.get(sym)
            if spans and spans[-1][1] is None:
                spans[-1][1] = d
        prev = ticks
    rows = [("SPX", f"{sym}.US", span[0], span[1], "history")
            for sym, spans in intervals.items() for span in spans]
    return pd.DataFrame(rows, columns=["index_code", "stock_code", "in_date", "out_date", "note"])


def _ndx_snapshot() -> set[str]:
    """Wikipedia NDX 现势成分。requests 带 UA 取页面再解析(pd.read_html 裸 urllib
    对 Wikipedia 会 SSL EOF,2026-07-11 实测)。"""
    resp = requests.get(NDX_WIKI_URL, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text))
    for t in tables:
        cols = [str(col).lower() for col in t.columns]
        if any("ticker" in col or "symbol" in col for col in cols):
            col = t.columns[[i for i, col in enumerate(cols) if "ticker" in col or "symbol" in col][0]]
            syms = {str(s).strip() for s in t[col] if str(s).strip() and str(s) != "nan"}
            if len(syms) > 80:
                return {f"{s}.US" for s in syms}
    raise RuntimeError("Wikipedia NDX 表结构变化,未找到 ticker 列")


def _current_members(conn, index_code: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT stock_code FROM index_member WHERE index_code=%s AND out_date IS NULL",
                    (index_code,))
        out = {r[0] for r in cur.fetchall()}
    conn.commit()
    return out


def _apply_diff(conn, index_code: str, snapshot: set[str], note: str) -> tuple[int, int]:
    """快照 vs 库内在册:新增开区间、消失闭区间。返回 (纳入数, 剔除数)。"""
    today = date.today()
    current = _current_members(conn, index_code)
    added, removed = snapshot - current, current - snapshot
    with conn.cursor() as cur:
        for sc in added:
            cur.execute(
                "INSERT INTO index_member (index_code, stock_code, in_date, out_date, note) "
                "VALUES (%s,%s,%s,NULL,%s) ON CONFLICT (index_code, stock_code, in_date) DO NOTHING",
                (index_code, sc, today, note))
        for sc in removed:
            cur.execute(
                "UPDATE index_member SET out_date=%s WHERE index_code=%s AND stock_code=%s "
                "AND out_date IS NULL", (today, index_code, sc))
    conn.commit()
    return len(added), len(removed)


def init_sp500(conn) -> None:
    df = _sp500_history()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM index_member WHERE index_code='SPX'")
        for r in df.itertuples(index=False):
            cur.execute(
                "INSERT INTO index_member (index_code, stock_code, in_date, out_date, note) "
                "VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (r.index_code, r.stock_code, r.in_date, r.out_date, r.note))
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("SELECT count(*), count(*) FILTER (WHERE out_date IS NULL) "
                    "FROM index_member WHERE index_code='SPX'")
        total, active = cur.fetchone()
    conn.commit()
    c.log.info("SPX 历史区间重建:%d 区间,在册 %d", total, active)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--init", action="store_true", help="首次:SP500 历史回填 + 各指数快照建档")
    args = ap.parse_args()

    conn = c.get_conn()
    try:
        if args.init:
            c.log.info("=== SPX 历史区间回填(GitHub) ===")
            init_sp500(conn)

        note = "snapshot-open" if args.init else "diff"
        for idx in FUTU_INDEXES:
            try:
                snap = _hk_snapshot(idx)
                a, r = _apply_diff(conn, idx, snap, note)
                c.log.info("%s: 快照 %d 只,纳入 %d / 剔除 %d", idx, len(snap), a, r)
            except Exception as exc:  # noqa: BLE001
                c.log.warning("%s 快照失败(跳过,明日重试): %s", idx, str(exc)[:80])

        try:
            snap = _ndx_snapshot()
            a, r = _apply_diff(conn, "NDX", snap, note)
            c.log.info("NDX: 快照 %d 只,纳入 %d / 剔除 %d", len(snap), a, r)
        except Exception as exc:  # noqa: BLE001
            c.log.warning("NDX 快照失败(Wikipedia,跳过): %s", str(exc)[:80])

        c.log.info("指数成分更新完成 ✅")
        return 0
    finally:
        c.close_futu()
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
