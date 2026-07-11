"""
19_board_intl.py — 港美板块(富途)建档 + 每日成分 diff。

设计: docs/superpowers/specs/2026-07-11-board-intl-design.md
  * 板块清单:get_plate_list(HK/US, INDUSTRY/CONCEPT)→ {p}board upsert(更名覆盖)。
  * 成分:逐板块 get_plate_stock 快照 → {p}board_member 区间 diff(同 index_member 模式:
    新增开区间 in_date=今日,消失闭区间 out_date=今日;首次建档 note='snapshot-open')。
  * 消失板块:其全部在册成员闭区间。
  * 历史限制:富途无板块历史成分,建档日前的归属不可追溯(README 声明)。
  * 预算:~200-400 板块 × 1 请求(全局 1.05s 节流)≈ 4-8 分钟;挂 18:50 富途链尾。

用法: python 19_board_intl.py [--init] [--market hk|us|all]
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

import common as c

_MK = {"hk": "hk_", "us": "us_"}


def _futu_to_local(code: str, market: str) -> str:
    """富途 'HK.00700'/'US.AAPL' → '00700.HK'/'AAPL.US'。"""
    mk, sym = code.split(".", 1)
    if market == "hk":
        sym = sym.zfill(5)
    return f"{sym}.{mk}"


def fetch_plates(market: str) -> list[tuple[str, str, str]]:
    """板块清单 [(board_code, board_name, board_type)]。US 概念板块无则只回行业。"""
    import futu
    mk = futu.Market.HK if market == "hk" else futu.Market.US
    out = []
    for ptype, label in ((futu.Plate.INDUSTRY, "industry"), (futu.Plate.CONCEPT, "concept")):
        try:
            df = c._futu_call("get_plate_list", mk, ptype)
            for r in df.itertuples(index=False):
                out.append((r.code, str(r.plate_name)[:64], label))
        except Exception as exc:  # noqa: BLE001
            c.log.warning("[%s] %s 板块清单不可得(源无此类目?): %s", market, label, str(exc)[:60])
    return out


def _current_members(conn, table: str, board_code: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT stock_code FROM {table} WHERE board_code=%s AND out_date IS NULL",
                    (board_code,))
        out = {r[0] for r in cur.fetchall()}
    conn.commit()
    return out


def sync_market(conn, market: str, init: bool) -> None:
    p = _MK[market]
    note = "snapshot-open" if init else "diff"
    today = date.today()

    plates = fetch_plates(market)
    if not plates:
        c.log.warning("[%s] 未获得任何板块,跳过(富途网关/权限?)", market)
        return
    # 同一 plate 码可能同时出现在 INDUSTRY 与 CONCEPT 清单(富途实测),按码去重保首见
    seen: set[str] = set()
    plates = [b for b in plates if not (b[0] in seen or seen.add(b[0]))]
    c.upsert(conn, f"{p}board", ["board_code", "board_name", "board_type"],
             plates, ["board_code"], update_cols=["board_name", "board_type"])
    c.log.info("[%s] 板块清单 %d 个(行业+概念)", market, len(plates))

    live_codes = {b[0] for b in plates}
    # 消失板块:全部在册成员闭区间
    with conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT board_code FROM {p}board_member WHERE out_date IS NULL")
        stale = {r[0] for r in cur.fetchall()} - live_codes
        for bc in stale:
            cur.execute(f"UPDATE {p}board_member SET out_date=%s "
                        f"WHERE board_code=%s AND out_date IS NULL", (today, bc))
            c.log.info("[%s] 板块 %s 已消失,成员全部闭区间", market, bc)
    conn.commit()

    n_add = n_rm = 0
    for i, (bc, bn, _bt) in enumerate(plates, 1):
        # get_plate_stock 有独立限频 10 次/30 秒(2026-07-11 实测,严于通用 30/30):
        # 在全局 1.05s 节流之外补足到 ~3.2s;撞限时 31s 退避重试最多 3 次
        snap = None
        for attempt in range(3):
            try:
                import time as _t
                _t.sleep(2.2)
                df = c._futu_call("get_plate_stock", bc)
                snap = {_futu_to_local(code, market) for code in df["code"].tolist()}
                break
            except Exception as exc:  # noqa: BLE001
                if "频率" in str(exc) and attempt < 2:
                    c.log.info("[%s] %s 撞板块限频,31s 后重试", market, bn)
                    import time as _t
                    _t.sleep(31)
                    continue
                c.log.warning("[%s] 板块 %s(%s) 成分拉取失败,跳过: %s", market, bn, bc, str(exc)[:60])
                break
        if snap is None:
            continue
        current = _current_members(conn, f"{p}board_member", bc)
        added, removed = snap - current, current - snap
        with conn.cursor() as cur:
            for sc in added:
                cur.execute(
                    f"INSERT INTO {p}board_member (board_code, stock_code, in_date, out_date, note) "
                    f"VALUES (%s,%s,%s,NULL,%s) "
                    f"ON CONFLICT (board_code, stock_code, in_date) DO NOTHING",
                    (bc, sc, today, note))
            for sc in removed:
                cur.execute(f"UPDATE {p}board_member SET out_date=%s "
                            f"WHERE board_code=%s AND stock_code=%s AND out_date IS NULL",
                            (today, bc, sc))
        conn.commit()
        n_add += len(added)
        n_rm += len(removed)
        if i % 50 == 0:
            c.log.info("[%s] 板块成分进度 %d / %d", market, i, len(plates))
    c.log.info("[%s] 成分 diff 完成:纳入 %d / 剔除 %d", market, n_add, n_rm)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--init", action="store_true", help="首次建档(note=snapshot-open)")
    ap.add_argument("--market", default="all", choices=("hk", "us", "all"))
    args = ap.parse_args()

    conn = c.get_conn()
    try:
        for mk in ("hk", "us") if args.market == "all" else (args.market,):
            sync_market(conn, mk, args.init)
        c.log.info("港美板块同步完成 ✅")
        return 0
    finally:
        c.close_futu()
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
