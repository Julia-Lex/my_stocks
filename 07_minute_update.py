"""
07_minute_update.py — A股 1 分钟线回填 + 每日增量(通达信 pytdx 源)。

设计:
  * 数据源:通达信行情服务器(pytdx)。免费源里唯一能回溯约 90 个交易日
    1 分钟线的;缺点是服务器 IP 会漂移,故启动时探测节点、失败自动换节点。
  * 单位:通达信分钟线成交量原生就是「股」,零换算入库(2026-07-09 与
    日线对账验证,比值精确 1.0000);成交额单位元。
  * 增量:按库内每只股票 max(trade_time) 为止点,向前翻页到止点即停;
    空表股票拉满服务器深度(~90 个交易日)。upsert 幂等,可随时中断重跑。
  * 数据清洗:pytdx 偶发浮点解析垃圾(如 5.9e-39),量/额做合理性过滤;
    未走完的当前分钟 bar(trade_time > 网络时间)不入库。
  * 对账:--recon 抽查分钟量加总 vs 日线量,校验数据源没有漂移。

用法:
  python 07_minute_update.py --limit 5            # 试跑
  python 07_minute_update.py --workers 3          # 全市场(首次 ~1-2 小时)
  python 07_minute_update.py --recon 20           # 抽 20 只对账,不拉数
"""

from __future__ import annotations

import argparse
import random
import socket
import sys
import threading
import time
from datetime import datetime, timedelta

import pandas as pd

import common as c

TASK = "minute_1m"
CATEGORY_1MIN = 8          # pytdx K 线种类:8 = 1 分钟
PAGE = 800                 # pytdx 单次上限

# 实测可用节点放前面;pytdx 自带列表作为候补
CURATED_SERVERS = [
    ("上海双线", "180.153.18.170", 7709),
    ("上海双线2", "180.153.18.171", 7709),
    ("北京联通", "202.108.253.131", 7709),
    ("深圳双线", "120.79.60.82", 7709),
]


def candidate_servers() -> list[tuple[str, str, int]]:
    servers = list(CURATED_SERVERS)
    try:
        from pytdx.config.hosts import hq_hosts
        servers += [(n, ip, p) for n, ip, p in hq_hosts]
    except Exception:  # noqa: BLE001 — 自带列表拿不到就用手工名单
        pass
    return servers


def probe_servers(k: int = 4, budget: int = 40) -> list[tuple[str, int]]:
    """探测候选节点,按延迟返回最快的 k 个。budget 限制最多探测多少个。"""
    alive: list[tuple[float, str, int]] = []
    for name, ip, port in candidate_servers()[:budget]:
        t0 = time.time()
        try:
            s = socket.create_connection((ip, port), timeout=1.5)
            s.close()
            alive.append((time.time() - t0, ip, port))
        except OSError:
            continue
        if len(alive) >= k * 3:      # 攒够一批就够挑了
            break
    alive.sort()
    picked = [(ip, port) for _, ip, port in alive[:k]]
    c.log.info("通达信节点探测:%d 个可用,选用 %s", len(alive), picked)
    if not picked:
        raise SystemExit("没有可用的通达信行情节点,稍后重试")
    return picked


_SERVERS: list[tuple[str, int]] = []
_tl = threading.local()


def _get_api():
    """线程本地的 TdxHq_API 连接;断线自动换节点重连。"""
    api = getattr(_tl, "api", None)
    if api is not None:
        return api
    from pytdx.hq import TdxHq_API
    last_exc: Exception | None = None
    for ip, port in random.sample(_SERVERS, len(_SERVERS)):
        try:
            api = TdxHq_API(auto_retry=True)
            if api.connect(ip, port, time_out=5):
                _tl.api = api
                return api
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
    raise ConnectionError(f"所有通达信节点连接失败: {last_exc}")


def _drop_api():
    api = getattr(_tl, "api", None)
    if api is not None:
        try:
            api.disconnect()
        except Exception:  # noqa: BLE001
            pass
        _tl.api = None


def tdx_market(stock_code: str) -> int | None:
    """通达信市场代码:0=深, 1=沪;北交所标准行情接口不支持,返回 None。"""
    ex = stock_code.rsplit(".", 1)[-1]
    return {"SZ": 0, "SH": 1}.get(ex)


def fetch_minute_bars(symbol: str, market: int, stop_at: datetime | None) -> list[dict]:
    """向前翻页拉 1 分钟 bar,直到早于 stop_at 或到服务器深度上限。"""
    def _paged():
        api = _get_api()
        chunks, offset = [], 0
        while True:
            chunk = api.get_security_bars(CATEGORY_1MIN, market, symbol, offset, PAGE)
            if not chunk:
                break
            chunks.append(chunk)
            offset += len(chunk)
            if stop_at is not None:
                oldest = datetime.strptime(chunk[0]["datetime"], "%Y-%m-%d %H:%M")
                if oldest <= stop_at:
                    break
            if len(chunk) < PAGE:
                break
        return [b for ch in reversed(chunks) for b in ch]

    try:
        return _paged()
    except Exception:  # noqa: BLE001 — 连接失效:换节点重试一次
        _drop_api()
        return _paged()


def clean_bars(bars: list[dict], stop_at: datetime | None, now: datetime) -> pd.DataFrame:
    """过滤解析垃圾/未定盘分钟,返回 trade_time 升序的 DataFrame。"""
    rows = []
    for b in bars:
        try:
            t = datetime.strptime(b["datetime"], "%Y-%m-%d %H:%M")
        except (KeyError, ValueError):
            continue
        if t > now:                          # 当前分钟未走完
            continue
        if stop_at is not None and t <= stop_at:   # 已入库
            continue
        try:
            o, h, l, cl = (float(b[k]) for k in ("open", "high", "low", "close"))
        except (KeyError, TypeError, ValueError):
            continue
        if not all(0 < x < 1e6 for x in (o, h, l, cl)):
            continue
        v = b.get("vol") or 0
        a = b.get("amount") or 0
        # pytdx 偶发 e-39 级浮点垃圾;真实的零量分钟存 0
        v = int(v) if 1 <= v < 1e12 else 0
        a = float(a) if 1 <= a < 1e15 else 0.0
        rows.append((t, o, h, l, cl, v, a))
    return pd.DataFrame(rows, columns=["trade_time", "open", "high", "low",
                                       "close", "volume", "amount"])


def existing_max_time(conn, stock_code: str) -> datetime | None:
    with conn.cursor() as cur:
        cur.execute("SELECT max(trade_time) FROM minute_price WHERE stock_code = %s",
                    (stock_code,))
        row = cur.fetchone()
        return row[0] if row else None


def load_one(conn, r, now: datetime) -> None:
    market = tdx_market(r.stock_code)
    if market is None:
        c.mark_progress(conn, TASK, r.stock_code, None, status="done",
                        message="bj-unsupported")
        return
    stop_at = existing_max_time(conn, r.stock_code)
    bars = fetch_minute_bars(r.symbol, market, stop_at)
    df = clean_bars(bars, stop_at, now)
    n = c.upsert_minute(conn, r.stock_code, df)
    last = df["trade_time"].max().date() if not df.empty else (
        stop_at.date() if stop_at else None)
    c.mark_progress(conn, TASK, r.stock_code, last, status="done", message=f"+{n}")


def reconcile(conn, sample: int) -> int:
    """抽样对账:最近一个双方都有的交易日,分钟量加总 vs 日线量。"""
    with conn.cursor() as cur:
        cur.execute("""
            WITH mp AS (      -- 每只股票近 10 天的分钟量按日加总
                SELECT stock_code, trade_time::date AS dt, sum(volume) AS mv
                FROM minute_price
                WHERE trade_time >= now() - interval '10 days'
                GROUP BY 1, 2
            ),
            paired AS (       -- 每只取最近一个「分钟线和日线都有」的日子
                SELECT DISTINCT ON (mp.stock_code)
                       mp.stock_code, mp.mv, dp.volume AS dv
                FROM mp
                JOIN daily_price dp
                  ON dp.stock_code = mp.stock_code AND dp.trade_date = mp.dt
                ORDER BY mp.stock_code, mp.dt DESC
            )
            SELECT stock_code, mv, dv FROM paired ORDER BY random() LIMIT %s
        """, (sample,))
        rows = cur.fetchall()
    bad = 0
    for code, mv, dv in rows:
        if dv and abs(mv - dv) / dv > 0.005:
            bad += 1
            c.log.warning("对账偏差 %s: 分钟合计 %s vs 日线 %s", code, mv, dv)
    c.log.info("对账 %d 只,偏差(>0.5%%) %d 只", len(rows), bad)
    return bad


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="只处理前 N 只(试跑)")
    ap.add_argument("--workers", type=int, default=3, help="并发线程数(默认 3)")
    ap.add_argument("--recon", type=int, default=0, metavar="N",
                    help="只做抽样对账(N 只),不拉数据")
    args = ap.parse_args()

    conn = c.get_conn()
    try:
        if args.recon:
            return 1 if reconcile(conn, args.recon) else 0

        now = c.beijing_now().replace(tzinfo=None)
        # 分区兜底:通达信各节点回溯深度不一(实测有到 7 个月前的),
        # 从 12 个月前铺到未来 3 个月;空分区几乎零成本,宁多勿缺
        c.ensure_minute_partitions(conn, (now - timedelta(days=365)).date().replace(day=1), 16)

        global _SERVERS
        _SERVERS = probe_servers(k=min(4, max(2, args.workers)))

        # 股票清单直接读库(stock_basic 由日线 ETL 维护),不依赖外部接口
        with conn.cursor() as cur:
            cur.execute("SELECT stock_code, symbol FROM stock_basic "
                        "WHERE is_active ORDER BY stock_code")
            stocks = pd.DataFrame(cur.fetchall(), columns=["stock_code", "symbol"])
        if args.limit:
            stocks = stocks.head(args.limit)
        todo = list(stocks.itertuples(index=False))
        c.log.info("分钟线增量:%d 只,并发 %d", len(todo), args.workers)

        c.run_stock_todo(todo, TASK, lambda cn, r: load_one(cn, r, now),
                         args.workers, max_consecutive_errors=15)
        c.log.info("分钟线更新完成 ✅")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
