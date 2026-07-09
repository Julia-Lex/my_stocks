"""
common.py — 数据库连接、AKShare 拉取与入库的公共层。

所有「易变」的东西都集中在这里:
  * 数据库连接参数(顶部 DB_CONFIG,密码优先读环境变量 ASTOCK_DB_PASSWORD)
  * AKShare 各接口的列名映射(RENAME_* 字典)—— 数据源改列名时只改这里
  * 带指数退避重试的接口调用
  * 通用的 upsert 批量入库

依赖:
  pip install akshare pandas psycopg2-binary
"""

from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime
from typing import Iterable, Optional, Sequence

import pandas as pd
import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# 数据库配置
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host":     os.getenv("ASTOCK_DB_HOST", "localhost"),
    "port":     int(os.getenv("ASTOCK_DB_PORT", "5432")),
    "dbname":   os.getenv("ASTOCK_DB_NAME", "astock"),
    "user":     os.getenv("ASTOCK_DB_USER", "postgres"),
    # 建议用环境变量:export ASTOCK_DB_PASSWORD=xxxx
    # 也可直接把下面的 "" 改成你的密码。
    "password": os.getenv("ASTOCK_DB_PASSWORD", ""),
}

# 指数列表(可自行增删)
INDEX_LIST = ["sh000001", "sz399001", "sz399006", "sh000300", "sh000905", "sh000016"]

# ---------------------------------------------------------------------------
# 日志
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("astock")


# ---------------------------------------------------------------------------
# 连接
# ---------------------------------------------------------------------------
def get_conn():
    """返回一个新的 psycopg2 连接(调用方负责 close)。"""
    return psycopg2.connect(**DB_CONFIG)


# ---------------------------------------------------------------------------
# 重试装饰:AKShare 偶尔超时/限流,做指数退避
# ---------------------------------------------------------------------------
def with_retry(fn, *args, retries: int = 4, base_delay: float = 2.0, **kwargs):
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 — 接口层什么都可能抛
            last_exc = exc
            delay = base_delay * (2 ** attempt)
            log.warning("接口调用失败(第 %d 次): %s — %.0fs 后重试", attempt + 1, exc, delay)
            time.sleep(delay)
    raise last_exc  # type: ignore[misc]


# ---------------------------------------------------------------------------
# 代码规范化:补交易所后缀
# ---------------------------------------------------------------------------
def to_full_code(symbol: str) -> str:
    """000001 -> 000001.SZ,600000 -> 600000.SH,830799 -> 830799.BJ。"""
    s = symbol.strip().zfill(6)
    if s[0] == "6":
        return f"{s}.SH"
    if s[0] in ("0", "3"):
        return f"{s}.SZ"
    if s[0] in ("4", "8", "9"):
        return f"{s}.BJ"
    # 兜底:按沪市处理
    return f"{s}.SH"


def to_sina_code(symbol: str) -> str:
    """000001 -> sz000001(供部分新浪接口使用)。"""
    full = to_full_code(symbol)
    sym, ex = full.split(".")
    return f"{ex.lower()}{sym}"


# ===========================================================================
# 列名映射 —— 数据源改列名时只改下面
# ===========================================================================
# ak.stock_zh_a_hist(period="daily", adjust="")
RENAME_HIST = {
    "日期": "trade_date",
    "开盘": "open",
    "收盘": "close",
    "最高": "high",
    "最低": "low",
    "成交量": "volume",   # 单位:手
    "成交额": "amount",   # 单位:元
    "涨跌幅": "pct_chg",
    "换手率": "turnover",
}

# ak.stock_zh_index_daily_em(symbol=...) / stock_zh_index_daily
RENAME_INDEX = {
    "date": "trade_date",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
    "amount": "amount",
}


# ===========================================================================
# AKShare 拉取
# ===========================================================================
def fetch_stock_list() -> pd.DataFrame:
    """
    全市场 A 股代码 + 名称。返回列: symbol, name, stock_code, exchange。
    """
    import akshare as ak

    df = with_retry(ak.stock_info_a_code_name)
    df = df.rename(columns={"code": "symbol", "名称": "name"})
    if "name" not in df.columns and "name" not in df:
        # 某些版本列名就是 code/name
        df = df.rename(columns={df.columns[0]: "symbol", df.columns[1]: "name"})
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    df["stock_code"] = df["symbol"].map(to_full_code)
    df["exchange"] = df["stock_code"].str.split(".").str[1]
    return df[["stock_code", "symbol", "name", "exchange"]]


def fetch_daily(symbol: str, start: str = "19900101", end: Optional[str] = None) -> pd.DataFrame:
    """
    单只股票不复权日线。start/end 格式 'YYYYMMDD'。
    返回列: trade_date, open, high, low, close, volume, amount, pct_chg, turnover。
    """
    import akshare as ak

    end = end or datetime.now().strftime("%Y%m%d")
    df = with_retry(
        ak.stock_zh_a_hist,
        symbol=symbol, period="daily",
        start_date=start, end_date=end, adjust="",
    )
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns=RENAME_HIST)
    keep = [c for c in RENAME_HIST.values() if c in df.columns]
    df = df[keep].copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    return df


def fetch_hfq_factor(symbol: str) -> pd.DataFrame:
    """
    单只股票的后复权因子。返回列: trade_date, adj_factor。

    优先用新浪 hfq-factor 接口;若失败则退化为 hfq_close/close 现算因子。
    """
    import akshare as ak

    sina = to_sina_code(symbol)
    # 途径 1:直接拿后复权因子
    try:
        df = with_retry(ak.stock_zh_a_daily, symbol=sina, adjust="hfq-factor")
        if df is not None and not df.empty:
            df = df.rename(columns={"date": "trade_date", "hfq_factor": "adj_factor"})
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
            df["adj_factor"] = pd.to_numeric(df["adj_factor"], errors="coerce")
            return df[["trade_date", "adj_factor"]].dropna()
    except Exception as exc:  # noqa: BLE001
        log.warning("%s hfq-factor 接口失败,改用 hfq/原始价 现算: %s", symbol, exc)

    # 途径 2:后复权价 ÷ 不复权价 = 因子
    raw = fetch_daily(symbol)
    hfq = with_retry(
        ak.stock_zh_a_hist,
        symbol=symbol, period="daily",
        start_date="19900101", end_date=datetime.now().strftime("%Y%m%d"),
        adjust="hfq",
    ).rename(columns=RENAME_HIST)
    if raw.empty or hfq is None or hfq.empty:
        return pd.DataFrame()
    hfq["trade_date"] = pd.to_datetime(hfq["trade_date"]).dt.date
    merged = raw[["trade_date", "close"]].merge(
        hfq[["trade_date", "close"]], on="trade_date", suffixes=("_raw", "_hfq")
    )
    merged["adj_factor"] = merged["close_hfq"] / merged["close_raw"]
    return merged[["trade_date", "adj_factor"]].dropna()


def fetch_calendar() -> pd.DataFrame:
    """交易日历。返回列: trade_date。"""
    import akshare as ak

    df = with_retry(ak.tool_trade_date_hist_sina)
    col = "trade_date" if "trade_date" in df.columns else df.columns[0]
    out = pd.DataFrame({"trade_date": pd.to_datetime(df[col]).dt.date})
    out["is_open"] = True
    return out


def fetch_index(index_code: str) -> pd.DataFrame:
    """指数日线。index_code 形如 'sh000001'。"""
    import akshare as ak

    df = with_retry(ak.stock_zh_index_daily, symbol=index_code)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns=RENAME_INDEX)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    keep = [c for c in ["trade_date", "open", "high", "low", "close", "volume", "amount"] if c in df.columns]
    return df[keep].copy()


# ===========================================================================
# 港股 / 美股(方案 B 分表)。表前缀、拉数函数按 MARKETS 配置分发。
# 成交量单位:股;货币按表隐含(hk_*=HKD,us_*=USD)。
# ===========================================================================
MARKETS = {
    "hk": {
        "prefix": "hk_", "suffix": ".HK",
        "indexes": ["HSI", "HSTECH"],          # 以 Task3 Step1 探测结果为准
        "start": "19800101",
        "mviews": ("hk_weekly_price_hfq", "hk_monthly_price_hfq"),
    },
    "us": {
        "prefix": "us_", "suffix": ".US",
        "indexes": [".INX", ".IXIC", ".DJI"],
        "start": "19700101",
        "mviews": ("us_weekly_price_hfq", "us_monthly_price_hfq"),
    },
}

_US_EXCHANGE = {"105": "NASDAQ", "106": "NYSE", "107": "AMEX"}


def fetch_hk_stock_list() -> pd.DataFrame:
    """东财港股全列表。返回列: stock_code, symbol, name, exchange。"""
    import akshare as ak

    df = with_retry(ak.stock_hk_spot_em)
    df = df.rename(columns={"代码": "symbol", "名称": "name"})
    df["symbol"] = df["symbol"].astype(str).str.zfill(5)
    df["stock_code"] = df["symbol"] + ".HK"
    df["exchange"] = "HKEX"
    return df[["stock_code", "symbol", "name", "exchange"]].drop_duplicates("stock_code")


def fetch_us_stock_list(top_n: int = 600) -> pd.DataFrame:
    """
    东财美股列表:总市值前 top_n。
    返回列: stock_code, symbol, name, exchange, em_symbol。

    中概股覆盖依赖市值前 top_n:本机 akshare(1.18.64)没有独立的
    中概股列表接口(stock_us_famous_spot_em 仅支持 6 个固定类目、
    不含"中概股";历史上的 stock_us_zh_spot 已被移除),而主要中概
    (BABA/PDD/JD/NTES 等)市值均在前 600 之内,故不做补充拉取。
    """
    import akshare as ak

    spot = with_retry(ak.stock_us_spot_em)
    spot = spot.rename(columns={"代码": "em_symbol", "名称": "name", "总市值": "mktcap"})
    spot["mktcap"] = pd.to_numeric(spot["mktcap"], errors="coerce")
    log.warning("美股清单无独立中概股列表接口,中概覆盖依赖市值前 %d", top_n)

    # 先按市值降序再去重:重复时保留的是市值榜(排名靠前)那一行,不会误删合法中概
    df = (spot.dropna(subset=["mktcap"])
              .sort_values("mktcap", ascending=False)
              .head(top_n)[["em_symbol", "name"]]
              .copy())
    df["em_symbol"] = df["em_symbol"].astype(str)
    n0 = len(df)
    df = df.drop_duplicates("em_symbol")
    if n0 - len(df) > 0:
        log.info("fetch_us_stock_list: em_symbol 去重丢弃 %d 行", n0 - len(df))
    df["symbol"] = df["em_symbol"].str.split(".").str[-1]
    df["stock_code"] = df["symbol"] + ".US"
    df["exchange"] = df["em_symbol"].str.split(".").str[0].map(_US_EXCHANGE).fillna("US")
    n1 = len(df)
    df = df.drop_duplicates("stock_code")
    if n1 - len(df) > 0:
        log.info("fetch_us_stock_list: stock_code 去重丢弃 %d 行(跨交易所同名代码)", n1 - len(df))
    return df[["stock_code", "symbol", "name", "exchange", "em_symbol"]]


def fetch_intl_daily(market: str, fetch_symbol: str,
                     start: Optional[str] = None, end: Optional[str] = None,
                     adjust: str = "") -> pd.DataFrame:
    """港/美单只不复权日线。fetch_symbol:港股 '00700',美股 '105.AAPL'。"""
    import akshare as ak

    cfg = MARKETS[market]
    start = start or cfg["start"]
    end = end or datetime.now().strftime("%Y%m%d")
    fn = ak.stock_hk_hist if market == "hk" else ak.stock_us_hist
    df = with_retry(fn, symbol=fetch_symbol, period="daily",
                    start_date=start, end_date=end, adjust=adjust)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns=RENAME_HIST)
    keep = [c for c in RENAME_HIST.values() if c in df.columns]
    df = df[keep].copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    return df


def fetch_intl_hfq_factor(market: str, fetch_symbol: str,
                          raw: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """后复权因子 = hfq 收盘 ÷ 原始收盘。raw 可传入已拉取的不复权日线省一次请求。"""
    if raw is None:
        raw = fetch_intl_daily(market, fetch_symbol)
    hfq = fetch_intl_daily(market, fetch_symbol, adjust="hfq")
    if raw.empty or hfq.empty:
        return pd.DataFrame()
    merged = raw[["trade_date", "close"]].merge(
        hfq[["trade_date", "close"]], on="trade_date", suffixes=("_raw", "_hfq"))
    merged = merged[merged["close_raw"] > 0]
    merged["adj_factor"] = merged["close_hfq"] / merged["close_raw"]
    return merged[["trade_date", "adj_factor"]].dropna()


def fetch_intl_index(market: str, index_code: str) -> pd.DataFrame:
    """港/美指数日线。港:HSI 等;美:.INX/.IXIC/.DJI(新浪代码)。"""
    import akshare as ak

    if market == "hk":
        df = with_retry(ak.stock_hk_index_daily_sina, symbol=index_code)  # 以探测结果为准
    else:
        df = with_retry(ak.index_us_stock_sina, symbol=index_code)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns=RENAME_INDEX)
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    keep = [c for c in ["trade_date", "open", "high", "low", "close", "volume", "amount"]
            if c in df.columns]
    return df[keep].copy()


def rebuild_intl_calendar(conn, market: str) -> None:
    """交易日历 = 指数日线出现过的日期(设计:从指数派生,无独立日历源)。"""
    p = MARKETS[market]["prefix"]
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {p}trade_calendar (trade_date, is_open) "
            f"SELECT DISTINCT trade_date, TRUE FROM {p}index_daily "
            f"ON CONFLICT (trade_date) DO NOTHING"
        )
    conn.commit()


# ===========================================================================
# 入库(upsert)
# ===========================================================================
def upsert(conn, table: str, cols: Sequence[str], rows: Iterable[Sequence],
           conflict_cols: Sequence[str], update_cols: Optional[Sequence[str]] = None) -> int:
    """
    通用批量 upsert。返回写入行数。
    update_cols 为 None 时,冲突则更新除冲突键外的所有列。
    """
    rows = list(rows)
    if not rows:
        return 0
    if update_cols is None:
        update_cols = [c for c in cols if c not in conflict_cols]

    col_sql = ", ".join(cols)
    conflict_sql = ", ".join(conflict_cols)
    if update_cols:
        set_sql = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        do_sql = f"DO UPDATE SET {set_sql}"
    else:
        do_sql = "DO NOTHING"

    sql = (
        f"INSERT INTO {table} ({col_sql}) VALUES %s "
        f"ON CONFLICT ({conflict_sql}) {do_sql}"
    )
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    return len(rows)


def upsert_daily(conn, stock_code: str, df: pd.DataFrame, table: str = "daily_price") -> int:
    if df.empty:
        return 0
    cols = ["stock_code", "trade_date", "open", "high", "low", "close",
            "volume", "amount", "pct_chg", "turnover"]
    rows = [
        (stock_code, r.trade_date,
         _num(r, "open"), _num(r, "high"), _num(r, "low"), _num(r, "close"),
         _int(r, "volume"), _num(r, "amount"), _num(r, "pct_chg"), _num(r, "turnover"))
        for r in df.itertuples(index=False)
    ]
    return upsert(conn, table, cols, rows, ["stock_code", "trade_date"])


def upsert_adj_factor(conn, stock_code: str, df: pd.DataFrame, table: str = "adj_factor") -> int:
    if df.empty:
        return 0
    cols = ["stock_code", "trade_date", "adj_factor"]
    rows = [(stock_code, r.trade_date, float(r.adj_factor)) for r in df.itertuples(index=False)]
    return upsert(conn, table, cols, rows, ["stock_code", "trade_date"])


def upsert_index(conn, index_code: str, df: pd.DataFrame, table: str = "index_daily") -> int:
    if df.empty:
        return 0
    cols = ["index_code", "trade_date", "open", "high", "low", "close", "volume", "amount"]
    rows = [
        (index_code, r.trade_date,
         _num(r, "open"), _num(r, "high"), _num(r, "low"), _num(r, "close"),
         _int(r, "volume"), _num(r, "amount"))
        for r in df.itertuples(index=False)
    ]
    return upsert(conn, table, cols, rows, ["index_code", "trade_date"])


# ---------------------------------------------------------------------------
# ETL 进度
# ---------------------------------------------------------------------------
def mark_progress(conn, task: str, stock_code: str, last_date: Optional[date],
                  status: str = "done", message: Optional[str] = None) -> None:
    upsert(
        conn, "etl_progress",
        ["task", "stock_code", "last_date", "status", "message", "updated_at"],
        [(task, stock_code, last_date, status, message, datetime.now())],
        ["task", "stock_code"],
    )


def get_done_codes(conn, task: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT stock_code FROM etl_progress WHERE task = %s AND status = 'done'",
            (task,),
        )
        return {r[0] for r in cur.fetchall()}


def get_max_trade_date(conn, stock_code: Optional[str] = None,
                       table: str = "daily_price") -> Optional[date]:
    with conn.cursor() as cur:
        if stock_code:
            cur.execute(f"SELECT max(trade_date) FROM {table} WHERE stock_code = %s", (stock_code,))
        else:
            cur.execute(f"SELECT max(trade_date) FROM {table}")
        row = cur.fetchone()
        return row[0] if row else None


def refresh_matviews(conn, names: Sequence[str] = ("weekly_price_hfq", "monthly_price_hfq")) -> None:
    """刷新周线/月线物化视图。"""
    with conn.cursor() as cur:
        for mv in names:
            try:
                cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}")
            except psycopg2.Error:
                conn.rollback()
                cur.execute(f"REFRESH MATERIALIZED VIEW {mv}")
    conn.commit()


# ---------------------------------------------------------------------------
# 小工具:安全取值
# ---------------------------------------------------------------------------
def _num(row, field):
    v = getattr(row, field, None)
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _int(row, field):
    v = _num(row, field)
    return int(v) if v is not None else None


# ---------------------------------------------------------------------------
# 并行执行:每个工作线程持有自己的数据库连接(psycopg2 连接不能跨线程共享)。
# 断点续传由 etl_progress 保证,与并发无关。
# ---------------------------------------------------------------------------
import itertools
import threading
from concurrent.futures import ThreadPoolExecutor

_tls = threading.local()
_all_conns: list = []
_conns_lock = threading.Lock()


def _thread_conn():
    """当前线程专属的数据库连接(懒创建,run_stock_todo 结束时统一关闭)。"""
    conn = getattr(_tls, "conn", None)
    if conn is None or conn.closed:
        conn = get_conn()
        _tls.conn = conn
        with _conns_lock:
            _all_conns.append(conn)
    return conn


def run_stock_todo(todo, task: str, load_fn, workers: int) -> None:
    """
    按 workers 数串行或并行处理股票清单。
    load_fn(conn, row):处理单只;抛异常则记 error 进度,不中断整体。
    """
    todo = list(todo)
    total = len(todo)
    counter = itertools.count(1)  # CPython 下 next() 原子,足够做进度计数

    def work(r):
        conn = _thread_conn()
        try:
            load_fn(conn, r)
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            mark_progress(conn, task, r.stock_code, None, status="error", message=str(exc))
            log.error("  %s 失败: %s", r.stock_code, exc)
        i = next(counter)
        if i % 100 == 0:
            log.info("进度 %d / %d", i, total)

    if workers <= 1:
        for r in todo:
            work(r)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(work, todo))
    with _conns_lock:
        for conn in _all_conns:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
        _all_conns.clear()
