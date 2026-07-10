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

import io
import logging
import os
import threading
import time
import urllib.request
from datetime import date, datetime, time as dt_time, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Iterable, Optional, Sequence

import pandas as pd
import psycopg2
import psycopg2.extras
import requests

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

# 港/美股数据源开关:tx = 腾讯(ifzq.gtimg.cn,默认);em = 东财(AKShare,IP 被封禁时不可用)。
# 背景见 Task 4b:东财 push2his 接口本机被连接级封禁,腾讯 K 线接口实测可达且快。
INTL_SOURCE = os.getenv("ASTOCK_INTL_SOURCE", "tx")

# A 股日线数据源开关:em = 东财(AKShare,默认、主源);tx = 腾讯(备源,仅东财
# IP 被封时手动切换)。与 INTL_SOURCE 默认值故意不同——东财是 A 股的主力
# 数据源(历史深度、字段完整度均优于腾讯:腾讯 A 股 day 数组只有 6 个字段,
# 无成交额/换手率),只有东财整体不可用时才应设 ASTOCK_ASHARE_SOURCE=tx 应急。
ASHARE_SOURCE = os.getenv("ASTOCK_ASHARE_SOURCE", "em")

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
    "成交量": "volume",   # 东财原始单位:手;入库前统一换算为股(见 _fetch_daily_em)
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
    按 ASHARE_SOURCE 分发(默认 em;东财被封时可设 ASTOCK_ASHARE_SOURCE=tx 切腾讯备源)。
    """
    if ASHARE_SOURCE == "tx":
        return _fetch_daily_tx(symbol, start, end)
    return _fetch_daily_em(symbol, start, end)


def _fetch_daily_em(symbol: str, start: str = "19900101", end: Optional[str] = None) -> pd.DataFrame:
    """东财实现(主源):单只股票不复权日线。start/end 格式 'YYYYMMDD'。"""
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
    # 东财成交量单位是手;全库(A/HK/US)统一存股(2026-07-09 起,历史数据已换算)
    if "volume" in df.columns:
        df["volume"] = df["volume"] * 100
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
    """指数日线。index_code 形如 'sh000001'。

    单位说明:新浪指数源的 volume 原生就是「股」(2026-07-09 用
    sh000001 与全市场成交量对账验证),无需像东财日线那样 ×100。
    """
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
    """港股全列表。返回列: stock_code, symbol, name, exchange。按 INTL_SOURCE 分发。"""
    if INTL_SOURCE == "em":
        return _fetch_hk_stock_list_em()
    return _fetch_hk_stock_list_tx()


def _fetch_hk_stock_list_em() -> pd.DataFrame:
    """东财港股全列表。返回列: stock_code, symbol, name, exchange。"""
    import akshare as ak

    df = with_retry(ak.stock_hk_spot_em)
    df = df.rename(columns={"代码": "symbol", "名称": "name"})
    df["symbol"] = df["symbol"].astype(str).str.zfill(5)
    df["stock_code"] = df["symbol"] + ".HK"
    df["exchange"] = "HKEX"
    return df[["stock_code", "symbol", "name", "exchange"]].drop_duplicates("stock_code")


# HKEX 官方全市场证券清单(xlsx)。表头在第 3 行(0-based index=2),
# 探测结果:列含 Stock Code / Name of Securities / Category 等;
# Category=='Equity' 即普通股(2809 只,已排除权证/牛熊证/债券/REITs 等)。
_HKEX_LIST_URL = "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx"


def _fetch_hk_stock_list_tx() -> pd.DataFrame:
    """HKEX 官方清单(供腾讯拉数使用,腾讯本身无股票列表接口)。
    返回列: stock_code, symbol, name, exchange。
    """
    resp = with_retry(requests.get, _HKEX_LIST_URL, timeout=30)
    resp.raise_for_status()
    df = pd.read_excel(io.BytesIO(resp.content), header=2)
    df = df[df["Category"] == "Equity"].copy()
    df["symbol"] = df["Stock Code"].astype(int).astype(str).str.zfill(5)
    df["stock_code"] = df["symbol"] + ".HK"
    df["name"] = df["Name of Securities"].astype(str).str.strip()
    df["exchange"] = "HKEX"
    return df[["stock_code", "symbol", "name", "exchange"]].drop_duplicates("stock_code")


def fetch_us_stock_list(top_n: int = 600) -> pd.DataFrame:
    """美股列表。返回列: stock_code, symbol, name, exchange, em_symbol。按 INTL_SOURCE 分发。"""
    if INTL_SOURCE == "em":
        return _fetch_us_stock_list_em(top_n)
    return _fetch_us_stock_list_tx()


def _fetch_us_stock_list_em(top_n: int = 600) -> pd.DataFrame:
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


# 腾讯拉数无独立美股清单接口,清单来源改为三路合并:
#   1) S&P 500 成分股(datasets/s-and-p-500-companies,GitHub raw CSV)
#   2) 纳指 100 成分股(Wikipedia Nasdaq-100 页面第 7 张表,pd.read_html)
#   3) 中概股精选清单(硬编码,覆盖上面两路遗漏的知名中概 ADR)
_SP500_CSV_URL = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
_NASDAQ100_WIKI_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"

# 精选中概股(symbol -> name)。选取依据:知名度 + 流动性,覆盖电商/出行/教育/
# 生物医药/金融科技等主要赛道;不追求穷尽(AKShare 东财口径下这些个股市值
# 均能进前 600,故与 em 路径覆盖大体一致)。
_CHINA_CONCEPT_STOCKS = {
    "BABA": "Alibaba Group",
    "PDD": "PDD Holdings",
    "JD": "JD.com",
    "BIDU": "Baidu",
    "NTES": "NetEase",
    "TME": "Tencent Music",
    "BILI": "Bilibili",
    "NIO": "NIO",
    "XPEV": "XPeng",
    "LI": "Li Auto",
    "TCOM": "Trip.com",
    "ZTO": "ZTO Express",
    "YUMC": "Yum China",
    "BEKE": "KE Holdings",
    "FUTU": "Futu Holdings",
    "HTHT": "H World Group",
    "IQ": "iQIYI",
    "WB": "Weibo",
    "VIPS": "Vipshop",
    "MNSO": "MINISO",
    "QFIN": "Qifu Technology",
    "ATHM": "Autohome",
    "ZLAB": "Zai Lab",
    "LEGN": "Legend Biotech",
    "GDS": "GDS Holdings",
    "BGNE": "BeiGene",
    "TAL": "TAL Education",
    "EDU": "New Oriental Education",
    "BZ": "Kanzhun",
    # 补充:上面两路(S&P500/纳指100)通常覆盖不到的知名中概
    "MOMO": "Hello Group",
    "TIGR": "UP Fintech",
    "LX": "LexinFintech",
    "GOTU": "Gaotu Techedu",
}


def _fetch_us_stock_list_tx() -> pd.DataFrame:
    """三路合并(S&P500 + 纳指100 + 中概精选)。
    返回列: stock_code, symbol, name, exchange, em_symbol。
    exchange 字段腾讯路径下不做精确交易所判定(清单阶段逐个探测太慢,
    详见 fetch_intl_daily 的 tx 实现里的 .OQ/.N 回退逻辑),统一置 'US'。
    em_symbol 存腾讯拉数代码前缀(不含交易所后缀),如 usAAPL、usBRK.B。
    """
    frames = []

    resp = with_retry(requests.get, _SP500_CSV_URL, timeout=20)
    resp.raise_for_status()
    sp500 = pd.read_csv(io.StringIO(resp.text))
    frames.append(sp500.rename(columns={"Symbol": "symbol", "Security": "name"})[["symbol", "name"]])

    resp = with_retry(requests.get, _NASDAQ100_WIKI_URL, timeout=20,
                      headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text))
    nasdaq100 = None
    for t in tables:
        cols = [str(c) for c in t.columns]
        if "Ticker" in cols and "Company" in cols:
            nasdaq100 = t.rename(columns={"Ticker": "symbol", "Company": "name"})[["symbol", "name"]]
            break
    if nasdaq100 is None:
        raise RuntimeError("Nasdaq-100 维基页面未找到 Ticker/Company 表,页面结构可能已变化")
    frames.append(nasdaq100)

    china = pd.DataFrame(
        {"symbol": list(_CHINA_CONCEPT_STOCKS.keys()),
         "name": list(_CHINA_CONCEPT_STOCKS.values())}
    )
    frames.append(china)

    df = pd.concat(frames, ignore_index=True)
    df["symbol"] = df["symbol"].astype(str).str.strip()
    n0 = len(df)
    df = df.drop_duplicates("symbol")
    if n0 - len(df) > 0:
        log.info("fetch_us_stock_list(tx): symbol 去重丢弃 %d 行(S&P500/纳指100/中概重叠)",
                 n0 - len(df))
    df["stock_code"] = df["symbol"] + ".US"
    df["exchange"] = "US"
    df["em_symbol"] = "us" + df["symbol"]
    return df[["stock_code", "symbol", "name", "exchange", "em_symbol"]]


def fetch_intl_daily(market: str, fetch_symbol: str,
                     start: Optional[str] = None, end: Optional[str] = None,
                     adjust: str = "") -> pd.DataFrame:
    """港/美单只日线。fetch_symbol:港股 '00700';美股 em 源 '105.AAPL',
    tx 源 'usAAPL'(即 em_symbol,不含交易所后缀,见 fetch_intl_daily 的 tx 实现)。
    按 INTL_SOURCE 分发。
    """
    if INTL_SOURCE == "em":
        return _fetch_intl_daily_em(market, fetch_symbol, start, end, adjust)
    return _fetch_intl_daily_tx(market, fetch_symbol, start, end, adjust)


def _fetch_intl_daily_em(market: str, fetch_symbol: str,
                         start: Optional[str] = None, end: Optional[str] = None,
                         adjust: str = "") -> pd.DataFrame:
    """东财实现:港/美单只日线。fetch_symbol:港股 '00700',美股 '105.AAPL'。"""
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


# ---------------------------------------------------------------------------
# 腾讯 K 线接口(ifzq.gtimg.cn)—— 探测结论见 .superpowers/sdd/task-4b-report.md:
#   * 响应结构: {"code":0,"data":{"<code>":{"day":[[date,open,close,high,low,
#     volume, ...可选尾随字段...], ...]}}} —— 注意字段顺序是
#     [日期, 开, 收, 高, 低, 量],不是常见的 [开,高,低,收]。
#   * count 上限:实测 2000 可用、2100 起报 "param error";按 7 年一段
#     (7*260≈1820 < 2000)分段请求再拼接,足够覆盖任意长历史且不触顶。
#   * 关键限制(重要发现):fq 参数(qfq/hfq)对港股/美股不生效 —— 服务端
#     只在 A 股(sz/sh 前缀)代码上才会返回 "hfqday"/"qfqday" 键;港股 hk*、
#     美股 us* 代码无论 fq 传什么,返回键恒为 "day" 且数值与不复权一致
#     (已用 00700 的 2014 年 1:5 拆股、AAPL 的 2014 年 7:1 拆股验证:跨拆股
#     日价格不连续,证明没有做复权)。因此腾讯只用来拉「不复权日线价格」,
#     复权因子(hk_adj_factor / us_adj_factor)改走新浪(见
#     _fetch_intl_hfq_factor_tx),Task 4c 已修复。
# ---------------------------------------------------------------------------
_TX_KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
_TX_WINDOW_YEARS = 7   # 每段跨度(年),配合 _TX_MAX_COUNT 避免触发 "param error"
_TX_MAX_COUNT = 2000   # 实测安全上限(2000 可用,2100 报错)

# 抗 WAF 三件套之二:全局请求节流。港股全量在 111 只后触发腾讯 WAF(501),
# 根因之一是请求无节流(2 workers ≈ 4-5 req/s)。用模块级锁 + 单调时钟强制
# 任意两次腾讯请求(含 with_retry 内部重试)间隔 ≥ TX_MIN_INTERVAL,锁内
# sleep 补足间隔以便跨线程也生效(持锁线程睡够时间才放行下一个请求方)。
TX_MIN_INTERVAL = float(os.getenv("ASTOCK_TX_MIN_INTERVAL", "0.35"))
_tx_throttle_lock = threading.Lock()
_tx_last_request_ts = 0.0


def _tx_throttle() -> None:
    """阻塞直到与上一次腾讯请求的间隔 ≥ TX_MIN_INTERVAL。跨线程生效。"""
    global _tx_last_request_ts
    with _tx_throttle_lock:
        wait = _tx_last_request_ts + TX_MIN_INTERVAL - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _tx_last_request_ts = time.monotonic()


def _tx_kline_request(code: str, start: str, end: str,
                      count: int = _TX_MAX_COUNT, fq: str = "") -> pd.DataFrame:
    """单次腾讯 K 线请求。start/end 为 'YYYY-MM-DD' 或空串(不限)。
    返回列: trade_date, open, close, high, low, volume(未复权/未改列名,
    供上层函数再加工)。
    """
    param = f"{code},day,{start},{end},{count},{fq}"

    def _do():
        _tx_throttle()
        resp = requests.get(_TX_KLINE_URL, params={"param": param}, timeout=15)
        resp.raise_for_status()
        return resp.json()

    data = with_retry(_do)
    if data.get("code") != 0:
        return pd.DataFrame()
    entry = data.get("data", {}).get(code)
    if not entry:
        return pd.DataFrame()
    # hfq/qfq 对港美股不生效(见上方模块说明),恒为 "day";保留这几个 key 的
    # 探测顺序以防未来腾讯补上支持。
    key = "hfqday" if "hfqday" in entry else ("qfqday" if "qfqday" in entry else "day")
    rows = entry.get(key) or []
    if not rows:
        return pd.DataFrame()
    out = [r[:6] for r in rows]  # 每行前 6 个字段恒为 [date,open,close,high,low,volume]
    df = pd.DataFrame(out, columns=["trade_date", "open", "close", "high", "low", "volume"])
    for col in ("open", "close", "high", "low", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    return df.dropna(subset=["trade_date"])


def _tx_year_windows(start_year: int, end_year: int) -> list[tuple[int, int]]:
    """按 _TX_WINDOW_YEARS 年一段切分 [start_year, end_year],窗口边界与旧的
    正序实现完全一致(只是遍历方向由调用方决定),保证输出等价。
    """
    windows = []
    y = start_year
    while y <= end_year:
        y2 = min(y + _TX_WINDOW_YEARS - 1, end_year)
        windows.append((y, y2))
        y = y2 + 1
    return windows


def _tx_fetch_full(code: str, start_year: int, end_year: int, fq: str = "") -> pd.DataFrame:
    """抗 WAF 三件套之一:窗口倒序遍历 + 空窗即停。

    从最新窗口向最早方向逐个请求(港/美股大多 2000 年后上市,若仍按老实现
    从 1980 正序拉,上市前的窗口全是空请求,白白浪费流量、加速触发 WAF)。
    某窗口返回 0 行即认为已越过该股上市前的历史起点,提前停止(正常情况下
    最多浪费 1 个空请求)。

    边界:最新(含今天)窗口若为空,不能就此断定"无历史"——该股可能只是
    近期停牌/退市,继续往前多试 1 个窗口;若那个窗口仍为空才真正停止(即
    连续 2 个空窗才停,最多浪费 2 个请求);一旦确认拿到过数据,后续窗口
    恢复"单个空窗即停"的正常规则。

    拼接后仍按 trade_date 正序返回、去重排序逻辑不变,窗口边界与旧的正序
    实现完全一致,故最终输出(行序/列/内容)与旧实现等价,调用方无感知。
    """
    windows = _tx_year_windows(start_year, end_year)
    frames = []
    is_first_window = True
    for y, y2 in reversed(windows):
        df = _tx_kline_request(code, f"{y}-01-01", f"{y2}-12-31", fq=fq)
        if not df.empty:
            frames.append(df)
            is_first_window = False
            continue
        if is_first_window:
            # 首窗(最新)为空:可能是停牌/退市,再往前多试 1 个窗口再判断
            is_first_window = False
            continue
        break  # 非首窗为空:已越过历史起点,停止
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True).drop_duplicates("trade_date").sort_values("trade_date")
    return out.reset_index(drop=True)


def _fetch_daily_tx(symbol: str, start: str = "19900101", end: Optional[str] = None) -> pd.DataFrame:
    """腾讯实现(备源,仅东财被封时用):单只 A 股不复权日线。

    symbol 经 to_sina_code 转腾讯 A 股代码(600000 -> sh600000,000001 ->
    sz000001,830799 -> bj830799),复用 _tx_fetch_full 的 7 年窗口分页基础
    设施(fq="" 走不复权 "day" 键;A 股代码下腾讯服务端也支持 "hfqday"/
    "qfqday",但本函数只取不复权价,复权因子仍固定走新浪 fetch_hfq_factor)。

    amount(成交额):_tx_kline_request 按现有实现只解析每行前 6 个字段
    [日期,开,收,高,低,量](见该函数顶部说明);HK/US 场景下已确认偶发的
    第 7 个字段是除权除息公告 dict,不是数值。本次未能对 A 股 sh/sz 代码的
    原始响应做实地探测复核(验证时腾讯接口对本机 IP 返回 501/WAF 拦截,
    详见任务报告"疑虑"一节),按现有 6 字段实现保守处理,固定置 NULL,
    与 _fetch_intl_daily_tx 对 HK/US 的处理一致;若后续证实 A 股行确有
    成交额字段,需要改造 _tx_kline_request 保留原始行尾部再在此提取。
    turnover(换手率)腾讯无对应字段来源,同样固定 NULL。
    pct_chg 本地用 close.pct_change()*100 计算(东财返回的涨跌幅是精确值,
    腾讯没有对应字段,只能反算;边界:窗口内第一行相对上一个自然年末尾
    交易日计算,过滤到 [start,end] 之前先算好,避免窄窗口首行 pct_chg 为 NaN)。
    """
    end = end or datetime.now().strftime("%Y%m%d")
    code = to_sina_code(symbol)
    start_year = int(str(start)[:4])
    end_year = int(str(end)[:4])

    df = _tx_fetch_full(code, start_year, end_year, fq="")
    if df.empty:
        return pd.DataFrame()
    df = df.sort_values("trade_date").reset_index(drop=True)
    df["pct_chg"] = df["close"].pct_change() * 100
    # 腾讯 A 股 K 线成交量单位按板块不同:主板/创业板是「手」(实测 000001、
    # 002830 与东财原始值一致),但科创板(688/689)原生就是「股」(2026-07-09
    # 实测 688469:腾讯值与通达信分钟加总一致,是主板口径的 100 倍)。
    # 全库统一存股:非科创板 ×100,科创板不换算。港/美(_fetch_intl_daily_tx)
    # 原生股,不换算;北交所腾讯无 K 线数据(daily=0),暂不涉及。
    if not symbol.lstrip().startswith(("688", "689")):
        df["volume"] = df["volume"] * 100
    df["amount"] = pd.NA
    df["turnover"] = pd.NA

    start_d = pd.to_datetime(start).date()
    end_d = pd.to_datetime(end).date()
    df = df[(df["trade_date"] >= start_d) & (df["trade_date"] <= end_d)].reset_index(drop=True)
    return df[["trade_date", "open", "high", "low", "close",
               "volume", "amount", "pct_chg", "turnover"]]


# 美股代码后缀解析缓存:清单阶段 em_symbol 不含交易所后缀(如 'usAAPL'),
# 逐个探测太慢,改为在首次实际拉数时按 "原样 -> .OQ -> .N" 试探并缓存
# (纳斯达克 = .OQ,纽交所 = .N;已用 AAPL/.OQ、GE/KO/IBM/.N、BABA/PDD/.N/.OQ
# 等验证)。
#
# 探测有两个陷阱(两轮探测的设计由此而来):
# 1. 错误后缀不会返回空,而是返回 1~2 行"稀疏假数据"(服务端按裸代码模糊
#    匹配到了真实标的的实时行情,但历史K线只给了极少缓存行),故不能以
#    "非空"判空,必须设行数阈值(≥10)。
# 2. 【换所股陷阱,Task 4b 修复】曾换过交易所的股票(如 DELL/CIEN/DECK/SCHW
#    都是纳斯达克 -> 纽交所),废弃的旧上市地代码(usDELL.OQ)在**无日期**
#    探测下仍会返回旧史缓存的最后 30 行(≥10 行,足以冒充命中),而 .OQ 排
#    在 .N 之前 —— 于是解析错选了死代码;随后 _tx_fetch_full 的"最新窗口
#    倒序 + 空窗即停"策略在死代码上最近两个窗口全空、直接早停,日线 0 行。
#    修复:第一轮探测改用**带日期的近 60 天窗口**(死代码对带日期的近期请求
#    返回 0 行,只有现役上市地会返回成片数据),窗口约 40 个交易日,阈值
#    ≥10 行留足假期/停牌余量;若三个候选全不达标(近 60 天无数据,例如
#    已退市、或腾讯没有该交易所的行情源,如 CBOE 的 Cboe BZX),再退回
#    第二轮无日期探测(旧行为),保证至少还能解析到有旧史缓存的代码。
#    边界:上市不足 10 个交易日的新股两轮都可能探测不中,维持裸码返回
#    (行为与修复前一致)。
_US_TX_SUFFIXES = ("", ".OQ", ".N")
_US_TX_PROBE_DAYS = 60      # 第一轮探测回看的自然日窗口
_US_TX_PROBE_MIN_ROWS = 10  # 命中所需最少行数(约 40 个交易日里出现 ≥10 行)
_us_tx_code_cache: dict[str, str] = {}


def _tx_resolve_us_code(em_symbol: str) -> str:
    if em_symbol in _us_tx_code_cache:
        return _us_tx_code_cache[em_symbol]
    today = date.today()
    p_start = (today - timedelta(days=_US_TX_PROBE_DAYS)).isoformat()
    p_end = today.isoformat()
    resolved = None
    # 第一轮:带日期的近 60 天窗口(只有现役上市地会返回成片近期数据,
    # 死代码/错误后缀返回 0~1 行,见上方"换所股陷阱")
    for suf in _US_TX_SUFFIXES:
        cand = em_symbol + suf
        probe = with_retry(_tx_kline_request, cand, p_start, p_end, 45, "")
        if len(probe) >= _US_TX_PROBE_MIN_ROWS:
            resolved = cand
            break
    # 第二轮(兜底):无日期探测。覆盖近 60 天无数据的标的(已退市/腾讯缺
    # 该交易所行情源),至少解析到有历史缓存的代码;注意此路径解析出的代码
    # 经 _tx_fetch_full 的"空窗即停"仍可能拉到 0 行(如 CBOE),属数据源缺口。
    if resolved is None:
        for suf in _US_TX_SUFFIXES:
            cand = em_symbol + suf
            probe = with_retry(_tx_kline_request, cand, "", "", 30, "")
            if len(probe) >= _US_TX_PROBE_MIN_ROWS:
                resolved = cand
                break
    if resolved is None:
        resolved = em_symbol
    _us_tx_code_cache[em_symbol] = resolved
    return resolved


def _fetch_intl_daily_tx(market: str, fetch_symbol: str,
                         start: Optional[str] = None, end: Optional[str] = None,
                         adjust: str = "") -> pd.DataFrame:
    """腾讯实现:港/美单只日线。fetch_symbol:港股 '00700'(5 位数字),
    美股 'usAAPL' 形式的 em_symbol(不含交易所后缀,内部自动解析)。

    返回前按 [start,end] 裁剪(与 A 股 _fetch_daily_tx 一致):腾讯按年
    窗口拉取,若不裁剪,增量调用会把窗口外(拉取起始年年初起)的旧行也
    upsert 回库,且拉取段首行 pct_chg 为 NaN,会把库里原本正确的值覆盖成
    NULL。pct_chg 先在完整窗口序列上计算再裁剪,窄窗口首行的 pct_chg 用
    的是窗口外前一交易日收盘,不再是 NaN。
    """
    cfg = MARKETS[market]
    start = start or cfg["start"]
    end = end or datetime.now().strftime("%Y%m%d")
    start_year = int(str(start)[:4])
    end_year = int(str(end)[:4])

    if market == "hk":
        code = "hk" + str(fetch_symbol).zfill(5)
    else:
        code = _tx_resolve_us_code(fetch_symbol)

    df = _tx_fetch_full(code, start_year, end_year, fq=adjust)
    if df.empty:
        return pd.DataFrame()
    df = df.sort_values("trade_date").reset_index(drop=True)
    df["pct_chg"] = df["close"].pct_change() * 100
    df["amount"] = pd.NA
    df["turnover"] = pd.NA

    start_d = pd.to_datetime(start).date()
    end_d = pd.to_datetime(end).date()
    df = df[(df["trade_date"] >= start_d) & (df["trade_date"] <= end_d)].reset_index(drop=True)
    return df[["trade_date", "open", "high", "low", "close",
               "volume", "amount", "pct_chg", "turnover"]]


def fetch_intl_hfq_factor(market: str, fetch_symbol: str,
                          raw: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """后复权因子 = hfq 收盘 ÷ 原始收盘。raw 可传入已拉取的不复权日线省一次请求。
    按 INTL_SOURCE 分发。
    """
    if INTL_SOURCE == "em":
        return _fetch_intl_hfq_factor_em(market, fetch_symbol, raw)
    return _fetch_intl_hfq_factor_tx(market, fetch_symbol, raw)


def _fetch_intl_hfq_factor_em(market: str, fetch_symbol: str,
                              raw: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    if raw is None:
        raw = _fetch_intl_daily_em(market, fetch_symbol)
    hfq = _fetch_intl_daily_em(market, fetch_symbol, adjust="hfq")
    if raw.empty or hfq.empty:
        return pd.DataFrame()
    merged = raw[["trade_date", "close"]].merge(
        hfq[["trade_date", "close"]], on="trade_date", suffixes=("_raw", "_hfq"))
    merged = merged[merged["close_raw"] > 0]
    merged["adj_factor"] = merged["close_hfq"] / merged["close_raw"]
    return merged[["trade_date", "adj_factor"]].dropna()


def _fetch_intl_hfq_factor_tx(market: str, fetch_symbol: str,
                              raw: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """港/美复权因子改走新浪(Task 4c)。返回列: trade_date, adj_factor。

    背景:腾讯 K 线 fq 参数对港/美股不生效(见 _tx_kline_request 上方说明),
    此前这里用 "hfq close ÷ raw close" 算出的 adj_factor 恒为 1.0,是数据源
    本身的限制。腾讯继续只负责拉「不复权日线价格」(_fetch_intl_daily_tx),
    因子改用新浪的 *-factor 接口,`raw` 参数不再需要(保留仅为兼容旧签名)。

    港股:ak.stock_hk_daily(adjust="hfq-factor") 直接给出后复权因子 —— 锚点
    在最早交易日(=1)、逐笔递增,与本库 A 股 adj_factor 的语义一致,列名
    重命名即可入库。响应里还有一列 "cash"(累计现金分红,港元)未使用——
    完整公式是 hfq_close=(raw_close+cash)*factor,但本库 schema 只有单一
    乘法因子列存不下 cash,忽略它是已知简化:只丢失现金分红再投资的效应,
    不影响拆股场景的连续性(已用 00700 2014-05 一拆五验证,见 task-4c 报告)。

    美股:ak.stock_us_daily 没有 hfq-factor,只有 qfq-factor(锚点在最新
    交易日=1、逐笔递减)。**qfq_factor 要直接当乘法因子用,不能取倒数**——
    实测取倒数会在拆股日制造出巨大的人为价格跳变(以 AAPL 2020-08-31
    四拆一为例:直接使用 close*qfq_factor 在拆股前后连续、偏差约 3.4%;
    取倒数 close*(1/qfq_factor) 则从约 1997 跳到 129,偏差 1447%,详见
    task-4c 报告的实测数字)。

    锚点重标定(Task 4c 复审修复):本库三市场统一「最早日因子=1、逐笔
    递增」的锚点约定(A 股新浪 hfq-factor、港股新浪 hfq-factor 天然如此)。
    新浪美股 qfq_factor 锚点却在最新日(最新一行恒为 1)——若原样入库,
    *_daily_price_qfq 视图里「除以该股最新因子」退化为除以 1,导致
    us hfq/qfq 两视图逐行恒等(复审实测确认)。故入库前整段除以最早一行
    的因子值重锚定为「最早日=1」。这是线性重标定,不改变任何日间相对关系,
    拆股连续性不受影响;重锚后最新因子≈累计拆股倍数(AAPL 约 224),
    NUMERIC(18,6) 足够。同样忽略了 "adjust" 累计分红调整列(简化,理由同
    港股)。symbol 需去掉 em_symbol 的 "us" 前缀 —— 新浪美股接口用
    裸 ticker(如 "AAPL"/"BRK.B"),不接受 "usAAPL"(实测 IndexError)。
    """
    import akshare as ak

    if market == "hk":
        symbol = str(fetch_symbol).zfill(5)
        df = with_retry(ak.stock_hk_daily, symbol=symbol, adjust="hfq-factor")
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.rename(columns={"date": "trade_date", "hfq_factor": "adj_factor"})
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        df["adj_factor"] = pd.to_numeric(df["adj_factor"], errors="coerce")
        return (df[["trade_date", "adj_factor"]].dropna()
                  .sort_values("trade_date").reset_index(drop=True))

    # us:去掉 em_symbol 的 "us" 前缀还原裸 ticker
    ticker = fetch_symbol[2:] if str(fetch_symbol).lower().startswith("us") else str(fetch_symbol)
    df = with_retry(ak.stock_us_daily, symbol=ticker, adjust="qfq-factor")
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns={"date": "trade_date", "qfq_factor": "adj_factor"})
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    df["adj_factor"] = pd.to_numeric(df["adj_factor"], errors="coerce")
    df = df[df["adj_factor"] > 0]   # 防御性过滤,避免脏数据/0 传导到下游视图
    df = (df[["trade_date", "adj_factor"]].dropna()
            .sort_values("trade_date").reset_index(drop=True))
    if df.empty:
        return df
    # 重锚定:最早日=1,与 A 股/港股约定一致(见 docstring「锚点重标定」)
    df["adj_factor"] = df["adj_factor"] / df["adj_factor"].iloc[0]
    return df


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
# 收盘防护:盘中跑 ETL 时,拒绝把当天未定盘的 A 股 bar 写进库
# ===========================================================================
# 仅适用于 A 股(北京时间 15:30 口径):15:00 收盘竞价结束后,创业板/科创板
# 还有盘后定价交易(15:05-15:30),其成交量计入当日总量,过 15:30 才算定盘。
# 港/美股时区不同,不适用本防护,由各自更新器的运行时点保证(见 06 脚本 cron)。
MARKET_CLOSE_TIME = dt_time(15, 30)

# 网络时间来源(取 HTTP 响应头的 Date 字段,GMT)。本机时钟不可信:
# 曾出现机器时钟快 1 小时,导致盘中快照被当成收盘数据入库。
_TIME_SOURCES = ["https://www.baidu.com", "https://qt.gtimg.cn"]
_CST = timezone(timedelta(hours=8))
# (cutoff, time.monotonic())
_cutoff_cache: Optional[tuple[date, float]] = None
# cutoff 统一只缓存这么久:长任务跨过 15:30/午夜后要能自动重算
_CUTOFF_TTL_OPEN = 600.0


def beijing_now() -> datetime:
    """当前北京时间。优先取网络时间;全部失败才退回本机时钟(带告警)。"""
    for url in _TIME_SOURCES:
        try:
            req = urllib.request.Request(url, method="HEAD")
            with urllib.request.urlopen(req, timeout=5) as resp:
                header = resp.headers.get("Date")
            if not header:
                continue
            net = parsedate_to_datetime(header)
            if net.tzinfo is None:          # 个别代理返回 '-0000',解析出 naive 时间,按 GMT 处理
                net = net.replace(tzinfo=timezone.utc)
            net = net.astimezone(_CST)
            drift = abs((datetime.now(_CST) - net).total_seconds())
            if drift > 120:
                log.warning("本机时钟与网络时间相差 %.0f 秒,以网络时间为准", drift)
            return net
        except Exception as exc:  # noqa: BLE001
            log.warning("获取网络时间失败(%s): %s", url, exc)
    log.warning("所有网络时间源不可用,退回本机时钟 —— 若本机时间不准,当日数据可能有误")
    return datetime.now(_CST)


def _cutoff_of(t: datetime) -> date:
    """按 15:30 口径把一个北京时间点折算成允许写入的最晚 trade_date。"""
    return t.date() if t.time() >= MARKET_CLOSE_TIME else t.date() - timedelta(days=1)


def safe_cutoff_date() -> date:
    """
    允许写入的最晚 trade_date(缓存,避免每只股票都发时间请求)。
    未到当天 15:30 → 只能写到昨天;否则可写到今天。

    取「网络时间算出的 cutoff」与「本机时钟算出的 cutoff」的较早者:任何一路
    时间读数出错(时间源/代理返回错误 Date 头、本机时钟漂移),只会让 cutoff
    更保守——当日定盘 bar 最多晚一次运行入库,由断点续传自动补齐;绝不会把
    盘中快照放进库。2026-07-10 实案:init 长跑在开盘时段(本机时钟正确)静默
    拿到 cutoff=当天,写入 327 只盘中 bar,事后无法复原是哪路时间出错——单一
    读数不可信,min() 则两路同时出错才会失守。

    缓存一律只存 10 分钟(收盘后也一样,一次 HEAD 请求的代价可忽略):跨夜/
    跨天长任务必须能重算,旧的「已收盘结果整进程有效」策略会把一次错误判定
    放大到整个进程周期。
    """
    global _cutoff_cache
    if _cutoff_cache is not None:
        cutoff, at = _cutoff_cache
        if time.monotonic() - at < _CUTOFF_TTL_OPEN:
            return cutoff
    net = beijing_now()
    local = datetime.now(_CST)
    cutoff = min(_cutoff_of(net), _cutoff_of(local))
    # 无论盘中还是盘后都留痕:07-10 事故里"已收盘"路径静默,导致无法归因
    log.info("收盘防护 cutoff=%s(网络 %s / 本机 %s)",
             cutoff, net.strftime("%m-%d %H:%M"), local.strftime("%m-%d %H:%M"))
    _cutoff_cache = (cutoff, time.monotonic())
    return cutoff


def drop_unclosed_bars(df: pd.DataFrame, label: str) -> pd.DataFrame:
    """丢弃 trade_date 晚于 cutoff 的行(盘中快照/异常未来日期)。"""
    if df.empty or "trade_date" not in df.columns:
        return df
    cutoff = safe_cutoff_date()
    mask = df["trade_date"] <= cutoff
    dropped = int((~mask).sum())
    if not dropped:
        return df          # 常见情形(盘后运行)直接返回,省一次整表拷贝
    log.info("%s: 跳过 %d 行未定盘数据(> %s)", label, dropped, cutoff)
    return df[mask]


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
    if table == "daily_price":          # 收盘防护仅限 A 股表(北京 15:30 口径)
        df = drop_unclosed_bars(df, stock_code)
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
    # 盘中跑到除权日时,fetch_hfq_factor 的现算路径会用未定盘价算出当日因子,同样要拦
    if table == "adj_factor":           # 仅 A 股
        df = drop_unclosed_bars(df, f"{stock_code}(adj)")
    if df.empty:
        return 0
    cols = ["stock_code", "trade_date", "adj_factor"]
    rows = [(stock_code, r.trade_date, float(r.adj_factor)) for r in df.itertuples(index=False)]
    return upsert(conn, table, cols, rows, ["stock_code", "trade_date"])


def upsert_minute(conn, stock_code: str, df: pd.DataFrame) -> int:
    """1 分钟线入库。trade_time 为 bar 结束时刻;volume 单位股(通达信原生即股)。

    不挂 drop_unclosed_bars(那是日线的 15:30 口径):分钟 bar 只要该分钟
    已走完即为定盘,由调用方按 beijing_now() 过滤未走完的最后一根。
    """
    if df.empty:
        return 0
    cols = ["stock_code", "trade_time", "open", "high", "low", "close", "volume", "amount"]
    rows = [
        (stock_code, r.trade_time,
         _num(r, "open"), _num(r, "high"), _num(r, "low"), _num(r, "close"),
         _int(r, "volume"), _num(r, "amount"))
        for r in df.itertuples(index=False)
    ]
    return upsert(conn, "minute_price", cols, rows, ["stock_code", "trade_time"])


def ensure_minute_partitions(conn, start: date, months: int) -> None:
    """确保 [start 所在月, +months) 的月度分区存在(调 schema 里的同名 SQL 函数)。"""
    with conn.cursor() as cur:
        cur.execute("SELECT ensure_minute_partitions(%s, %s)", (start, months))
    conn.commit()


def upsert_index(conn, index_code: str, df: pd.DataFrame, table: str = "index_daily") -> int:
    if table == "index_daily":          # 仅 A 股
        df = drop_unclosed_bars(df, index_code)
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


def run_stock_todo(todo, task: str, load_fn, workers: int,
                   max_consecutive_errors: Optional[int] = None) -> None:
    """
    按 workers 数串行或并行处理股票清单。
    load_fn(conn, row):处理单只;抛异常则记 error 进度,不中断整体。

    抗 WAF 三件套之三:熔断器。max_consecutive_errors 为 None(默认)时行为
    与之前完全一致、不开启熔断。开启后:连续失败次数(任一成功即清零,
    锁保护、线程安全)达到阈值即视为疑似数据源被封禁 —— 记一条
    log.critical(说明疑似源封禁、已处理 N / 共 M、建议冷却后重跑续传),
    并停止派发剩余待办:并行模式下置停止标志,work() 开头检查到标志直接
    return 跳过(不标 error,留给下次续传自动补上);串行模式直接 break。
    """
    todo = list(todo)
    total = len(todo)
    counter = itertools.count(1)  # CPython 下 next() 原子,足够做进度计数

    breaker_lock = threading.Lock()
    breaker_state = {"consecutive": 0, "tripped": False}

    def _note_result(success: bool, i: int) -> None:
        if max_consecutive_errors is None:
            return
        with breaker_lock:
            if success:
                breaker_state["consecutive"] = 0
                return
            breaker_state["consecutive"] += 1
            if breaker_state["consecutive"] >= max_consecutive_errors and not breaker_state["tripped"]:
                breaker_state["tripped"] = True
                log.critical(
                    "连续 %d 只失败,疑似数据源被封禁(WAF/限流),停止派发剩余待办"
                    "(已处理 %d / 共 %d)。建议冷却一段时间后重跑本任务续传"
                    "(未派发的股票不会被标记为 error,断点续传会自动补上)。",
                    breaker_state["consecutive"], i, total,
                )

    def work(r):
        if max_consecutive_errors is not None and breaker_state["tripped"]:
            return  # 熔断已触发:跳过剩余待办(不标 error),留给下次续传
        conn = _thread_conn()
        success = False
        try:
            load_fn(conn, r)
            success = True
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            mark_progress(conn, task, r.stock_code, None, status="error", message=str(exc))
            log.error("  %s 失败: %s", r.stock_code, exc)
        i = next(counter)
        _note_result(success, i)
        if i % 100 == 0:
            log.info("进度 %d / %d", i, total)

    if workers <= 1:
        for r in todo:
            work(r)
            if max_consecutive_errors is not None and breaker_state["tripped"]:
                break
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


# ===========================================================================
# 基本面(二期)。设计: docs/superpowers/specs/2026-07-10-ashare-fundamental-design.md
# 截面接口(东财,含公告日)供指标骨干;新浪按股供全科目 JSONB。
# ===========================================================================
from datetime import date as _date

FUND_START = _date(2015, 12, 31)

# stock_yjbb_em 实探列(2026-07-10):序号/股票代码/股票简称/每股收益/营业总收入-营业总收入/
# 营业总收入-同比增长/营业总收入-季度环比增长/净利润-净利润/净利润-同比增长/净利润-季度环比增长/
# 每股净资产/净资产收益率/每股经营现金流量/销售毛利率/所处行业/最新公告日期
RENAME_YJBB = {
    "股票代码": "symbol", "每股收益": "eps", "营业总收入-营业总收入": "revenue",
    "营业总收入-同比增长": "revenue_yoy", "净利润-净利润": "net_profit",
    "净利润-同比增长": "net_profit_yoy", "每股净资产": "bps",
    "净资产收益率": "roe", "每股经营现金流量": "ocf_ps", "销售毛利率": "gross_margin",
    "所处行业": "industry", "最新公告日期": "ann_date",
}
# stock_lrb_em 实探列(2026-07-10,20250331,5221 行):序号/股票代码/股票简称/净利润/净利润同比/
# 营业总收入/营业总收入同比/营业总支出-营业支出/营业总支出-销售费用/营业总支出-管理费用/
# 营业总支出-财务费用/营业总支出-营业总支出/营业利润/利润总额/公告日期
RENAME_LRB = {
    "股票代码": "symbol", "净利润": "net_profit", "净利润同比": "net_profit_yoy",
    "营业总收入": "revenue", "营业总收入同比": "revenue_yoy",
    "营业利润": "operating_profit", "利润总额": "total_profit",
    "公告日期": "ann_date",
}
# stock_zcfz_em 实探列(2026-07-10,20250331,5166 行):序号/股票代码/股票简称/资产-货币资金/
# 资产-应收账款/资产-存货/资产-总资产/资产-总资产同比/负债-应付账款/负债-预收账款/
# 负债-总负债/负债-总负债同比/资产负债率/股东权益合计/公告日期
RENAME_ZCFZ = {
    "股票代码": "symbol", "资产-货币资金": "cash", "资产-应收账款": "accounts_recv",
    "资产-存货": "inventory", "资产-总资产": "total_assets",
    "负债-应付账款": "accounts_pay", "负债-总负债": "total_liab",
    "资产负债率": "debt_ratio", "股东权益合计": "total_equity",
    "公告日期": "ann_date",
}
# stock_xjll_em 实探列(2026-07-10,20250331,5221 行):序号/股票代码/股票简称/净现金流-净现金流/
# 净现金流-同比增长/经营性现金流-现金流量净额/经营性现金流-净现金流占比/投资性现金流-现金流量净额/
# 投资性现金流-净现金流占比/融资性现金流-现金流量净额/融资性现金流-净现金流占比/公告日期
RENAME_XJLL = {
    "股票代码": "symbol", "净现金流-净现金流": "net_cash_flow",
    "经营性现金流-现金流量净额": "ocf", "投资性现金流-现金流量净额": "icf",
    "融资性现金流-现金流量净额": "fcf", "公告日期": "ann_date",
}

_CROSS_FN = {"yjbb": "stock_yjbb_em", "lrb": "stock_lrb_em",
             "zcfz": "stock_zcfz_em", "xjll": "stock_xjll_em"}
_CROSS_RENAME = {"yjbb": RENAME_YJBB, "lrb": RENAME_LRB,
                 "zcfz": RENAME_ZCFZ, "xjll": RENAME_XJLL}


def quarter_ends(start: _date, end: _date) -> list[date]:
    """start~end 间全部季末日(3/31, 6/30, 9/30, 12/31),含端点。"""
    out, y = [], start.year
    while y <= end.year:
        for m, d in ((3, 31), (6, 30), (9, 30), (12, 31)):
            q = _date(y, m, d)
            if start <= q <= end:
                out.append(q)
        y += 1
    return out


def fetch_fin_cross(kind: str, period: str) -> pd.DataFrame:
    """东财按报告期截面。period 'YYYYMMDD'(季末日)。返回含 stock_code/ann_date 的重命名帧。"""
    import akshare as ak

    fn = getattr(ak, _CROSS_FN[kind])
    df = with_retry(fn, date=period)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns=_CROSS_RENAME[kind])
    keep = [c for c in set(_CROSS_RENAME[kind].values()) if c in df.columns]
    df = df[keep].copy()
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    df["stock_code"] = df["symbol"].map(to_full_code)
    if "ann_date" in df.columns:
        df["ann_date"] = pd.to_datetime(df["ann_date"], errors="coerce").dt.date
    return df


_SINA_STMT = {"balance": "资产负债表", "income": "利润表", "cashflow": "现金流量表"}


def fetch_fin_report_sina(symbol: str, stmt_type: str) -> pd.DataFrame:
    """新浪全科目报表(单请求全历史)。返回 report_date + 原始中文科目列。"""
    import akshare as ak

    df = with_retry(ak.stock_financial_report_sina,
                    stock=to_sina_code(symbol), symbol=_SINA_STMT[stmt_type])
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns={"报告日": "report_date"})
    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce").dt.date
    return df.dropna(subset=["report_date"])


def upsert_jsonb_statement(conn, stock_code: str, stmt_type: str, df: pd.DataFrame) -> int:
    """新浪报表帧 → fin_statement JSONB(过滤 report_date >= FUND_START;NaN 键剔除)。"""
    import json

    if df.empty:
        return 0
    df = df[df["report_date"] >= FUND_START]
    rows = []
    for _, r in df.iterrows():
        payload = {k: (None if pd.isna(v) else (float(v) if isinstance(v, (int, float)) else str(v)))
                   for k, v in r.items() if k != "report_date" and not pd.isna(v)}
        rows.append((stock_code, r["report_date"], stmt_type, json.dumps(payload, ensure_ascii=False)))
    return upsert(conn, "fin_statement",
                  ["stock_code", "report_date", "stmt_type", "data"],
                  rows, ["stock_code", "report_date", "stmt_type"], update_cols=["data"])


# 股本结构直连东财 datacenter API(RPT_F10_EH_EQUITY,即 ak.stock_zh_a_gbjg_em 的底层源)。
# 不走 akshare 是因为其硬编码 pageNumber=1&pageSize=20,按 END_DATE 降序只给最近 20 条,
# 变动频繁的股票(如 000004.SZ 共 39 条)2016 年前后的历史被截断。这里 pageSize=500 并按
# result.pages 翻页拉全历史。600519.SH 实测总股本=1,250,081,601(股口径,非万股,与实际
# 已知总股本量级吻合),故不做 ×10000 换算。
_EM_GBJG_URL = "https://datacenter.eastmoney.com/securities/api/data/v1/get"


def fetch_share_structure(symbol: str) -> pd.DataFrame:
    """东财股本结构变动(全历史)。列: change_date, total_shares, float_shares, reason。单位:股。"""
    full = to_full_code(symbol)  # 需 '600519.SH' 形式

    def _page(page_no: int) -> dict:
        resp = requests.get(_EM_GBJG_URL, params={
            "reportName": "RPT_F10_EH_EQUITY",
            "columns": "SECUCODE,END_DATE,TOTAL_SHARES,LISTED_A_SHARES,CHANGE_REASON",
            "filter": f'(SECUCODE="{full}")',
            "pageNumber": str(page_no),
            "pageSize": "500",
            "sortTypes": "-1",
            "sortColumns": "END_DATE",
            "source": "HSF10",
            "client": "PC",
        }, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
        resp.raise_for_status()
        js = resp.json()
        # 东财响应信封:success=False 代表请求本身失败(限流/参数错误/临时故障等,
        # 实测报表配置类错误 message="报表配置不存在,..."、code!=0),不是"翻页翻完了"
        # ——翻页结束的正常信号是 success=True 但 result.data 为空列表(见下方调用处的
        # `if not data: break`)。两者混为一谈会把请求失败静默当成"该股无股本数据",
        # 丢数据不报错。这里 raise 交给 with_retry 的指数退避重试,重试耗尽后按现有
        # 异常传播规则处理(run_stock_todo 记 error、断点续传补跑)。
        if js.get("success") is not True:
            raise RuntimeError(
                f"东财股本接口返回失败 (SECUCODE={full}, page={page_no}): "
                f"code={js.get('code')}, message={js.get('message')}"
            )
        return js

    records: list[dict] = []
    page_no, total_pages = 1, 1
    while page_no <= total_pages:
        data_json = with_retry(_page, page_no)
        result = (data_json or {}).get("result") or {}
        data = result.get("data") or []
        if not data:
            break
        records.extend(data)
        total_pages = int(result.get("pages") or 1)
        page_no += 1
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records).rename(columns={
        "END_DATE": "change_date", "TOTAL_SHARES": "total_shares",
        "LISTED_A_SHARES": "float_shares", "CHANGE_REASON": "reason",
    })
    df["change_date"] = pd.to_datetime(df["change_date"], errors="coerce").dt.date
    for col in ("total_shares", "float_shares"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["change_date"])
    # 同日多条时保留最新一条(源按 END_DATE 降序),防 upsert 单批内主键冲突
    df = df.drop_duplicates(subset=["change_date"], keep="first")
    return df[["change_date", "total_shares", "float_shares", "reason"]]


# stock_value_em 实探(2026-07-10,symbol='600519' 六位裸代码):按股全历史时间序列
# (2018-01-02 起至今 2065 行),非按日截面 —— 与 fetch_valuation 按股循环调用的设计一致,
# Task 3 阶段 3 的"按股循环"假设不需要改动。列 = 数据日期/当日收盘价/当日涨跌幅/总市值/
# 流通市值/总股本/流通股本/PE(TTM)/PE(静)/市净率/PEG值/市现率/市销率。源不含股息率(dv_ratio)
# 与市销率TTM(ps_ttm),两列按 brief 签名保留但恒为 NaN(下游/Task 4 需知悉此限制)。
def fetch_valuation(symbol: str) -> pd.DataFrame:
    """东财估值历史。列: trade_date, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio, total_mv。

    源对部分股票(实测次新股 301583、北交所 920081)无数据,result 为 null,
    akshare 内部抛 TypeError——这是"无数据"而非瞬时故障,直接按空返回,
    不进 with_retry 的指数退避(否则这类股票每次都白等 30s 且永远记 error)。
    """
    import akshare as ak

    def _value_em(sym: str) -> pd.DataFrame:
        try:
            return ak.stock_value_em(symbol=sym)
        except TypeError as exc:
            log.warning("估值接口 %s 返回异常(疑似无数据,按空处理): %s", sym, exc)
            return pd.DataFrame()

    df = with_retry(_value_em, symbol.strip().zfill(6))
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns={
        "数据日期": "trade_date", "PE(静)": "pe", "PE(TTM)": "pe_ttm",
        "市净率": "pb", "市销率": "ps", "总市值": "total_mv",
    })
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    for col in ("ps_ttm", "dv_ratio"):  # 源不提供,补空列以满足下游固定列集
        if col not in df.columns:
            df[col] = pd.NA
    keep = ["trade_date", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "total_mv"]
    return df[keep].dropna(subset=["trade_date"])


# ===========================================================================
# 基本面·港美(三期)。设计: docs/superpowers/specs/2026-07-10-hkus-fundamental-design.md
# 富途(FutuOpenD 网关)主源 + 东财 ann_date 提供者。两层架构(报表 JSONB + 指标
# 宽表)镜像二期,ann_date 宁缺勿假 —— 拿不到就 NULL,绝不用报告期估算冒充。
# ===========================================================================
INTL_FUND_SOURCE = os.getenv("ASTOCK_INTL_FUND_SOURCE", "futu")

_futu_ctx = None
_futu_lock = threading.Lock()
_futu_last_req = [0.0]
FUTU_MIN_INTERVAL = float(os.getenv("ASTOCK_FUTU_MIN_INTERVAL", "1.05"))


def _futu_context():
    """懒建常驻富途连接;网关不可达时给出可操作报错。"""
    global _futu_ctx
    with _futu_lock:
        if _futu_ctx is None:
            try:
                from futu import OpenQuoteContext
                _futu_ctx = OpenQuoteContext(host="127.0.0.1", port=11111)
            except Exception as exc:
                raise ConnectionError(
                    "无法连接 FutuOpenD 网关(127.0.0.1:11111)。"
                    "请启动 FutuOpenD 并登录后重试。原始错误: %s" % exc) from exc
        return _futu_ctx


def _futu_call(fn_name, *args, **kwargs):
    """全局节流的富途调用:任意两次请求间隔 >= FUTU_MIN_INTERVAL;ret!=0 抛异常。"""
    ctx = _futu_context()
    with _futu_lock:
        wait = FUTU_MIN_INTERVAL - (time.monotonic() - _futu_last_req[0])
        if wait > 0:
            time.sleep(wait)
        _futu_last_req[0] = time.monotonic()
    ret, data = getattr(ctx, fn_name)(*args, **kwargs)
    if ret != 0:
        raise RuntimeError(f"futu {fn_name} ret={ret}: {data}")
    return data


def close_futu() -> None:
    """进程收尾关闭富途连接(幂等:未建立过连接或已关闭时安全空操作)。"""
    global _futu_ctx
    with _futu_lock:
        if _futu_ctx is not None:
            try:
                _futu_ctx.close()
            except Exception:  # noqa: BLE001
                pass
            _futu_ctx = None


def futu_code(stock_code: str) -> str:
    """'00700.HK' -> 'HK.00700';'AAPL.US' -> 'US.AAPL'。纯函数,无网络调用。"""
    symbol, _, suffix = stock_code.strip().partition(".")
    return f"{suffix.upper()}.{symbol}"


_FUTU_STMT_TYPE = {"income": 1, "balance": 2, "cashflow": 3}
_FUTU_INDICATOR_TYPE = 4


def _futu_fetch_reports(code: str, stype: int) -> list[dict]:
    """分页拉 report_list,翻到整页 report_date 都早于 FUND_START 即停。"""
    out, nk = [], None
    while True:
        d = _futu_call("get_financials_statements", code,
                       statement_type=stype, num=50, next_key=nk)
        rl = d.get("report_list", [])
        out.extend(rl)
        nk = d.get("next_key")
        if not nk or not rl:
            break
        oldest = min(r["date_time_str"] for r in rl)
        if oldest < FUND_START.isoformat():
            break
    return out


# ---------------------------------------------------------------------------
# Step 1b 探测结论(00700.HK / AAPL.US 全历史响应实测,2026-07-10,见 task-2-report):
# financial_type 枚举 —— 1=Q1(单季即累计首季)/2=Q2(H1 累计)/3=Q3(9 个月累计)/
# 4=Q4(单季度,非累计)/7=FY(全年累计)。income(利润表)同一 report_date 会同时出现
# financial_type=7(FY)与 4(Q4)两行;balance(资产负债表)两者逐项数值相同(同一期末
# 快照,只是打了两个标签);cashflow(现金流量表)两者数值明显不同 —— 已实测
# AAPL 2025-09-26:经营活动现金流量净额 FY=1,114.82 亿 vs Q4(单季)=297.28 亿,
# 差 3.75 倍,证实 Q4 行确是"仅第四季度净额"而非累计口径。
# **入库规则:只保留累计/年度口径 —— financial_type ∈ {1,2,3,7},剔除单季 Q4(=4)**,
# 与 A 股"累计报表"惯例一致;这条规则同时天然消除了同一 report_date 两行撞主键的问题
# (HK 00700 的 balance/cashflow 源本身全历史只出现过 1/7 两档,不受影响;
# US AAPL 的三张报表均全量出现过 1/2/3/4/7 五档,规则对三表通用)。
# ---------------------------------------------------------------------------
_FUTU_CUMULATIVE_TYPES = {1, 2, 3, 7}
_FUTU_PERIOD_KIND = {1: "Q1", 2: "H1", 3: "9M", 7: "FY"}


def _futu_reports_to_df(report_list: list[dict]) -> pd.DataFrame:
    """report_list -> DataFrame[report_date, currency, period_kind, data(dict)]。

    按 _FUTU_CUMULATIVE_TYPES 过滤单季 Q4 + FUND_START 过滤;item_list 的节标题行
    (无 data 字段,futu SDK 里此时 dict 干脆没有 "data" 键)跳过。
    """
    rows = []
    for r in report_list:
        if r.get("financial_type") not in _FUTU_CUMULATIVE_TYPES:
            continue
        rd_ts = pd.to_datetime(r.get("date_time_str"), errors="coerce")
        if pd.isna(rd_ts):
            continue
        rd = rd_ts.date()
        if rd < FUND_START:
            continue
        data = {it["display_name"]: it.get("data")
                for it in r.get("item_list", [])
                if it.get("data") is not None and it.get("display_name")}
        rows.append({
            "report_date": rd,
            "currency": r.get("currency_code") or None,
            "period_kind": _FUTU_PERIOD_KIND.get(r.get("financial_type")),
            "data": data,
        })
    if not rows:
        return pd.DataFrame(columns=["report_date", "currency", "period_kind", "data"])
    df = pd.DataFrame(rows).drop_duplicates(subset=["report_date"], keep="first")
    return df.sort_values("report_date").reset_index(drop=True)


def fetch_intl_fund_statements(stock_code: str, stmt_type: str) -> pd.DataFrame:
    """港/美三大报表。stmt_type ∈ income|balance|cashflow。

    列: report_date, currency, period_kind, data(dict)。只含 report_date >= FUND_START
    且累计/年度口径的行(单季 Q4 剔除,规则见 _FUTU_CUMULATIVE_TYPES 上方注释)。
    按 INTL_FUND_SOURCE 分发:futu(默认,主源)/ em(备源,东财长表 pivot)。
    """
    if INTL_FUND_SOURCE == "em":
        return _fetch_intl_fund_statements_em(stock_code, stmt_type)
    code = futu_code(stock_code)
    stype = _FUTU_STMT_TYPE[stmt_type]
    reports = _futu_fetch_reports(code, stype)
    return _futu_reports_to_df(reports)


# ---------------------------------------------------------------------------
# Step 1c 探测结论(type4 关键指标完整 display_name 清单,00700.HK + AAPL.US,2026-07-10,
# 见 task-2-report):**两市场指标层可得列集合有系统性差异** —— HK 含"每股指标"分节
# (EPS/EPS 稀释/BPS/每股经营现金流)+ 比率类(ROE/ROA/毛利率/净利率/资产负债率/流动比率
# 等);US 的 type4 只有 TTM 比率类(毛利率/归母净利率/ROE/ROA/流动比率等),**完全没有
# 每股指标分节**(EPS/BPS/OCF_PS 只在利润表/资产负债表/现金流量表的 item_list 里,不在
# type4),也没有营业收入/净利润绝对值科目,更没有等价于"资产负债率"的科目(US 有"有息
# 负债率",定义不同,不可替代映射)。两市场的成长率科目(HK"近3年增长率"/US"成长能力"
# 分节实测为空)都不是同比(yoy)口径,故 revenue_yoy/net_profit_yoy 改用最接近的"每股"
# 科目的 yoy 字段代理(HK:每股营业收入→revenue_yoy,基本每股收益→net_profit_yoy;
# 股本变动不大时约等于对应绝对值同比增速)。US 无任何可代理科目,revenue/net_profit
# 及二者 yoy 在 US 指标层恒为 NULL(这是已知的、如实记录的市场覆盖差异,不是 bug)。
# ---------------------------------------------------------------------------
_FUND_INDICATOR_COLS = [
    "eps", "eps_diluted", "bps", "ocf_ps", "roe", "roa", "gross_margin", "net_margin",
    "debt_ratio", "current_ratio", "revenue", "revenue_yoy", "net_profit", "net_profit_yoy",
]

_FUTU_MAININDEX_MAP_HK = {
    "基本每股收益（元）": "eps",
    "稀释每股收益（元）": "eps_diluted",
    "每股净资产（元）": "bps",
    "每股经营现金净流量（元）": "ocf_ps",
    "净资产收益率(ROE)": "roe",
    "总资产净利率(ROA)": "roa",
    "销售毛利率": "gross_margin",
    "销售净利率": "net_margin",
    "资产负债率": "debt_ratio",
    "流动比率": "current_ratio",
    # revenue/net_profit 绝对值:HK type4 未提供任何绝对值科目(只有每股/比率类),置 NULL。
}
_FUTU_YOY_PROXY_HK = {"revenue_yoy": "每股营业收入（元）", "net_profit_yoy": "基本每股收益（元）"}

_FUTU_MAININDEX_MAP_US = {
    "毛利率": "gross_margin",
    "归母净利率": "net_margin",
    "净资产收益率（ROE）": "roe",
    "总资产净利率（ROA）": "roa",
    "流动比率": "current_ratio",
    # eps/eps_diluted/bps/ocf_ps/debt_ratio/revenue/net_profit(及二者 yoy):US type4
    # 完全未提供对应科目,全部置 NULL(已知市场覆盖差异,见上方说明)。
}
_FUTU_YOY_PROXY_US: dict[str, str] = {}  # US type4 无可代理科目,revenue_yoy/net_profit_yoy 恒 NULL


def fetch_intl_fund_indicator(stock_code: str) -> pd.DataFrame:
    """富途 type4 关键指标。列 = 指标宽表数值列(_FUND_INDICATOR_COLS)+ report_date, currency。

    节标题行(item 无 data 字段)跳过;revenue_yoy/net_profit_yoy 无直接科目时取代理科目
    的 yoy 字段(见上方说明;美股无代理,恒 NULL)。累计口径过滤规则与 fetch_intl_fund_statements
    相同(排除单季 Q4,同样避免同 report_date 撞行——US type4 实测也存在 FY/Q4 同日两行)。
    """
    market = "hk" if stock_code.upper().endswith(".HK") else "us"
    mapping = _FUTU_MAININDEX_MAP_HK if market == "hk" else _FUTU_MAININDEX_MAP_US
    yoy_proxy = _FUTU_YOY_PROXY_HK if market == "hk" else _FUTU_YOY_PROXY_US

    code = futu_code(stock_code)
    reports = _futu_fetch_reports(code, _FUTU_INDICATOR_TYPE)
    rows = []
    for r in reports:
        if r.get("financial_type") not in _FUTU_CUMULATIVE_TYPES:
            continue
        rd_ts = pd.to_datetime(r.get("date_time_str"), errors="coerce")
        if pd.isna(rd_ts):
            continue
        rd = rd_ts.date()
        if rd < FUND_START:
            continue
        items = {it["display_name"]: it for it in r.get("item_list", []) if it.get("data") is not None}
        row = {"report_date": rd, "currency": r.get("currency_code") or None}
        for col in _FUND_INDICATOR_COLS:
            row[col] = None
        for name, col in mapping.items():
            if name in items:
                row[col] = items[name].get("data")
        for col, proxy_name in yoy_proxy.items():
            if proxy_name in items:
                row[col] = items[proxy_name].get("yoy")
        rows.append(row)
    if not rows:
        return pd.DataFrame(columns=["report_date", "currency"] + _FUND_INDICATOR_COLS)
    df = pd.DataFrame(rows).drop_duplicates(subset=["report_date"], keep="first")
    return df.sort_values("report_date").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 1d 港股 ann_date 三级探测结论(2026-07-10,见 task-2-report 完整记录):
# ① stock_financial_hk_analysis_indicator_em("00700","报告期") 全部 36 列打印确认 ——
#    无 NOTICE_DATE 或任何公告日语义字段(逐列检查过,只有 REPORT_DATE/START_DATE/
#    FISCAL_YEAR 等报告期字段,没有披露日期)。
# ② stock_financial_hk_report_em(indicator="报告期") 的资产负债表(唯一带 STD_REPORT_DATE
#    的报表)实测 20 期:STD_REPORT_DATE 与 REPORT_DATE 逐行恒等(0 处不同),故无"滞后差"
#    可用来代理公告日。
# ③ akshare 港股相关函数扫描(hk*financial*/hk*notice*/hk*gg*)未发现独立的港股公告日
#    接口(stock_hk_financial_indicator_em 同样只有报告期,无公告日)。
# **结论:港股 ann_date 三级全部落空,不可得** —— 按"宁缺勿假"原则,fetch_intl_ann_dates
# 对 hk 市场返回 report_date 非空 + ann_date 恒 NULL 的帧,不抛异常;Task 3/README 需据此
# 声明"港股基本面 as-of 防未来函数不可用"。
#
# 美股 ann_date:stock_financial_us_analysis_indicator_em 的 NOTICE_DATE 字段直取,已实测
# 非 NULL(AAPL "年报" 6 期全部有 NOTICE_DATE)。"年报"(DATE_TYPE_CODE=001)与"累计季报"
# (002/004,对应 H1/9M)取并集(brief 定义的两枚举);注意 akshare 合法枚举是"累计季报"
# 而非"累计"(误传枚举会直接 raise ValueError,已实测确认)。"单季报"(003/006/007/008,
# 含 Q1)未纳入并集 —— 这是 brief 定义范围内的已知覆盖缺口:Q1 单季报告期在 ann_date
# 回填时会落空(NULL),不影响 H1/9M/FY 三档覆盖。
#
# 已知的日期口径差异(供 Task 3 参考):em 的 REPORT_DATE(本函数用作 report_date)是
# 实际财报截止日,与富途 date_time_str 同源但存在**系统性 1 天误差**(实测 AAPL 近 4 个
# 年报期:futu 2025-09-26 vs em 2025-09-27、2024-09-27 vs 09-28、2023-09-29 vs 09-30、
# 2022-09-23 vs 09-24,均相差恰好 1 天,疑似 UTC/美东时区换算差异);calendar 端的
# STD_REPORT_DATE(如 2025-12-31)则与 futu 日期相差可达 ~90 天(非日历年结公司如 AAPL),
# 不适合做 join 键,因此本函数刻意选 REPORT_DATE 而非 STD_REPORT_DATE。Task 3 按
# report_date 做精确 UPDATE 前需评估:是否需要 ±1~2 天容差 join,否则 US ann_date 回填
# 命中率会因这 1 天系统偏差而显著偏低。
# ---------------------------------------------------------------------------
def fetch_intl_ann_dates(market: str, symbol: str) -> pd.DataFrame:
    """列: report_date, ann_date。us=东财美股指标接口 NOTICE_DATE("年报"+"累计季报"并集,
    以 REPORT_DATE 为 report_date,理由见上方 docstring);hk=三级探测落空,report_date
    取指标接口报告期、ann_date 恒 NULL(不抛异常)。
    """
    import akshare as ak

    if market == "us":
        frames = []
        for ind in ("年报", "累计季报"):
            df = with_retry(ak.stock_financial_us_analysis_indicator_em, symbol=symbol, indicator=ind)
            if df is not None and not df.empty and {"REPORT_DATE", "NOTICE_DATE"} <= set(df.columns):
                frames.append(df[["REPORT_DATE", "NOTICE_DATE"]])
        if not frames:
            return pd.DataFrame(columns=["report_date", "ann_date"])
        df = pd.concat(frames, ignore_index=True)
        df = df.rename(columns={"REPORT_DATE": "report_date", "NOTICE_DATE": "ann_date"})
        df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce").dt.date
        df["ann_date"] = pd.to_datetime(df["ann_date"], errors="coerce").dt.date
        df = df.dropna(subset=["report_date"])
        # 年报/累计季报两路偶有重叠 report_date 时,取公告日较早者(更贴近真实披露时点)
        return (df.sort_values("ann_date")
                  .drop_duplicates(subset=["report_date"], keep="first")
                  .sort_values("report_date").reset_index(drop=True))

    # hk: 三级探测均落空(见上方模块说明),report_date 取指标接口报告期、ann_date 恒 NULL
    df = with_retry(ak.stock_financial_hk_analysis_indicator_em, symbol=symbol, indicator="报告期")
    if df is None or df.empty or "REPORT_DATE" not in df.columns:
        return pd.DataFrame(columns=["report_date", "ann_date"])
    rd = pd.to_datetime(df["REPORT_DATE"], errors="coerce").dt.date
    out = pd.DataFrame({"report_date": rd, "ann_date": [None] * len(df)})
    return out.dropna(subset=["report_date"]).drop_duplicates(subset=["report_date"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# em 备源(ASTOCK_INTL_FUND_SOURCE=em 时启用):东财长表 pivot 成与 futu 路径同构的
# DataFrame[report_date, currency, period_kind, data(dict)]。currency/period_kind 东财
# 长表无对应字段,如实置 NULL(不臆造)。美股按 brief"美股季报科目在'累计'"的实测结论,
# "年报"+"累计季报"两枚举取并集(与 fetch_intl_ann_dates 的 us 分支同一策略),覆盖
# FY/H1/9M;"单季报"(含 Q1)同样不纳入 —— 与 futu 路径的累计口径规则保持一致语义。
# ---------------------------------------------------------------------------
_EM_STMT_HK = {"income": "利润表", "balance": "资产负债表", "cashflow": "现金流量表"}
_EM_STMT_US = {"income": "综合损益表", "balance": "资产负债表", "cashflow": "现金流量表"}


def _fetch_intl_fund_statements_em(stock_code: str, stmt_type: str) -> pd.DataFrame:
    import akshare as ak

    market = "hk" if stock_code.upper().endswith(".HK") else "us"
    symbol = stock_code.split(".")[0]

    if market == "hk":
        df = with_retry(ak.stock_financial_hk_report_em,
                        stock=symbol, symbol=_EM_STMT_HK[stmt_type], indicator="报告期")
        if df is None or df.empty:
            return pd.DataFrame(columns=["report_date", "currency", "period_kind", "data"])
        df = df.rename(columns={"REPORT_DATE": "report_date"})
        item_col, amount_col = "STD_ITEM_NAME", "AMOUNT"
    else:
        frames = []
        for ind in ("年报", "累计季报"):
            d = with_retry(ak.stock_financial_us_report_em,
                           stock=symbol, symbol=_EM_STMT_US[stmt_type], indicator=ind)
            if d is not None and not d.empty:
                frames.append(d)
        if not frames:
            return pd.DataFrame(columns=["report_date", "currency", "period_kind", "data"])
        df = pd.concat(frames, ignore_index=True).rename(columns={"REPORT_DATE": "report_date"})
        item_col, amount_col = "ITEM_NAME", "AMOUNT"

    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce").dt.date
    df = df.dropna(subset=["report_date"])
    rows = []
    for rd, g in df.groupby("report_date"):
        data = {getattr(r, item_col): getattr(r, amount_col)
                for r in g.itertuples() if pd.notna(getattr(r, amount_col))}
        rows.append({"report_date": rd, "currency": None, "period_kind": None, "data": data})
    if not rows:
        return pd.DataFrame(columns=["report_date", "currency", "period_kind", "data"])
    out = pd.DataFrame(rows)
    out = out[out["report_date"] >= FUND_START].sort_values("report_date").reset_index(drop=True)
    return out


# ===========================================================================
# 板块(行业/概念)。设计: docs/superpowers/specs/2026-07-10-board-rotation-design.md
# 全部为东财行情族(push2/push2his)接口,与个股日线共享 IP 限流预算。
# hist/资金流接口按板块名称查询(板块改名需先刷新 board 表);cons 接口支持
# 直接传 BK 代码(akshare 源码 re.match('^BK\\d+')),不受改名影响。
# ===========================================================================
RENAME_BOARD_HIST = {
    "日期": "trade_date", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low",
    "成交量": "volume",   # 源单位:手;入库前 ×100 统一为股(全库约定)
    "成交额": "amount", "涨跌幅": "pct_chg", "换手率": "turnover",
}
RENAME_BOARD_FLOW = {
    "日期": "trade_date",
    "主力净流入-净额": "main_net", "主力净流入-净占比": "main_net_pct",
    "超大单净流入-净额": "xlarge_net", "超大单净流入-净占比": "xlarge_net_pct",
    "大单净流入-净额": "large_net", "大单净流入-净占比": "large_net_pct",
    "中单净流入-净额": "mid_net", "中单净流入-净占比": "mid_net_pct",
    "小单净流入-净额": "small_net", "小单净流入-净占比": "small_net_pct",
}
_BOARD_FLOW_COLS = ["trade_date", "main_net", "main_net_pct", "xlarge_net", "xlarge_net_pct",
                    "large_net", "large_net_pct", "mid_net", "mid_net_pct",
                    "small_net", "small_net_pct"]


def fetch_board_list() -> pd.DataFrame:
    """东财行业+概念板块列表。列: board_code, board_name, board_type。"""
    import akshare as ak

    frames = []
    for btype, fn in (("industry", ak.stock_board_industry_name_em),
                      ("concept", ak.stock_board_concept_name_em)):
        df = with_retry(fn)
        df = df.rename(columns={"板块代码": "board_code", "板块名称": "board_name"})
        df = df[["board_code", "board_name"]].copy()
        df["board_type"] = btype
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def fetch_board_daily(board_name: str, board_type: str, start: str = "19900101") -> pd.DataFrame:
    """板块指数日线(不复权)。行业与概念的 period 参数拼写不同(akshare 实况)。"""
    import akshare as ak

    if board_type == "industry":
        df = with_retry(ak.stock_board_industry_hist_em, symbol=board_name,
                        start_date=start, end_date="20500101", period="日k", adjust="")
    else:
        df = with_retry(ak.stock_board_concept_hist_em, symbol=board_name,
                        start_date=start, end_date="20500101", period="daily", adjust="")
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns=RENAME_BOARD_HIST)
    keep = [c for c in RENAME_BOARD_HIST.values() if c in df.columns]
    df = df[keep].copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce") * 100  # 手 → 股
    return df.dropna(subset=["trade_date"])


def fetch_board_cons(board_code: str, board_type: str) -> set[str]:
    """板块当前成分股(全代码集合)。传 BK 代码调用,规避板块改名。"""
    import akshare as ak

    fn = (ak.stock_board_industry_cons_em if board_type == "industry"
          else ak.stock_board_concept_cons_em)
    df = with_retry(fn, symbol=board_code)
    if df is None or df.empty:
        return set()
    return {to_full_code(str(s)) for s in df["代码"].astype(str)}


def fetch_board_fund_flow(board_name: str, board_type: str) -> pd.DataFrame:
    """板块历史资金流(lmt=0 全部可用历史)。净额单位:元;占比单位:%。"""
    import akshare as ak

    fn = (ak.stock_sector_fund_flow_hist if board_type == "industry"
          else ak.stock_concept_fund_flow_hist)
    df = with_retry(fn, symbol=board_name)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.rename(columns=RENAME_BOARD_FLOW)
    keep = [c for c in _BOARD_FLOW_COLS if c in df.columns]
    df = df[keep].copy()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    for col in keep[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["trade_date"])
