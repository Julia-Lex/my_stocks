# 板块数据层实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 按 `docs/superpowers/specs/2026-07-10-board-rotation-design.md` 建立东财行业+概念板块数据层:4 张表、common.py fetch 层、全量初始化与每日增量脚本。

**Architecture:** 沿用本仓库既有 ETL 模式——common.py 提供 fetch/upsert/防护,编号脚本做编排,etl_progress 断点续传,run_stock_todo 并发+熔断。成分股用区间表(valid_from/valid_to)存观测历史。

**Tech Stack:** Python 3.12(仓库 `.venv`)、akshare(东财 push2/push2his 接口)、PostgreSQL(psycopg2)。无 pytest——本仓库惯例是"真实源探测 + 数据库断言脚本"验证,纯逻辑用事务内断言+ROLLBACK 测试。

## Global Constraints

- 工作目录:`/Users/zhu/own/my_stocks/.claude/worktrees/local-vs-cloud-env-5be7f8`(基本面二期分支)
- 运行命令一律带 `ASTOCK_DB_USER=zhu`,Python 用 `.venv/bin/python`
- 成交量全库统一**股**:板块日 K 源单位为手,入库前 ×100
- 所有日线/资金流写入过收盘防护(`drop_unclosed_bars` / cutoff 过滤)
- 东财并发 ≤3;**push2 行情族 2026-07-10 下午正对本机限流**——Task 2 的实探步骤和 Task 4-6 的运行步骤若遇连续 RemoteDisconnected,暂停等解封(通常隔夜),代码实现步骤不受影响
- 提交只 add 本计划涉及的文件(工作树里有其他会话未提交的改动,勿裹挟)

---

### Task 1: 板块表结构(11_schema_board.sql)

**Files:**
- Create: `11_schema_board.sql`

**Interfaces:**
- Produces: 表 `board(board_code PK, board_name, board_type, is_active, updated_at)`、`board_member(board_code, stock_code, valid_from, valid_to, PK(board_code,stock_code,valid_from))`、`board_daily(board_code, trade_date, open/high/low/close/volume/amount/pct_chg/turnover, PK(board_code,trade_date))`、`board_fund_flow(board_code, trade_date, main_net…small_net_pct, PK(board_code,trade_date))`

- [ ] **Step 1: 写 11_schema_board.sql**

```sql
-- 11_schema_board.sql — 板块数据层(行业/概念)。
-- 设计: docs/superpowers/specs/2026-07-10-board-rotation-design.md
-- 应用: psql -U zhu -d astock -f 11_schema_board.sql(幂等)

CREATE TABLE IF NOT EXISTS board (
    board_code  TEXT PRIMARY KEY,          -- 东财代码,如 BK0475
    board_name  TEXT NOT NULL,             -- 最新名称(改名时更新)
    board_type  TEXT NOT NULL CHECK (board_type IN ('industry', 'concept')),
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,  -- 从东财列表消失置 false,历史数据保留
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 成分区间表:valid_from 是"观测到纳入"的日期(首次建库日=观测起点,非真实纳入日),
-- valid_to NULL 表示当前仍在板块内;精度=每日快照粒度。
-- 某日 d 的成分: valid_from <= d AND (valid_to IS NULL OR valid_to > d)
CREATE TABLE IF NOT EXISTS board_member (
    board_code  TEXT NOT NULL REFERENCES board(board_code),
    stock_code  TEXT NOT NULL,
    valid_from  DATE NOT NULL,
    valid_to    DATE,
    PRIMARY KEY (board_code, stock_code, valid_from)
);
CREATE INDEX IF NOT EXISTS idx_board_member_stock ON board_member (stock_code);
CREATE INDEX IF NOT EXISTS idx_board_member_open  ON board_member (board_code) WHERE valid_to IS NULL;

CREATE TABLE IF NOT EXISTS board_daily (
    board_code  TEXT NOT NULL REFERENCES board(board_code),
    trade_date  DATE NOT NULL,
    open  NUMERIC(12,3), high NUMERIC(12,3), low NUMERIC(12,3), close NUMERIC(12,3),
    volume BIGINT,                          -- 股(源为手,入库 ×100)
    amount NUMERIC(20,2),                   -- 元
    pct_chg  NUMERIC(8,4),
    turnover NUMERIC(8,4),
    PRIMARY KEY (board_code, trade_date)
);

CREATE TABLE IF NOT EXISTS board_fund_flow (
    board_code  TEXT NOT NULL REFERENCES board(board_code),
    trade_date  DATE NOT NULL,
    main_net   NUMERIC(20,2), main_net_pct   NUMERIC(8,4),  -- 主力净流入 额(元)/占比(%)
    xlarge_net NUMERIC(20,2), xlarge_net_pct NUMERIC(8,4),  -- 超大单
    large_net  NUMERIC(20,2), large_net_pct  NUMERIC(8,4),  -- 大单
    mid_net    NUMERIC(20,2), mid_net_pct    NUMERIC(8,4),  -- 中单
    small_net  NUMERIC(20,2), small_net_pct  NUMERIC(8,4),  -- 小单
    PRIMARY KEY (board_code, trade_date)
);
```

- [ ] **Step 2: 应用并验证**

Run: `psql -U zhu -d astock -f 11_schema_board.sql && psql -U zhu -d astock -Atc "SELECT count(*) FROM information_schema.tables WHERE table_name IN ('board','board_member','board_daily','board_fund_flow');"`
Expected: 无报错,输出 `4`

- [ ] **Step 3: Commit**

```bash
git add 11_schema_board.sql
git commit -m "feat: 板块数据层表结构(board/board_member/board_daily/board_fund_flow)"
```

---

### Task 2: common.py 板块 fetch 层

**Files:**
- Modify: `common.py`(追加到文件末尾,基本面 fetch 区之后)

**Interfaces:**
- Consumes: 既有 `with_retry`、`to_full_code`、`pd`
- Produces:
  - `fetch_board_list() -> pd.DataFrame`,列 `board_code, board_name, board_type`
  - `fetch_board_daily(board_name: str, board_type: str, start: str = "19900101") -> pd.DataFrame`,列 `trade_date/open/high/low/close/volume(股)/amount/pct_chg/turnover`
  - `fetch_board_cons(board_code: str, board_type: str) -> set[str]`(全代码集合,如 `{'600519.SH',...}`)
  - `fetch_board_fund_flow(board_name: str, board_type: str) -> pd.DataFrame`,列 `trade_date, main_net, main_net_pct, xlarge_net, xlarge_net_pct, large_net, large_net_pct, mid_net, mid_net_pct, small_net, small_net_pct`

- [ ] **Step 1: 追加 fetch 层代码到 common.py 末尾**

```python
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
```

- [ ] **Step 2: 网络前置检查(push2 是否解封)**

Run: `curl -s -m 8 -A "Mozilla/5.0" "https://17.push2.eastmoney.com/api/qt/clist/get?pn=1&pz=1&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:90+t:2&fields=f12,f14" -o /dev/null -w "%{http_code}\n"`
Expected: `200`。若为 `000`(连接被断)= 仍在限流:跳过 Step 3 实探,先做 Task 3(纯本地),稍后回补本步与 Step 3。

- [ ] **Step 3: 四个函数实探(限流解除后)**

Run:
```bash
cd /Users/zhu/own/my_stocks/.claude/worktrees/local-vs-cloud-env-5be7f8 && .venv/bin/python - <<'EOF'
import common as c
lst = c.fetch_board_list()
ind = lst[lst.board_type == "industry"]; con = lst[lst.board_type == "concept"]
assert len(ind) >= 50 and len(con) >= 200, f"板块数异常: {len(ind)}/{len(con)}"
assert lst.board_code.str.match("BK").all()
b = ind.iloc[0]
d = c.fetch_board_daily(b.board_name, "industry")
assert len(d) > 500 and set(d.columns) == {"trade_date","open","close","high","low","volume","amount","pct_chg","turnover"}
cons = c.fetch_board_cons(b.board_code, "industry")
assert len(cons) >= 5 and all("." in s for s in cons)
f = c.fetch_board_fund_flow(b.board_name, "industry")
assert len(f) > 50 and "main_net" in f.columns
# 概念板块各接口走一遍
b2 = con.iloc[0]
assert not c.fetch_board_daily(b2.board_name, "concept").empty
assert c.fetch_board_cons(b2.board_code, "concept")
assert not c.fetch_board_fund_flow(b2.board_name, "concept").empty
print("OK: 板块", len(lst), "| 日线", len(d), "行, 最早", d.trade_date.min(), "| 成分", len(cons), "| 资金流", len(f), "行")
EOF
```
Expected: 末行 `OK: ...` 且行业日线最早日期在 2010 年以前。

- [ ] **Step 4: Commit**

```bash
git add common.py
git commit -m "feat: 板块 fetch 层(列表/日线/成分/资金流,东财行情族)"
```

---

### Task 3: common.py 板块 upsert 与成分区间 diff

**Files:**
- Modify: `common.py`(紧接 Task 2 代码之后)

**Interfaces:**
- Consumes: Task 1 的四张表;既有 `upsert`、`drop_unclosed_bars`、`safe_cutoff_date`、`_num`、`_int`
- Produces:
  - `upsert_board_daily(conn, board_code: str, df) -> int`(内部挂收盘防护)
  - `upsert_board_fund_flow(conn, board_code: str, df) -> int`(内部按 cutoff 过滤)
  - `sync_board_members(conn, board_code: str, current: set[str], today: date) -> tuple[int, int]`(返回(开区间数, 关区间数);`current` 为空时调用方必须跳过,函数内部 assert 拦截)

- [ ] **Step 1: 追加代码**

```python
def upsert_board_daily(conn, board_code: str, df: pd.DataFrame) -> int:
    df = drop_unclosed_bars(df, f"{board_code}(board)")   # A股 15:30 口径同样适用板块指数
    if df.empty:
        return 0
    cols = ["board_code", "trade_date", "open", "high", "low", "close",
            "volume", "amount", "pct_chg", "turnover"]
    rows = [(board_code, r.trade_date,
             _num(r, "open"), _num(r, "high"), _num(r, "low"), _num(r, "close"),
             _int(r, "volume"), _num(r, "amount"), _num(r, "pct_chg"), _num(r, "turnover"))
            for r in df.itertuples(index=False)]
    return upsert(conn, "board_daily", cols, rows, ["board_code", "trade_date"])


def upsert_board_fund_flow(conn, board_code: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    df = df[df["trade_date"] <= safe_cutoff_date()]       # 资金流盘中也有当日快照行,同口径过滤
    if df.empty:
        return 0
    cols = ["board_code"] + _BOARD_FLOW_COLS
    rows = [(board_code, r.trade_date,
             _num(r, "main_net"), _num(r, "main_net_pct"),
             _num(r, "xlarge_net"), _num(r, "xlarge_net_pct"),
             _num(r, "large_net"), _num(r, "large_net_pct"),
             _num(r, "mid_net"), _num(r, "mid_net_pct"),
             _num(r, "small_net"), _num(r, "small_net_pct"))
            for r in df.itertuples(index=False)]
    return upsert(conn, "board_fund_flow", cols, rows, ["board_code", "trade_date"])


def sync_board_members(conn, board_code: str, current: set[str], today: date) -> tuple[int, int]:
    """成分区间表 diff:新出现开区间(valid_from=today),消失关区间(valid_to=today)。

    current 为空集时禁止调用(接口故障与"板块清空"无法区分,宁可当天不更新)——
    调用方负责跳过,这里再 assert 一道防线。
    重开同日关闭的区间(极端:当天误关又回来)由 ON CONFLICT 恢复 valid_to=NULL。
    """
    assert current, f"{board_code}: current 成分为空,调用方应跳过而非同步"
    with conn.cursor() as cur:
        cur.execute("SELECT stock_code FROM board_member "
                    "WHERE board_code = %s AND valid_to IS NULL", (board_code,))
        open_set = {r[0] for r in cur.fetchall()}
        to_open = sorted(current - open_set)
        to_close = sorted(open_set - current)
        if to_close:
            cur.execute("UPDATE board_member SET valid_to = %s "
                        "WHERE board_code = %s AND valid_to IS NULL AND stock_code = ANY(%s)",
                        (today, board_code, to_close))
        if to_open:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO board_member (board_code, stock_code, valid_from) VALUES %s "
                "ON CONFLICT (board_code, stock_code, valid_from) DO UPDATE SET valid_to = NULL",
                [(board_code, s, today) for s in to_open])
    conn.commit()
    return len(to_open), len(to_close)
```

- [ ] **Step 2: 区间 diff 逻辑测试(真实库,事务内造数+ROLLBACK,不留痕)**

Run:
```bash
cd /Users/zhu/own/my_stocks/.claude/worktrees/local-vs-cloud-env-5be7f8 && ASTOCK_DB_USER=zhu .venv/bin/python - <<'EOF'
from datetime import date
import common as c

conn = c.get_conn()
_real_commit = conn.commit
conn.commit = lambda: None            # 拦截 commit,测试结束统一 ROLLBACK
try:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO board VALUES ('BKTEST','测试板块','industry',TRUE,now())")
    d1, d2 = date(2026, 7, 10), date(2026, 7, 11)
    # 首日建仓:3 只全开区间
    o, cl = c.sync_board_members(conn, "BKTEST", {"000001.SZ","000002.SZ","600519.SH"}, d1)
    assert (o, cl) == (3, 0), (o, cl)
    # 次日:600519 移出,300308 纳入
    o, cl = c.sync_board_members(conn, "BKTEST", {"000001.SZ","000002.SZ","300308.SZ"}, d2)
    assert (o, cl) == (1, 1), (o, cl)
    with conn.cursor() as cur:
        cur.execute("SELECT stock_code, valid_from, valid_to FROM board_member "
                    "WHERE board_code='BKTEST' ORDER BY stock_code, valid_from")
        rows = cur.fetchall()
    assert ("600519.SH", d1, d2) in rows            # 移出:区间正确关闭
    assert ("300308.SZ", d2, None) in rows          # 纳入:新开区间
    assert ("000001.SZ", d1, None) in rows          # 未动:保持开区间
    # as-of 语义:d1 当天 600519 仍属于板块
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM board_member WHERE board_code='BKTEST' "
                    "AND valid_from <= %s AND (valid_to IS NULL OR valid_to > %s)", (d1, d1))
        assert cur.fetchone()[0] == 3
    # 空集必须拒绝
    try:
        c.sync_board_members(conn, "BKTEST", set(), d2); assert False, "空集未被拦截"
    except AssertionError as e:
        assert "为空" in str(e)
    print("OK: sync_board_members 全部断言通过")
finally:
    conn.rollback(); conn.close()
EOF
```
Expected: `OK: sync_board_members 全部断言通过`,且 `psql -U zhu -d astock -Atc "SELECT count(*) FROM board WHERE board_code='BKTEST'"` 输出 `0`(未留痕)。

- [ ] **Step 3: Commit**

```bash
git add common.py
git commit -m "feat: 板块 upsert(收盘防护)与成分区间 diff 同步"
```

---

### Task 4: 12_init_board.py 全量初始化

**Files:**
- Create: `12_init_board.py`

**Interfaces:**
- Consumes: Task 2/3 的全部函数;既有 `run_stock_todo`、`mark_progress`、`get_done_codes`、`upsert`、`beijing_now`
- Produces: `BoardRow = namedtuple("BoardRow", "stock_code board_name board_type")`(stock_code 字段借存 board_code,适配 run_stock_todo/mark_progress 惯例)与 `load_one_board(conn, r: BoardRow) -> None`,供 13 复用(补拉新板块全历史)。

- [ ] **Step 1: 写 12_init_board.py**

```python
"""
12_init_board.py — 板块数据层全量初始化(行业+概念,断点续传)。

流程:
  1. 板块列表 → board(幂等 upsert);
  2. 逐板块:日K全历史 → board_daily(收盘防护),资金流全历史 → board_fund_flow,
     当前成分 → board_member 开区间(valid_from=今天,观测起点语义见 schema 注释);
  3. 断点续传:etl_progress task='init_board',stock_code 字段借存 board_code。

用法:
  ASTOCK_DB_USER=zhu .venv/bin/python 12_init_board.py --workers 3
  ASTOCK_DB_USER=zhu .venv/bin/python 12_init_board.py --limit 5   # 试跑
  ASTOCK_DB_USER=zhu .venv/bin/python 12_init_board.py --reset

请求量 ≈ 板块数×3 ≈ 1600 次,3 并发约 15-20 分钟。全部为东财行情族接口,
限流特征见 memory eastmoney-rate-limit;遇熔断等冷却后重跑即续传。
"""

from __future__ import annotations

import argparse
import sys
from collections import namedtuple

import common as c

TASK = "init_board"
BoardRow = namedtuple("BoardRow", "stock_code board_name board_type")  # stock_code=board_code


def upsert_board_list(conn) -> "c.pd.DataFrame":
    boards = c.fetch_board_list()
    n = c.upsert(conn, "board",
                 ["board_code", "board_name", "board_type"],
                 [(r.board_code, r.board_name, r.board_type)
                  for r in boards.itertuples(index=False)],
                 ["board_code"], update_cols=["board_name", "board_type"])
    c.log.info("板块列表 %d 个(行业 %d / 概念 %d)", n,
               (boards.board_type == "industry").sum(), (boards.board_type == "concept").sum())
    return boards


def load_one_board(conn, r: BoardRow) -> None:
    """单板块全量:日K + 资金流 + 当前成分。r.stock_code 即 board_code。"""
    code = r.stock_code
    n_d = c.upsert_board_daily(conn, code, c.fetch_board_daily(r.board_name, r.board_type))
    n_f = c.upsert_board_fund_flow(conn, code, c.fetch_board_fund_flow(r.board_name, r.board_type))
    cons = c.fetch_board_cons(code, r.board_type)
    today = c.beijing_now().date()
    n_o, n_c = c.sync_board_members(conn, code, cons, today) if cons else (0, 0)
    if not cons:
        c.log.warning("  %s %s: 成分为空,跳过成分同步", code, r.board_name)
    c.mark_progress(conn, TASK, code, None, "done", f"daily={n_d},flow={n_f},cons=+{n_o}/-{n_c}")
    c.log.info("  %s %s: 日线 %d / 资金流 %d / 成分 %d", code, r.board_name, n_d, n_f, n_o)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--limit", type=int, default=None, help="只处理前 N 个板块(试跑)")
    ap.add_argument("--reset", action="store_true", help="清空 init_board 进度重来")
    args = ap.parse_args()

    conn = c.get_conn()
    try:
        if args.reset:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM etl_progress WHERE task = %s", (TASK,))
            conn.commit()
            c.log.info("已清空 %s 进度", TASK)

        boards = upsert_board_list(conn)
        if args.limit:
            boards = boards.head(args.limit)
        done = c.get_done_codes(conn, TASK)
        todo = [BoardRow(r.board_code, r.board_name, r.board_type)
                for r in boards.itertuples(index=False) if r.board_code not in done]
        c.log.info("待处理 %d 个板块(已完成 %d,并发 %d)", len(todo), len(done), args.workers)
        conn.commit()

        c.run_stock_todo(todo, TASK, load_one_board, args.workers, max_consecutive_errors=15)
        c.log.info("板块全量初始化完成 ✅")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: 试跑 5 个板块**

Run: `cd /Users/zhu/own/my_stocks/.claude/worktrees/local-vs-cloud-env-5be7f8 && ASTOCK_DB_USER=zhu .venv/bin/python 12_init_board.py --limit 5 --workers 2`
Expected: 5 行 `BKxxxx ...: 日线 N / 资金流 N / 成分 N`(N 均 >0),无 ERROR。若盘中运行,日志应出现"收盘防护 cutoff=..."且当日 bar 被拦。

- [ ] **Step 3: 试跑结果抽查**

Run: `psql -U zhu -d astock -Atc "SELECT count(*) FROM board_daily; SELECT count(*) FROM board_member WHERE valid_to IS NULL;" && psql -U zhu -d astock -Atc "SELECT max(trade_date) <= current_date FROM board_daily;"`
Expected: 两个 count > 0;末查询输出 `t`(无未来日期)。

- [ ] **Step 4: Commit**

```bash
git add 12_init_board.py
git commit -m "feat: 板块全量初始化脚本(断点续传,复用 run_stock_todo)"
```

---

### Task 5: 13_board_update.py 每日增量

**Files:**
- Create: `13_board_update.py`

**Interfaces:**
- Consumes: Task 2/3 函数;`12_init_board` 的 `BoardRow`、`load_one_board`、`upsert_board_list`(经 `import_module("12_init_board")`,惯例同 10 引 09)
- Produces: 无(终端脚本);cron 挂载见 Task 6

- [ ] **Step 1: 写 13_board_update.py**

```python
"""
13_board_update.py — 板块每日增量(cron 18:10,排在 03 日线 18:00 之后)。

流程:
  1. 刷新板块列表:新板块 → 插入并补拉全历史(复用 12 的 load_one_board);
     改名 → upsert 覆盖 board_name;从列表消失 → is_active=false(数据保留);
     列表数量异常(行业<50 或 概念<200)→ 判定源故障,直接退出不做任何 diff。
  2. 逐 active 板块:日K自 max(trade_date)+1 增量;资金流全拉幂等覆盖;
     成分 diff(接口失败/空返回则跳过该板块成分,宁可不更新不误判全员移出)。
  3. 进度:etl_progress task='daily_board',按板块记 done/error。
"""

from __future__ import annotations

import sys
from datetime import timedelta
from importlib import import_module

import common as c

init_board = import_module("12_init_board")
TASK = "daily_board"


def refresh_board_list(conn) -> list:
    boards = c.fetch_board_list()
    n_ind = (boards.board_type == "industry").sum()
    n_con = (boards.board_type == "concept").sum()
    if n_ind < 50 or n_con < 200:
        c.log.critical("板块列表数量异常(行业 %d / 概念 %d),疑似源故障,本次退出", n_ind, n_con)
        return []
    with conn.cursor() as cur:
        cur.execute("SELECT board_code FROM board")
        known = {r[0] for r in cur.fetchall()}
        listed = set(boards.board_code)
        gone = sorted(known - listed)
        if gone:
            cur.execute("UPDATE board SET is_active = FALSE, updated_at = now() "
                        "WHERE board_code = ANY(%s)", (gone,))
            c.log.info("板块退场 %d 个: %s", len(gone), gone[:10])
        cur.execute("UPDATE board SET is_active = TRUE, updated_at = now() "
                    "WHERE board_code = ANY(%s) AND NOT is_active", (sorted(listed),))
    conn.commit()
    init_board.upsert_board_list(conn)   # 幂等:改名覆盖 + 新板块插入
    new = sorted(listed - known)
    rows = [init_board.BoardRow(r.board_code, r.board_name, r.board_type)
            for r in boards.itertuples(index=False)]
    if new:
        c.log.info("新板块 %d 个,补拉全历史: %s", len(new), new)
        for r in [x for x in rows if x.stock_code in new]:
            init_board.load_one_board(conn, r)
    return [x for x in rows if x.stock_code not in new]


def update_one_board(conn, r) -> None:
    code = r.stock_code
    with conn.cursor() as cur:
        cur.execute("SELECT max(trade_date) FROM board_daily WHERE board_code = %s", (code,))
        max_d = cur.fetchone()[0]
    start = (max_d + timedelta(days=1)).strftime("%Y%m%d") if max_d else "19900101"
    n_d = c.upsert_board_daily(conn, code, c.fetch_board_daily(r.board_name, r.board_type, start=start))
    n_f = c.upsert_board_fund_flow(conn, code, c.fetch_board_fund_flow(r.board_name, r.board_type))
    cons = c.fetch_board_cons(code, r.board_type)
    if cons:
        n_o, n_c = c.sync_board_members(conn, code, cons, c.beijing_now().date())
    else:
        n_o = n_c = 0
        c.log.warning("  %s: 成分为空,跳过成分 diff", code)
    last = max_d if n_d == 0 else None   # 简化:成功即记 done,last_date 仅参考
    c.mark_progress(conn, TASK, code, last, "done", f"daily=+{n_d},flow={n_f},cons=+{n_o}/-{n_c}")
    if n_d or n_o or n_c:
        c.log.info("  %s %s: 日线 +%d / 成分 +%d/-%d", code, r.board_name, n_d, n_o, n_c)


def main() -> int:
    conn = c.get_conn()
    try:
        rows = refresh_board_list(conn)
        if not rows:
            return 1
        with conn.cursor() as cur:   # 只更新 active 板块
            cur.execute("SELECT board_code FROM board WHERE is_active")
            active = {r[0] for r in cur.fetchall()}
        todo = [r for r in rows if r.stock_code in active]
        c.log.info("板块增量:%d 个(并发 3)", len(todo))
        conn.commit()
        c.run_stock_todo(todo, TASK, update_one_board, 3, max_consecutive_errors=15)
        c.log.info("板块增量完成 ✅")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: 增量幂等试跑(紧跟 Task 4 试跑之后)**

Run: `cd /Users/zhu/own/my_stocks/.claude/worktrees/local-vs-cloud-env-5be7f8 && ASTOCK_DB_USER=zhu .venv/bin/python - <<'EOF'
import sys; sys.argv = ["13_board_update.py"]
import importlib; m = importlib.import_module("13_board_update"); sys.exit(m.main())
EOF`
Expected: 正常完成;已试跑过的 5 个板块日线增量为 0(或仅当日一根,若已过 15:30)、成分 +0/-0;etl_progress 出现 task='daily_board'。注:全量 init 未完成时其余板块会走全历史补拉,属预期(等同于续传)。

- [ ] **Step 3: Commit**

```bash
git add 13_board_update.py
git commit -m "feat: 板块每日增量脚本(列表刷新/退场/新板块补拉/成分diff)"
```

---

### Task 6: 全量初始化运行 + 验证 + 交接

**Files:**
- Modify: 无(运行与验证)

- [ ] **Step 1: 全量初始化(须 push2 解封,建议盘后)**

Run: `cd /Users/zhu/own/my_stocks/.claude/worktrees/local-vs-cloud-env-5be7f8 && ASTOCK_DB_USER=zhu .venv/bin/python 12_init_board.py --workers 3`(后台运行,遇熔断冷却后重跑续传)
Expected: `板块全量初始化完成 ✅`;etl_progress init_board done ≈ 536。

- [ ] **Step 2: 验证清单(spec 验证节)**

Run:
```bash
psql -U zhu -d astock <<'SQL'
-- 覆盖与范围
SELECT board_type, count(*) FROM board GROUP BY 1;                     -- industry ~86, concept ~450
SELECT count(*) FROM board_daily;                                      -- ≈ 百万级
SELECT min(trade_date) FROM board_daily;                               -- ≲ 2010
SELECT count(*) FROM board_member WHERE valid_to IS NULL;              -- ≈ 3~4 万
SELECT count(*) FROM board_daily WHERE trade_date > current_date;      -- 0
SELECT count(*) FROM board_fund_flow WHERE trade_date > current_date;  -- 0
-- 资金流自洽抽查(主力=超大+大;五档和≈0,容差 1%)
SELECT count(*) FROM board_fund_flow
WHERE abs(main_net - (xlarge_net + large_net)) > greatest(abs(main_net) * 0.01, 1e4);  -- ≈ 0
SQL
```
Expected: 注释中的量级;最后一项接近 0(个别源数据缺档可容忍,>1% 需排查)。

- [ ] **Step 3: 板块-成分交叉校验(抽 3 板块)**

Run:
```bash
psql -U zhu -d astock <<'SQL'
-- 最近一个交易日:板块涨跌幅 vs 成分股流通市值加权涨跌幅(容差 ±1pp,口径近似)
WITH d AS (SELECT max(trade_date) t FROM board_daily),
samp AS (SELECT board_code FROM board WHERE is_active ORDER BY board_code LIMIT 3),
calc AS (
  SELECT bd.board_code, bd.pct_chg AS board_pct,
         sum(dp.pct_chg * dv.total_mv) / nullif(sum(dv.total_mv), 0) AS member_pct
  FROM samp s JOIN board_daily bd ON bd.board_code = s.board_code JOIN d ON bd.trade_date = d.t
  JOIN board_member m ON m.board_code = s.board_code
       AND m.valid_from <= d.t AND (m.valid_to IS NULL OR m.valid_to > d.t)
  JOIN daily_price dp ON dp.stock_code = m.stock_code AND dp.trade_date = d.t
  JOIN daily_valuation dv ON dv.stock_code = m.stock_code AND dv.trade_date = d.t
  GROUP BY bd.board_code, bd.pct_chg)
SELECT *, abs(board_pct - member_pct) < 1.0 AS ok FROM calc;
SQL
```
Expected: 3 行,`ok` 全为 `t`(东财板块指数为自由流通加权,近似口径下 1pp 容差内)。

- [ ] **Step 4: 更新 README(板块层小节)+ Commit**

在 README.md 基本面小节之后追加(风格对齐现有文档):表清单(board/board_member/board_daily/board_fund_flow)、初始化与增量命令、成分区间表 as-of 查询示例、"valid_from=观测起点"警示、cron 行(合并到主分支后由主仓库挂载):
```
10 18 * * 1-5 cd /Users/zhu/own/my_stocks && ASTOCK_DB_USER=zhu .venv/bin/python 13_board_update.py >> update_board.log 2>&1
```

```bash
git add README.md
git commit -m "docs: README 板块数据层使用说明与 cron 交接"
```

- [ ] **Step 5: 更新 memory**

在 `astock-local-env.md` 追加一行:板块数据层四表已建(行业+概念),13 脚本待合并主分支后挂 cron 18:10。

## Self-Review 记录

- Spec 覆盖:4 张表(T1)、fetch 层(T2)、区间 diff+防护(T3)、init(T4)、daily(T5)、验证与 cron(T6)——spec 各节均有对应任务;"明确不做"节无泄漏进任务。
- 占位符:无 TBD/伪代码;所有代码块完整可执行。
- 类型一致:`BoardRow.stock_code` 借存 board_code 的约定在 T4 定义、T5 消费一致;`_BOARD_FLOW_COLS` 在 T2 定义、T3 消费一致;`sync_board_members(conn, code, set, date)` 签名三处一致。
