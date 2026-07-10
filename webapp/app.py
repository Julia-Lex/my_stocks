"""财报查询页面 — FastAPI 后端。

只读查询本机 astock 库(经 common.get_conn(),遵守 ASTOCK_DB_* 环境变量)。
启动:.venv/bin/uvicorn webapp.app:app --port 8500
"""
from __future__ import annotations

import sys
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query

# 根目录的 common.py(uvicorn 从仓库根启动时 cwd 在 sys.path,这里再保险一层)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common import get_conn  # noqa: E402

app = FastAPI(title="财报查询")

# market → 表名映射。has_currency=False 表示无币种列(A股恒为 CNY)
MARKETS = {
    "cn": {"basic": "stock_basic", "stmt": "fin_statement", "has_currency": False},
    "hk": {"basic": "hk_stock_basic", "stmt": "hk_fin_statement", "has_currency": True},
    "us": {"basic": "us_stock_basic", "stmt": "us_fin_statement", "has_currency": True},
}


@app.get("/api/search")
def search(q: str = Query(..., min_length=1, max_length=32)):
    """按名字/代码模糊搜索三个市场,A股优先,最多 10 条。"""
    pat = f"%{q.strip()}%"
    sql = " UNION ALL ".join(
        f"SELECT stock_code, name, '{m}' AS market, "
        f"EXISTS (SELECT 1 FROM {cfg['stmt']} f WHERE f.stock_code = s.stock_code) AS has_statements "
        f"FROM {cfg['basic']} s WHERE s.name ILIKE %s OR s.stock_code ILIKE %s"
        for m, cfg in MARKETS.items()
    )
    # A股在前,有财报的在前,名字短的在前(更可能是精确命中)
    sql = (f"SELECT * FROM ({sql}) t "
           f"ORDER BY (market <> 'cn'), (NOT has_statements), length(name), stock_code LIMIT 10")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, [pat, pat] * len(MARKETS))
            rows = cur.fetchall()
    finally:
        conn.close()
    return [{"code": r[0], "name": r[1], "market": r[2], "has_statements": r[3]} for r in rows]
