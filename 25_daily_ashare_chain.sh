#!/bin/zsh
# 25_daily_ashare_chain.sh — A股日线弹性链(cron 16:00 入口)。
#
# 探测优先:先用单个请求探东财行情族(push2his)是否可用——
#   * 可用 → 走东财主源(03),成交额/换手率原生齐全;
#   * 被封 → 直接走腾讯源(03 --tx)+ 24 快照补北交所与成交额/换手率,
#            全程不碰被封端点(避免"封禁期持续重试续期封禁",见 project-notes 2)。
# 单次探测代价可忽略,且东财恢复后自动切回主源,无需人工改 cron。
set -u
cd /Users/zhu/own/my_stocks || exit 1
export ASTOCK_DB_USER=zhu
export PATH="/opt/homebrew/bin:$PATH"   # cron 的 PATH 精简,psql 需显式加入

# --- 东财行情族单次探测(1 请求,不是全量硬跑) ---
em_ok=$(.venv/bin/python - <<'PY'
import sys
try:
    import requests
    r = requests.get("https://push2his.eastmoney.com/api/qt/stock/kline/get",
                     params={"secid":"1.600519","klt":"101","fqt":"0",
                             "beg":"20260701","end":"20260710",
                             "fields1":"f1","fields2":"f51,f57"}, timeout=8)
    print("1" if r.status_code == 200 and r.json().get("data") else "0")
except Exception:
    print("0")
PY
)

if [ "$em_ok" = "1" ]; then
  echo "[chain $(date '+%F %T')] 东财行情族可用,走主源"
  .venv/bin/python 03_daily_update.py
else
  echo "[chain $(date '+%F %T')] 东财行情族被封,走腾讯源 + 快照兜底"
  ASTOCK_ASHARE_SOURCE=tx .venv/bin/python 03_daily_update.py
  .venv/bin/python 24_spot_refill.py
fi

# --- 收尾核对(不论走哪条,当天开市则报当日行数) ---
is_open=$(psql -U zhu -d astock -Atc "SELECT coalesce((SELECT is_open::int FROM trade_calendar WHERE trade_date = current_date), 0)")
today_rows=$(psql -U zhu -d astock -Atc "SELECT count(*) FROM daily_price WHERE trade_date = current_date")
echo "[chain $(date '+%F %T')] 完成:当日 ${today_rows} 行(开市=${is_open},源=$([ "$em_ok" = "1" ] && echo em || echo tx))"
if [ "$is_open" = "1" ] && [ "$today_rows" -lt 4500 ]; then
  echo "[chain $(date '+%F %T')] ⚠️ 当日行数偏少,请检查数据源"
fi
