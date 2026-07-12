#!/bin/zsh
# 25_daily_ashare_chain.sh — A股日线弹性链(cron 18:00 入口)。
# 东财主源先跑(被封时 03 的熔断器 ~10 分钟内退出);当天开市且产出不足时
# 自动切腾讯源补跑,再用腾讯快照补北交所与成交额/换手率(24_spot_refill.py)。
# 东财恢复后无需改动:主源产出充足则兜底分支不触发。
set -u
cd /Users/zhu/own/my_stocks || exit 1
export ASTOCK_DB_USER=zhu

.venv/bin/python 03_daily_update.py
rc=$?

is_open=$(psql -U zhu -d astock -Atc "SELECT coalesce((SELECT is_open::int FROM trade_calendar WHERE trade_date = current_date), 0)")
today_rows=$(psql -U zhu -d astock -Atc "SELECT count(*) FROM daily_price WHERE trade_date = current_date")

if [ "$is_open" = "1" ] && [ "$today_rows" -lt 4500 ]; then
  echo "[resilient $(date '+%F %T')] 东财产出不足(今日 ${today_rows} 行,rc=${rc}),切腾讯源兜底"
  ASTOCK_ASHARE_SOURCE=tx .venv/bin/python 03_daily_update.py
  .venv/bin/python 24_spot_refill.py
else
  echo "[resilient $(date '+%F %T')] 主源正常(今日 ${today_rows} 行,开市=${is_open}),无需兜底"
fi
