# CLAUDE.md

my_stocks — A股/港股/美股行情与基本面数据库(PostgreSQL + Python ETL)+ 财报查询 webapp。

## 必读

- **项目部落知识统一记录在 [docs/project-notes.md](docs/project-notes.md)**(本机环境、数据源限流特征、全库约定、事故教训、待决事项)。做任何 ETL/数据层改动前先读它;产生新的此类知识时更新该文件(用户明确要求:记到仓库文档,不要记到会话记忆)。
- 数据库超级用户是 `zhu`,所有命令带 `ASTOCK_DB_USER=zhu`;venv 用 uv(`.venv`),运行 `ASTOCK_DB_USER=zhu .venv/bin/python <脚本>`。
- 测试:`ASTOCK_DB_USER=zhu .venv/bin/python -m pytest tests/`(直连真实库,只读)。
- 成交量全库统一单位"股";复权因子锚点"最早日=1";盘中禁止手动跑 A股日线 ETL(收盘防护见 common.py)。
- 东财接口有 IP 封禁风险(行情族 ≤3 并发错峰;datacenter 较宽松),细节见 project-notes 第 2 节。
