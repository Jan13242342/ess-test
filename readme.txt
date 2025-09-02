项目结构说明  
Project Structure Description

ess-test/
├── docker-compose.yml      # Docker编排文件，定义各服务 (Docker Compose file, defines all services)
├── .env                    # 环境变量配置 (Environment variables)
├── install_docker.sh       # 一键安装Docker及依赖脚本 (Install Docker and dependencies script)
├── readme.txt              # 项目说明文档 (Project documentation)
├── init/
│   └── schema.sql          # 数据库初始化建表脚本 (Database schema/init script)
├── pgadmin/
│   └── servers.json        # pgAdmin4 服务器配置 (pgAdmin4 server config)
├── api/
│   └── main.py             # FastAPI 后端主程序 (FastAPI backend main program)
├── frontend/               # 前端项目目录 (Frontend project directory)
├── ...                     # 其他服务或代码目录 (Other service/code directories)

主要目录和文件说明：  
- `docker-compose.yml`：定义 PostgreSQL、pgAdmin4、EMQX、ingestor、API、前端等服务的启动方式和依赖关系  
  (Defines how PostgreSQL, pgAdmin4, EMQX, ingestor, API, frontend and other services are started and related)
- `init/schema.sql`：数据库表结构、分区、定时任务等初始化SQL  
  (Database schema, partitioning, scheduled tasks and other initialization SQL)
- `pgadmin/servers.json`：pgAdmin4 的服务器连接预设  
  (pgAdmin4 server connection presets)
- `api/main.py`：FastAPI 后端主程序，包含所有接口及中英文注释  
  (FastAPI backend main program, includes all APIs and bilingual comments)
- `frontend/`：前端项目目录，包含页面、静态资源等  
  (Frontend project directory, includes pages, static assets, etc.)
- `install_docker.sh`：一键安装Docker环境的脚本  
  (Script for one-click Docker environment installation)
- `.env`：所有服务的环境变量配置  
  (Environment variable configuration for all services)
- `readme.txt`：本说明文档  
  (This documentation file)

----------------------------------------

首次运行  
First run

1.  
cd /opt         # 进入 /opt 目录 (Enter /opt directory)
cd ess          # 进入 ess 项目目录 (Enter ess project directory)

chmod +x install_docker.sh   # 赋予安装脚本执行权限 (Make install script executable)
./install_docker.sh          # 运行安装脚本 (Run the install script)

2.  
cd /opt         # 进入 /opt 目录 (Enter /opt directory)
cd ess          # 进入 ess 项目目录 (Enter ess project directory)
docker compose down          # 停止并移除所有容器 (Stop and remove all containers)
docker compose up -d         # 后台启动所有服务 (Start all services in detached mode)

----------------------------------------

后端 API 说明  
Backend API Description

- 所有接口均有中英文注释，详见 `api/main.py` 或访问 `/docs` (Swagger UI) 查看自动生成的接口文档。
- 支持用户注册、登录、设备绑定、实时数据、历史数据、数据库监控等功能。
- 管理员和客服可通过 `/api/v1/db/metrics` 获取数据库健康与性能指标。

All APIs have bilingual comments. See `api/main.py` or visit `/docs` (Swagger UI) for auto-generated API documentation.
Supports user registration, login, device binding, realtime data, history data, database monitoring, etc.
Admins and service staff can get DB health & performance metrics via `/api/v1/db/metrics`.

----------------------------------------

前端说明  
Frontend Description

- 前端项目位于 `frontend/` 目录，建议使用 Vue、React 等现代前端框架开发。
- 前端通过 HTTP(S) 调用后端 API，实现用户登录、数据展示、设备管理等功能。
- 开发完成后可通过 Nginx/Caddy 等反向代理实现 HTTPS 访问。

Frontend project is in `frontend/` directory. Recommended to use Vue, React or other modern frameworks.
Frontend calls backend APIs via HTTP(S) for user login, data display, device management, etc.
After development, use Nginx/Caddy for HTTPS reverse proxy.

----------------------------------------

安全与上线建议  
Security & Deployment Suggestions

- 生产环境务必配置 HTTPS/证书，关闭数据库 trust 认证，EMQX 关闭匿名、开启认证和 TLS。
- 所有安全加固建议请参考本项目文档及各服务官方文档。

In production, enable HTTPS/certificates, disable DB trust auth, disable EMQX anonymous, enable authentication and TLS.
See this doc and official docs for all security hardening建议。

