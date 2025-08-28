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
├── ...                     # 其他服务或代码目录 (Other service/code directories)

主要目录和文件说明：  
- `docker-compose.yml`：定义 PostgreSQL、pgAdmin4、EMQX、ingestor 等服务的启动方式和依赖关系  
  (Defines how PostgreSQL, pgAdmin4, EMQX, ingestor and other services are started and related)
- `init/schema.sql`：数据库表结构、分区、定时任务等初始化SQL  
  (Database schema, partitioning, scheduled tasks and other initialization SQL)
- `pgadmin/servers.json`：pgAdmin4 的服务器连接预设  
  (pgAdmin4 server connection presets)
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
docker compose up -d        # 后台启动所有服务 (Start all services in detached mode)

