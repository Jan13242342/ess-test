#!/bin/bash

# 一键启动 EMQX、Postgres、pgAdmin 和 2个 ingestor 实例
# One-click start for EMQX, Postgres, pgAdmin, and 2 ingestor instances

echo "正在构建镜像... (Building Docker images...)"
docker compose build --no-cache

echo "启动所有服务... (Starting all services...)"
docker compose up -d

echo "覆盖 pg_hba.conf，只允许本地和 Docker 网络访问..."
docker exec -i postgres_db bash -c "echo -e 'local   all   all   md5\nhost    all   all   127.0.0.1/32   md5\nhost    all   all   172.18.0.0/16   md5' > /var/lib/postgresql/data/pg_hba.conf"
docker restart postgres_db



echo "所有服务已启动！ (All services started!)"
echo "EMQX Dashboard: http://localhost:18083"
echo "pgAdmin:        http://localhost:5050"

echo "已设置防火墙规则：只允许本地访问 5432 端口"
iptables -A INPUT -p tcp -s 127.0.0.1 --dport 5432 -j ACCEPT
iptables -A INPUT -p tcp --dport 5432 -j DROP



echo "开始实时监控所有服务日志中的关键字ERROR/WARNING/Exception..."
docker compose logs -f | grep --line-buffered -E "ERROR|WARNING|Exception"

