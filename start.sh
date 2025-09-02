#!/bin/bash

# 一键启动 EMQX、Postgres、pgAdmin 和 2个 ingestor 实例
# One-click start for EMQX, Postgres, pgAdmin, and 2 ingestor instances

echo "正在构建镜像... (Building Docker images...)"
docker compose build --no-cache

echo "启动所有服务... (Starting all services...)"
docker compose up -d

echo "所有服务已启动！ (All services started!)"
echo "EMQX Dashboard: http://localhost:18083"
echo "pgAdmin:        http://localhost:5050"

echo "开始实时监控所有服务日志中的关键字ERROR/WARNING/Exception..."
docker compose logs -f | grep --line-buffered -E "ERROR|WARNING|Exception"