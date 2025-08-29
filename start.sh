#!/bin/bash

# 一键启动 EMQX、Postgres、pgAdmin 和 2个 ingestor 实例
# One-click start for EMQX, Postgres, pgAdmin, and 2 ingestor instances

echo "正在构建镜像..."  # Building Docker images...
docker compose build --no-cache

echo "启动所有服务，并将 ingestor 扩容为2个实例..."  # Starting all services and scaling ingestor to 2 instances...
docker compose up -d --scale ingestor=2

echo "所有服务已启动！"  # All services started!
