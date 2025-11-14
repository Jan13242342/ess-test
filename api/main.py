from datetime import datetime, timedelta, date, time as dtime, timezone
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text, func
from pydantic import BaseModel, Field
from typing import Optional, List
import asyncio
import json
import decimal

from deps import get_current_user
from config import DATABASE_URL, DEVICE_FRESH_SECS, ALARM_HISTORY_RETENTION_DAYS, RPC_LOG_RETENTION_DAYS

app = FastAPI(title="ESS Realtime API", version="1.0.0")

engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)

# 放到前面，供 routers 导入
async_session = async_sessionmaker(engine, expire_on_commit=False)

# ---------------- 公共模型与常量 ----------------

COLUMNS = """
r.device_id, d.device_sn, r.updated_at,
r.soc, r.soh, r.pv, r.load, r.grid, r.grid_q, r.batt,
r.ac_v, r.ac_f, r.v_a, r.v_b, r.v_c, r.i_a, r.i_b, r.i_c,
r.p_a, r.p_b, r.p_c, r.q_a, r.q_b, r.q_c,
r.e_pv_today, r.e_load_today, r.e_charge_today, r.e_discharge_today
"""

def online_flag(updated_at: datetime, fresh_secs: int) -> bool:
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - updated_at) <= timedelta(seconds=fresh_secs)

# ---------------- 挂载拆分后的路由 ----------------
from routers import user as user_router, admin as admin_router, ota as ota_router, rpc as rpc_router, alarm as alarm_router, auth as auth_router
app.include_router(auth_router.router)
app.include_router(user_router.router)
app.include_router(admin_router.router)
app.include_router(ota_router.router)
app.include_router(rpc_router.router)
app.include_router(alarm_router.router)

# 管理员/客服共用：根据设备SN获取实时数据
@app.get(
    "/api/v1/realtime/by_sn/{device_sn}",
    tags=["管理员/客服 | Admin/Service"],
    summary="根据设备SN获取实时数据 | Get Realtime Data by Device SN",
    description="""
**权限要求 | Required Role**: admin, service, support

根据设备序列号查询该设备的实时数据（包含在线状态）。

Query real-time data by device serial number (including online status).
"""
)
async def get_realtime_by_sn(device_sn: str, user=Depends(get_current_user)):
    if user["role"] not in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="无权限")
    sql = text(f"""
        SELECT {COLUMNS}
        FROM ess_realtime_data r
        JOIN devices d ON r.device_id = d.id
        WHERE d.device_sn=:sn
    """)
    async with engine.connect() as conn:
        row = (await conn.execute(sql, {"sn": device_sn})).mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="实时数据不存在")
    d = dict(row)
    d["online"] = online_flag(d["updated_at"], DEVICE_FRESH_SECS)
    return d

# ---------------- 历史能耗聚合 ----------------

class HistoryDataAgg(BaseModel):
    device_id: int
    device_sn: str
    day: Optional[date] = None
    hour: Optional[datetime] = None
    month: Optional[date] = None
    charge_wh_total: Optional[int]
    discharge_wh_total: Optional[int]
    pv_wh_total: Optional[int]
    grid_wh_total: Optional[int]
    load_wh_total: Optional[int]

class HistoryAggListResponse(BaseModel):
    items: List[HistoryDataAgg]
    page: int
    page_size: int
    total: int

@app.get(
    "/api/v1/history/admin/by_sn",
    response_model=HistoryAggListResponse,
    tags=["管理员/客服 | Admin/Service"],
    summary="管理员按设备SN查询历史能耗聚合数据",
    description="""
**权限要求 | Required Role**: admin, service, support

管理员或客服可通过设备SN查询该设备的历史能耗聚合数据，支持按固定周期或指定日期（按小时）聚合。

Admin/Service/Support can query device historical energy aggregation data by device SN.

**支持的聚合周期 | Supported Periods**:
- `today`: 今日按小时聚合 | Hourly aggregation for today
- `week`: 本周按天聚合 | Daily aggregation for this week
- `month`: 本月按天聚合 | Daily aggregation for this month
- `quarter`: 本季度按月聚合 | Monthly aggregation for this quarter
- `year`: 本年按月聚合 | Monthly aggregation for this year
- `date`: 指定日期按小时聚合（覆盖period）| Hourly aggregation for specific date (overrides period)
"""
)
async def admin_history_by_sn(
    device_sn: str = Query(..., description="设备序列号"),
    period: str = Query("today", description="聚合周期: today/week/month/quarter/year，默认today"),
    date: Optional[date] = Query(None, description="指定日期（YYYY-MM-DD），按该日期按小时聚合，优先于period | Specific date (YYYY-MM-DD), aggregate by hour for that day, takes precedence over period"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=200, description="每页数量"),
    admin=Depends(get_current_user)
):
    # 只允许管理员和客服访问
    if admin["role"] not in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="无权限")
    async with engine.connect() as conn:
        device_row = (await conn.execute(
            text("SELECT id, device_sn FROM devices WHERE device_sn=:sn"),
            {"sn": device_sn}
        )).mappings().first()
        if not device_row:
            raise HTTPException(status_code=404, detail="设备不存在")
        device_id = device_row["id"]
        device_sn_map = {device_id: device_sn}
    now = datetime.now(timezone.utc)
    if date:
        start = datetime.combine(date, dtime.min).replace(tzinfo=timezone.utc)
        end = datetime.combine(date, dtime.max).replace(tzinfo=timezone.utc)
        group_expr = "date_trunc('hour', ts)"
        group_label = "hour"
    else:
        # 同上 period 逻辑
        if period == "today":
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
            group_expr = "date_trunc('hour', ts)"
            group_label = "hour"
        elif period == "week":
            start_of_week = now - timedelta(days=now.weekday())
            start = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
            end = start + timedelta(days=6, hours=23, minutes=59, seconds=59, microseconds=999999)
            group_expr = "date_trunc('day', ts)"
            group_label = "day"
        elif period == "month":
            start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            next_month = (now.replace(day=28) + timedelta(days=4)).replace(day=1)
            end = next_month - timedelta(seconds=1)
            group_expr = "date_trunc('day', ts)"
            group_label = "day"
        elif period == "quarter":
            quarter = (now.month - 1) // 3 + 1
            start_month = (quarter - 1) * 3 + 1
            start = now.replace(month=start_month, day=1, hour=0, minute=0, second=0, microsecond=0)
            end_month = quarter * 3
            next_quarter = start.replace(month=end_month + 1, day=1) if end_month < 12 else start.replace(year=start.year + 1, month=1, day=1)
            end = next_quarter - timedelta(seconds=1)
            group_expr = "date_trunc('month', ts)"
            group_label = "month"
        elif period == "year":
            start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            end = now.replace(month=12, day=31, hour=23, minute=59, second=59, microsecond=999999)
            group_expr = "date_trunc('month', ts)"
            group_label = "month"
        else:
            raise HTTPException(status_code=400, detail="无效的 period 值")
    params = {"id0": device_id, "start": start, "end": end}
    where = ["device_id = :id0", "ts >= :start", "ts <= :end"]
    cond = "WHERE " + " AND ".join(where) if where else ""
    offset = (page - 1) * page_size
    async with engine.connect() as conn:
        count_sql = text(f"""
            SELECT COUNT(*) FROM (
                SELECT {group_expr} AS {group_label}, device_id
                FROM history_energy
                {cond}
                GROUP BY device_id, {group_label}
            ) t
        """)
        query_sql = text(f"""
            SELECT
                device_id,
                {group_expr} AS {group_label},
                SUM(charge_wh_total) AS charge_wh_total,
                SUM(discharge_wh_total) AS discharge_wh_total,
                SUM(pv_wh_total) AS pv_wh_total,
                SUM(grid_wh_total) AS grid_wh_total,
                SUM(load_wh_total) AS load_wh_total      -- 新增
            FROM history_energy
            {cond}
            GROUP BY device_id, {group_label}
            ORDER BY {group_label} DESC
            LIMIT :limit OFFSET :offset
        """)
        total = (await conn.execute(count_sql, params)).scalar_one()
        rows = (await conn.execute(query_sql, {**params, "limit": page_size, "offset": offset})).mappings().all()
    items = []
    for r in rows:
        d = dict(r)
        d["device_sn"] = device_sn_map.get(d["device_id"], "")
        # 只保留当前聚合粒度的字段
        if group_label == "hour":
            d["hour"] = d.pop("hour")
            d["day"] = None
            d["month"] = None
        elif group_label == "day":
            d["day"] = d.pop("day")
            d["hour"] = None
            d["month"] = None
        elif group_label == "month":
            d["month"] = d.pop("month")
            d["hour"] = None
            d["day"] = None
        items.append(d)
    return {"items": items, "page": page, "page_size": page_size, "total": total}

@app.get(
    "/api/v1/db/metrics",
    tags=["管理员/客服 | Admin/Service"],
    summary="数据库健康与性能指标 | Database Health & Performance Metrics",
    description="""
**权限要求 | Required Role**: admin, service

仅管理员和客服可用。返回数据库连接数、活跃连接、慢查询、缓存命中率、死锁数、慢SQL历史等多项数据库健康与性能指标。

Admin and service only. Returns database connection count, active connections, slow queries, cache hit rate, deadlocks, slow SQL history and other health/performance metrics.

📊 **返回指标 | Metrics Returned**:
- 连接数统计（总数/活跃/空闲）| Connection stats (total/active/idle)
- 慢查询列表（>5秒）| Slow queries (>5s)
- 数据库大小 & 表大小 | DB size & table sizes
- 缓存命中率 | Cache hit rate
- 死锁数量 | Deadlock count
- 历史慢SQL统计 | Historical slow SQL stats
"""
)
async def db_metrics(user=Depends(get_current_user)):
    # 只允许管理员和客服访问
    if user["role"] not in ("admin", "service"):
        raise HTTPException(status_code=403, detail="无权限")

    async with engine.connect() as conn:
        # 当前连接数
        conn_count = (await conn.execute(text("SELECT count(*) FROM pg_stat_activity"))).scalar_one()

        def safe_dict(row):
            d = dict(row)
            if "client_addr" in d and d["client_addr"] is not None:
                d["client_addr"] = str(d["client_addr"])
            if "duration" in d and d["duration"] is not None:
                d["duration"] = str(d["duration"])
            return d

        # 活跃连接详情（非 idle）
        active_sql = text("""
            SELECT pid, usename, application_name, client_addr, state, query, now() - query_start AS duration
            FROM pg_stat_activity
            WHERE state != 'idle'
            ORDER BY duration DESC
            LIMIT 20
        """)
        active_rows = (await conn.execute(active_sql)).mappings().all()
        active_connections = [safe_dict(row) for row in active_rows]

        # 慢查询（运行超过5秒的）
        slow_sql = text("""
            SELECT pid, usename, application_name, client_addr, state, query, now() - query_start AS duration
            FROM pg_stat_activity
            WHERE state != 'idle' AND now() - query_start > interval '5 seconds'
            ORDER BY duration DESC
            LIMIT 20
        """)
        slow_rows = (await conn.execute(slow_sql)).mappings().all()
        slow_queries = [safe_dict(row) for row in slow_rows]

        # 数据库总大小
        db_size = (await conn.execute(text("SELECT pg_size_pretty(pg_database_size(current_database()))"))).scalar_one()

        # 每个表的大小（前10大表）
        table_sizes = (await conn.execute(text("""
            SELECT relname AS table, pg_size_pretty(pg_total_relation_size(relid)) AS size
            FROM pg_catalog.pg_statio_user_tables
            ORDER BY pg_total_relation_size(relid) DESC
            LIMIT 10
        """))).mappings().all()
        table_sizes = [dict(row) for row in table_sizes]

        # 缓存命中率
        hit_rate = (await conn.execute(text("""
            SELECT
                ROUND(SUM(blks_hit) / NULLIF(SUM(blks_hit) + SUM(blks_read),0)::numeric, 4) AS hit_rate
            FROM pg_stat_database
        """))).scalar_one()

        # 死锁数
        deadlocks = (await conn.execute(text("""
            SELECT SUM(deadlocks) FROM pg_stat_database
        """))).scalar_one()

        # 事务数（当前活跃事务数）
        tx_count = (await conn.execute(text("""
            SELECT count(*) FROM pg_stat_activity WHERE state = 'active'
        """))).scalar_one()

        # 当前等待的查询数
        waiting_count = (await conn.execute(text("""
            SELECT count(*) FROM pg_stat_activity WHERE wait_event IS NOT NULL
        """))).scalar_one()

        # 数据库启动时间
        start_time = (await conn.execute(text("""
            SELECT pg_postmaster_start_time()
        """))).scalar_one()

        # 服务器版本
        version = (await conn.execute(text("SHOW server_version"))).scalar_one()

        # 最大连接数
        max_conn = (await conn.execute(text("SHOW max_connections"))).scalar_one()

        # 当前空闲连接数
        idle_conn = (await conn.execute(
            text("SELECT count(*) FROM pg_stat_activity WHERE state = 'idle'")
        )).scalar_one()

        # 历史慢SQL统计（PostgreSQL 13+ 字段）
        try:
            stat_sql = text("""
                SELECT
                    query,
                    calls,
                    total_exec_time,
                    mean_exec_time,
                    max_exec_time
                FROM pg_stat_statements
                WHERE calls > 10
                ORDER BY mean_exec_time DESC
                LIMIT 10
            """)
            stat_rows = (await conn.execute(stat_sql)).mappings().all()
            slow_sql_history = [dict(row) for row in stat_rows]
        except Exception:
            await conn.rollback()
            slow_sql_history = []

    result = {
        "connection_count": conn_count,
        "active_connections": active_connections,
        "slow_queries": slow_queries,
        "db_size": db_size,
        "table_sizes": table_sizes,
        "cache_hit_rate": float(hit_rate) if hit_rate is not None else None,
        "deadlocks": deadlocks,
        "active_tx_count": tx_count,
        "waiting_query_count": waiting_count,
        "start_time": str(start_time),
        "server_version": version,
        "max_connections": int(max_conn),
        "idle_connections": idle_conn,
        "slow_sql_history": slow_sql_history
    }
    return JSONResponse(convert_decimal(result))

def convert_decimal(obj):
    if isinstance(obj, list):
        return [convert_decimal(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, decimal.Decimal):
        # 你可以根据实际情况转 float 或 int
        return float(obj)
    else:
        return obj

async def cleanup_alarm_history():
    while True:
        await asyncio.sleep(86400)  # 每天运行一次
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=ALARM_HISTORY_RETENTION_DAYS)
            async with engine.begin() as conn:
                await conn.execute(
                    text("DELETE FROM alarm_history WHERE archived_at < :cutoff"),
                    {"cutoff": cutoff}
                )
        except Exception as e:
            print(f"清理历史报警失败: {e}")

async def cleanup_rpc_logs():
    while True:
        await asyncio.sleep(86400)  # 每天运行一次
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=RPC_LOG_RETENTION_DAYS)
            async with engine.begin() as conn:
                await conn.execute(
                    text("DELETE FROM device_rpc_change_log WHERE created_at < :cutoff"),
                    {"cutoff": cutoff}
                )
        except Exception as e:
            print(f"清理RPC日志失败: {e}")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(cleanup_rpc_logs())
    asyncio.create_task(cleanup_alarm_history())

@app.get(
    "/api/v1/devices/online_summary",
    tags=["管理员/客服 | Admin/Service"],
    summary="设备在线统计 | Device Online Summary",
    description="""
**权限要求 | Required Role**: admin, service, support

返回设备总数、在线数、离线数；按最近60秒内有上报判定为在线。

Returns total devices, online devices, and offline devices. Devices with data reported within the last 60 seconds are considered online.

📝 **在线判断标准 | Online Criteria**: 60秒内有数据上报 | Data reported within 60 seconds
"""
)
async def devices_online_summary(
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="无权限")

    fresh = 60  # 固定60秒

    async with engine.connect() as conn:
        total_devices = (await conn.execute(
            text("SELECT COUNT(*) FROM devices")
        )).scalar_one()

        online_devices = (await conn.execute(
            text("""
                WITH latest AS (
                  SELECT device_id, MAX(updated_at) AS updated_at
                  FROM ess_realtime_data

                  GROUP BY device_id
                )
                SELECT COUNT(*)
                FROM devices d
                LEFT JOIN latest r ON r.device_id = d.id
                WHERE r.updated_at IS NOT NULL
                  AND r.updated_at >= now() - make_interval(secs => :fresh)
            """),
            {"fresh": fresh}
        )).scalar_one()

    offline_devices = max(0, int(total_devices) - int(online_devices))
    return {
        "total_devices": int(total_devices),
        "online_devices": int(online_devices),
        "offline_devices": offline_devices,
        "fresh_secs": fresh
    }
