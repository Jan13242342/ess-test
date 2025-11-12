import os
from datetime import datetime, timezone, timedelta, date
from datetime import time as dtime
from typing import List, Optional, Any
import decimal, time, random, string, asyncio, json
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(".env"), override=True)
from fastapi import FastAPI, Query, HTTPException, Depends, Body
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import UploadFile, File, Request
from pydantic import BaseModel, EmailStr, Field
from pydantic_settings import BaseSettings
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text, select, func
import bcrypt, jwt
from fastapi.responses import JSONResponse
import aiosmtplib
from email.message import EmailMessage
from random import randint
import uuid
import paho.mqtt.publish as publish
import hashlib

from deps import get_current_user

class Settings(BaseSettings):
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql+asyncpg://admin:123456@pgbouncer:6432/energy")
    FRESH_SECS: int = int(os.getenv("FRESH_SECS", "60"))

settings = Settings()
engine = create_async_engine(settings.DATABASE_URL, pool_pre_ping=True)
app = FastAPI(title="ESS Realtime API", version="1.0.0")
FIRMWARE_DIR = os.getenv("FIRMWARE_DIR", "./firmware")
os.makedirs(FIRMWARE_DIR, exist_ok=True)

# 放到前面，供 routers 导入
async_session = async_sessionmaker(engine, expire_on_commit=False)

# ---------------- 公共模型与常量 ----------------

# RealtimeData / ListResponse 已迁移到 routers.user
# /api/v1/realtime, /api/v1/realtime/by_sn/{sn}, /api/v1/register, /api/v1/login,
# /api/v1/getinfo, /api/v1/logout 均已迁移到 routers（此处删除原定义）

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
from routers import user as user_router, admin as admin_router
app.include_router(user_router.router)
app.include_router(admin_router.router)

# 管理员/客服共用：根据设备SN获取实时数据
@app.get(
    "/api/v1/admin/realtime/by_sn/{device_sn}",
    tags=["管理员/客服 | Admin/Service"],
    summary="根据设备SN获取实时数据 | Get Realtime Data by Device SN"
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
    d["online"] = online_flag(d["updated_at"], settings.FRESH_SECS)
    return d

# ---------------- 其余未迁出接口保持不变 ----------------
from sqlalchemy import func

class HistoryDataAgg(BaseModel):
    device_id: int
    device_sn: str
    day: Optional[date] = None
    hour: Optional[datetime] = None
    month: Optional[date] = None
    charge_wh_total: Optional[int]
    discharge_wh_total: Optional[int]
    pv_wh_total: Optional[int]
    grid_wh_total: Optional[int]   # 新增
    load_wh_total: Optional[int]   # 新增：家庭/负载累计用电量

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
    description="管理员或客服可通过设备SN查询该设备的历史能耗聚合数据，支持按固定周期或指定日期（按小时）聚合。"
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
仅管理员和客服可用。返回数据库连接数、活跃连接、慢查询、缓存命中率、死锁数、慢SQL历史等多项数据库健康与性能指标。

Admin and service only. Returns database connection count, active connections, slow queries, cache hit rate, deadlocks, slow SQL history and other health/performance metrics.
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

class EmailCodeRequest(BaseModel):
    email: EmailStr

@app.post(
    "/api/v1/send_email_code_register",
    tags=["用户 | User"],
    summary="【测试用】发送注册验证码（直接返回code）| [Test] Send Register Code (Return Code)",
    description="""
开发测试用：向指定邮箱生成注册验证码，验证码5分钟内有效，直接返回验证码（生产环境请勿返回code）。

For development/testing: generate a registration code for the given email, valid for 5 minutes, and return the code directly (do NOT do this in production).
"""
)
async def send_email_code_register(data: EmailCodeRequest):
    # 检查邮箱是否已注册
    async with engine.connect() as conn:
        result = await conn.execute(
            text("SELECT 1 FROM users WHERE email=:email"),
            {"email": data.email}
        )
        if result.first():
            raise HTTPException(
                status_code=400,
                detail={"msg": "该邮箱已注册", "msg_en": "This email is already registered"}
            )
    code = f"{randint(100000, 999999)}"
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    # 写入数据库
    async with engine.begin() as conn:
        await conn.execute(
            text("""
                INSERT INTO email_codes (email, code, purpose, expires_at)
                VALUES (:email, :code, :purpose, :expires_at)
            """),
            {
                "email": data.email,
                "code": code,
                "purpose": "register",
                "expires_at": expires_at
            }
        )
    # 直接返回验证码（仅测试用）
    return {
        "msg": "验证码已生成（测试环境直接返回）",
        "msg_en": "Verification code generated (returned for testing)",
        "code": code
    }

from typing import Any

class AlarmItem(BaseModel):
    alarm_id: int = Field(..., alias="alarm_id")
    device_id: Optional[int]
    alarm_type: str
    code: int
    level: str
    extra: Optional[Any]
    status: str
    first_triggered_at: datetime
    last_triggered_at: datetime
    repeat_count: int
    remark: Optional[str]
    confirmed_at: Optional[datetime]
    confirmed_by: Optional[str]
    cleared_at: Optional[datetime]
    cleared_by: Optional[str]

    class Config:
        allow_population_by_field_name = True

class AlarmListResponse(BaseModel):
    items: List[AlarmItem]
    page: int
    page_size: int
    total: int

# 批量按 code 确认报警的请求模型
class AlarmBatchConfirmByCodeRequest(BaseModel):
    code: int = Field(..., description="报警码 | Alarm code")

# 管理员/客服可查所有报警（当前+历史，类似方式可加 device_sn、alarm_type 等筛选）
@app.get(
    "/api/v1/alarms/admin",
    response_model=AlarmListResponse,
    tags=["管理员/客服 | Admin/Service"],
    summary="报警管理查询 | Query All Alarms (Admin/Service)",
    description="管理员/客服可按设备序列号、状态、级别、code等筛选报警。"
)
async def list_all_alarms(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    status: Optional[str] = Query(None),
    device_sn: Optional[str] = Query(None),
    level: Optional[str] = Query(None),
    code: Optional[int] = Query(None),
    alarm_type: Optional[str] = Query(None),
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="无权限")
    where = []
    params = {}
    # 始终关联 devices，便于选择 d.device_sn 且避免未 join 报错
    join_sql = "LEFT JOIN devices d ON alarms.device_id = d.id"
    if device_sn:
        where.append("d.device_sn = :device_sn")
        params["device_sn"] = device_sn
    if status:
        where.append("alarms.status = :status")
        params["status"] = status
    if level:
        where.append("alarms.level = :level")
        params["level"] = level
    if code:
        where.append("alarms.code = :code")
        params["code"] = code
    if alarm_type:
        where.append("alarms.alarm_type = :alarm_type")
        params["alarm_type"] = alarm_type
    cond = "WHERE " + " AND ".join(where) if where else ""
    offset = (page - 1) * page_size

    async with engine.connect() as conn:
        count_sql = text(f"SELECT COUNT(*) FROM alarms {join_sql} {cond}")
        total = (await conn.execute(count_sql, params)).scalar_one()
        query_sql = text(f"""
            SELECT alarms.*, d.device_sn
            FROM alarms
            {join_sql}
            {cond}
            ORDER BY alarms.first_triggered_at DESC
            LIMIT :limit OFFSET :offset
        """)
        rows = (await conn.execute(query_sql, {**params, "limit": page_size, "offset": offset})).mappings().all()
        items = []
        for row in rows:
            d = dict(row)
            d["alarm_id"] = d.pop("id")
            items.append(d)
    return {"items": items, "page": page, "page_size": page_size, "total": total}

@app.post(
    "/api/v1/alarms/admin/batch_confirm",
    tags=["管理员/客服 | Admin/Service"],
    summary="按code批量确认报警 | Batch Confirm Alarms By Code",
    description="管理员/客服按报警code批量确认所有未确认的报警（只操作当前报警表，历史报警不能确认）。"
)
async def batch_confirm_alarm_by_code(
    data: AlarmBatchConfirmByCodeRequest,
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service"):
        raise HTTPException(status_code=403, detail="无权限")
    async with engine.begin() as conn:
        result = await conn.execute(
            text("""
                UPDATE alarms
                SET confirmed_at = now(), confirmed_by = :by
                WHERE code = :code
                  AND confirmed_at IS NULL
            """),
            {"code": data.code, "by": user["username"]}
        )
    return {"msg": f"已确认所有 code={data.code} 的报警", "confirmed_count": result.rowcount}

@app.get(
    "/api/v1/alarms/unhandled_count",
    tags=["管理员/客服 | Admin/Service"],
    summary="统计未处理报警数量（含各等级） | Count Unhandled Alarms (by Level)",
    description="""
仅管理员和客服可用。统计所有未处理（active/confirmed）报警的总数量及各等级数量。
"""
)
async def count_unhandled_alarms(
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="无权限")
    async with engine.connect() as conn:
        # 总数
        total_sql = text("""
            SELECT COUNT(*) FROM alarms
            WHERE status IN ('active', 'confirmed')
        """)
        total_count = (await conn.execute(total_sql)).scalar_one()
        # 按等级统计
        level_sql = text("""
            SELECT level, COUNT(*) AS count
            FROM alarms
            WHERE status IN ('active', 'confirmed')
            GROUP BY level
        """)
        rows = (await conn.execute(level_sql)).mappings().all()
        level_counts = {row["level"]: row["count"] for row in rows}
    return {
        "unhandled_alarm_count": total_count,
        "level_counts": level_counts
    }

# 设备参数查询
@app.get(
    "/api/v1/device/para",
    tags=["管理员/客服 | Admin/Service"],
    summary="查询设备参数",
)
async def get_device_para(
    device_sn: str = Query(..., description="设备序列号"),
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="无权限")
    async with engine.connect() as conn:
        device_row = (await conn.execute(
            text("SELECT id FROM devices WHERE device_sn=:sn"),
            {"sn": device_sn}
        )).mappings().first()
        if not device_row:
            raise HTTPException(status_code=404, detail="设备不存在")
        device_id = device_row["id"]
        row = (await conn.execute(
            text("SELECT device_id, para, updated_at FROM device_para WHERE device_id=:id"),
            {"id": device_id}
        )).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="设备参数不存在")
    return row

class RPCChangeRequest(BaseModel):
    device_sn: str
    para_name: str
    para_value: str
    message: Optional[str] = None

# RPC参数下发
@app.post(
    "/api/v1/device/rpc_change",
    tags=["管理员/客服 | Admin/Service"],
    summary="参数下发 | Send Device RPC Change",
)
async def rpc_change(
    req: RPCChangeRequest,
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service"):
        raise HTTPException(status_code=403, detail="无权限")
    async with engine.begin() as conn:
        device_row = (await conn.execute(
            text("SELECT id FROM devices WHERE device_sn=:sn"),
            {"sn": req.device_sn}
        )).mappings().first()
        if not device_row:
            raise HTTPException(status_code=404, detail="设备不存在")
        device_id = device_row["id"]
        # 生成唯一 request_id
        timestamp = str(int(time.time()))
        random_letters = ''.join(random.choices(string.ascii_uppercase, k=4))
        request_id = f"{timestamp}{random_letters}"
        # 写入变更日志
        await conn.execute(
            text("""
                INSERT INTO device_rpc_change_log (
                  device_id, operator, request_id, para_name, para_value, status, message
                ) VALUES (
                  :device_id, :operator, :request_id, :para_name, :para_value, 'pending', :message
                )
            """),
            {
                "device_id": device_id,
                "operator": user["username"],
                "request_id": request_id,
                "para_name": req.para_name,
                "para_value": req.para_value,
                "message": req.message or f"change {req.para_name} = {req.para_value}"
            }
        )
    # 下发MQTT RPC消息
    mqtt_topic = f"devices/{req.device_sn}/rpc"
    mqtt_payload = {
        "request_id": request_id,
        "para_name": req.para_name,
        "para_value": req.para_value,
        "operator": user["username"],
        "message": req.message or f"change {req.para_name} = {req.para_value}",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    publish.single(mqtt_topic, json.dumps(mqtt_payload), hostname=os.getenv("MQTT_HOST"), port=int(os.getenv("MQTT_PORT", "1883")))
    return {"status": "ok", "request_id": request_id, "message": req.message}

class RPCChangeLog(BaseModel):
    id: int
    device_id: int
    device_sn: Optional[str] = None
    operator: str
    request_id: str
    para_name: str
    para_value: str
    status: str  # pending/success/failed/error/timeout
    message: Optional[str] = None
    created_at: datetime
    confirmed_at: Optional[datetime] = None

class RPCLogListResponse(BaseModel):
    items: List[RPCChangeLog]
    page: int
    page_size: int
    total: int

# RPC变更历史
@app.get(
    "/api/v1/device/rpc_history",
    response_model=RPCLogListResponse,
    tags=["管理员/客服 | Admin/Service"],
    summary="查询RPC变更历史",
)
async def get_rpc_history(
    device_sn: Optional[str] = Query(None, description="设备序列号"),
    status: Optional[str] = Query(None, description="状态: pending/success/failed/error/timeout"),
    operator: Optional[str] = Query(None, description="操作人用户名"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="无权限")
    where = []
    params = {}
    if device_sn:
        where.append("d.device_sn = :device_sn")
        params["device_sn"] = device_sn
    if status:
        where.append("r.status = :status")
        params["status"] = status
    if operator:
        where.append("r.operator = :operator")
        params["operator"] = operator
    cond = "WHERE " + " AND ".join(where) if where else ""
    offset = (page - 1) * page_size
    async with engine.connect() as conn:
        count_sql = text(f"""
            SELECT COUNT(*) 
            FROM device_rpc_change_log r
            JOIN devices d ON r.device_id = d.id
            {cond}
        """)
        total = (await conn.execute(count_sql, params)).scalar_one()
        query_sql = text(f"""
            SELECT r.*, d.device_sn 
            FROM device_rpc_change_log r
            JOIN devices d ON r.device_id = d.id
            {cond}
            ORDER BY r.created_at DESC
            LIMIT :limit OFFSET :offset
        """)
        rows = (await conn.execute(query_sql, {**params, "limit": page_size, "offset": offset})).mappings().all()
    items = [dict(row) for row in rows]
    return {"items": items, "page": page, "page_size": page_size, "total": total}
    
    
async def cleanup_alarm_history():
    """每周一凌晨2点分批清理1年前的历史报警记录，每批最多500条，并写入操作日志"""
    while True:
        now = datetime.now()
        # 计算下一个周一凌晨2点
        days_ahead = (7 - now.weekday()) % 7  # 0=周一
        if days_ahead == 0 and now.hour >= 2:
            days_ahead = 7
        next_run = (now + timedelta(days=days_ahead)).replace(hour=2, minute=0, second=0, microsecond=0)
        sleep_seconds = (next_run - now).total_seconds()
        await asyncio.sleep(sleep_seconds)
        try:
            async with engine.begin() as conn:
                total_deleted = 0
                while True:
                    result = await conn.execute(
                        text("""
                            DELETE FROM alarm_history
                            WHERE id IN (
                                SELECT id FROM alarm_history
                                WHERE first_triggered_at < now() - INTERVAL '1 year'
                                ORDER BY id
                                LIMIT 500
                            )
                        """)
                    )
                    deleted = result.rowcount
                    total_deleted += deleted
                    if deleted < 500:
                        break
                if total_deleted > 0:
                    await conn.execute(
                        text("""
                            INSERT INTO admin_audit_log (operator, action, params, result)
                            VALUES (:operator, :action, :params, :result)
                        """),
                        {
                            "operator": "system_task",
                            "action": "cleanup_alarm_history",
                            "params": json.dumps({}),
                            "result": json.dumps({"deleted_count": total_deleted})
                        }
                    )
                    print(f"清理了 {total_deleted} 条1年前的历史报警记录")
        except Exception as e:
            print(f"清理历史报警失败: {e}")



async def cleanup_rpc_logs():
    """每周一凌晨2点分批清理1年前的RPC日志，每批最多500条，并写入操作日志"""
    while True:
        now = datetime.now()
        days_ahead = (7 - now.weekday()) % 7
        if days_ahead == 0 and now.hour >= 2:
            days_ahead = 7
        next_run = (now + timedelta(days=days_ahead)).replace(hour=2, minute=0, second=0, microsecond=0)
        sleep_seconds = (next_run - now).total_seconds()
        await asyncio.sleep(sleep_seconds)
        try:
            async with engine.begin() as conn:
                total_deleted = 0
                while True:
                    result = await conn.execute(
                        text("""
                            DELETE FROM device_rpc_change_log
                            WHERE id IN (
                                SELECT id FROM device_rpc_change_log
                                WHERE created_at < now() - INTERVAL '1 year'
                                ORDER BY id
                                LIMIT 500
                            )
                        """)
                    )
                    deleted = result.rowcount
                    total_deleted += deleted
                    if deleted < 500:
                        break
                if total_deleted > 0:
                    await conn.execute(
                        text("""
                            INSERT INTO admin_audit_log (operator, action, params, result)
                            VALUES (:operator, :action, :params, :result)
                        """),
                        {
                            "operator": "system_task",
                            "action": "cleanup_rpc_logs",
                            "params": json.dumps({}),
                            "result": json.dumps({"deleted_count": total_deleted})
                        }
                    )
                    print(f"清理了 {total_deleted} 条1年前的RPC历史记录")
        except Exception as e:
            print(f"清理RPC日志失败: {e}")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(cleanup_rpc_logs())
    asyncio.create_task(cleanup_alarm_history())


class AlarmConfirmBySNAndCodeRequest(BaseModel):
    device_sn: str
    code: int

@app.post(
    "/api/v1/alarms/admin/confirm",
    tags=["管理员/客服 | Admin/Service"],
    summary="按设备SN和code确认报警 | Confirm Alarms By Device SN and Code",
    description="管理员/客服按设备SN和code确认所有未确认的报警（只操作当前报警表，历史报警不能确认）。"
)
async def confirm_alarm_by_sn_and_code(
    data: AlarmConfirmBySNAndCodeRequest,
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service"):
        raise HTTPException(status_code=403, detail="无权限")
    async with engine.begin() as conn:
        # 获取设备ID
        device_row = (await conn.execute(
            text("SELECT id FROM devices WHERE device_sn=:sn"),
            {"sn": data.device_sn}
        )).mappings().first()
        if not device_row:
            raise HTTPException(status_code=404, detail="设备不存在")
        device_id = device_row["id"]

        # 更新 confirmed_at 和 confirmed_by
        result = await conn.execute(
            text("""
                UPDATE alarms
                SET confirmed_at = now(), confirmed_by = :by
                WHERE device_id = :device_id AND code = :code
                AND confirmed_at IS NULL
            """),
            {"device_id": device_id, "code": data.code, "by": user["username"]}
        )

        # 查询 critical 且 cleared 的报警
        critical_cleared_alarms = await conn.execute(
            text("""
                SELECT *
                FROM alarms
                WHERE device_id = :device_id AND code = :code
                AND level = 'critical' AND status = 'cleared'
            """),
            {"device_id": device_id, "code": data.code}
        )
        rows = critical_cleared_alarms.mappings().all()

        # 将符合条件的报警归档到 alarm_history
        for row in rows:
            cleared_at = row["cleared_at"]
            first_triggered_at = row["first_triggered_at"]
            last_triggered_at = row["last_triggered_at"]

            # 确保 duration 至少为 1 秒
            if last_triggered_at and first_triggered_at:
                duration = max(1, int((last_triggered_at - first_triggered_at).total_seconds()))
            else:
                duration = None

            # 将 extra 转换为 JSON 字符串
            extra = json.dumps(row["extra"]) if isinstance(row["extra"], dict) else row["extra"]

            # 将 duration 转换为 timedelta
            duration = timedelta(seconds=duration) if duration else None

            await conn.execute(
                text("""
                    INSERT INTO alarm_history (
                        device_id, alarm_type, code, level, extra, status,
                        first_triggered_at, last_triggered_at, repeat_count, remark,
                        confirmed_at, confirmed_by, cleared_at, cleared_by, archived_at, duration
                    ) VALUES (
                        :device_id, :alarm_type, :code, :level, :extra, :status,
                        :first_triggered_at, :last_triggered_at, :repeat_count, :remark,
                        :confirmed_at, :confirmed_by, :cleared_at, :cleared_by, now(), :duration
                    )
                """),
                {
                    "device_id": row["device_id"],
                    "alarm_type": row["alarm_type"],
                    "code": row["code"],
                    "level": row["level"],
                    "extra": extra,
                    "status": row["status"],
                    "first_triggered_at": row["first_triggered_at"],
                    "last_triggered_at": row["last_triggered_at"],
                    "repeat_count": row["repeat_count"],
                    "remark": row["remark"],
                    "confirmed_at": row["confirmed_at"],
                    "confirmed_by": row["confirmed_by"],
                    "cleared_at": row["cleared_at"],
                    "cleared_by": row["cleared_by"],
                    "duration": duration,  # 传递 timedelta 对象
                }
            )

            # 删除已归档的报警
            await conn.execute(
                text("""
                    DELETE FROM alarms
                    WHERE id = :id
                """),
                {"id": row["id"]}
            )

    return {"msg": f"已确认设备 {data.device_sn} code={data.code} 的所有报警", "confirmed_count": result.rowcount}


# 将统计接口从函数体内移到顶层
@app.get(
    "/api/v1/devices/online_summary",
    tags=["管理员/客服 | Admin/Service"],
    summary="设备在线统计 | Device Online Summary",
    description="返回设备总数、在线数、离线数；按最近60秒内有上报判定为在线。"
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

def _parse_semver(v: str) -> tuple[int, int, int]:
    try:
        a, b, c = (int(x) for x in (v.split(".") + ["0", "0", "0"])[:3])
        return a, b, c
    except Exception:
        return (0, 0, 0)

@app.post(
    "/api/v1/firmware/upload",
    tags=["管理员/客服 | Admin/Service"],
    summary="上传固件 | Upload Firmware",
    description="仅 admin/service。写入共享目录，由 Nginx 静态下载（/ota/{device_type}-{version}.bin）。"
)
async def upload_firmware(
    device_type: str = Query(..., description="设备类型，如 esp32"),
    version: str = Query(..., description="版本号，如 1.2.3"),
    notes: Optional[str] = Query(None, description="发布备注"),
    file: UploadFile = File(...),
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service"):
        raise HTTPException(status_code=403, detail="无权限")
    if not file.filename.lower().endswith(".bin"):
        raise HTTPException(status_code=400, detail="只能上传 .bin")

    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="文件为空")
    md5 = hashlib.md5(data).hexdigest()
    safe_name = f"{device_type}-{version}.bin"
    path = os.path.join(FIRMWARE_DIR, safe_name)

    # 保存二进制与 .md5
    with open(path, "wb") as f:
        f.write(data)
    with open(path + ".md5", "w") as f:
        f.write(md5)

    # 写入元数据表（firmware_files）
    async with engine.begin() as conn:
        await conn.execute(
            text("""
                INSERT INTO firmware_files (device_type, version, filename, file_size, md5, notes, uploaded_by)
                VALUES (:device_type, :version, :filename, :file_size, :md5, :notes, :uploaded_by)
                ON CONFLICT (device_type, version) DO UPDATE
                  SET filename=EXCLUDED.filename,
                      file_size=EXCLUDED.file_size,
                      md5=EXCLUDED.md5,
                      notes=EXCLUDED.notes,
                      uploaded_by=EXCLUDED.uploaded_by,
                      uploaded_at=now()
            """),
            {
                "device_type": device_type,
                "version": version,
                "filename": safe_name,
                "file_size": len(data),
                "md5": md5,
                "notes": notes,
                "uploaded_by": user["username"],
            },
        )

    return {
        "status": "ok",
        "device_type": device_type,
        "version": version,
        "filename": safe_name,
        "size": len(data),
        "md5": md5,
        "download_url": f"/ota/{safe_name}",
        "notes": notes,
    }

@app.get(
    "/api/v1/firmware/latest",
    tags=["管理员/客服 | Admin/Service"],
    summary="获取最新固件 | Get Latest Firmware",
    description="返回最新版本的元数据与下载地址（只读：admin/service/support）。"
)
async def get_latest_firmware(
    device_type: str = Query(..., description="设备类型，如 esp32"),
    current: Optional[str] = Query(None, description="设备当前版本（可选，用于比较是否需升级）"),
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="无权限")
    async with engine.connect() as conn:
        row = (await conn.execute(
            text("""
                SELECT device_type, version, filename, file_size, md5, notes, uploaded_at
                FROM firmware_files
                WHERE device_type=:device_type
                ORDER BY
                  COALESCE(NULLIF(split_part(version,'.',1),''),'0')::int DESC,
                  COALESCE(NULLIF(split_part(version,'.',2),''),'0')::int DESC,
                  COALESCE(NULLIF(split_part(version,'.',3),''),'0')::int DESC,
                  uploaded_at DESC
                LIMIT 1
            """),
            {"device_type": device_type},
        )).mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="未找到固件")

    latest_ver = row["version"]
    has_update = (current is not None) and (_parse_semver(latest_ver) > _parse_semver(current))
    return {
        "device_type": row["device_type"],
        "latest_version": latest_ver,
        "has_update": has_update,
        "download_url": f"/ota/{row['filename']}",
        "md5": row["md5"],
        "size": row["file_size"],
        "notes": row["notes"],
        "uploaded_at": row["uploaded_at"],
    }

@app.get(
    "/api/v1/firmware/list",
    tags=["管理员/客服 | Admin/Service"],
    summary="列出固件版本 | List Firmware Versions",
    description="分页列出指定 device_type 的历史版本（只读：admin/service/support）。"
)
async def list_firmware(
    device_type: str = Query(...),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="无权限")
    offset = (page - 1) * page_size
    async with engine.connect() as conn:
        count_sql = text(f"""
            SELECT COUNT(*) FROM firmware_files
            WHERE device_type = :device_type
        """)
        total = (await conn.execute(count_sql, {"device_type": device_type})).scalar_one()
        query_sql = text(f"""
            SELECT device_type, version, filename, file_size, md5, notes, uploaded_at
            FROM firmware_files
            WHERE device_type = :device_type
            ORDER BY uploaded_at DESC
            LIMIT :limit OFFSET :offset
        """)
        rows = (await conn.execute(query_sql, {"device_type": device_type, "limit": page_size, "offset": offset})).mappings().all()
    items = [dict(row) for row in rows]
    return {"items": items, "page": page, "page_size": page_size, "total": total}
