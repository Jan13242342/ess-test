import os
from datetime import datetime, timezone, timedelta, date
from typing import List, Optional, Any
import decimal
import time
import random
import string
import asyncio
import json


from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(".env"), override=True)

from fastapi import FastAPI, Query, HTTPException, Depends, Body
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr, Field
from pydantic_settings import BaseSettings
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text, select, func
import bcrypt
import jwt
from fastapi.responses import JSONResponse
import aiosmtplib
from email.message import EmailMessage
from random import randint
import uuid
import paho.mqtt.publish as publish



class Settings(BaseSettings):
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql+asyncpg://admin:123456@pgbouncer:6432/energy")
    FRESH_SECS: int = int(os.getenv("FRESH_SECS", "60"))

settings = Settings()
engine = create_async_engine(settings.DATABASE_URL, pool_pre_ping=True)
app = FastAPI(title="ESS Realtime API", version="1.0.0")

# 修改模型，增加 device_sn
class RealtimeData(BaseModel):
    device_id: int
    device_sn: str
    updated_at: datetime
    soc: int
    soh: int
    pv: int
    load: int
    grid: int
    grid_q: int
    batt: int
    ac_v: int
    ac_f: int
    v_a: int
    v_b: int
    v_c: int
    i_a: int
    i_b: int
    i_c: int
    p_a: int
    p_b: int
    p_c: int
    q_a: int
    q_b: int
    q_c: int
    e_pv_today: int
    e_load_today: int
    e_charge_today: int
    e_discharge_today: int
    online: bool

class ListResponse(BaseModel):
    items: List[RealtimeData]
    page: int
    page_size: int
    total: int

# 修改 COLUMNS，join devices 表时取 device_sn
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

bearer_scheme = HTTPBearer()

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)):
    token = credentials.credentials
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload  # payload 里有 user_id、username、role
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token已过期")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="无效的Token")

# ================= 用户专用接口 / User APIs =================

@app.get(
    "/api/v1/realtime",
    response_model=ListResponse,
    tags=["用户 | User"],
    summary="获取用户实时数据 | Get User Realtime Data",
    description="""
获取当前用户所有设备的最新实时数据，支持分页。仅普通用户可用。

Get the latest realtime data for all devices of the current user, supports pagination. Only available for normal users.
"""
)
async def list_realtime(
    page: int = Query(1, ge=1, description="页码 | Page number"),
    page_size: int = Query(20, ge=1, le=200, description="每页数量 | Page size"),
    fresh_secs: Optional[int] = Query(None, description="判定在线的秒数，默认60秒 | Seconds to judge online, default 60s"),
    user=Depends(get_current_user)
):
    # 只允许普通用户访问，管理员和客服不用这个接口
    if user["role"] in ("admin", "service"):
        raise HTTPException(status_code=403, detail="管理员和客服请使用专用接口")
    fresh = fresh_secs or settings.FRESH_SECS
    where = ["d.user_id = :user_id"]
    params = {"user_id": user["user_id"]}
    join_sql = "JOIN devices d ON r.device_id = d.id"
    cond = "WHERE " + " AND ".join(where)

    count_sql = text(f"SELECT COUNT(*) FROM ess_realtime_data r {join_sql} {cond}")
    query_sql = text(f"""
        SELECT {COLUMNS}
        FROM ess_realtime_data r
        {join_sql}
        {cond}
        ORDER BY r.updated_at DESC
        LIMIT :limit OFFSET :offset
    """)

    offset = (page - 1) * page_size
    async with engine.connect() as conn:
        total = (await conn.execute(count_sql, params)).scalar_one()
        rows = (await conn.execute(query_sql, {**params, "limit": page_size, "offset": offset})).mappings().all()

    items = []
    for r in rows:
        d = dict(r)
        d["online"] = online_flag(d["updated_at"], fresh)
        items.append(d)
    return {"items": items, "page": page, "page_size": page_size, "total": total}

# ================= 管理员/客服专用接口 / Admin & Service APIs =================

@app.get(
    "/api/v1/realtime/by_sn/{device_sn}",
    response_model=RealtimeData,
    tags=["管理员/客服 | Admin/Service"],
    summary="根据设备SN获取实时数据 | Get Realtime Data by Device SN",
    description="""
管理员或客服可通过设备SN查询该设备的最新实时数据。

Admin or service staff can query the latest realtime data of a device by its SN.
"""
)
async def get_realtime_by_sn(
    device_sn: str,
    user=Depends(get_current_user)
):
    # 只允许管理员和客服访问
    if user["role"] not in ("admin", "service"):
        raise HTTPException(status_code=403, detail="无权限")
    # 查找设备ID和实时数据
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

# 其它接口如注册、登录、设备绑定等可以加 tags=["用户 | User"]，如需更多管理员/客服接口也可加 tags=["管理员/客服 | Admin/Service"]

async_session = async_sessionmaker(engine, expire_on_commit=False)

class UserRegister(BaseModel):
    username: str
    email: EmailStr
    password: str
    code: str  # 新增验证码字段

class UserLogin(BaseModel):
    username: str
    password: str

JWT_SECRET = os.getenv("JWT_SECRET", "your_jwt_secret_key")
JWT_ALGORITHM = "HS256"

@app.post(
    "/api/v1/register",
    tags=["用户 | User"],
    summary="用户注册 | User Register",
    description="""
注册新用户，需提供用户名、邮箱、密码和验证码。

Register a new user. Username, email, password and verification code are required.
"""
)
async def register(user: UserRegister):
    async with async_session() as session:
        # 校验验证码
        result = await session.execute(
            text("""
                SELECT id FROM email_codes
                WHERE email=:e AND code=:c AND purpose='register'
                  AND expires_at > now() AND used=FALSE
                ORDER BY expires_at DESC LIMIT 1
            """),
            {"e": user.email, "c": user.code}
        )
        code_row = result.first()
        if not code_row:
            raise HTTPException(status_code=400, detail={"msg": "验证码错误或已过期", "msg_en": "Verification code error or expired"})
        # 标记验证码已用
        await session.execute(
            text("UPDATE email_codes SET used=TRUE WHERE id=:id"),
            {"id": code_row.id}
        )
        # 检查用户名或邮箱是否已存在
        result = await session.execute(
            text("SELECT 1 FROM users WHERE username=:u OR email=:e"),
            {"u": user.username, "e": user.email}
        )
        if result.first():
            raise HTTPException(
                status_code=400,
                detail={"msg": "用户名或邮箱已存在", "msg_en": "Username or email already exists"}
            )
        # 密码加密
        pw_hash = bcrypt.hashpw(user.password.encode(), bcrypt.gensalt()).decode()
        await session.execute(
            text("INSERT INTO users (username, email, password_hash) VALUES (:u, :e, :p)"),
            {"u": user.username, "e": user.email, "p": pw_hash}
        )
        await session.commit()
    return {"msg": "注册成功", "msg_en": "Register success"}

@app.post(
    "/api/v1/login",
    tags=["用户 | User"],
    summary="用户登录 | User Login",
    description="""
用户登录，成功后返回JWT令牌。

User login, returns JWT token if successful.
"""
)
async def login(user: UserLogin):
    async with async_session() as session:
        result = await session.execute(
            text("SELECT id, username, role, password_hash FROM users WHERE username=:u"),
            {"u": user.username}
        )
        row = result.first()
        if not row or not bcrypt.checkpw(user.password.encode(), row.password_hash.encode()):
            raise HTTPException(
                status_code=401,
                detail={"msg": "用户名或密码错误", "msg_en": "Invalid username or password"}
            )
        # 生成JWT token
        payload = {
            "user_id": row.id,
            "username": row.username,
            "role": row.role,
            "exp": datetime.utcnow() + timedelta(hours=1)
        }
        token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return {"msg": "登录成功", "msg_en": "Login success", "token": token}

@app.post(
    "/api/v1/device/bind",
    tags=["用户 | User"],
    summary="绑定设备 | Bind Device",
    description="""
将设备SN绑定到指定用户名下。需登录后操作。

Bind a device SN to the specified username. Requires login.
"""
)
async def bind_device(
    device_sn: str = Body(..., embed=True, description="设备SN | Device SN"),
    username: str = Body(..., embed=True, description="用户名 | Username"),
    user=Depends(get_current_user)
):
    async with engine.begin() as conn:
        result = await conn.execute(
            text("SELECT id FROM users WHERE username=:username"),
            {"username": username}
        )
        user_row = result.first()
        if not user_row:
            raise HTTPException(
                status_code=404,
                detail={"msg": "用户不存在", "msg_en": "User not found"}
            )
        user_id = user_row.id

        result = await conn.execute(
            text("SELECT id, user_id FROM devices WHERE device_sn=:sn"),
            {"sn": device_sn}
        )
        device = result.first()
        if not device:
            raise HTTPException(
                status_code=404,
                detail={"msg": "设备不存在", "msg_en": "Device not found"}
            )
        # 新增判断：如果设备已绑定其他用户，禁止绑定
        if device.user_id and device.user_id != user_id:
            raise HTTPException(
                status_code=403,
                detail={"msg": "设备已绑定其他用户，请先解绑", "msg_en": "Device is already bound to another user, please unbind first"}
            )
        if device.user_id == user_id:
            return {
                "msg": "设备已绑定到该用户",
                "msg_en": "Device already bound to this user",
                "device_sn": device_sn,
                "username": username
            }
        await conn.execute(
            text("UPDATE devices SET user_id=:user_id WHERE device_sn=:sn"),
            {"user_id": user_id, "sn": device_sn}
        )
    return {
        "msg": "绑定成功",
        "msg_en": "Bind success",
        "device_sn": device_sn,
        "username": username
    }

@app.post(
    "/api/v1/device/unbind",
    tags=["用户 | User"],
    summary="解绑设备 | Unbind Device",
    description="""
将设备SN从指定用户名下解绑。需登录后操作。

Unbind a device SN from the specified username. Requires login.
"""
)
async def unbind_device(
    device_sn: str = Body(..., embed=True, description="设备SN | Device SN"),
    username: str = Body(..., embed=True, description="用户名 | Username"),
    user=Depends(get_current_user)
):
    async with engine.begin() as conn:
        result = await conn.execute(
            text("SELECT id FROM users WHERE username=:username"),
            {"username": username}
        )
        user_row = result.first()
        if not user_row:
            raise HTTPException(
                status_code=404,
                detail={"msg": "用户不存在", "msg_en": "User not found"}
            )
        user_id = user_row.id

        result = await conn.execute(
            text("SELECT id, user_id FROM devices WHERE device_sn=:sn"),
            {"sn": device_sn}
        )
        device = result.first()
        if not device:
            raise HTTPException(
                status_code=404,
                detail={"msg": "设备不存在", "msg_en": "Device not found"}
            )
        if device.user_id != user_id:
            raise HTTPException(
                status_code=403,
                detail={"msg": "设备未绑定到该用户，无法解绑", "msg_en": "Device is not bound to this user, cannot unbind"}
            )
        await conn.execute(
            text("UPDATE devices SET user_id=NULL WHERE device_sn=:sn"),
            {"sn": device_sn}
        )
    return {
        "msg": "解绑成功",
        "msg_en": "Unbind success",
        "device_sn": device_sn,
        "username": username
    }

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

class HistoryAggListResponse(BaseModel):
    items: List[HistoryDataAgg]
    page: int
    page_size: int
    total: int

@app.get(
    "/api/v1/history",
    response_model=HistoryAggListResponse,
    tags=["用户 | User"],
    summary="历史能耗聚合数据 | Aggregated History Energy Data",
    description="""
获取当前用户设备的历史能耗聚合数据，支持按小时、天、月聚合，支持时间范围筛选和分页。

Get aggregated historical energy data for user's devices, supports aggregation by hour, day, or month, with time range filter and pagination.
"""
)
async def list_history(
    start: Optional[datetime] = Query(None, description="开始时间（ISO8601，默认当天0点）| Start time (ISO8601, default today 00:00)"),
    end: Optional[datetime] = Query(None, description="结束时间（ISO8601，默认当天23:59:59）| End time (ISO8601, default today 23:59:59)"),
    page: int = Query(1, ge=1, description="页码 | Page number"),
    page_size: int = Query(20, ge=1, le=200, description="每页数量 | Page size"),
    user=Depends(get_current_user)
):
    if user["role"] in ("admin", "service"):
        raise HTTPException(status_code=403, detail="管理员和客服请使用专用接口")

    now = datetime.now(timezone.utc)
    # 判断聚合粒度
    if not start and not end:
        # 默认查当天每小时
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
        group_expr = "date_trunc('hour', ts)"
        group_label = "hour"
    else:
        if not start:
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if not end:
            end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
        # 判断是否跨2个月
        months = (end.year - start.year) * 12 + (end.month - start.month)
        if months >= 2:
            group_expr = "date_trunc('month', ts)"
            group_label = "month"
        else:
            group_expr = "date_trunc('day', ts)"
            group_label = "day"

    async with engine.connect() as conn:
        devices = (await conn.execute(
            text("SELECT id, device_sn FROM devices WHERE user_id=:uid"),
            {"uid": user["user_id"]}
        )).mappings().all()
        if not devices:
            return {"items": [], "page": page, "page_size": page_size, "total": 0}

        device_ids = [d["id"] for d in devices]
        device_sn_map = {d["id"]: d["device_sn"] for d in devices}
        placeholders = ",".join([f":id{i}" for i in range(len(device_ids))])
        params = {f"id{i}": did for i, did in enumerate(device_ids)}
        params.update({"start": start, "end": end})

        where = [f"device_id IN ({placeholders})", "ts >= :start", "ts <= :end"]
        cond = "WHERE " + " AND ".join(where) if where else ""
        offset = (page - 1) * page_size

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
                SUM(grid_wh_total) AS grid_wh_total      -- 新增
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
    "/api/v1/history/admin/by_sn",
    response_model=HistoryAggListResponse,
    tags=["管理员/客服 | Admin/Service"],
    summary="管理员按设备SN查询历史能耗聚合数据",
    description="管理员或客服可通过设备SN查询该设备的历史能耗聚合数据，支持时间范围和聚合粒度。"
)
async def admin_history_by_sn(
    device_sn: str = Query(..., description="设备序列号"),
    start: Optional[datetime] = Query(None, description="开始时间"),
    end: Optional[datetime] = Query(None, description="结束时间"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=200, description="每页数量"),
    admin=Depends(get_current_user)
):
    if admin["role"] not in ("admin", "service"):
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
    if not start and not end:
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
        group_expr = "date_trunc('hour', ts)"
        group_label = "hour"
    else:
        if not start:
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if not end:
            end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
        months = (end.year - start.year) * 12 + (end.month - start.month)
        if months >= 2:
            group_expr = "date_trunc('month', ts)"
            group_label = "month"
        else:
            group_expr = "date_trunc('day', ts)"
            group_label = "day"
    params = {"id0": device_id, "start": start, "end": end}
    where = ["device_id = :id0", "ts >= :start", "ts <= :end"]
    cond = "WHERE " + " AND ".join(where)
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
                SUM(grid_wh_total) AS grid_wh_total
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

@app.get(
    "/api/v1/getinfo",
    tags=["用户 | User"],
    summary="获取当前用户信息 | Get Current User Info",
    description="""
返回当前登录用户的基本信息，包括用户名、邮箱、角色（权限）等。

Return basic info of the current logged-in user, including username, email, role (permission), etc.
"""
)
async def get_info(user=Depends(get_current_user)):
    async with engine.connect() as conn:
        result = await conn.execute(
            text("SELECT username, email, role FROM users WHERE id=:uid"),
            {"uid": user["user_id"]}
        )
        row = result.first()
        if not row:
            raise HTTPException(status_code=404, detail="用户不存在 | User not found")
        info = row._mapping
        return {
            "username": info["username"],
            "email": info["email"],
            "role": info["role"]
        }

@app.post(
    "/api/v1/logout",
    tags=["用户 | User"],
    summary="用户登出 | User Logout",
    description="""
前端调用后应删除本地JWT令牌，后端不做实际操作，仅返回成功。

Frontend should delete the local JWT token after calling this API. Backend does not perform any operation, just returns success.
"""
)
async def logout():
    return {"msg": "登出成功", "msg_en": "Logout success"}

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

# 查询当前报警（alarms）
@app.get(
    "/api/v1/alarms/user",
    response_model=AlarmListResponse,
    tags=["用户 | User"],
    summary="查询本人设备报警 | Query My Device Alarms",
    description="普通用户只能查询自己设备的报警。"
)
async def list_my_alarms(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    status: Optional[str] = Query(None),
    level: Optional[str] = Query(None),
    code: Optional[str] = Query(None),
    user=Depends(get_current_user)
):
    if user["role"] in ("admin", "service"):
        raise HTTPException(status_code=403, detail="管理员/客服请用专用接口")
    async with engine.connect() as conn:
        devices = (await conn.execute(
            text("SELECT id FROM devices WHERE user_id=:uid"),
            {"uid": user["user_id"]}
        )).scalars().all()
    if not devices:
        return {"items": [], "page": page, "page_size": page_size, "total": 0}
    placeholders = ",".join([f":id{i}" for i in range(len(devices))])
    params = {f"id{i}": did for i, did in enumerate(devices)}
    where = [f"device_id IN ({placeholders})"]
    if status:
        where.append("status = :status")
        params["status"] = status
    if level:
        where.append("level = :level")
        params["level"] = level
    if code:
        where.append("code = :code")
        params["code"] = code
    cond = "WHERE " + " AND ".join(where)
    offset = (page - 1) * page_size

    async with engine.connect() as conn:
        count_sql = text(f"SELECT COUNT(*) FROM alarms {cond}")
        total = (await conn.execute(count_sql, params)).scalar_one()
        query_sql = text(f"""
            SELECT *
            FROM alarms
            {cond}
            ORDER BY first_triggered_at DESC
            LIMIT :limit OFFSET :offset
        """)
        rows = (await conn.execute(query_sql, {**params, "limit": page_size, "offset": offset})).mappings().all()
        items = []
        for row in rows:
            d = dict(row)
            d["alarm_id"] = d.pop("id")
            items.append(d)
    return {"items": items, "page": page, "page_size": page_size, "total": total}

# 查询历史报警（alarm_history）
@app.get(
    "/api/v1/alarms/history",
    response_model=AlarmListResponse,
    tags=["用户 | User"],
    summary="查询本人设备历史报警 | Query My Device Alarm History",
    description="普通用户只能查询自己设备的历史报警。"
)
async def list_my_alarm_history(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    status: Optional[str] = Query(None),
    level: Optional[str] = Query(None),
    code: Optional[str] = Query(None),
    user=Depends(get_current_user)
):
    if user["role"] in ("admin", "service"):
        raise HTTPException(status_code=403, detail="管理员/客服请用专用接口")
    async with engine.connect() as conn:
        devices = (await conn.execute(
            text("SELECT id FROM devices WHERE user_id=:uid"),
            {"uid": user["user_id"]}
        )).scalars().all()
    if not devices:
        return {"items": [], "page": page, "page_size": page_size, "total": 0}
    placeholders = ",".join([f":id{i}" for i in range(len(devices))])
    params = {f"id{i}": did for i, did in enumerate(devices)}
    where = [f"device_id IN ({placeholders})"]
    if status:
        where.append("status = :status")
        params["status"] = status
    if level:
        where.append("level = :level")
        params["level"] = level
    if code:
        where.append("code = :code")
        params["code"] = code
    cond = "WHERE " + " AND ".join(where)
    offset = (page - 1) * page_size

    async with engine.connect() as conn:
        count_sql = text(f"SELECT COUNT(*) FROM alarm_history {cond}")
        total = (await conn.execute(count_sql, params)).scalar_one()
        query_sql = text(f"""
            SELECT *
            FROM alarm_history
            {cond}
            ORDER BY first_triggered_at DESC
            LIMIT :limit OFFSET :offset
        """)
        rows = (await conn.execute(query_sql, {**params, "limit": page_size, "offset": offset})).mappings().all()
        items = []
        for row in rows:
            d = dict(row)
            d["alarm_id"] = d.pop("id")
            items.append(d)
    return {"items": items, "page": page, "page_size": page_size, "total": total}

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
    code: Optional[str] = Query(None),
    alarm_type: Optional[str] = Query(None),
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service"):
        raise HTTPException(status_code=403, detail="无权限")
    where = []
    params = {}
    join_sql = ""
    if device_sn:
        join_sql = "JOIN devices d ON alarms.device_id = d.id"
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

from pydantic import BaseModel



class AlarmBatchConfirmByCodeRequest(BaseModel):
    code: int

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
                WHERE device_id = :device_id AND code = :code
                AND confirmed_at IS NULL
            """),
            {"device_id": device_id, "code": data.code, "by": user["username"]}
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
    if user["role"] not in ("admin", "service"):
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
    if user["role"] not in ("admin", "service"):
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
    if user["role"] not in ("admin", "service"):
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
                            WHERE first_triggered_at < now() - INTERVAL '1 year'
                            LIMIT 500
                        """)
                    )
                    deleted = result.rowcount
                    total_deleted += deleted
                    if deleted < 500:
                        break
                if total_deleted > 0:
                    # 写入操作日志
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
                            DELETE FROM device_rpc_change_log
                            WHERE created_at < now() - INTERVAL '1 year'
                            LIMIT 500
                        """)
                    )
                    deleted = result.rowcount
                    total_deleted += deleted
                    if deleted < 500:
                        break
                if total_deleted > 0:
                    # 写入操作日志
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

class UserChangePasswordRequest(BaseModel):
    old_password: str = Field(..., description="当前密码 | Current password")
    new_password: str = Field(..., description="新密码 | New password")

@app.post(
    "/api/v1/user/change_password",
    tags=["用户 | User"],
    summary="修改自己的密码 | Change Own Password",
    description="""
用户可自助修改密码，需验证当前密码。

User can change their own password after verifying the current password.
"""
)
async def change_user_password(
    req: UserChangePasswordRequest,
    user=Depends(get_current_user)
):
    async with async_session() as session:
        # 校验旧密码
        result = await session.execute(
            text("SELECT password_hash FROM users WHERE id=:uid"),
            {"uid": user["user_id"]}
        )
        row = result.first()
        if not row or not bcrypt.checkpw(req.old_password.encode(), row.password_hash.encode()):
            raise HTTPException(status_code=401, detail={"msg": "当前密码错误", "msg_en": "Incorrect current password"})
        # 更新新密码
        new_hash = bcrypt.hashpw(req.new_password.encode(), bcrypt.gensalt()).decode()
        await session.execute(
            text("UPDATE users SET password_hash=:p WHERE id=:uid"),
            {"p": new_hash, "uid": user["user_id"]}
        )
        await session.commit()
    return {"msg": "密码修改成功", "msg_en": "Password changed successfully"}


class AdminBatchDeleteAlarmHistoryBySNRequest(BaseModel):
    device_sn: str

@app.delete(
    "/api/v1/admin/alarm_history/delete_by_sn",
    tags=["管理员 | Admin Only"],
    summary="管理员按设备SN批量删除历史报警",
    description="只有管理员可以按设备SN批量删除历史报警记录，客服无权限。"
)
async def admin_delete_alarm_history_by_sn(
    data: AdminBatchDeleteAlarmHistoryBySNRequest,
    user=Depends(get_current_user)
):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="只有管理员可以删除历史报警记录")
    async with engine.begin() as conn:
        device_row = (await conn.execute(
            text("SELECT id FROM devices WHERE device_sn=:sn"),
            {"sn": data.device_sn}
        )).mappings().first()
        if not device_row:
            raise HTTPException(status_code=404, detail="设备不存在")
        device_id = device_row["id"]
        result = await conn.execute(
            text("DELETE FROM alarm_history WHERE device_id=:id"),
            {"id": device_id}
        )
    return {
        "msg": f"已删除设备 {data.device_sn} 的所有历史报警记录",
        "deleted_count": result.rowcount,
        "device_sn": data.device_sn
    }


class AdminBatchDeleteRPCLogBySNRequest(BaseModel):
    device_sn: str

@app.delete(
    "/api/v1/admin/rpc_log/delete_by_sn",
    tags=["管理员 | Admin Only"],
    summary="管理员按设备SN批量删除RPC日志",
    description="只有管理员可以按设备SN批量删除RPC日志，客服无权限。"
)
async def admin_delete_rpc_log_by_sn(
    data: AdminBatchDeleteRPCLogBySNRequest,
    user=Depends(get_current_user)
):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="只有管理员可以删除RPC日志")
    async with engine.begin() as conn:
        device_row = (await conn.execute(
            text("SELECT id FROM devices WHERE device_sn=:sn"),
            {"sn": data.device_sn}
        )).mappings().first()
        if not device_row:
            raise HTTPException(status_code=404, detail="设备不存在")
        device_id = device_row["id"]
        result = await conn.execute(
            text("DELETE FROM device_rpc_change_log WHERE device_id=:id"),
            {"id": device_id}
        )
    return {
        "msg": f"已删除设备 {data.device_sn} 的所有RPC日志",
        "deleted_count": result.rowcount,
        "device_sn": data.device_sn
    }

@app.delete(
    "/api/v1/admin/alarm_history/clear_all",
    tags=["管理员 | Admin Only"],
    summary="管理员清除所有历史报警记录（分批）",
    description="只有管理员可以分批清除所有历史报警记录，客服无权限。"
)
async def admin_clear_all_alarm_history(
    user=Depends(get_current_user)
):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="只有管理员可以清除所有历史报警记录")
    batch_size = 5000
    total_deleted = 0
    async with engine.begin() as conn:
        while True:
            result = await conn.execute(
                text(f"DELETE FROM alarm_history WHERE id IN (SELECT id FROM alarm_history LIMIT {batch_size})")
            )
            deleted = result.rowcount
            total_deleted += deleted
            if deleted < batch_size:
                break
        # 写入操作日志
        await conn.execute(
            text("""
                INSERT INTO admin_audit_log (operator, action, params, result)
                VALUES (:operator, :action, :params, :result)
            """),
            {
                "operator": user["username"],
                "action": "admin_clear_all_alarm_history",
                "params": json.dumps({}),
                "result": json.dumps({"deleted_count": total_deleted})
            }
        )
    return {"msg": "所有历史报警记录已清除", "deleted_count": total_deleted}

@app.delete(
    "/api/v1/admin/rpc_log/clear_all",
    tags=["管理员 | Admin Only"],
    summary="管理员清除所有RPC日志（分批）",
    description="只有管理员可以分批清除所有RPC日志，客服无权限。"
)
async def admin_clear_all_rpc_log(
    user=Depends(get_current_user)
):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="只有管理员可以清除所有RPC日志")
    batch_size = 5000
    total_deleted = 0
    async with engine.begin() as conn:
        while True:
            result = await conn.execute(
                text(f"DELETE FROM device_rpc_change_log WHERE id IN (SELECT id FROM device_rpc_change_log LIMIT {batch_size})")
            )
            deleted = result.rowcount
            total_deleted += deleted
            if deleted < batch_size:
                break
        # 写入操作日志
        await conn.execute(
            text("""
                INSERT INTO admin_audit_log (operator, action, params, result)
                VALUES (:operator, :action, :params, :result)
            """),
            {
                "operator": user["username"],
                "action": "admin_clear_all_rpc_log",
                "params": json.dumps({}),
                "result": json.dumps({"deleted_count": total_deleted})
            }
        )
    return {"msg": "所有RPC日志已清除", "deleted_count": total_deleted}

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
                    "duration": duration,
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