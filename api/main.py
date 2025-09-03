import os
from datetime import datetime, timezone, timedelta, date
from typing import List, Optional
import decimal

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(".env"), override=True)

from fastapi import FastAPI, Query, HTTPException, Depends, Body
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr
from pydantic_settings import BaseSettings
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text, select, func
import bcrypt
import jwt
from fastapi.responses import JSONResponse

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
注册新用户，需提供用户名、邮箱和密码。

Register a new user. Username, email and password are required.
"""
)
async def register(user: UserRegister):
    async with async_session() as session:
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
        cond = "WHERE " + " AND ".join(where)

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

        offset = (page - 1) * page_size
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
