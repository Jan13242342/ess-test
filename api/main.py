import os
from datetime import datetime, timezone, timedelta, date
from typing import List, Optional

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

@app.get("/api/v1/realtime", response_model=ListResponse, tags=["用户 | User"])
async def list_realtime(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    fresh_secs: Optional[int] = None,
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

@app.get("/api/v1/realtime/by_sn/{device_sn}", response_model=RealtimeData, tags=["管理员/客服 | Admin/Service"])
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

@app.post("/api/v1/register", tags=["用户 | User"])
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

@app.post("/api/v1/login", tags=["用户 | User"])
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

@app.post("/api/v1/device/bind", tags=["用户 | User"])
async def bind_device(
    device_sn: str = Body(..., embed=True, description="设备SN"),
    username: str = Body(..., embed=True, description="用户名"),
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

@app.post("/api/v1/device/unbind", tags=["用户 | User"])
async def unbind_device(
    device_sn: str = Body(..., embed=True, description="设备SN"),
    username: str = Body(..., embed=True, description="用户名"),
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
    day: date
    charge_wh_total: Optional[int]
    discharge_wh_total: Optional[int]
    pv_wh_total: Optional[int]

class HistoryAggListResponse(BaseModel):
    items: List[HistoryDataAgg]
    page: int
    page_size: int
    total: int

@app.get("/api/v1/history", response_model=HistoryAggListResponse, tags=["用户 | User"])
async def list_history(
    start: Optional[datetime] = Query(None, description="开始时间（ISO8601，默认当天0点）"),
    end: Optional[datetime] = Query(None, description="结束时间（ISO8601，默认当天23:59:59）"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    user=Depends(get_current_user)
):
    if user["role"] in ("admin", "service"):
        raise HTTPException(status_code=403, detail="管理员和客服请使用专用接口")

    now = datetime.now(timezone.utc)
    group_by = "hour"
    if not start and not end:
        # 默认查当天每小时
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
        group_expr = "date_trunc('hour', ts)"
        group_label = "hour"
    else:
        # 有时间范围则按天聚合
        if not start:
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if not end:
            end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
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
                SUM(pv_wh_total) AS pv_wh_total
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
        # 兼容返回字段
        if group_label == "hour":
            d["hour"] = d.pop("hour")
        else:
            d["day"] = d.pop("day")
        items.append(d)
    return {"items": items, "page": page, "page_size": page_size, "total": total}
