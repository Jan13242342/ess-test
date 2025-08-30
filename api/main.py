import os
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(".env"), override=True)

from fastapi import FastAPI, Query, HTTPException, Depends, Body
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr
from pydantic_settings import BaseSettings
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text, select
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

@app.get("/healthz")
async def healthz():
    return {"ok": True}

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

@app.get("/api/v1/realtime/{device_id}", response_model=RealtimeData)
async def get_device_realtime(
    device_id: int,
    fresh_secs: Optional[int] = None,
    user=Depends(get_current_user)
):
    fresh = fresh_secs or settings.FRESH_SECS
    sql = text(f"""
        SELECT {COLUMNS}
        FROM ess_realtime_data r
        JOIN devices d ON r.device_id = d.id
        WHERE r.device_id=:device_id
    """)
    async with engine.connect() as conn:
        row = (await conn.execute(sql, {"device_id": device_id})).mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="device not found")
        d = dict(row)
        d["online"] = online_flag(d["updated_at"], fresh)
        return d

@app.get("/api/v1/realtime", response_model=ListResponse)
async def list_realtime(
    dealer_id: Optional[int] = None,
    only_my: bool = Query(False, description="只查当前用户绑定的设备"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    fresh_secs: Optional[int] = None,
    user=Depends(get_current_user)
):
    fresh = fresh_secs or settings.FRESH_SECS
    where = []
    params = {}
    join_sql = "JOIN devices d ON r.device_id = d.id"
    if dealer_id:
        where.append("d.dealer_id = :dealer_id")
        params["dealer_id"] = dealer_id
    if only_my:
        where.append("d.user_id = :user_id")
        params["user_id"] = user["user_id"]
    cond = "WHERE " + " AND ".join(where) if where else ""

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

@app.get("/api/v1/realtime/batch", response_model=ListResponse)
async def batch_realtime(
    device_ids: str,
    fresh_secs: Optional[int] = None,
    user=Depends(get_current_user)
):
    fresh = fresh_secs or settings.FRESH_SECS
    ids = [x.strip() for x in device_ids.split(",") if x.strip()]
    if not ids:
        return {"items": [], "page": 1, "page_size": 0, "total": 0}

    placeholders = ",".join([f":id{i}" for i in range(len(ids))])
    params = {f"id{i}": v for i, v in enumerate(ids)}
    sql = text(f"""
        SELECT {COLUMNS}
        FROM ess_realtime_data r
        JOIN devices d ON r.device_id = d.id
        WHERE r.device_id IN ({placeholders})
    """)
    async with engine.connect() as conn:
        rows = (await conn.execute(sql, params)).mappings().all()

    items = []
    for r in rows:
        d = dict(r)
        d["online"] = online_flag(d["updated_at"], fresh)
        items.append(d)
    return {"items": items, "page": 1, "page_size": len(items), "total": len(items)}

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

@app.post("/api/v1/register")
async def register(user: UserRegister):
    async with async_session() as session:
        # 检查用户名或邮箱是否已存在
        result = await session.execute(
            text("SELECT 1 FROM users WHERE username=:u OR email=:e"),
            {"u": user.username, "e": user.email}
        )
        if result.first():
            raise HTTPException(status_code=400, detail="用户名或邮箱已存在")
        # 密码加密
        pw_hash = bcrypt.hashpw(user.password.encode(), bcrypt.gensalt()).decode()
        await session.execute(
            text("INSERT INTO users (username, email, password_hash) VALUES (:u, :e, :p)"),
            {"u": user.username, "e": user.email, "p": pw_hash}
        )
        await session.commit()
    return {"msg": "注册成功"}

@app.post("/api/v1/login")
async def login(user: UserLogin):
    async with async_session() as session:
        result = await session.execute(
            text("SELECT id, username, role, password_hash FROM users WHERE username=:u"),
            {"u": user.username}
        )
        row = result.first()
        if not row or not bcrypt.checkpw(user.password.encode(), row.password_hash.encode()):
            raise HTTPException(status_code=401, detail="用户名或密码错误")
        # 生成JWT token
        payload = {
            "user_id": row.id,
            "username": row.username,
            "role": row.role,
            "exp": datetime.utcnow() + timedelta(hours=1)
        }
        token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return {"msg": "登录成功", "token": token}

@app.post("/api/v1/device/bind")
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
            raise HTTPException(status_code=404, detail="用户不存在")
        user_id = user_row.id

        result = await conn.execute(
            text("SELECT id, user_id FROM devices WHERE device_sn=:sn"),
            {"sn": device_sn}
        )
        device = result.first()
        if not device:
            raise HTTPException(status_code=404, detail="设备不存在")
        if device.user_id == user_id:
            return {"msg": "设备已绑定到该用户", "device_sn": device_sn, "username": username}
        await conn.execute(
            text("UPDATE devices SET user_id=:user_id WHERE device_sn=:sn"),
            {"user_id": user_id, "sn": device_sn}
        )
    return {"msg": "绑定成功", "device_sn": device_sn, "username": username}

@app.post("/api/v1/device/unbind")
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
            raise HTTPException(status_code=404, detail="用户不存在")
        user_id = user_row.id

        result = await conn.execute(
            text("SELECT id, user_id FROM devices WHERE device_sn=:sn"),
            {"sn": device_sn}
        )
        device = result.first()
        if not device:
            raise HTTPException(status_code=404, detail="设备不存在")
        if device.user_id != user_id:
            raise HTTPException(status_code=403, detail="设备未绑定到该用户，无法解绑")
        await conn.execute(
            text("UPDATE devices SET user_id=NULL WHERE device_sn=:sn"),
            {"sn": device_sn}
        )
    return {"msg": "解绑成功", "device_sn": device_sn, "username": username}

@app.get("/api/v1/realtime/by_sn/{device_sn}", response_model=RealtimeData)
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
