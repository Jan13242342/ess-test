import os
from datetime import datetime, timezone, timedelta
from typing import List, Optional

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(".env"), override=True)

from fastapi import FastAPI, Query, HTTPException, Depends
from pydantic import BaseModel, EmailStr
from pydantic_settings import BaseSettings
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text, select
import bcrypt

class Settings(BaseSettings):
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql+asyncpg://admin:123456@pgbouncer:6432/energy")
    FRESH_SECS: int = int(os.getenv("FRESH_SECS", "60"))

settings = Settings()
engine = create_async_engine(settings.DATABASE_URL, pool_pre_ping=True)
app = FastAPI(title="ESS Realtime API", version="1.0.0")

class RealtimeData(BaseModel):
    dealer_id: int
    device_id: int
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

COLUMNS = """
dealer_id, device_id, updated_at,
soc, soh, pv, load, grid, grid_q, batt,
ac_v, ac_f, v_a, v_b, v_c, i_a, i_b, i_c,
p_a, p_b, p_c, q_a, q_b, q_c,
e_pv_today, e_load_today, e_charge_today, e_discharge_today
"""

def online_flag(updated_at: datetime, fresh_secs: int) -> bool:
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - updated_at) <= timedelta(seconds=fresh_secs)

@app.get("/healthz")
async def healthz():
    return {"ok": True}

@app.get("/api/v1/realtime/{device_id}", response_model=RealtimeData)
async def get_device_realtime(device_id: int, fresh_secs: Optional[int] = None):
    fresh = fresh_secs or settings.FRESH_SECS
    sql = text(f"SELECT {COLUMNS} FROM ess_realtime_data WHERE device_id=:device_id")
    async with engine.connect() as conn:
        row = (await conn.execute(sql, {"device_id": device_id})).mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="device not found")
        d = dict(row)
        d["online"] = online_flag(d["updated_at"], fresh)
        return d

@app.get("/api/v1/realtime", response_model=ListResponse)
async def list_realtime(
    dealer_id: Optional[int] = None,   # 这里改为 int
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    fresh_secs: Optional[int] = None,
):
    fresh = fresh_secs or settings.FRESH_SECS
    where = []
    params = {}
    if dealer_id:
        where.append("dealer_id = :dealer_id")
        params["dealer_id"] = dealer_id
    cond = "WHERE " + " AND ".join(where) if where else ""

    count_sql = text(f"SELECT COUNT(*) FROM ess_realtime_data {cond}")
    query_sql = text(f"""
        SELECT {COLUMNS}
        FROM ess_realtime_data
        {cond}
        ORDER BY updated_at DESC
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
    device_ids: str, fresh_secs: Optional[int] = None,
):
    fresh = fresh_secs or settings.FRESH_SECS
    ids = [x.strip() for x in device_ids.split(",") if x.strip()]
    if not ids:
        return {"items": [], "page": 1, "page_size": 0, "total": 0}

    placeholders = ",".join([f":id{i}" for i in range(len(ids))])
    params = {f"id{i}": v for i, v in enumerate(ids)}
    sql = text(f"SELECT {COLUMNS} FROM ess_realtime_data WHERE device_id IN ({placeholders})")
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
            text("SELECT password_hash FROM users WHERE username=:u"),
            {"u": user.username}
        )
        row = result.first()
        if not row or not bcrypt.checkpw(user.password.encode(), row[0].encode()):
            raise HTTPException(status_code=401, detail="用户名或密码错误")
    return {"msg": "登录成功"}
