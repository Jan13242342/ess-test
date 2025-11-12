```python
// filepath: c:\Users\orson\Desktop\New folder\ess-test\api\routers\user.py
from datetime import datetime, timezone, timedelta
from typing import Optional, List
from fastapi import APIRouter, Query, HTTPException, Depends
from pydantic import BaseModel, EmailStr
from sqlalchemy import text
from ..deps import get_current_user
from ..main import engine, async_session, settings, online_flag, COLUMNS
import bcrypt, jwt, os

JWT_SECRET = os.getenv("JWT_SECRET", "your_jwt_secret_key")
JWT_ALGORITHM = "HS256"

router = APIRouter(prefix="/api/v1", tags=["用户 | User"])

class UserRegister(BaseModel):
    username: str
    email: EmailStr
    password: str
    code: str

class UserLogin(BaseModel):
    username: str
    password: str

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

@router.post("/register", summary="用户注册")
async def register(user: UserRegister):
    async with async_session() as session:
        code_row = (await session.execute(
            text("""
                SELECT id FROM email_codes
                WHERE email=:e AND code=:c AND purpose='register'
                  AND expires_at > now() AND used=FALSE
                ORDER BY expires_at DESC LIMIT 1
            """),
            {"e": user.email, "c": user.code}
        )).first()
        if not code_row:
            raise HTTPException(status_code=400, detail="验证码错误或已过期")
        await session.execute(text("UPDATE email_codes SET used=TRUE WHERE id=:id"), {"id": code_row.id})
        exists = (await session.execute(
            text("SELECT 1 FROM users WHERE username=:u OR email=:e"),
            {"u": user.username, "e": user.email}
        )).first()
        if exists:
            raise HTTPException(status_code=400, detail="用户名或邮箱已存在")
        pw_hash = bcrypt.hashpw(user.password.encode(), bcrypt.gensalt()).decode()
        await session.execute(
            text("INSERT INTO users (username, email, password_hash) VALUES (:u, :e, :p)"),
            {"u": user.username, "e": user.email, "p": pw_hash}
        )
        await session.commit()
    return {"msg": "注册成功"}

@router.post("/login", summary="用户登录")
async def login(user: UserLogin):
    async with async_session() as session:
        row = (await session.execute(
            text("SELECT id, username, role, password_hash FROM users WHERE username=:u"),
            {"u": user.username}
        )).first()
        if not row or not bcrypt.checkpw(user.password.encode(), row.password_hash.encode()):
            raise HTTPException(status_code=401, detail="用户名或密码错误")
        payload = {
            "user_id": row.id,
            "username": row.username,
            "role": row.role,
            "exp": datetime.utcnow() + timedelta(hours=1)
        }
        token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return {"token": token}

@router.get("/getinfo", summary="获取当前用户信息")
async def get_info(user=Depends(get_current_user)):
    async with engine.connect() as conn:
        row = (await conn.execute(
            text("SELECT username, email, role FROM users WHERE id=:uid"),
            {"uid": user["user_id"]}
        )).first()
        if not row:
            raise HTTPException(status_code=404, detail="用户不存在")
    return {"username": row.username, "email": row.email, "role": row.role}

@router.post("/logout", summary="用户登出")
async def logout():
    return {"msg": "登出成功"}

@router.get("/realtime", response_model=ListResponse, summary="获取用户实时数据")
async def list_realtime(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    fresh_secs: Optional[int] = Query(None),
    user=Depends(get_current_user)
):
    if user["role"] != "user":
        raise HTTPException(status_code=403, detail="权限错误")
    fresh = fresh_secs or settings.FRESH_SECS
    join_sql = "JOIN devices d ON r.device_id = d.id"
    cond = "WHERE d.user_id = :user_id"
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
    params = {"user_id": user["user_id"], "limit": page_size, "offset": offset}
    async with engine.connect() as conn:
        total = (await conn.execute(count_sql, {"user_id": user["user_id"]})).scalar_one()
        rows = (await conn.execute(query_sql, params)).mappings().all()
    items = []
    for r in rows:
        d = dict(r)
        d["online"] = online_flag(d["updated_at"], fresh)
        items.append(d)
    return {"items": items, "page": page, "page_size": page_size, "total": total}
```