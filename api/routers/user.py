from datetime import datetime, timezone, timedelta
from typing import Optional, List
from fastapi import APIRouter, Query, HTTPException, Depends
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import text
from deps import get_current_user
from main import engine, async_session, settings, online_flag, COLUMNS
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

class BindDeviceRequest(BaseModel):
    device_sn: str = Field(..., description="设备序列号 | Device SN")

class UnbindDeviceRequest(BaseModel):
    device_sn: str = Field(..., description="设备序列号 | Device SN")

# 新增：修改密码请求模型
class ChangePasswordRequest(BaseModel):
    old_password: str = Field(..., min_length=6, description="旧密码 | Old password")
    new_password: str = Field(..., min_length=6, description="新密码 | New password")

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

@router.post("/device/bind", summary="绑定设备 | Bind Device", tags=["用户 | User"])
async def bind_device(data: BindDeviceRequest, user=Depends(get_current_user)):
    if user["role"] != "user":
        raise HTTPException(status_code=403, detail="权限错误")
    async with async_session() as session:
        async with session.begin():
            # 行级锁避免并发抢占
            row = (await session.execute(
                text("SELECT id, user_id FROM devices WHERE device_sn=:sn FOR UPDATE"),
                {"sn": data.device_sn}
            )).mappings().first()
            if not row:
                raise HTTPException(status_code=404, detail="设备不存在")
            if row["user_id"] is None:
                await session.execute(
                    text("UPDATE devices SET user_id=:uid WHERE id=:id"),
                    {"uid": user["user_id"], "id": row["id"]}
                )
                return {"msg": "绑定成功", "device_sn": data.device_sn}
            if row["user_id"] == user["user_id"]:
                return {"msg": "设备已绑定到当前用户", "device_sn": data.device_sn}
            raise HTTPException(status_code=409, detail="设备已绑定到其他用户")

@router.post("/device/unbind", summary="解绑设备 | Unbind Device", tags=["用户 | User"])
async def unbind_device(data: UnbindDeviceRequest, user=Depends(get_current_user)):
    if user["role"] != "user":
        raise HTTPException(status_code=403, detail="权限错误")
    async with async_session() as session:
        async with session.begin():
            result = await session.execute(
                text("""
                    UPDATE devices
                    SET user_id = NULL
                    WHERE device_sn=:sn AND user_id=:uid
                """),
                {"sn": data.device_sn, "uid": user["user_id"]}
            )
            if result.rowcount == 0:
                # 设备不存在或不属于当前用户
                owned = (await session.execute(
                    text("SELECT 1 FROM devices WHERE device_sn=:sn"),
                    {"sn": data.device_sn}
                )).first()
                if not owned:
                    raise HTTPException(status_code=404, detail="设备不存在")
                raise HTTPException(status_code=403, detail="设备不属于当前用户")
            return {"msg": "解绑成功", "device_sn": data.device_sn}

# 新增：修改密码
@router.post("/user/change_password", summary="修改密码 | Change Password", tags=["用户 | User"])
async def change_password(data: ChangePasswordRequest, user=Depends(get_current_user)):
    if user["role"] != "user":
        raise HTTPException(status_code=403, detail="权限错误")
    async with async_session() as session:
        row = (await session.execute(
            text("SELECT password_hash FROM users WHERE id=:uid"),
            {"uid": user["user_id"]}
        )).first()
        if not row:
            raise HTTPException(status_code=404, detail="用户不存在")
        if not bcrypt.checkpw(data.old_password.encode(), row.password_hash.encode()):
            raise HTTPException(status_code=400, detail="旧密码不正确")
        new_hash = bcrypt.hashpw(data.new_password.encode(), bcrypt.gensalt()).decode()
        await session.execute(
            text("UPDATE users SET password_hash=:ph WHERE id=:uid"),
            {"ph": new_hash, "uid": user["user_id"]}
        )
        await session.commit()
    return {"msg": "修改成功"}

# 新增：我的设备列表（分页）
@router.get("/user/devices", summary="我的设备列表 | My Devices", tags=["用户 | User"])
async def list_my_devices(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    user=Depends(get_current_user)
):
    if user["role"] != "user":
        raise HTTPException(status_code=403, detail="权限错误")
    offset = (page - 1) * page_size
    async with async_session() as session:
        total = (await session.execute(
            text("SELECT COUNT(*) FROM devices WHERE user_id=:uid"),
            {"uid": user["user_id"]}
        )).scalar_one()
        rows = (await session.execute(
            text("""
                SELECT id, device_sn
                FROM devices
                WHERE user_id=:uid
                ORDER BY id DESC
                LIMIT :limit OFFSET :offset
            """),
            {"uid": user["user_id"], "limit": page_size, "offset": offset}
        )).mappings().all()
    return {"items": rows, "page": page, "page_size": page_size, "total": total}