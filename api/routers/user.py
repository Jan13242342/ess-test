from datetime import datetime, timezone, timedelta, date
from datetime import time as dtime
from typing import Optional, List, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, EmailStr, Field
import os, bcrypt, jwt, json, time as ttime, random, string
import paho.mqtt.publish as publish
from sqlalchemy import text
from deps import get_current_user
from main import engine, async_session, settings, online_flag, COLUMNS

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

# 历史能耗聚合 - 响应模型
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

@router.get(
    "/history",
    response_model=HistoryAggListResponse,
    tags=["用户 | User"],
    summary="历史能耗聚合数据 | Aggregated History Energy Data",
)
async def list_history(
    period: str = Query("today", description="today/week/month/quarter/year"),
    date: Optional[date] = Query(None, description="YYYY-MM-DD, 小时级聚合"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    user=Depends(get_current_user)
):
    if user["role"] in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="管理员/客服/支持请使用专用接口")

    now = datetime.now(timezone.utc)
    if date:
        start = datetime.combine(date, dtime.min).replace(tzinfo=timezone.utc)
        end = datetime.combine(date, dtime.max).replace(tzinfo=timezone.utc)
        group_expr, group_label = "date_trunc('hour', ts)", "hour"
    else:
        if period == "today":
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
            group_expr, group_label = "date_trunc('hour', ts)", "hour"
        elif period == "week":
            start_of_week = now - timedelta(days=now.weekday())
            start = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
            end = start + timedelta(days=6, hours=23, minutes=59, seconds=59, microseconds=999999)
            group_expr, group_label = "date_trunc('day', ts)", "day"
        elif period == "month":
            start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            next_month = (now.replace(day=28) + timedelta(days=4)).replace(day=1)
            end = next_month - timedelta(seconds=1)
            group_expr, group_label = "date_trunc('day', ts)", "day"
        elif period == "quarter":
            quarter = (now.month - 1) // 3 + 1
            start_month = (quarter - 1) * 3 + 1
            start = now.replace(month=start_month, day=1, hour=0, minute=0, second=0, microsecond=0)
            end_month = quarter * 3
            next_quarter = start.replace(month=end_month + 1, day=1) if end_month < 12 else start.replace(year=start.year + 1, month=1, day=1)
            end = next_quarter - timedelta(seconds=1)
            group_expr, group_label = "date_trunc('month', ts)", "month"
        elif period == "year":
            start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            end = now.replace(month=12, day=31, hour=23, minute=59, second=59, microsecond=999999)
            group_expr, group_label = "date_trunc('month', ts)", "month"
        else:
            raise HTTPException(status_code=400, detail="无效的 period 值")

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
                SUM(grid_wh_total) AS grid_wh_total,
                SUM(load_wh_total) AS load_wh_total
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

# 报警模型
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

# 我的报警（当前）
@router.get(
    "/alarms/user",
    response_model=AlarmListResponse,
    tags=["用户 | User"],
    summary="查询本人设备报警 | Query My Device Alarms",
)
async def list_my_alarms(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    status: Optional[str] = Query(None),
    level: Optional[str] = Query(None),
    code: Optional[int] = Query(None),
    user=Depends(get_current_user)
):
    if user["role"] in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="管理员/客服/支持请用专用接口")
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
        where.append("status = :status"); params["status"] = status
    if level:
        where.append("level = :level"); params["level"] = level
    if code:
        where.append("code = :code"); params["code"] = code
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

# 我的历史报警
@router.get(
    "/alarms/history",
    response_model=AlarmListResponse,
    tags=["用户 | User"],
    summary="查询本人设备历史报警 | Query My Device Alarm History",
)
async def list_my_alarm_history(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    status: Optional[str] = Query(None),
    level: Optional[str] = Query(None),
    code: Optional[int] = Query(None),
    user=Depends(get_current_user)
):
    if user["role"] in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="管理员/客服/支持请用专用接口")
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
        where.append("status = :status"); params["status"] = status
    if level:
        where.append("level = :level"); params["level"] = level
    if code:
        where.append("code = :code"); params["code"] = code
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