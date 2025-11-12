import os
import json
import time
import random
import string
from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
import paho.mqtt.publish as publish
from deps import get_current_user
from main import engine

router = APIRouter(prefix="/api/v1/device", tags=["RPC管理 | RPC Management"])

# RPC 请求模型
class RPCChangeRequest(BaseModel):
    device_sn: str
    para_name: str
    para_value: str
    message: Optional[str] = None

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

# 用户 RPC 参数白名单
USER_RPC_ALLOWED = {"control_mode"}

# 用户端 RPC 下发
@router.post(
    "/user_rpc_change",
    tags=["用户 | User"],
    summary="用户参数下发 | User RPC Change",
    description="普通用户仅可对自己名下设备发起 RPC 请求（参数白名单：control_mode）。"
)
async def user_rpc_change(
    req: RPCChangeRequest,
    user=Depends(get_current_user)
):
    if user["role"] in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="管理员/客服/支持请使用管理员接口")
    if req.para_name not in USER_RPC_ALLOWED:
        raise HTTPException(status_code=403, detail=f"不允许修改参数: {req.para_name}")

    async with engine.begin() as conn:
        device_row = (await conn.execute(
            text("SELECT id FROM devices WHERE device_sn=:sn AND user_id=:uid"),
            {"sn": req.device_sn, "uid": user["user_id"]}
        )).mappings().first()
        if not device_row:
            raise HTTPException(status_code=404, detail="设备不存在或不属于当前用户")
        device_id = device_row["id"]

        ts = str(int(time.time()))
        rnd = ''.join(random.choices(string.ascii_uppercase, k=4))
        request_id = f"{ts}{rnd}"

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
                "operator": "user",
                "request_id": request_id,
                "para_name": req.para_name,
                "para_value": req.para_value,
                "message": req.message or f"user change {req.para_name} = {req.para_value}"
            }
        )

    topic = f"devices/{req.device_sn}/rpc"
    payload = {
        "request_id": request_id,
        "para_name": req.para_name,
        "para_value": req.para_value,
        "operator": "user",
        "message": req.message or f"user change {req.para_name} = {req.para_value}",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    publish.single(
        topic,
        json.dumps(payload),
        hostname=os.getenv("MQTT_HOST"),
        port=int(os.getenv("MQTT_PORT", "1883"))
    )
    return {"status": "ok", "request_id": request_id, "message": req.message}

# 管理员/客服 RPC 下发
@router.post(
    "/rpc_change",
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
        
        timestamp = str(int(time.time()))
        random_letters = ''.join(random.choices(string.ascii_uppercase, k=4))
        request_id = f"{timestamp}{random_letters}"
        
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
    
    mqtt_topic = f"devices/{req.device_sn}/rpc"
    mqtt_payload = {
        "request_id": request_id,
        "para_name": req.para_name,
        "para_value": req.para_value,
        "operator": user["username"],
        "message": req.message or f"change {req.para_name} = {req.para_value}",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    publish.single(
        mqtt_topic, 
        json.dumps(mqtt_payload), 
        hostname=os.getenv("MQTT_HOST"), 
        port=int(os.getenv("MQTT_PORT", "1883"))
    )
    return {"status": "ok", "request_id": request_id, "message": req.message}

# RPC 变更历史
@router.get(
    "/rpc_history",
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

# 设备参数查询
@router.get(
    "/para",
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