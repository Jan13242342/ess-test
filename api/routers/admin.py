from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from pydantic import BaseModel, Field
from deps import get_current_user
from main import engine, settings, online_flag, COLUMNS

router = APIRouter(prefix="/api/v1/admin", tags=["管理员专用 | Admin Only"])

# 管理员专用删除接口模型
class AdminBatchDeleteAlarmHistoryBySNRequest(BaseModel):
    device_sn: str

class AdminBatchDeleteRPCLogBySNRequest(BaseModel):
    device_sn: str

# 管理员专用：按设备SN删除历史报警
@router.delete(
    "/alarm_history/delete_by_sn",
    tags=["管理员专用 | Admin Only"],
    summary="按设备SN删除历史报警 | Delete Alarm History by Device SN",
    description="""
**权限要求 | Required Role**: admin

仅管理员可用，删除指定设备的所有历史报警记录。

Admin only. Delete all alarm history records for the specified device.

⚠️ **警告 | Warning**: 此操作不可逆！| This operation is irreversible!
"""
)
async def admin_delete_alarm_history_by_sn(
    data: AdminBatchDeleteAlarmHistoryBySNRequest,
    user=Depends(get_current_user)
):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="仅管理员可用")
    async with engine.begin() as conn:
        device = (await conn.execute(
            text("SELECT id FROM devices WHERE device_sn=:sn"),
            {"sn": data.device_sn}
        )).first()
        if not device:
            raise HTTPException(status_code=404, detail="设备不存在")
        result = await conn.execute(
            text("DELETE FROM alarm_history WHERE device_id=:did"),
            {"did": device.id}
        )
    return {"msg": f"已删除设备 {data.device_sn} 的历史报警", "deleted_count": result.rowcount}

# 管理员专用：按设备SN删除RPC日志
@router.delete(
    "/rpc_log/delete_by_sn",
    tags=["管理员专用 | Admin Only"],
    summary="按设备SN删除RPC日志 | Delete RPC Log by Device SN",
    description="""
**权限要求 | Required Role**: admin

仅管理员可用，删除指定设备的所有RPC变更日志。

Admin only. Delete all RPC change logs for the specified device.

⚠️ **警告 | Warning**: 此操作不可逆！| This operation is irreversible!
"""
)
async def admin_delete_rpc_log_by_sn(
    data: AdminBatchDeleteRPCLogBySNRequest,
    user=Depends(get_current_user)
):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="仅管理员可用")
    async with engine.begin() as conn:
        device = (await conn.execute(
            text("SELECT id FROM devices WHERE device_sn=:sn"),
            {"sn": data.device_sn}
        )).first()
        if not device:
            raise HTTPException(status_code=404, detail="设备不存在")
        result = await conn.execute(
            text("DELETE FROM device_rpc_change_log WHERE device_id=:did"),
            {"did": device.id}
        )
    return {"msg": f"已删除设备 {data.device_sn} 的RPC日志", "deleted_count": result.rowcount}

# 管理员专用：清空所有历史报警
@router.delete(
    "/alarm_history/clear_all",
    tags=["管理员专用 | Admin Only"],
    summary="清空所有历史报警 | Clear All Alarm History",
    description="""
**权限要求 | Required Role**: admin

仅管理员可用，删除所有历史报警记录（全系统范围）。

Admin only. Delete all alarm history records (system-wide).

⚠️ **危险操作 | Dangerous Operation**: 此操作将清空所有设备的历史报警，不可逆！

This operation will clear all devices' alarm history and is irreversible!
"""
)
async def admin_clear_all_alarm_history(user=Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="仅管理员可用")
    async with engine.begin() as conn:
        result = await conn.execute(text("DELETE FROM alarm_history"))
    return {"msg": "已清空所有历史报警", "deleted_count": result.rowcount}

# 管理员专用：清空所有RPC日志
@router.delete(
    "/rpc_log/clear_all",
    tags=["管理员专用 | Admin Only"],
    summary="清空所有RPC日志 | Clear All RPC Logs",
    description="""
**权限要求 | Required Role**: admin

仅管理员可用，删除所有RPC变更日志（全系统范围）。

Admin only. Delete all RPC change logs (system-wide).

⚠️ **危险操作 | Dangerous Operation**: 此操作将清空所有设备的RPC日志，不可逆！

This operation will clear all devices' RPC logs and is irreversible!
"""
)
async def admin_clear_all_rpc_logs(user=Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="仅管理员可用")
    async with engine.begin() as conn:
        result = await conn.execute(text("DELETE FROM device_rpc_change_log"))
    return {"msg": "已清空所有RPC日志", "deleted_count": result.rowcount}