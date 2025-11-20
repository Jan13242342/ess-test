from typing import Optional, List, Any
from datetime import datetime, timedelta
import json
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from deps import get_current_user
from main import engine
from config import DEVICE_FRESH_SECS

router = APIRouter(prefix="/api/v1/alarms", tags=["报警管理 | Alarm Management"])

# 报警模型
class AlarmItem(BaseModel):
    alarm_id: int = Field(..., alias="alarm_id")
    device_id: Optional[int]
    device_sn: Optional[str] = None  # 新增设备SN字段
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

class AlarmBatchConfirmByCodeRequest(BaseModel):
    code: int = Field(..., description="报警码 | Alarm code")

class AlarmConfirmBySNAndCodeRequest(BaseModel):
    device_sn: str
    code: int

# 管理员/客服查询所有报警
@router.get(
    "/admin",
    response_model=AlarmListResponse,
    summary="报警管理查询 | Query All Alarms (Admin/Service)",
    description="""
**权限要求 | Required Role**: admin, service, support

管理员/客服可按设备序列号、状态、级别、code等筛选报警。

Admin/Service/Support can filter alarms by device SN, status, level, code, etc.
"""
)
async def list_all_alarms(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    status: Optional[str] = Query(None),
    device_sn: Optional[str] = Query(None),
    level: Optional[str] = Query(None),
    code: Optional[int] = Query(None),
    alarm_type: Optional[str] = Query(None),
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="无权限")
    where = []
    params = {}
    join_sql = "LEFT JOIN devices d ON alarms.device_id = d.id"
    if device_sn:
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
            # device_sn 字段已包含在 row 里，无需额外处理
            items.append(d)
    return {"items": items, "page": page, "page_size": page_size, "total": total}

# 按code批量确认报警
@router.post(
    "/admin/batch_confirm",
    summary="按code批量确认报警 | Batch Confirm Alarms By Code",
    description="""
**权限要求 | Required Role**: admin, service

管理员/客服按报警code批量确认所有未确认的报警（只操作当前报警表，历史报警不能确认）。

Admin/Service can batch confirm all unconfirmed alarms by code (only operates on current alarms table, history alarms cannot be confirmed).
"""
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
                WHERE code = :code
                  AND confirmed_at IS NULL
            """),
            {"code": data.code, "by": user["username"]}
        )
    return {"msg": f"已确认所有 code={data.code} 的报警", "confirmed_count": result.rowcount}

# 统计未处理报警数量
@router.get(
    "/unhandled_count",
    summary="统计未处理报警数量（含各等级） | Count Unhandled Alarms (by Level)",
    description="""
**权限要求 | Required Role**: admin, service, support

仅管理员和客服可用。统计所有未处理（active/confirmed）报警的总数量及各等级数量。

Admin/Service/Support only. Count total and per-level unhandled (active/confirmed) alarms.
"""
)
async def count_unhandled_alarms(user=Depends(get_current_user)):
    if user["role"] not in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="无权限")
    async with engine.connect() as conn:
        total_sql = text("""
            SELECT COUNT(*) FROM alarms
            WHERE status IN ('active', 'confirmed')
        """)
        total_count = (await conn.execute(total_sql)).scalar_one()
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

# 按设备SN和code确认报警
@router.post(
    "/admin/confirm",
    summary="按设备SN和code确认报警 | Confirm Alarms By Device SN and Code",
    description="""
**权限要求 | Required Role**: admin, service

管理员/客服按设备SN和code确认所有未确认的报警（只操作当前报警表，历史报警不能确认）。
确认critical级别cleared状态的报警时，会自动归档到alarm_history表。

Admin/Service can confirm all unconfirmed alarms by device SN and code (only operates on current alarms table).
Critical level cleared alarms will be automatically archived to alarm_history table.
"""
)
async def confirm_alarm_by_sn_and_code(
    data: AlarmConfirmBySNAndCodeRequest,
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service"):
        raise HTTPException(status_code=403, detail="无权限")
    async with engine.begin() as conn:
        device_row = (await conn.execute(
            text("SELECT id FROM devices WHERE device_sn=:sn"),
            {"sn": data.device_sn}
        )).mappings().first()
        if not device_row:
            raise HTTPException(status_code=404, detail="设备不存在")
        device_id = device_row["id"]

        result = await conn.execute(
            text("""
                UPDATE alarms
                SET confirmed_at = now(), confirmed_by = :by
                WHERE device_id = :device_id AND code = :code
                AND confirmed_at IS NULL
            """),
            {"device_id": device_id, "code": data.code, "by": user["username"]}
        )

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

        for row in rows:
            cleared_at = row["cleared_at"]
            first_triggered_at = row["first_triggered_at"]
            last_triggered_at = row["last_triggered_at"]

            if last_triggered_at and first_triggered_at:
                duration = max(1, int((last_triggered_at - first_triggered_at).total_seconds()))
            else:
                duration = None

            extra = json.dumps(row["extra"]) if isinstance(row["extra"], dict) else row["extra"]
            duration = timedelta(seconds=duration) if duration else None

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

            await conn.execute(
                text("DELETE FROM alarms WHERE id = :id"),
                {"id": row["id"]}
            )

    return {"msg": f"已确认设备 {data.device_sn} code={data.code} 的所有报警", "confirmed_count": result.rowcount}