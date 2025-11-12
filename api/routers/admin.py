```python
// filepath: c:\Users\orson\Desktop\New folder\ess-test\api\routers\admin.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from datetime import datetime, timezone
from ..deps import get_current_user
from main import engine, async_session, settings, online_flag, COLUMNS

router = APIRouter(prefix="/api/v1/admin", tags=["管理员/客服 | Admin/Service"])

@router.get("/realtime/by_sn/{device_sn}", summary="根据设备SN获取实时数据")
async def get_realtime_by_sn(device_sn: str, user=Depends(get_current_user)):
    if user["role"] not in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="无权限")
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
```