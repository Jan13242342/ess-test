import hashlib
from typing import Optional
from fastapi import APIRouter, Query, HTTPException, Depends, UploadFile, File
from pydantic import BaseModel
from sqlalchemy import text
from deps import get_current_user
from main import engine
from config import FIRMWARE_DIR

router = APIRouter(prefix="/api/v1/firmware", tags=["固件管理 | Firmware/OTA"])

def _parse_semver(v: str) -> tuple[int, int, int]:
    try:
        a, b, c = (int(x) for x in (v.split(".") + ["0", "0", "0"])[:3])
        return a, b, c
    except Exception:
        return (0, 0, 0)

@router.post(
    "/upload",
    summary="上传固件 | Upload Firmware",
    description="仅 admin/service。写入共享目录，由 Nginx 静态下载（/ota/{device_type}-{version}.bin）。"
)
async def upload_firmware(
    device_type: str = Query(..., description="设备类型，如 esp32"),
    version: str = Query(..., description="版本号，如 1.2.3"),
    notes: Optional[str] = Query(None, description="发布备注"),
    file: UploadFile = File(...),
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service"):
        raise HTTPException(status_code=403, detail="无权限")
    if not file.filename.lower().endswith(".bin"):
        raise HTTPException(status_code=400, detail="只能上传 .bin")

    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="文件为空")
    md5 = hashlib.md5(data).hexdigest()
    safe_name = f"{device_type}-{version}.bin"
    path = os.path.join(FIRMWARE_DIR, safe_name)

    # 保存二进制与 .md5
    with open(path, "wb") as f:
        f.write(data)
    with open(path + ".md5", "w") as f:
        f.write(md5)

    # 写入元数据表（firmware_files）
    async with engine.begin() as conn:
        await conn.execute(
            text("""
                INSERT INTO firmware_files (device_type, version, filename, file_size, md5, notes, uploaded_by)
                VALUES (:device_type, :version, :filename, :file_size, :md5, :notes, :uploaded_by)
                ON CONFLICT (device_type, version) DO UPDATE
                  SET filename=EXCLUDED.filename,
                      file_size=EXCLUDED.file_size,
                      md5=EXCLUDED.md5,
                      notes=EXCLUDED.notes,
                      uploaded_by=EXCLUDED.uploaded_by,
                      uploaded_at=now()
            """),
            {
                "device_type": device_type,
                "version": version,
                "filename": safe_name,
                "file_size": len(data),
                "md5": md5,
                "notes": notes,
                "uploaded_by": user["username"],
            },
        )

    return {
        "status": "ok",
        "device_type": device_type,
        "version": version,
        "filename": safe_name,
        "size": len(data),
        "md5": md5,
        "download_url": f"/ota/{safe_name}",
        "notes": notes,
    }

@router.get(
    "/latest",
    summary="获取最新固件 | Get Latest Firmware",
    description="返回最新版本的元数据与下载地址（只读：admin/service/support）。"
)
async def get_latest_firmware(
    device_type: str = Query(..., description="设备类型，如 esp32"),
    current: Optional[str] = Query(None, description="设备当前版本（可选，用于比较是否需升级）"),
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="无权限")
    async with engine.connect() as conn:
        row = (await conn.execute(
            text("""
                SELECT device_type, version, filename, file_size, md5, notes, uploaded_at
                FROM firmware_files
                WHERE device_type=:device_type
                ORDER BY
                  COALESCE(NULLIF(split_part(version,'.',1),''),'0')::int DESC,
                  COALESCE(NULLIF(split_part(version,'.',2),''),'0')::int DESC,
                  COALESCE(NULLIF(split_part(version,'.',3),''),'0')::int DESC,
                  uploaded_at DESC
                LIMIT 1
            """),
            {"device_type": device_type},
        )).mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="未找到固件")

    latest_ver = row["version"]
    has_update = (current is not None) and (_parse_semver(latest_ver) > _parse_semver(current))
    return {
        "device_type": row["device_type"],
        "latest_version": latest_ver,
        "has_update": has_update,
        "download_url": f"/ota/{row['filename']}",
        "md5": row["md5"],
        "size": row["file_size"],
        "notes": row["notes"],
        "uploaded_at": row["uploaded_at"],
    }

@router.get(
    "/list",
    summary="列出固件版本 | List Firmware Versions",
    description="分页列出指定 device_type 的历史版本（只读：admin/service/support）。"
)
async def list_firmware(
    device_type: str = Query(...),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="无权限")
    offset = (page - 1) * page_size
    async with engine.connect() as conn:
        count_sql = text(f"""
            SELECT COUNT(*) FROM firmware_files
            WHERE device_type = :device_type
        """)
        total = (await conn.execute(count_sql, {"device_type": device_type})).scalar_one()
        query_sql = text(f"""
            SELECT device_type, version, filename, file_size, md5, notes, uploaded_at
            FROM firmware_files
            WHERE device_type = :device_type
            ORDER BY uploaded_at DESC
            LIMIT :limit OFFSET :offset
        """)
        rows = (await conn.execute(query_sql, {"device_type": device_type, "limit": page_size, "offset": offset})).mappings().all()
    items = [dict(row) for row in rows]
    return {"items": items, "page": page, "page_size": page_size, "total": total}