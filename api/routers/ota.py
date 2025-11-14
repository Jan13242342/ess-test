import os
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
    status: str = Query("released", description="draft/testing/released/deprecated"),
    force_update: bool = Query(False, description="是否强制更新"),
    min_hardware_version: Optional[str] = Query(None, description="最低硬件版本要求"),
    notes: Optional[str] = Query(None, description="简短备注"),
    release_notes: Optional[str] = Query(None, description="详细发布说明（Markdown）"),
    file: UploadFile = File(...),
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service"):
        raise HTTPException(status_code=403, detail="无权限")
    if status not in {"draft", "testing", "released", "deprecated"}:
        raise HTTPException(status_code=400, detail="状态非法")
    if not file.filename.lower().endswith(".bin"):
        raise HTTPException(status_code=400, detail="只能上传 .bin")

    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="文件为空")
    md5 = hashlib.md5(data).hexdigest()
    sha256 = hashlib.sha256(data).hexdigest()
    safe_name = f"{device_type}-{version}.bin"
    path = os.path.join(FIRMWARE_DIR, safe_name)

    with open(path, "wb") as f:
        f.write(data)
    with open(path + ".md5", "w") as f:
        f.write(md5)
    with open(path + ".sha256", "w") as f:
        f.write(sha256)

    async with engine.begin() as conn:
        await conn.execute(
            text("""
                INSERT INTO firmware_files (
                    device_type, version, filename, file_size, md5, sha256,
                    notes, release_notes, status, is_active, force_update,
                    min_hardware_version, uploaded_by
                )
                VALUES (
                    :device_type, :version, :filename, :file_size, :md5, :sha256,
                    :notes, :release_notes, :status, TRUE, :force_update,
                    :min_hardware_version, :uploaded_by
                )
                ON CONFLICT (device_type, version) DO UPDATE
                  SET filename=EXCLUDED.filename,
                      file_size=EXCLUDED.file_size,
                      md5=EXCLUDED.md5,
                      sha256=EXCLUDED.sha256,
                      notes=EXCLUDED.notes,
                      release_notes=EXCLUDED.release_notes,
                      status=EXCLUDED.status,
                      force_update=EXCLUDED.force_update,
                      min_hardware_version=EXCLUDED.min_hardware_version,
                      uploaded_by=EXCLUDED.uploaded_by,
                      uploaded_at=now()
            """),
            {
                "device_type": device_type,
                "version": version,
                "filename": safe_name,
                "file_size": len(data),
                "md5": md5,
                "sha256": sha256,
                "notes": notes,
                "release_notes": release_notes,
                "status": status,
                "force_update": force_update,
                "min_hardware_version": min_hardware_version,
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
        "sha256": sha256,
        "download_url": f"/ota/{safe_name}",
        "force_update": force_update,
        "notes": notes,
        "release_notes": release_notes,
    }

@router.get(
    "/latest",
    summary="获取最新固件 | Get Latest Firmware",
    description="返回最新版本元数据。"
)
async def get_latest_firmware(
    device_type: str = Query(...),
    current: Optional[str] = Query(None),
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="无权限")
    async with engine.connect() as conn:
        row = (await conn.execute(
            text("""
                SELECT id, device_type, version, filename, file_size, md5, sha256,
                       notes, release_notes, uploaded_at, force_update, download_count
                FROM firmware_files
                WHERE device_type=:device_type
                  AND status='released'
                  AND is_active=TRUE
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
        "firmware_id": row["id"],
        "device_type": row["device_type"],
        "latest_version": latest_ver,
        "has_update": has_update,
        "download_url": f"/ota/{row['filename']}",
        "md5": row["md5"],
        "sha256": row["sha256"],
        "size": row["file_size"],
        "force_update": row["force_update"],
        "download_count": row["download_count"],
        "notes": row["notes"],
        "release_notes": row["release_notes"],
        "uploaded_at": row["uploaded_at"],
    }

@router.get(
    "/list",
    summary="列出固件版本 | List Firmware Versions"
)
async def list_firmware(
    device_type: str = Query(...),
    status: Optional[str] = Query(None, description="过滤状态：draft/testing/released/deprecated"),
    include_inactive: bool = Query(False, description="是否包含 is_active=false"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="无权限")
    if status and status not in {"draft", "testing", "released", "deprecated"}:
        raise HTTPException(status_code=400, detail="状态非法")

    filters = ["device_type = :device_type"]
    params = {"device_type": device_type, "limit": page_size, "offset": (page - 1) * page_size}
    if status:
        filters.append("status = :status")
        params["status"] = status
    if not include_inactive:
        filters.append("is_active = TRUE")
    where_clause = " AND ".join(filters)

    async with engine.connect() as conn:
        total = (await conn.execute(
            text(f"SELECT COUNT(*) FROM firmware_files WHERE {where_clause}"), params
        )).scalar_one()
        rows = (await conn.execute(
            text(f"""
                SELECT id, device_type, version, filename, file_size, md5, sha256,
                       notes, release_notes, status, is_active, force_update,
                       min_hardware_version, download_count, uploaded_at, deprecated_at
                FROM firmware_files
                WHERE {where_clause}
                ORDER BY uploaded_at DESC
                LIMIT :limit OFFSET :offset
            """),
            params
        )).mappings().all()

    return {"items": [dict(row) for row in rows], "page": page, "page_size": page_size, "total": total}