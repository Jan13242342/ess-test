import os
import re
import hashlib
from typing import Optional
from fastapi import APIRouter, Query, HTTPException, Depends, UploadFile, File
from pydantic import BaseModel
from sqlalchemy import text
from deps import get_current_user
from main import engine
from config import FIRMWARE_DIR

router = APIRouter(prefix="/api/v1/firmware", tags=["固件管理 | Firmware/OTA"])

VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+-\d{8}$")
HW_VERSION_PATTERN = re.compile(r"^V\d+\.\d+$")

def _parse_version(v: str) -> tuple[int, int, int, int]:
    try:
        base, date = v.split("-", 1)
        major, minor, patch = (int(x) for x in base.split("."))
        return major, minor, patch, int(date)
    except Exception:
        return (0, 0, 0, 0)

@router.post(
    "/upload",
    summary="上传固件 | Upload Firmware",
    description="仅 admin/service。写入共享目录，由 Nginx 静态下载（/ota/{device_type}-{version}.bin）。"
)
async def upload_firmware(
    device_type: str = Query(..., description="设备类型，如 ESP32"),
    version: str = Query(..., description="版本号，格式 1.0.0-YYYYMMDD"),
    status: str = Query(..., description="draft/testing/released/deprecated（必填）"),
    force_update: bool = Query(False, description="是否强制更新"),
    min_hardware_version: str = Query(..., description="最低硬件版本要求"),
    notes: Optional[str] = Query(None, description="简短备注"),
    release_notes: Optional[str] = Query(None, description="详细发布说明（Markdown）"),
    file: UploadFile = File(...),
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service"):
        raise HTTPException(status_code=403, detail="无权限 | Forbidden")
    if status not in {"draft", "testing", "released", "deprecated"}:
        raise HTTPException(status_code=400, detail="状态非法 | Invalid status")
    if not file.filename.lower().endswith(".bin"):
        raise HTTPException(status_code=400, detail="只能上传 .bin | Only .bin files allowed")

    device_type = device_type.strip().upper()
    version = version.strip()
    if not VERSION_PATTERN.fullmatch(version):
        raise HTTPException(status_code=400, detail="版本号必须为 1.0.0-YYYYMMDD 格式 | Version must be 1.0.0-YYYYMMDD")
    min_hardware_version = min_hardware_version.strip().upper()
    if not HW_VERSION_PATTERN.fullmatch(min_hardware_version):
        raise HTTPException(status_code=400, detail="最低硬件版本号必须为 V1.0 格式 | Min HW version must be V1.0")
    status = status.strip().lower()
    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="文件为空 | File is empty")
    md5 = hashlib.md5(data).hexdigest()
    sha256 = hashlib.sha256(data).hexdigest()
    safe_name = f"{device_type}-{version}.bin"
    path = os.path.join(FIRMWARE_DIR, safe_name)

    # 冲突检测：数据库 + 文件
    async with engine.connect() as conn:
        exists = await conn.scalar(
            text("SELECT 1 FROM firmware_files WHERE device_type=:device_type AND version=:version"),
            {"device_type": device_type, "version": version},
        )
        if exists:
            raise HTTPException(status_code=409, detail="该设备类型与版本的固件已存在 | Firmware already exists")
    if os.path.exists(path):
        raise HTTPException(status_code=409, detail="同名固件文件已存在 | File name already exists")

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
        "min_hardware_version": min_hardware_version,
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
    hardware_version: str = Query(..., description="设备硬件版本，格式如 V1.0"),
    current: Optional[str] = Query(None),
    user=Depends(get_current_user)
):
    if user["role"] not in ("admin", "service", "support"):
        raise HTTPException(status_code=403, detail="无权限 | Forbidden")
    device_type = device_type.strip().upper()
    hardware_version = hardware_version.strip().upper()
    if not HW_VERSION_PATTERN.fullmatch(hardware_version):
        raise HTTPException(status_code=400, detail="硬件版本号必须为 V1.0 格式 | Hardware version must be V1.0")
    hw_major = hardware_version.split(".", 1)[0]
    async with engine.connect() as conn:
        row = (await conn.execute(
            text("""
                SELECT id, device_type, version, filename, file_size, md5, sha256,
                       notes, release_notes, uploaded_at, force_update, download_count,
                       min_hardware_version
                FROM firmware_files
                WHERE device_type=:device_type
                  AND status='released'
                  AND is_active=TRUE
                  AND split_part(upper(min_hardware_version), '.', 1) = :hw_major
                ORDER BY
                  COALESCE(NULLIF(split_part(split_part(version,'-',1),'.',1),''),'0')::int DESC,
                  COALESCE(NULLIF(split_part(split_part(version,'-',1),'.',2),''),'0')::int DESC,
                  COALESCE(NULLIF(split_part(split_part(version,'-',1),'.',3),''),'0')::int DESC,
                  uploaded_at DESC
                LIMIT 1
            """),
            {"device_type": device_type, "hw_major": hw_major},
        )).mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="未找到固件 | Firmware not found")

    latest_ver = row["version"]
    current_version = current.strip() if current else None
    has_update = (
        current_version is not None
        and VERSION_PATTERN.fullmatch(current_version)
        and _parse_version(latest_ver) > _parse_version(current_version)
    )
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
        "min_hardware_version": row["min_hardware_version"],
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
        raise HTTPException(status_code=403, detail="无权限 | Forbidden")
    if status and status not in {"draft", "testing", "released", "deprecated"}:
        raise HTTPException(status_code=400, detail="状态非法 | Invalid status")
    device_type = device_type.strip().upper()

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

    items = []
    for row in rows:
        data = dict(row)
        data["download_url"] = f"/ota/{row['filename']}"
        items.append(data)

    return {"items": items, "page": page, "page_size": page_size, "total": total}