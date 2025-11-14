-- ==================== OTA 固件管理模块 ====================
-- OTA Firmware Management Module
-- 作者 Author: Orson
-- 日期 Date: 2025-11-14
-- 说明 Description: 固件上传、版本管理、设备升级相关表

-- 固件状态枚举
-- Firmware status enum
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'firmware_status') THEN
    CREATE TYPE firmware_status AS ENUM ('draft', 'testing', 'released', 'deprecated');
  END IF;
END$$;

-- 固件文件表：存储设备固件文件信息（增强版）
-- Firmware files table: stores device firmware file information (enhanced)
CREATE TABLE IF NOT EXISTS firmware_files (
  id BIGSERIAL PRIMARY KEY,
  device_type TEXT NOT NULL CHECK (device_type <> ''),
  version TEXT NOT NULL CHECK (version ~ '^\d+(\.\d+){0,2}$'), -- 1 or 1.2 or 1.2.3
  filename TEXT NOT NULL CHECK (filename ~* '\.bin$'),
  file_size INT NOT NULL CHECK (file_size > 0),
  md5 TEXT NOT NULL CHECK (md5 ~ '^[0-9a-f]{32}$'),
  sha256 TEXT CHECK (sha256 ~ '^[0-9a-f]{64}$'),               -- 新增：SHA256 校验
  notes TEXT,                                                   -- 简短备注
  release_notes TEXT,                                           -- 新增：详细发布说明（Markdown）
  status firmware_status NOT NULL DEFAULT 'released',           -- 新增：固件状态
  is_active BOOLEAN NOT NULL DEFAULT TRUE,                      -- 新增：是否激活
  force_update BOOLEAN NOT NULL DEFAULT FALSE,                  -- 新增：是否强制更新
  min_hardware_version TEXT,                                    -- 新增：最低硬件版本要求
  download_count BIGINT NOT NULL DEFAULT 0,                     -- 新增：下载次数统计
  uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  uploaded_by TEXT NOT NULL CHECK (uploaded_by <> ''),
  deprecated_at TIMESTAMPTZ,                                    -- 新增：废弃时间
  UNIQUE (device_type, version),
  CONSTRAINT chk_deprecated_requires_timestamp 
    CHECK (status != 'deprecated' OR deprecated_at IS NOT NULL)
);

-- 索引优化
-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_fw_type_uploaded_at
  ON firmware_files (device_type, uploaded_at DESC);

CREATE INDEX IF NOT EXISTS idx_fw_type_semver_desc
  ON firmware_files (
    device_type,
    (COALESCE(NULLIF(split_part(version, '.', 1), ''), '0')::int) DESC,
    (COALESCE(NULLIF(split_part(version, '.', 2), ''), '0')::int) DESC,
    (COALESCE(NULLIF(split_part(version, '.', 3), ''), '0')::int) DESC,
    uploaded_at DESC
  );

CREATE INDEX IF NOT EXISTS idx_fw_status 
  ON firmware_files(device_type, status) WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS idx_fw_download_count 
  ON firmware_files(download_count DESC);

CREATE INDEX IF NOT EXISTS idx_fw_deprecated_at 
  ON firmware_files(deprecated_at) WHERE deprecated_at IS NOT NULL;

-- 字段注释
-- Column comments
COMMENT ON TABLE firmware_files IS '固件文件表：存储设备固件文件信息';
COMMENT ON COLUMN firmware_files.status IS '固件状态：draft(草稿)/testing(测试中)/released(已发布)/deprecated(已废弃)';
COMMENT ON COLUMN firmware_files.is_active IS '是否激活（软删除标记）';
COMMENT ON COLUMN firmware_files.force_update IS '是否强制更新';
COMMENT ON COLUMN firmware_files.min_hardware_version IS '最低硬件版本要求';
COMMENT ON COLUMN firmware_files.download_count IS '下载次数统计';
COMMENT ON COLUMN firmware_files.release_notes IS '详细发布说明（Markdown格式）';
COMMENT ON COLUMN firmware_files.deprecated_at IS '废弃时间（自动设置）';
COMMENT ON COLUMN firmware_files.sha256 IS 'SHA256 校验和（比 MD5 更安全）';

-- 自动更新 deprecated_at 的触发器
-- Trigger to auto update deprecated_at
CREATE OR REPLACE FUNCTION update_firmware_deprecated_at()
RETURNS TRIGGER AS $$
BEGIN
  -- 当状态改为 deprecated 且 deprecated_at 为空时，自动设置
  IF NEW.status = 'deprecated' AND NEW.deprecated_at IS NULL THEN
    NEW.deprecated_at = now();
  END IF;
  -- 当状态从 deprecated 改为其他状态时，清空 deprecated_at
  IF NEW.status != 'deprecated' AND OLD.status = 'deprecated' THEN
    NEW.deprecated_at = NULL;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_update_firmware_deprecated_at ON firmware_files;
CREATE TRIGGER trg_update_firmware_deprecated_at
  BEFORE UPDATE OF status ON firmware_files
  FOR EACH ROW
  EXECUTE FUNCTION update_firmware_deprecated_at();

-- 设备固件升级记录表（可选，追踪每台设备的升级历史）
-- Device firmware upgrade log table (optional, tracks upgrade history for each device)
CREATE TABLE IF NOT EXISTS device_firmware_upgrade_log (
  id BIGSERIAL PRIMARY KEY,
  device_id BIGINT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
  firmware_id BIGINT REFERENCES firmware_files(id) ON DELETE SET NULL,
  from_version TEXT,                              -- 升级前版本
  to_version TEXT NOT NULL,                       -- 升级目标版本
  status TEXT NOT NULL DEFAULT 'pending' CHECK (
    status IN ('pending', 'downloading', 'installing', 'success', 'failed', 'rolled_back')
  ),
  download_started_at TIMESTAMPTZ,                -- 下载开始时间
  download_completed_at TIMESTAMPTZ,              -- 下载完成时间
  install_started_at TIMESTAMPTZ,                 -- 安装开始时间
  install_completed_at TIMESTAMPTZ,               -- 安装完成时间
  error_message TEXT,                             -- 错误信息
  initiated_by TEXT NOT NULL DEFAULT 'user',      -- user/auto/admin
  operator TEXT,                                   -- 操作人（如果是手动）
  download_size BIGINT,                           -- 下载大小（字节）
  download_speed_kbps INT,                        -- 下载速度（KB/s）
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 升级记录表索引
-- Upgrade log indexes
CREATE INDEX IF NOT EXISTS idx_fw_upgrade_log_device_id 
  ON device_firmware_upgrade_log(device_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_fw_upgrade_log_status 
  ON device_firmware_upgrade_log(status);
CREATE INDEX IF NOT EXISTS idx_fw_upgrade_log_created_at 
  ON device_firmware_upgrade_log(created_at DESC);

-- 升级记录表注释
COMMENT ON TABLE device_firmware_upgrade_log IS '设备固件升级记录表：追踪每台设备的升级历史';
COMMENT ON COLUMN device_firmware_upgrade_log.initiated_by IS '触发方式：user(用户手动)/auto(自动)/admin(管理员强制)';

-- 自动更新 updated_at
CREATE OR REPLACE FUNCTION update_fw_upgrade_log_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_fw_upgrade_log_timestamp
  BEFORE UPDATE ON device_firmware_upgrade_log
  FOR EACH ROW
  EXECUTE FUNCTION update_fw_upgrade_log_timestamp();

-- 固件依赖关系表（可选，支持增量更新/A-B测试）
-- Firmware dependencies table (optional, for incremental updates/A-B testing)
CREATE TABLE IF NOT EXISTS firmware_dependencies (
  id BIGSERIAL PRIMARY KEY,
  firmware_id BIGINT NOT NULL REFERENCES firmware_files(id) ON DELETE CASCADE,
  depends_on_firmware_id BIGINT REFERENCES firmware_files(id) ON DELETE CASCADE,
  dependency_type TEXT NOT NULL CHECK (dependency_type IN ('requires', 'recommends', 'conflicts')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (firmware_id, depends_on_firmware_id, dependency_type)
);

CREATE INDEX IF NOT EXISTS idx_fw_deps_firmware_id 
  ON firmware_dependencies(firmware_id);

COMMENT ON TABLE firmware_dependencies IS '固件依赖关系表：定义固件之间的依赖/推荐/冲突关系';
COMMENT ON COLUMN firmware_dependencies.dependency_type IS 'requires(必须)/recommends(推荐)/conflicts(冲突)';

-- 完成日志
DO $$
BEGIN
  RAISE NOTICE '✅ OTA module tables created successfully';
END$$;