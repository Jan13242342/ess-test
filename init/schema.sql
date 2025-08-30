-- 1. 只建表结构，初始化部分超级管理员和客服用户
-- 1. Only create table structure, and initialize some super admins and service users

-- 用户表：存储所有用户，包括管理员、客服、普通用户
-- Users table: stores all users, including admins, service staff, and normal users
CREATE TABLE IF NOT EXISTS users (
  id BIGSERIAL PRIMARY KEY, -- 用户ID，自增 / User ID, auto-increment
  username TEXT NOT NULL UNIQUE, -- 用户名 / Username
  email TEXT NOT NULL UNIQUE,    -- 邮箱 / Email
  password_hash TEXT NOT NULL,   -- 密码哈希 / Password hash
  role TEXT NOT NULL DEFAULT 'user' -- 角色 / Role
);

-- 经销商表：存储所有经销商
-- Dealers table: stores all dealers
CREATE TABLE IF NOT EXISTS dealers (
  id BIGSERIAL PRIMARY KEY, -- 经销商ID / Dealer ID
  name TEXT NOT NULL UNIQUE -- 经销商名称 / Dealer name
);

-- 设备状态类型定义
-- Device status type definition
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'device_status') THEN
    CREATE TYPE device_status AS ENUM ('active','inactive','installing','fault','retired');
  END IF;
END$$;

-- 设备表：存储所有设备
-- Devices table: stores all devices
CREATE TABLE IF NOT EXISTS devices (
  id BIGSERIAL PRIMARY KEY, -- 设备ID / Device ID
  device_sn TEXT NOT NULL UNIQUE, -- 设备序列号 / Device serial number
  model TEXT, -- 设备型号 / Device model
  firmware_version TEXT, -- 固件版本 / Firmware version
  user_id BIGINT REFERENCES users(id) ON DELETE SET NULL, -- 绑定用户ID / Bound user ID
  dealer_id BIGINT REFERENCES dealers(id) ON DELETE SET NULL, -- 经销商ID / Dealer ID
  location JSONB, -- 设备位置信息 / Device location info
  status device_status, -- 设备状态 / Device status
  installed_at TIMESTAMPTZ, -- 安装时间 / Installation time
  created_at TIMESTAMPTZ NOT NULL DEFAULT now() -- 创建时间 / Creation time
);

-- 设备表索引
-- Device table indexes
CREATE INDEX IF NOT EXISTS ix_devices_user_id    ON devices(user_id);
CREATE INDEX IF NOT EXISTS ix_devices_dealer_id  ON devices(dealer_id);
CREATE INDEX IF NOT EXISTS ix_devices_location   ON devices USING GIN(location);

-- 未绑定设备的创建时间索引
-- Index for unbound devices by creation time
CREATE INDEX IF NOT EXISTS ix_devices_unbound_created
  ON devices(created_at DESC)
  WHERE user_id IS NULL;

-- 实时数据表：存储设备的最新数据
-- Realtime data table: stores latest data for each device
CREATE TABLE IF NOT EXISTS ess_realtime_data (
  dealer_id BIGINT REFERENCES dealers(id) ON DELETE SET NULL, -- 经销商ID / Dealer ID
  device_id BIGINT PRIMARY KEY REFERENCES devices(id) ON DELETE CASCADE, -- 设备ID / Device ID
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- 更新时间 / Update time
  soc SMALLINT NOT NULL CHECK (soc BETWEEN 0 AND 100), -- 剩余电量 / State of charge
  soh SMALLINT NOT NULL DEFAULT 100 CHECK (soh BETWEEN 0 AND 100), -- 健康度 / State of health
  pv INTEGER NOT NULL DEFAULT 0, -- 光伏功率 / PV power
  load INTEGER NOT NULL DEFAULT 0, -- 负载功率 / Load power
  grid INTEGER NOT NULL DEFAULT 0, -- 电网功率 / Grid power
  grid_q INTEGER NOT NULL DEFAULT 0, -- 电网无功功率 / Grid reactive power
  batt INTEGER NOT NULL DEFAULT 0, -- 电池功率 / Battery power
  ac_v INTEGER NOT NULL DEFAULT 0, -- 交流电压 / AC voltage
  ac_f INTEGER NOT NULL DEFAULT 0, -- 交流频率 / AC frequency
  v_a INTEGER NOT NULL DEFAULT 0, -- A相电压 / Phase A voltage
  v_b INTEGER NOT NULL DEFAULT 0, -- B相电压 / Phase B voltage
  v_c INTEGER NOT NULL DEFAULT 0, -- C相电压 / Phase C voltage
  i_a INTEGER NOT NULL DEFAULT 0, -- A相电流 / Phase A current
  i_b INTEGER NOT NULL DEFAULT 0, -- B相电流 / Phase B current
  i_c INTEGER NOT NULL DEFAULT 0, -- C相电流 / Phase C current
  p_a INTEGER NOT NULL DEFAULT 0, -- A相有功功率 / Phase A active power
  p_b INTEGER NOT NULL DEFAULT 0, -- B相有功功率 / Phase B active power
  p_c INTEGER NOT NULL DEFAULT 0, -- C相有功功率 / Phase C active power
  q_a INTEGER NOT NULL DEFAULT 0, -- A相无功功率 / Phase A reactive power
  q_b INTEGER NOT NULL DEFAULT 0, -- B相无功功率 / Phase B reactive power
  q_c INTEGER NOT NULL DEFAULT 0, -- C相无功功率 / Phase C reactive power
  e_pv_today BIGINT NOT NULL DEFAULT 0, -- 今日光伏发电量 / Today's PV energy
  e_load_today BIGINT NOT NULL DEFAULT 0, -- 今日负载用电量 / Today's load energy
  e_charge_today BIGINT NOT NULL DEFAULT 0, -- 今日充电量 / Today's charge energy
  e_discharge_today BIGINT NOT NULL DEFAULT 0 -- 今日放电量 / Today's discharge energy
);

-- 实时数据表索引
-- Realtime data table indexes
CREATE INDEX IF NOT EXISTS idx_ess_dealer   ON ess_realtime_data (dealer_id);
CREATE INDEX IF NOT EXISTS idx_ess_updated  ON ess_realtime_data (updated_at DESC);

-- 历史能耗数据表（分区表）
-- History energy table (partitioned)
CREATE TABLE IF NOT EXISTS history_energy (
  id BIGSERIAL, -- 主键ID / Primary key ID
  device_id BIGINT NOT NULL REFERENCES devices(id) ON DELETE CASCADE, -- 设备ID / Device ID
  ts TIMESTAMPTZ NOT NULL, -- 时间戳 / Timestamp
  charge_wh_total BIGINT, -- 累计充电量 / Total charge (Wh)
  discharge_wh_total BIGINT, -- 累计放电量 / Total discharge (Wh)
  pv_wh_total BIGINT, -- 累计光伏发电量 / Total PV (Wh)
  CONSTRAINT pk_history_energy PRIMARY KEY (device_id, ts),
  CONSTRAINT chk_nonneg_charge CHECK (charge_wh_total IS NULL OR charge_wh_total >= 0),
  CONSTRAINT chk_nonneg_discharge CHECK (discharge_wh_total IS NULL OR discharge_wh_total >= 0),
  CONSTRAINT chk_nonneg_pv CHECK (pv_wh_total IS NULL OR pv_wh_total >= 0)
) PARTITION BY RANGE (ts);

-- 自动创建本月分区
-- Auto create partition for this month
DO $$
DECLARE
  this_month date := date_trunc('month', now());
  next_month date := date_trunc('month', now()) + interval '1 month';
  partition_name text := 'history_energy_' || to_char(this_month, 'YYYY_MM');
BEGIN
  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I PARTITION OF history_energy FOR VALUES FROM (%L) TO (%L);',
    partition_name, this_month, next_month
  );
END;
$$;

-- 启用pg_cron扩展
-- Enable pg_cron extension
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- 自动创建下月分区的函数
-- Function to auto create next month's partition
CREATE OR REPLACE FUNCTION create_next_history_energy_partition()
RETURNS void AS $$
DECLARE
  next_month date := date_trunc('month', now()) + interval '1 month';
  next_next_month date := date_trunc('month', now()) + interval '2 month';
  partition_name text := 'history_energy_' || to_char(next_month, 'YYYY_MM');
BEGIN
  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I PARTITION OF history_energy FOR VALUES FROM (%L) TO (%L);',
    partition_name, next_month, next_next_month
  );
END;
$$ LANGUAGE plpgsql;

-- 每月1号自动创建下月分区
-- Schedule to auto create next month's partition on the 1st of each month
SELECT cron.schedule(
  'create_next_history_energy_partition',
  '0 0 1 * *',
  'SELECT create_next_history_energy_partition();'
);

-- 初始化2个超级管理员和5个客服，密码都是123456
-- Initialize 2 super admins and 5 service users, password is 123456
INSERT INTO users (username, email, password_hash, role) VALUES
  ('admin1', 'admin1@example.com', '$2b$12$KIXQ4b6zQ6b6Q6b6Q6b6QeQ6b6Q6b6Q6b6Q6b6Q6b6Q6b6Q6b6Q', 'admin'),
  ('admin2', 'admin2@example.com', '$2b$12$KIXQ4b6zQ6b6Q6b6Q6b6QeQ6b6Q6b6Q6b6Q6b6Q6b6Q6b6Q6b6Q', 'admin'),
  ('service1', 'service1@example.com', '$2b$12$KIXQ4b6zQ6b6Q6b6Q6b6QeQ6b6Q6b6Q6b6Q6b6Q6b6Q6b6Q6b6Q', 'service'),
  ('service2', 'service2@example.com', '$2b$12$KIXQ4b6zQ6b6Q6b6Q6b6QeQ6b6Q6b6Q6b6Q6b6Q6b6Q6b6Q6b6Q', 'service'),
  ('service3', 'service3@example.com', '$2b$12$KIXQ4b6zQ6b6Q6b6Q6b6QeQ6b6Q6b6Q6b6Q6b6Q6b6Q6b6Q6b6Q', 'service'),
  ('service4', 'service4@example.com', '$2b$12$KIXQ4b6zQ6b6Q6b6Q6b6QeQ6b6Q6b6Q6b6Q6b6Q6b6Q6b6Q6b6Q', 'service'),
  ('service5', 'service5@example.com', '$2b$12$KIXQ4b6zQ6b6Q6b6Q6b6QeQ6b6Q6b6Q6b6Q6b6Q6b6Q6b6Q6b6Q', 'service')
ON CONFLICT DO NOTHING;
