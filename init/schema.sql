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
  grid_wh_total BIGINT, -- 累计电网用电量 / Total grid energy (Wh)   -- 可正可负
  CONSTRAINT pk_history_energy PRIMARY KEY (device_id, ts),
  CONSTRAINT chk_nonneg_charge CHECK (charge_wh_total IS NULL OR charge_wh_total >= 0),
  CONSTRAINT chk_nonneg_discharge CHECK (discharge_wh_total IS NULL OR discharge_wh_total >= 0),
  CONSTRAINT chk_nonneg_pv CHECK (pv_wh_total IS NULL OR pv_wh_total >= 0)
  -- 不加 grid_wh_total 的非负约束
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

-- 初始化2个超级管理员和5个客服
INSERT INTO users (username, email, password_hash, role) VALUES
  ('admin1', 'admin1@example.com', '$2b$12$MQtJ8NIlVywD69WqqldUlOttvE2DKE.k44WFniAzLa6aCjMpzv.G.', 'admin'),
  ('admin2', 'admin2@example.com', '$2b$12$MQtJ8NIlVywD69WqqldUlOttvE2DKE.k44WFniAzLa6aCjMpzv.G.', 'admin'),
  ('service1', 'service1@example.com', '$2b$12$MQtJ8NIlVywD69WqqldUlOttvE2DKE.k44WFniAzLa6aCjMpzv.G.', 'service'),
  ('service2', 'service2@example.com', '$2b$12$MQtJ8NIlVywD69WqqldUlOttvE2DKE.k44WFniAzLa6aCjMpzv.G.', 'service'),
  ('service3', 'service3@example.com', '$2b$12$MQtJ8NIlVywD69WqqldUlOttvE2DKE.k44WFniAzLa6aCjMpzv.G.', 'service'),
  ('service4', 'service4@example.com', '$2b$12$MQtJ8NIlVywD69WqqldUlOttvE2DKE.k44WFniAzLa6aCjMpzv.G.', 'service'),
  ('service5', 'service5@example.com', '$2b$12$MQtJ8NIlVywD69WqqldUlOttvE2DKE.k44WFniAzLa6aCjMpzv.G.', 'service')
ON CONFLICT DO NOTHING;

-- 启用pg_stat_statements扩展
-- Enable pg_stat_statements extension
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- 邮箱验证码表：存储邮箱验证码信息
-- Email verification codes table: stores email verification code information
CREATE TABLE IF NOT EXISTS email_codes (
  id BIGSERIAL PRIMARY KEY,           -- 主键
  email TEXT NOT NULL,                -- 邮箱
  code TEXT NOT NULL,                 -- 验证码
  purpose TEXT NOT NULL,              -- 用途（如 register, reset_password 等）
  expires_at TIMESTAMPTZ NOT NULL,    -- 过期时间
  used BOOLEAN NOT NULL DEFAULT FALSE, -- 是否已用
  created_at TIMESTAMPTZ NOT NULL DEFAULT now() -- 创建时间
);

-- 清理过期或已使用的邮箱验证码
-- Clean up expired or used email verification codes
SELECT cron.schedule(
  'clean_expired_email_codes',
  '0 3 * * *',
  $$DELETE FROM email_codes WHERE expires_at < now() OR used = TRUE$$
);



ALTER USER admin WITH PASSWORD '123456';

-- 设备参数表：存储设备的可调参数（所有参数放在para字段，JSONB格式，方便扩展）
CREATE TABLE IF NOT EXISTS device_para (
  id BIGSERIAL PRIMARY KEY,                -- 参数ID，自增主键
  device_id BIGINT NOT NULL UNIQUE REFERENCES devices(id) ON DELETE CASCADE, -- 设备ID，唯一
  para JSONB NOT NULL,                     -- 参数集合，JSONB格式
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now() -- 更新时间
);

-- 按 device_id 建索引，加速查询
CREATE INDEX IF NOT EXISTS idx_device_para_device_id ON device_para(device_id);

-- 设备RPC参数变更记录表：存储设备RPC参数变更的历史记录
-- Device RPC change log table: stores history of device RPC parameter changes
CREATE TABLE IF NOT EXISTS device_rpc_change_log (
  id BIGSERIAL PRIMARY KEY,
  device_id BIGINT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
  operator VARCHAR(64) NOT NULL,           -- 操作人用户名
  request_id VARCHAR(64) NOT NULL,         -- RPC唯一ID
  para_name TEXT NOT NULL,                 -- 参数名
  para_value TEXT NOT NULL,                -- 参数值（可用JSON存储复杂类型）
  status TEXT NOT NULL DEFAULT 'pending',  -- pending/success/failed/error/timeout
  message TEXT,                            -- 确认消息（设备回复的详细信息）
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  confirmed_at TIMESTAMPTZ
);

-- 添加 RPC 变更日志索引
CREATE INDEX IF NOT EXISTS idx_rpc_change_log_device_id ON device_rpc_change_log(device_id);
CREATE INDEX IF NOT EXISTS idx_rpc_change_log_request_id ON device_rpc_change_log(request_id);
CREATE INDEX IF NOT EXISTS idx_rpc_change_log_status ON device_rpc_change_log(status);
CREATE INDEX IF NOT EXISTS idx_rpc_change_log_created_at ON device_rpc_change_log(created_at DESC);

-- 当前报警表 alarms
CREATE TABLE IF NOT EXISTS alarms (
  id BIGSERIAL PRIMARY KEY,                        
  device_id BIGINT REFERENCES devices(id) ON DELETE SET NULL, 
  alarm_type TEXT NOT NULL,                        
  code INTEGER NOT NULL,        -- 这里改为 INTEGER
  level TEXT NOT NULL DEFAULT 'info',              
  extra JSONB,                                     
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'confirmed', 'cleared')), 
  first_triggered_at TIMESTAMPTZ NOT NULL DEFAULT now(), 
  last_triggered_at TIMESTAMPTZ NOT NULL DEFAULT now(),  
  repeat_count INT NOT NULL DEFAULT 1,             
  remark TEXT,                                     
  confirmed_at TIMESTAMPTZ,                        
  confirmed_by TEXT,                               
  cleared_at TIMESTAMPTZ,                          
  cleared_by TEXT,
  UNIQUE (device_id, alarm_type, code, status)      -- 唯一约束，防止重复报警
);

CREATE INDEX IF NOT EXISTS idx_alarms_device_id ON alarms(device_id);
CREATE INDEX IF NOT EXISTS idx_alarms_level ON alarms(level);
CREATE INDEX IF NOT EXISTS idx_alarms_status ON alarms(status);
CREATE INDEX IF NOT EXISTS idx_alarms_first_triggered_at ON alarms(first_triggered_at DESC);

-- 历史报警表 alarm_history
CREATE TABLE IF NOT EXISTS alarm_history (
  id BIGSERIAL PRIMARY KEY,                        
  device_id BIGINT,                                
  alarm_type TEXT NOT NULL,                        
  code INTEGER NOT NULL,        -- 这里改为 INTEGER
  level TEXT NOT NULL,                             
  extra JSONB,                                     
  status TEXT NOT NULL CHECK (status IN ('active', 'confirmed', 'cleared')), 
  first_triggered_at TIMESTAMPTZ NOT NULL,         
  last_triggered_at TIMESTAMPTZ NOT NULL,          
  repeat_count INT NOT NULL DEFAULT 1,             
  remark TEXT,                                     
  confirmed_at TIMESTAMPTZ,                        
  confirmed_by TEXT,                               
  cleared_at TIMESTAMPTZ,                          
  cleared_by TEXT,                                 
  archived_at TIMESTAMPTZ NOT NULL DEFAULT now(),  
  duration INTERVAL                                
);

CREATE INDEX IF NOT EXISTS idx_alarm_history_device_id ON alarm_history(device_id);
CREATE INDEX IF NOT EXISTS idx_alarm_history_level ON alarm_history(level);
CREATE INDEX IF NOT EXISTS idx_alarm_history_status ON alarm_history(status);
CREATE INDEX IF NOT EXISTS idx_alarm_history_first_triggered_at ON alarm_history(first_triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_alarm_history_archived_at ON alarm_history(archived_at DESC);

-- 操作日志表：记录所有敏感操作（管理员/系统任务）
CREATE TABLE IF NOT EXISTS admin_audit_log (
  id BIGSERIAL PRIMARY KEY,                -- 日志ID
  operator VARCHAR(64) NOT NULL,           -- 操作人（用户名或system_task等）
  action VARCHAR(64) NOT NULL,             -- 操作类型（如 delete_alarm_history, cleanup_rpc_log 等）
  params JSONB,                            -- 操作参数（如请求体、条件等）
  result JSONB,                            -- 操作结果（如删除数量、异常信息等）
  created_at TIMESTAMPTZ NOT NULL DEFAULT now() -- 操作时间
);

CREATE INDEX IF NOT EXISTS idx_admin_audit_log_operator ON admin_audit_log(operator);
CREATE INDEX IF NOT EXISTS idx_admin_audit_log_action ON admin_audit_log(action);
CREATE INDEX IF NOT EXISTS idx_admin_audit_log_created_at ON admin_audit_log(created_at DESC);

-- 参数模板表：存储设备参数的模板定义
-- Parameter template table: stores template definitions for device parameters
CREATE TABLE IF NOT EXISTS para_template (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  device_type   TEXT NOT NULL,
  para_code     TEXT NOT NULL,
  para_name     TEXT NOT NULL,
  data_type     TEXT NOT NULL,   -- int/float/string/bool（客户端校验为主）
  unit          TEXT,
  default_value TEXT,
  min_value     TEXT,
  max_value     TEXT,
  editable      BOOLEAN NOT NULL DEFAULT TRUE,
  group_name    TEXT,            -- 可选：用于前端分组
  remark        TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_para_template_data_type CHECK (data_type IN ('int','float','string','bool')),
  CONSTRAINT uq_para_def UNIQUE (device_type, para_code)
);
CREATE INDEX IF NOT EXISTS idx_para_template_group ON para_template(device_type, group_name);

-- 国家参数表（与 para_template 列对齐，按国家+设备类型+参数代号逐行定义覆盖值）
CREATE TABLE IF NOT EXISTS country_para (
  id BIGSERIAL PRIMARY KEY,
  country_code  TEXT NOT NULL,                -- 如 CN/DE/US
  device_type   TEXT NOT NULL,                -- 与 para_template.device_type 对应
  para_code     TEXT NOT NULL,                -- 与 para_template.para_code 对应
  para_name     TEXT NOT NULL,                -- 可冗余，便于直接展示（也可由应用层联表获取）
  data_type     TEXT NOT NULL,                -- int/float/string/bool（校验放后端/客户端）
  unit          TEXT,
  default_value TEXT,                         -- 该国家的默认值（用于覆盖模板默认值）
  min_value     TEXT,                         -- 该国家的范围（如与模板不同）
  max_value     TEXT,
  editable      BOOLEAN NOT NULL DEFAULT TRUE,
  group_name    TEXT,                         -- 可选：前端分组
  remark        TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_country_para_data_type CHECK (data_type IN ('int','float','string','bool')),
  CONSTRAINT uq_country_para UNIQUE (country_code, device_type, para_code),
  CONSTRAINT fk_country_para_template
    FOREIGN KEY (device_type, para_code)
    REFERENCES para_template (device_type, para_code)
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_country_para_group
  ON country_para(country_code, device_type, group_name);

CREATE INDEX IF NOT EXISTS idx_country_para_code
  ON country_para(country_code, device_type, para_code);

-- 客户端设备参数表：存储客户自定义的设备参数
CREATE TABLE IF NOT EXISTS customer_para (
  id BIGSERIAL PRIMARY KEY,
  user_id       BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  device_type   TEXT NOT NULL,
  para_code     TEXT NOT NULL,
  para_name     TEXT NOT NULL,
  data_type     TEXT NOT NULL,
  unit          TEXT,
  default_value TEXT,
  min_value     TEXT,
  max_value     TEXT,
  editable      BOOLEAN NOT NULL DEFAULT TRUE,
  group_name    TEXT,
  remark        TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_customer_para_data_type CHECK (data_type IN ('int','float','string','bool')),
  CONSTRAINT uq_customer_para UNIQUE (user_id, device_type, para_code),
  CONSTRAINT fk_customer_para_template
    FOREIGN KEY (device_type, para_code)
    REFERENCES para_template (device_type, para_code)
    ON DELETE CASCADE
);

