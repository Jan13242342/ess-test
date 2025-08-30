-- 1. 先建依赖表 (Create dependency tables first)
CREATE TABLE IF NOT EXISTS users (
  id BIGSERIAL PRIMARY KEY,
  username TEXT NOT NULL UNIQUE,
  email TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dealers (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

-- 2. 再建其它表和类型 (Then create other tables and types)
-- 枚举类型 (Enum type)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'device_status') THEN
    CREATE TYPE device_status AS ENUM ('active','inactive','installing','fault','retired');
  END IF;
END$$;

-- 设备主表 (Device main table)
CREATE TABLE IF NOT EXISTS devices (
  id BIGSERIAL PRIMARY KEY,
  device_sn TEXT NOT NULL UNIQUE,
  model TEXT,
  firmware_version TEXT,
  user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
  dealer_id BIGINT REFERENCES dealers(id) ON DELETE SET NULL,
  location JSONB,
  status device_status,
  installed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 常用索引 (Common indexes)
CREATE INDEX IF NOT EXISTS ix_devices_user_id    ON devices(user_id);
CREATE INDEX IF NOT EXISTS ix_devices_dealer_id  ON devices(dealer_id);
CREATE INDEX IF NOT EXISTS ix_devices_location   ON devices USING GIN(location);

-- 可选：待绑定设备的加速索引 (Optional: index for unbound devices)
CREATE INDEX IF NOT EXISTS ix_devices_unbound_created
  ON devices(created_at DESC)
  WHERE user_id IS NULL;

-- ess_realtime_data 实时数据表 (Real-time data table)
CREATE TABLE IF NOT EXISTS ess_realtime_data (
  dealer_id BIGINT REFERENCES dealers(id) ON DELETE SET NULL,
  device_id BIGINT PRIMARY KEY REFERENCES devices(id) ON DELETE CASCADE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  soc SMALLINT NOT NULL CHECK (soc BETWEEN 0 AND 100),
  soh SMALLINT NOT NULL DEFAULT 100 CHECK (soh BETWEEN 0 AND 100),
  pv INTEGER NOT NULL DEFAULT 0,
  load INTEGER NOT NULL DEFAULT 0,
  grid INTEGER NOT NULL DEFAULT 0,
  grid_q INTEGER NOT NULL DEFAULT 0,
  batt INTEGER NOT NULL DEFAULT 0,
  ac_v INTEGER NOT NULL DEFAULT 0,
  ac_f INTEGER NOT NULL DEFAULT 0,
  v_a INTEGER NOT NULL DEFAULT 0,
  v_b INTEGER NOT NULL DEFAULT 0,
  v_c INTEGER NOT NULL DEFAULT 0,
  i_a INTEGER NOT NULL DEFAULT 0,
  i_b INTEGER NOT NULL DEFAULT 0,
  i_c INTEGER NOT NULL DEFAULT 0,
  p_a INTEGER NOT NULL DEFAULT 0,
  p_b INTEGER NOT NULL DEFAULT 0,
  p_c INTEGER NOT NULL DEFAULT 0,
  q_a INTEGER NOT NULL DEFAULT 0,
  q_b INTEGER NOT NULL DEFAULT 0,
  q_c INTEGER NOT NULL DEFAULT 0,
  e_pv_today BIGINT NOT NULL DEFAULT 0,
  e_load_today BIGINT NOT NULL DEFAULT 0,
  e_charge_today BIGINT NOT NULL DEFAULT 0,
  e_discharge_today BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_ess_dealer   ON ess_realtime_data (dealer_id);
CREATE INDEX IF NOT EXISTS idx_ess_updated  ON ess_realtime_data (updated_at DESC);

-- history_energy 历史能耗主表 (History energy main table, partitioned)
CREATE TABLE IF NOT EXISTS history_energy (
  id BIGSERIAL,
  device_id BIGINT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
  ts TIMESTAMPTZ NOT NULL,
  charge_wh_total BIGINT,
  discharge_wh_total BIGINT,
  pv_wh_total BIGINT,
  CONSTRAINT pk_history_energy PRIMARY KEY (device_id, ts),
  CONSTRAINT chk_nonneg_charge CHECK (charge_wh_total IS NULL OR charge_wh_total >= 0),
  CONSTRAINT chk_nonneg_discharge CHECK (discharge_wh_total IS NULL OR discharge_wh_total >= 0),
  CONSTRAINT chk_nonneg_pv CHECK (pv_wh_total IS NULL OR pv_wh_total >= 0)
) PARTITION BY RANGE (ts);

-- 自动创建本月分区（首次部署时保证本月可用）
-- (Auto-create current month's partition to ensure availability on first deployment)
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

-- 安装 pg_cron 扩展 (Install pg_cron extension)
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- 自动创建下个月分区的函数 (Function to auto-create next month's partition)
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

-- 每月1号自动创建下个月分区 (Schedule: auto-create next month's partition on the 1st of each month)
SELECT cron.schedule(
  'create_next_history_energy_partition',
  '0 0 1 * *',
  'SELECT create_next_history_energy_partition();'
);

-- 初始化10个测试用户 (Init 10 test users)
INSERT INTO users (username, email, password_hash) VALUES
  ('testuser1', 'test1@example.com', 'dummyhash'),
  ('testuser2', 'test2@example.com', 'dummyhash'),
  ('testuser3', 'test3@example.com', 'dummyhash'),
  ('testuser4', 'test4@example.com', 'dummyhash'),
  ('testuser5', 'test5@example.com', 'dummyhash'),
  ('testuser6', 'test6@example.com', 'dummyhash'),
  ('testuser7', 'test7@example.com', 'dummyhash'),
  ('testuser8', 'test8@example.com', 'dummyhash'),
  ('testuser9', 'test9@example.com', 'dummyhash'),
  ('testuser10', 'test10@example.com', 'dummyhash')
ON CONFLICT DO NOTHING;

-- 初始化10个测试经销商 (Init 10 test dealers)
INSERT INTO dealers (id, name) VALUES
  (1, '测试经销商1'),
  (2, '测试经销商2'),
  (3, '测试经销商3'),
  (4, '测试经销商4'),
  (5, '测试经销商5'),
  (6, '测试经销商6'),
  (7, '测试经销商7'),
  (8, '测试经销商8'),
  (9, '测试经销商9'),
  (10, '测试经销商10')
ON CONFLICT DO NOTHING;
------------------------------------------
------------------------------------------
------------------------------------------
-- 初始化10个测试设备 (Init 10 test devices)
INSERT INTO devices (
  id, device_sn, model, firmware_version, user_id, dealer_id, location, status, installed_at, created_at
) VALUES
  (1, 'SN001', 'MODEL_X', 'v1.0', 1, 1, NULL, 'active', now(), now()),
  (2, 'SN002', 'MODEL_X', 'v1.0', 2, 2, NULL, 'active', now(), now()),
  (3, 'SN003', 'MODEL_X', 'v1.0', 3, 3, NULL, 'active', now(), now()),
  (4, 'SN004', 'MODEL_X', 'v1.0', 4, 4, NULL, 'active', now(), now()),
  (5, 'SN005', 'MODEL_X', 'v1.0', 5, 5, NULL, 'active', now(), now()),
  (6, 'SN006', 'MODEL_X', 'v1.0', 6, 6, NULL, 'active', now(), now()),
  (7, 'SN007', 'MODEL_X', 'v1.0', 7, 7, NULL, 'active', now(), now()),
  (8, 'SN008', 'MODEL_X', 'v1.0', 8, 8, NULL, 'active', now(), now()),
  (9, 'SN009', 'MODEL_X', 'v1.0', 9, 9, NULL, 'active', now(), now()),
  (10, 'SN010', 'MODEL_X', 'v1.0', 10, 10, NULL, 'active', now(), now())
ON CONFLICT DO NOTHING;
