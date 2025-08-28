-- 1. 先建依赖表
CREATE TABLE IF NOT EXISTS users (
  id BIGSERIAL PRIMARY KEY,
  username TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dealers (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

-- 2. 再建其它表和类型
-- 枚举类型
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'device_status') THEN
    CREATE TYPE device_status AS ENUM ('active','inactive','installing','fault','retired');
  END IF;
END$$;

-- 设备主表
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

-- 常用索引
CREATE INDEX IF NOT EXISTS ix_devices_user_id    ON devices(user_id);
CREATE INDEX IF NOT EXISTS ix_devices_dealer_id  ON devices(dealer_id);
CREATE INDEX IF NOT EXISTS ix_devices_location   ON devices USING GIN(location);

-- 可选：待绑定设备的加速索引
CREATE INDEX IF NOT EXISTS ix_devices_unbound_created
  ON devices(created_at DESC)
  WHERE user_id IS NULL;

-- ess_realtime_data
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

-- history_energy
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

CREATE TABLE IF NOT EXISTS history_energy_2025_08 PARTITION OF history_energy
  FOR VALUES FROM ('2025-08-01 00:00:00') TO ('2025-09-01 00:00:00');

-- 以后每月建一个分区即可

CREATE EXTENSION IF NOT EXISTS pg_cron;

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

SELECT cron.schedule(
  'create_next_history_energy_partition',
  '0 0 1 * *',
  'SELECT create_next_history_energy_partition();'
);
