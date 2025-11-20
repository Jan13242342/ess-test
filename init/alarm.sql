-- 当前报警表 alarms
CREATE TABLE IF NOT EXISTS alarms (
  id BIGSERIAL PRIMARY KEY,
  device_id BIGINT REFERENCES devices(id) ON DELETE SET NULL,
  alarm_type TEXT NOT NULL,
  code INTEGER NOT NULL,
  level TEXT NOT NULL DEFAULT 'info',
  extra JSONB,
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'cleared')),  -- 只保留两种状态
  first_triggered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_triggered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  repeat_count INT NOT NULL DEFAULT 1,
  remark TEXT,
  cleared_at TIMESTAMPTZ,
  cleared_by TEXT,
  ack BOOLEAN NOT NULL DEFAULT FALSE,         -- 新增：是否已确认
  ack_by TEXT,                                -- 新增：确认人
  ack_time TIMESTAMPTZ,                       -- 新增：确认时间
  UNIQUE (device_id, alarm_type, code)
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
  code INTEGER NOT NULL,
  level TEXT NOT NULL,
  extra JSONB,
  status TEXT NOT NULL CHECK (status IN ('active', 'cleared')),  -- 只保留两种状态
  first_triggered_at TIMESTAMPTZ NOT NULL,
  last_triggered_at TIMESTAMPTZ NOT NULL,
  repeat_count INT NOT NULL DEFAULT 1,
  remark TEXT,
  cleared_at TIMESTAMPTZ,
  cleared_by TEXT,
  ack BOOLEAN NOT NULL DEFAULT FALSE,         -- 新增：是否已确认
  ack_by TEXT,                                -- 新增：确认人
  ack_time TIMESTAMPTZ,                       -- 新增：确认时间
  archived_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  duration INTERVAL
);

CREATE INDEX IF NOT EXISTS idx_alarm_history_device_id ON alarm_history(device_id);
CREATE INDEX IF NOT EXISTS idx_alarm_history_level ON alarm_history(level);
CREATE INDEX IF NOT EXISTS idx_alarm_history_status ON alarm_history(status);
CREATE INDEX IF NOT EXISTS idx_alarm_history_first_triggered_at ON alarm_history(first_triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_alarm_history_archived_at ON alarm_history(archived_at DESC);