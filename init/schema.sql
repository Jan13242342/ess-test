CREATE TABLE IF NOT EXISTS ess_realtime_data (
  customer_id       TEXT    NOT NULL,
  dealer_id         TEXT    NOT NULL,
  device_id         TEXT    PRIMARY KEY,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  soc               SMALLINT NOT NULL CHECK (soc BETWEEN 0 AND 100),
  soh               SMALLINT NOT NULL DEFAULT 100 CHECK (soh BETWEEN 0 AND 100),
  pv                INTEGER  NOT NULL DEFAULT 0,
  load              INTEGER  NOT NULL DEFAULT 0,
  grid              INTEGER  NOT NULL DEFAULT 0,
  grid_q            INTEGER  NOT NULL DEFAULT 0,
  batt              INTEGER  NOT NULL DEFAULT 0,
  ac_v              INTEGER  NOT NULL DEFAULT 0,
  ac_f              INTEGER  NOT NULL DEFAULT 0,
  v_a               INTEGER  NOT NULL DEFAULT 0,
  v_b               INTEGER  NOT NULL DEFAULT 0,
  v_c               INTEGER  NOT NULL DEFAULT 0,
  i_a               INTEGER  NOT NULL DEFAULT 0,
  i_b               INTEGER  NOT NULL DEFAULT 0,
  i_c               INTEGER  NOT NULL DEFAULT 0,
  p_a               INTEGER  NOT NULL DEFAULT 0,
  p_b               INTEGER  NOT NULL DEFAULT 0,
  p_c               INTEGER  NOT NULL DEFAULT 0,
  q_a               INTEGER  NOT NULL DEFAULT 0,
  q_b               INTEGER  NOT NULL DEFAULT 0,
  q_c               INTEGER  NOT NULL DEFAULT 0,
  e_pv_today        BIGINT   NOT NULL DEFAULT 0,
  e_load_today      BIGINT   NOT NULL DEFAULT 0,
  e_charge_today    BIGINT   NOT NULL DEFAULT 0,
  e_discharge_today BIGINT   NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_ess_customer ON ess_realtime_data (customer_id);
CREATE INDEX IF NOT EXISTS idx_ess_dealer   ON ess_realtime_data (dealer_id);
CREATE INDEX IF NOT EXISTS idx_ess_updated  ON ess_realtime_data (updated_at DESC);
