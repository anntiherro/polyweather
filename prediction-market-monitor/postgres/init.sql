-- polyweather schema

-- Correlations: one row per market × weather observation
CREATE TABLE IF NOT EXISTS correlations (
    id                      SERIAL PRIMARY KEY,
    condition_id            TEXT NOT NULL,
    question                TEXT,
    question_type           TEXT,          -- HIGHEST / LOWEST / EXACT
    location_name           TEXT,
    yes_price               NUMERIC(6,4),
    target_temp             NUMERIC(6,2),
    actual_temp             NUMERIC(6,2),
    data_type               TEXT,          -- forecast / historical
    market_closed           BOOLEAN,
    actual_outcome          SMALLINT,      -- 1 = correct, 0 = incorrect
    prediction_error        NUMERIC(6,4),
    correlation_latency_sec INTEGER,
    arbitrage_flag          BOOLEAN,
    poll_timestamp          TIMESTAMPTZ,
    inserted_at             TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_correlations_location  ON correlations (location_name);
CREATE INDEX IF NOT EXISTS idx_correlations_condition ON correlations (condition_id);
CREATE INDEX IF NOT EXISTS idx_correlations_poll_ts   ON correlations (poll_timestamp);

-- Accuracy aggregates: sliding window summaries per city
CREATE TABLE IF NOT EXISTS accuracy_windows (
    id                      SERIAL PRIMARY KEY,
    location_name           TEXT NOT NULL,
    window_start            TIMESTAMPTZ,
    window_end              TIMESTAMPTZ,
    total_count             INTEGER,
    correct_count           INTEGER,
    accuracy_rate           NUMERIC(6,4),
    avg_prediction_error    NUMERIC(6,4),
    max_prediction_error    NUMERIC(6,4),
    bias_score              NUMERIC(8,4),
    closed_count            INTEGER,
    closed_correct_count    INTEGER,
    closed_accuracy_rate    NUMERIC(6,4),
    inserted_at             TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_accuracy_location   ON accuracy_windows (location_name);
CREATE INDEX IF NOT EXISTS idx_accuracy_window_end ON accuracy_windows (window_end);

-- Anomaly alerts
CREATE TABLE IF NOT EXISTS alerts (
    id                SERIAL PRIMARY KEY,
    alert_type        TEXT NOT NULL,     -- ACCURACY_DROP / ACCURACY_ANOMALY / ARBITRAGE / HIGH_ERROR
    location_name     TEXT,
    condition_id      TEXT,
    detail            TEXT,
    poll_timestamp    TIMESTAMPTZ,
    alert_timestamp   TIMESTAMPTZ,
    inserted_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_type        ON alerts (alert_type);
CREATE INDEX IF NOT EXISTS idx_alerts_location    ON alerts (location_name);
CREATE INDEX IF NOT EXISTS idx_alerts_alert_ts    ON alerts (alert_timestamp);
