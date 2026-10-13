-- =========================================================================
-- DataVault Dedicated Application Database Initialization
-- Used by the DataVault Telemetry & Operations Dashboard
-- =========================================================================

-- Telemetry snapshots table (stores periodic raw collector samples & metric states)
CREATE TABLE IF NOT EXISTS datavault_snapshots (
    id SERIAL PRIMARY KEY,
    metric_type VARCHAR(64) NOT NULL,
    ts DOUBLE PRECISION NOT NULL,
    payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dv_snapshots_type_ts ON datavault_snapshots(metric_type, ts);

-- Operational audit and failover history log
CREATE TABLE IF NOT EXISTS datavault_audit_logs (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(64) NOT NULL,
    details TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index recommendations historical log
CREATE TABLE IF NOT EXISTS datavault_index_recommendations (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(128) NOT NULL,
    suggested_columns TEXT[],
    confidence VARCHAR(32),
    seq_scan_ratio NUMERIC,
    estimated_rows BIGINT,
    generated_sql TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO datavault_audit_logs (event_type, details)
VALUES ('SYSTEM_INIT', 'DataVault application database initialized successfully.');
