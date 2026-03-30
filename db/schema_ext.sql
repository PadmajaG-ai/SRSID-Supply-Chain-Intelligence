-- ============================================================
-- SRSID Schema Extension — Alternatives + Anomalies
-- Run with: psql -U srsid_user -d srsid_db -h localhost -f db/schema_ext.sql
-- ============================================================

-- Alternative supplier recommendations
-- (one row per at-risk vendor × alternative vendor pair)
DROP TABLE IF EXISTS vendor_alternatives CASCADE;
CREATE TABLE vendor_alternatives (
    id                      SERIAL PRIMARY KEY,
    vendor_id               VARCHAR(20) REFERENCES vendors(vendor_id) ON DELETE CASCADE,
    supplier_name           VARCHAR(500),
    risk_score              FLOAT,
    risk_tier               VARCHAR(20),

    alt_vendor_id           VARCHAR(20),
    alt_supplier_name       VARCHAR(500),
    alt_risk_score          FLOAT,
    alt_risk_tier           VARCHAR(20),
    alt_country             VARCHAR(10),
    alt_industry            VARCHAR(200),

    alternative_rank        INT,          -- 1 = best match
    similarity_score        FLOAT,        -- cosine similarity 0-1
    recommendation_reason   TEXT,

    run_date                TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_alt_vendor_id  ON vendor_alternatives(vendor_id);
CREATE INDEX idx_alt_risk_tier  ON vendor_alternatives(risk_tier);
CREATE INDEX idx_alt_rank       ON vendor_alternatives(alternative_rank);

-- Anomaly detection results
-- (one row per vendor per run)
DROP TABLE IF EXISTS vendor_anomalies CASCADE;
CREATE TABLE vendor_anomalies (
    id                      SERIAL PRIMARY KEY,
    vendor_id               VARCHAR(20) REFERENCES vendors(vendor_id) ON DELETE CASCADE,
    supplier_name           VARCHAR(500),

    is_anomalous            BOOLEAN DEFAULT FALSE,
    total_anomaly_flags     INT DEFAULT 0,

    -- Isolation Forest
    anomaly_if_score        FLOAT,        -- higher = more anomalous
    anomaly_if_flag         BOOLEAN DEFAULT FALSE,

    -- Z-score
    max_zscore              FLOAT,
    zscore_feature          VARCHAR(100), -- which feature had max z-score
    anomaly_zscore_flag     BOOLEAN DEFAULT FALSE,

    -- Rule-based
    rule_based_flag         BOOLEAN DEFAULT FALSE,
    rule_based_reason       TEXT,

    -- Context
    composite_risk_score    FLOAT,
    total_annual_spend      FLOAT,
    financial_stability     FLOAT,

    run_date                TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_anom_vendor_id   ON vendor_anomalies(vendor_id);
CREATE INDEX idx_anom_flag        ON vendor_anomalies(is_anomalous) WHERE is_anomalous;
CREATE INDEX idx_anom_score       ON vendor_anomalies(anomaly_if_score DESC);

SELECT 'Schema extension applied: vendor_alternatives + vendor_anomalies' AS status;

-- ============================================================
-- Vendor Evaluation Metrics Extension
-- Adds computable + manual scorecard columns to vendors table
-- ============================================================

-- Computable from SAP delivery_events
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS ottr_rate            FLOAT;        -- On-Time To Request %
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS lead_time_variability FLOAT;       -- STDDEV of delay_days
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS order_accuracy_rate  FLOAT;        -- actual_qty / promised_qty avg
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS avg_price_variance_pct FLOAT;      -- PPV %

-- Manual / external scorecard (entered by procurement team)
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS defect_rate_ppm      FLOAT;        -- Parts Per Million defects
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS innovation_score      FLOAT;        -- 0–100 manual
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS cybersecurity_score   FLOAT;        -- 0–100 manual
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS esg_score             FLOAT;        -- 0–100 manual
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS scorecard_notes       TEXT;
ALTER TABLE vendors ADD COLUMN IF NOT EXISTS scorecard_updated_at  TIMESTAMP;

SELECT 'Vendor evaluation metric columns added' AS status;
