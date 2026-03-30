-- ============================================================
-- SRSID PostgreSQL Schema
-- Run with: psql -U srsid_user -d srsid_db -f db/schema.sql
-- ============================================================

-- Clean slate (drop in reverse dependency order)
DROP TABLE IF EXISTS vendor_news        CASCADE;
DROP TABLE IF EXISTS explanations       CASCADE;
DROP TABLE IF EXISTS segments           CASCADE;
DROP TABLE IF EXISTS risk_scores        CASCADE;
DROP TABLE IF EXISTS delivery_events    CASCADE;
DROP TABLE IF EXISTS contracts          CASCADE;
DROP TABLE IF EXISTS transactions       CASCADE;
DROP TABLE IF EXISTS vendors            CASCADE;

-- ============================================================
-- 1. VENDORS  (master — one row per real SAP vendor)
-- Source: LFA1
-- ============================================================
CREATE TABLE vendors (
    vendor_id               VARCHAR(20)  PRIMARY KEY,   -- SAP LIFNR
    supplier_name           VARCHAR(500) NOT NULL,
    country_code            VARCHAR(5),                 -- ISO e.g. DE, US
    city                    VARCHAR(200),
    industry_code           VARCHAR(20),                -- SAP BRSCH code
    industry                VARCHAR(200),               -- decoded label
    industry_category       VARCHAR(200),               -- SRSID category

    -- Risk scores (computed by ML pipeline)
    financial_stability     FLOAT,                      -- 0–100
    delivery_performance    FLOAT,                      -- 0–100 (OTIF %)
    supply_risk_score       FLOAT,                      -- 0–1
    composite_risk_score    FLOAT,                      -- 0–1
    risk_label              VARCHAR(20),                -- High / Medium / Low
    geo_risk                VARCHAR(20),                -- High / Medium / Low

    -- Spend (from EKKO+EKPO)
    total_annual_spend      FLOAT,
    transaction_count       INT,
    avg_order_value         FLOAT,
    spend_pct_of_portfolio  FLOAT,                      -- % of total spend

    -- News signals (updated by news_ingestion.py)
    news_sentiment_30d      FLOAT,                      -- -1 to +1
    disruption_count_30d    INT,
    last_disruption_type    VARCHAR(100),
    last_news_at            TIMESTAMP,

    -- OTIF (updated by sap_phase1_rebuild.py)
    otif_rate               FLOAT,                      -- 0–1
    avg_delay_days          FLOAT,
    total_deliveries        INT,

    -- Metadata
    is_active               BOOLEAN DEFAULT TRUE,
    created_at              TIMESTAMP DEFAULT NOW(),
    updated_at              TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_vendors_risk      ON vendors(risk_label);
CREATE INDEX idx_vendors_industry  ON vendors(industry_category);
CREATE INDEX idx_vendors_country   ON vendors(country_code);
CREATE INDEX idx_vendors_spend     ON vendors(total_annual_spend DESC NULLS LAST);
CREATE INDEX idx_vendors_name      ON vendors USING gin(to_tsvector('english', supplier_name));

-- ============================================================
-- 2. TRANSACTIONS  (one row per PO line item)
-- Source: EKKO + EKPO
-- ============================================================
CREATE TABLE transactions (
    id                  SERIAL       PRIMARY KEY,
    po_number           VARCHAR(20),                    -- EBELN
    po_line             VARCHAR(10),                    -- EBELP
    vendor_id           VARCHAR(20)  REFERENCES vendors(vendor_id) ON DELETE SET NULL,
    supplier_name       VARCHAR(500),
    po_date             DATE,
    year                INT,
    quarter             INT,                            -- 1–4
    month               INT,                            -- 1–12
    year_quarter        VARCHAR(10),                    -- e.g. 2024-Q3
    material_number     VARCHAR(50),
    material_group      VARCHAR(50),
    quantity            FLOAT,
    unit_of_measure     VARCHAR(10),
    unit_price          FLOAT,
    transaction_amount  FLOAT,
    currency            VARCHAR(5),
    plant               VARCHAR(10),
    purchasing_org      VARCHAR(10),
    company_code        VARCHAR(10),
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_tx_vendor_id   ON transactions(vendor_id);
CREATE INDEX idx_tx_date        ON transactions(po_date DESC);
CREATE INDEX idx_tx_quarter     ON transactions(year, quarter);
CREATE INDEX idx_tx_amount      ON transactions(transaction_amount DESC);

-- ============================================================
-- 3. DELIVERY EVENTS  (one row per PO line — promised vs actual)
-- Source: EKET + EKBE
-- ============================================================
CREATE TABLE delivery_events (
    id              SERIAL  PRIMARY KEY,
    po_number       VARCHAR(20),
    po_line         VARCHAR(10),
    vendor_id       VARCHAR(20) REFERENCES vendors(vendor_id) ON DELETE SET NULL,
    supplier_name   VARCHAR(500),
    material_number VARCHAR(50),
    plant           VARCHAR(10),
    promised_date   DATE,
    actual_date     DATE,
    promised_qty    FLOAT,
    actual_qty      FLOAT,
    delay_days      INT,                -- actual - promised (negative = early)
    on_time         SMALLINT,           -- 1 = on time, 0 = late
    in_full         SMALLINT,           -- 1 = full qty, 0 = short
    otif            SMALLINT,           -- 1 = on time AND in full
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_del_vendor_id  ON delivery_events(vendor_id);
CREATE INDEX idx_del_otif       ON delivery_events(otif);
CREATE INDEX idx_del_delay      ON delivery_events(delay_days);
CREATE INDEX idx_del_date       ON delivery_events(actual_date DESC);

-- ============================================================
-- 4. CONTRACTS  (one row per contract)
-- Source: EKKO where BSTYP = 'K'
-- ============================================================
CREATE TABLE contracts (
    contract_number     VARCHAR(20) PRIMARY KEY,        -- EBELN
    vendor_id           VARCHAR(20) REFERENCES vendors(vendor_id) ON DELETE SET NULL,
    supplier_name       VARCHAR(500),
    contract_start      DATE,                           -- KDATB
    contract_end        DATE,                           -- KDATE
    days_to_expiry      INT,                            -- computed
    contract_status     VARCHAR(50),                    -- Active / Expires <30d / Expired
    currency            VARCHAR(5),
    purchasing_org      VARCHAR(10),
    company_code        VARCHAR(10),
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_contracts_vendor_id    ON contracts(vendor_id);
CREATE INDEX idx_contracts_expiry       ON contracts(days_to_expiry);
CREATE INDEX idx_contracts_status       ON contracts(contract_status);
CREATE INDEX idx_contracts_end_date     ON contracts(contract_end);

-- ============================================================
-- 5. RISK SCORES  (ML output — one row per vendor per run)
-- Source: phase3_risk_prediction.py
-- ============================================================
CREATE TABLE risk_scores (
    id                  SERIAL PRIMARY KEY,
    vendor_id           VARCHAR(20) REFERENCES vendors(vendor_id) ON DELETE CASCADE,
    supplier_name       VARCHAR(500),
    run_id              VARCHAR(50),                    -- pipeline run identifier
    run_date            TIMESTAMP DEFAULT NOW(),

    -- Model outputs
    risk_probability    FLOAT,                          -- 0–1 probability of High risk
    risk_label          VARCHAR(20),                    -- High / Medium / Low
    risk_label_3class   VARCHAR(20),                    -- same, explicit 3-class
    predicted_tier      VARCHAR(20),

    -- Feature values used
    composite_risk_score    FLOAT,
    financial_stability     FLOAT,
    delivery_performance    FLOAT,
    supply_risk_score       FLOAT,
    news_sentiment_30d      FLOAT,
    disruption_count_30d    INT,

    -- Model metadata
    model_type          VARCHAR(50),                    -- RF / XGBoost / Ensemble
    model_version       VARCHAR(20),
    confidence          FLOAT
);

CREATE INDEX idx_risk_vendor_id ON risk_scores(vendor_id);
CREATE INDEX idx_risk_run_date  ON risk_scores(run_date DESC);
CREATE INDEX idx_risk_label     ON risk_scores(risk_label);

-- View: latest risk score per vendor
CREATE OR REPLACE VIEW latest_risk_scores AS
SELECT DISTINCT ON (vendor_id) *
FROM risk_scores
ORDER BY vendor_id, run_date DESC;

-- ============================================================
-- 6. SEGMENTS  (Kraljic + K-Means + ABC output)
-- Source: phase3_supplier_segmentation.py
-- ============================================================
CREATE TABLE segments (
    id                  SERIAL PRIMARY KEY,
    vendor_id           VARCHAR(20) REFERENCES vendors(vendor_id) ON DELETE CASCADE,
    supplier_name       VARCHAR(500),
    run_date            TIMESTAMP DEFAULT NOW(),

    -- Kraljic matrix
    kraljic_segment     VARCHAR(50),                    -- Strategic / Leverage / Bottleneck / Tactical
    supply_risk_score   FLOAT,
    profit_impact_score FLOAT,

    -- K-Means clustering
    cluster_id          INT,
    cluster_label       VARCHAR(100),

    -- ABC spend analysis
    abc_class           VARCHAR(5),                     -- A / B / C
    spend_rank          INT,

    -- Risk-spend quadrant
    risk_spend_quadrant VARCHAR(100),

    -- Recommended action
    strategic_action    TEXT
);

CREATE INDEX idx_seg_vendor_id  ON segments(vendor_id);
CREATE INDEX idx_seg_kraljic    ON segments(kraljic_segment);
CREATE INDEX idx_seg_abc        ON segments(abc_class);

-- View: latest segment per vendor
CREATE OR REPLACE VIEW latest_segments AS
SELECT DISTINCT ON (vendor_id) *
FROM segments
ORDER BY vendor_id, run_date DESC;

-- ============================================================
-- 7. EXPLANATIONS  (SHAP / XAI output per vendor)
-- Source: phase3_explainability.py
-- ============================================================
CREATE TABLE explanations (
    id                  SERIAL PRIMARY KEY,
    vendor_id           VARCHAR(20) REFERENCES vendors(vendor_id) ON DELETE CASCADE,
    supplier_name       VARCHAR(500),
    run_date            TIMESTAMP DEFAULT NOW(),

    -- Top risk drivers
    driver_1_label      VARCHAR(200),
    driver_1_value      VARCHAR(200),
    driver_1_shap       FLOAT,
    driver_2_label      VARCHAR(200),
    driver_2_value      VARCHAR(200),
    driver_2_shap       FLOAT,
    driver_3_label      VARCHAR(200),
    driver_3_value      VARCHAR(200),
    driver_3_shap       FLOAT,

    -- Main mitigator (positive factor)
    mitigator_label     VARCHAR(200),
    mitigator_value     VARCHAR(200),
    mitigator_shap      FLOAT,

    -- Human-readable narrative
    narrative           TEXT,

    -- Metadata
    predicted_risk_tier VARCHAR(20),
    methods_agree       VARCHAR(5)                      -- YES / NO
);

CREATE INDEX idx_expl_vendor_id ON explanations(vendor_id);

-- View: latest explanation per vendor
CREATE OR REPLACE VIEW latest_explanations AS
SELECT DISTINCT ON (vendor_id) *
FROM explanations
ORDER BY vendor_id, run_date DESC;

-- ============================================================
-- 8. VENDOR NEWS  (per-vendor news articles)
-- Source: news_ingestion.py
-- ============================================================
CREATE TABLE vendor_news (
    id                  SERIAL PRIMARY KEY,
    vendor_id           VARCHAR(20) REFERENCES vendors(vendor_id) ON DELETE CASCADE,
    supplier_name       VARCHAR(500),
    article_id          VARCHAR(64) UNIQUE,             -- SHA256 dedup key
    title               TEXT,
    description         TEXT,
    url                 TEXT,
    source_name         VARCHAR(200),
    api_source          VARCHAR(50),                    -- newsapi / guardian / gdelt
    published_at        TIMESTAMP,
    fetched_at          TIMESTAMP DEFAULT NOW(),
    sentiment_score     FLOAT,                          -- -1 to +1
    disruption_type     VARCHAR(100),
    disruption_flag     BOOLEAN DEFAULT FALSE,
    days_lookback       INT
);

CREATE INDEX idx_news_vendor_id     ON vendor_news(vendor_id);
CREATE INDEX idx_news_published     ON vendor_news(published_at DESC);
CREATE INDEX idx_news_disruption    ON vendor_news(disruption_flag) WHERE disruption_flag = TRUE;
CREATE INDEX idx_news_sentiment     ON vendor_news(sentiment_score);

-- ============================================================
-- HELPER: update vendors.updated_at automatically
-- ============================================================
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER vendors_updated_at
    BEFORE UPDATE ON vendors
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ============================================================
-- SUMMARY VIEW  (used by chatbot sidebar KPIs)
-- ============================================================
CREATE OR REPLACE VIEW portfolio_summary AS
SELECT
    COUNT(*)                                        AS total_vendors,
    COUNT(*) FILTER (WHERE risk_label = 'High')     AS high_risk_count,
    COUNT(*) FILTER (WHERE risk_label = 'Medium')   AS medium_risk_count,
    COUNT(*) FILTER (WHERE risk_label = 'Low')      AS low_risk_count,
    SUM(total_annual_spend)                         AS total_portfolio_spend,
    AVG(otif_rate)                                  AS avg_otif_rate,
    AVG(news_sentiment_30d)                         AS avg_news_sentiment,
    SUM(disruption_count_30d)                       AS total_disruption_alerts
FROM vendors
WHERE is_active = TRUE;

-- ============================================================
-- Done
-- ============================================================
SELECT 'SRSID schema created successfully' AS status;
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;
