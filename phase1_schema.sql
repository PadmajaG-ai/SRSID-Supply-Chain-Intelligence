-- ========================================
-- SUPPLIER RISK DATABASE - PHASE 1 SCHEMA
-- ========================================
-- This script creates all tables needed for Phase 1: Data Ingestion & Normalization

-- Create database (if needed)
-- CREATE DATABASE supplier_risk_db;

-- ========================================
-- TABLE 1: RAW SUPPLIER RISK ASSESSMENT
-- ========================================
CREATE TABLE IF NOT EXISTS raw_supplier_risk_assessment (
    id SERIAL PRIMARY KEY,
    source_file_name VARCHAR(255),
    source_file_hash VARCHAR(64),
    
    -- Original CSV fields (standardized column names)
    supplier_name VARCHAR(500) NOT NULL,
    financial_stability_score FLOAT,
    delivery_performance_score FLOAT,
    historical_risk_category VARCHAR(50),  -- High, Medium, Low
    
    -- Audit fields
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_status VARCHAR(50) DEFAULT 'clean',  -- clean, null_found, duplicate, error
    quality_flags JSONB,  -- {"missing_fields": [...], "outlier": true}
    
    CONSTRAINT unique_raw_assessment UNIQUE(supplier_name, source_file_name, ingestion_timestamp)
);

CREATE INDEX IF NOT EXISTS idx_raw_assessment_supplier ON raw_supplier_risk_assessment(supplier_name);
CREATE INDEX IF NOT EXISTS idx_raw_assessment_ingestion ON raw_supplier_risk_assessment(ingestion_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_raw_assessment_status ON raw_supplier_risk_assessment(row_status);

-- ========================================
-- TABLE 2: RAW SUPPLY CHAIN TRANSACTIONS
-- ========================================
CREATE TABLE IF NOT EXISTS raw_supply_chain_transactions (
    id SERIAL PRIMARY KEY,
    source_file_name VARCHAR(255),
    source_file_hash VARCHAR(64),
    
    -- Original CSV fields
    supplier_name VARCHAR(500) NOT NULL,
    transaction_amount NUMERIC(15, 2),
    transaction_date DATE,
    disruption_type VARCHAR(100),  -- Strike, Weather, Shortage, etc.
    disruption_details TEXT,
    
    -- Additional fields from CSV
    procurement_category VARCHAR(255),
    invoice_number VARCHAR(100),
    purchase_order_number VARCHAR(100),
    
    -- Audit fields
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_status VARCHAR(50) DEFAULT 'clean',
    quality_flags JSONB,
    
    CONSTRAINT unique_transaction UNIQUE(supplier_name, invoice_number, ingestion_timestamp)
);

CREATE INDEX IF NOT EXISTS idx_transactions_supplier ON raw_supply_chain_transactions(supplier_name);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON raw_supply_chain_transactions(transaction_date DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_disruption ON raw_supply_chain_transactions(disruption_type);
CREATE INDEX IF NOT EXISTS idx_transactions_amount ON raw_supply_chain_transactions(transaction_amount);

-- ========================================
-- TABLE 3: RAW KRALJIC MATRIX
-- ========================================
CREATE TABLE IF NOT EXISTS raw_kraljic_matrix (
    id SERIAL PRIMARY KEY,
    source_file_name VARCHAR(255),
    source_file_hash VARCHAR(64),
    
    -- Original CSV fields
    supplier_name VARCHAR(500) NOT NULL,
    supply_risk_score FLOAT,  -- 0 to 1
    profit_impact_score FLOAT,  -- 0 to 1
    kraljic_quadrant VARCHAR(100),  -- Strategic, Tactical, Bottleneck, Leverage
    
    -- Additional metadata
    category VARCHAR(255),
    segment VARCHAR(100),
    
    -- Audit fields
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_status VARCHAR(50) DEFAULT 'clean',
    quality_flags JSONB,
    
    CONSTRAINT unique_kraljic UNIQUE(supplier_name, ingestion_timestamp)
);

CREATE INDEX IF NOT EXISTS idx_kraljic_supplier ON raw_kraljic_matrix(supplier_name);
CREATE INDEX IF NOT EXISTS idx_kraljic_quadrant ON raw_kraljic_matrix(kraljic_quadrant);

-- ========================================
-- TABLE 4: DATA QUALITY LOG
-- ========================================
CREATE TABLE IF NOT EXISTS data_quality_log (
    id SERIAL PRIMARY KEY,
    ingestion_run_id UUID NOT NULL,
    pipeline_phase VARCHAR(100),  -- Phase 1, Phase 2, etc.
    source_table VARCHAR(100),  -- raw_supplier_risk_assessment, etc.
    issue_type VARCHAR(100),  -- missing_field, duplicate, outlier, null_value
    row_id INT,
    row_data JSONB,
    issue_description TEXT,
    severity VARCHAR(50),  -- error, warning, info
    resolution VARCHAR(255),  -- dropped, imputed, flagged
    detected_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_quality_run ON data_quality_log(ingestion_run_id);
CREATE INDEX IF NOT EXISTS idx_quality_severity ON data_quality_log(severity);
CREATE INDEX IF NOT EXISTS idx_quality_source ON data_quality_log(source_table);

-- ========================================
-- TABLE 5: PIPELINE RUN LOG
-- ========================================
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    id SERIAL PRIMARY KEY,
    run_id UUID UNIQUE NOT NULL,
    pipeline_phase VARCHAR(100),  -- Phase 1, Phase 2, etc.
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50),  -- success, partial_success, failed
    
    -- Phase 1 Specific Metrics
    files_processed INT,
    rows_ingested_risk_assessment INT,
    rows_ingested_transactions INT,
    rows_ingested_kraljic INT,
    rows_with_errors INT,
    duplicates_removed INT,
    
    -- Timing
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INT,
    
    -- Error handling
    error_message TEXT,
    warning_count INT,
    
    -- Configuration used
    config_json JSONB,
    
    -- Next phase info
    ready_for_next_phase BOOLEAN DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_status ON pipeline_run_log(status);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_timestamp ON pipeline_run_log(run_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_run_phase ON pipeline_run_log(pipeline_phase);

-- ========================================
-- TABLE 6: FILE INTEGRITY LOG
-- ========================================
CREATE TABLE IF NOT EXISTS file_integrity_log (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255),
    file_path VARCHAR(500),
    file_hash VARCHAR(64),  -- MD5 hash of file
    file_size_bytes INT,
    file_modified_timestamp TIMESTAMP,
    last_check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50),  -- new, unchanged, modified, missing
    row_count INT,
    column_count INT
);

CREATE INDEX IF NOT EXISTS idx_file_integrity_name ON file_integrity_log(file_name);
CREATE INDEX IF NOT EXISTS idx_file_integrity_hash ON file_integrity_log(file_hash);

-- ========================================
-- TABLE 7: SUPPLIER SPENDING SUMMARY (Helper Table)
-- ========================================
-- Will be populated during Phase 3, but create structure now
CREATE TABLE IF NOT EXISTS supplier_annual_spend (
    id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(500) NOT NULL UNIQUE,
    total_annual_spend NUMERIC(15, 2),
    transaction_count INT,
    last_transaction_date DATE,
    calculated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_supplier_spend ON supplier_annual_spend(total_annual_spend DESC);

-- ========================================
-- GRANTS (Optional - Adjust as needed)
-- ========================================
-- GRANT CONNECT ON DATABASE supplier_risk_db TO srsid_user;
-- GRANT USAGE ON SCHEMA public TO srsid_user;
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO srsid_user;
