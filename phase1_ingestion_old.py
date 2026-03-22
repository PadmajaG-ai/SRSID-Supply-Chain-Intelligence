"""
Phase 1: Data Ingestion & Normalization
Main Script for Loading and Validating Raw Data into PostgreSQL
"""

import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import logging
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple, List, Optional
import uuid
from config_phase1 import (
    DATABASE_CONFIG,
    SOURCE_FILES,
    COLUMN_MAPPINGS,
    VALIDATION_RULES,
    ERROR_TOLERANCE,
    DATA_QUALITY_CHECKS,
    LOGGING_CONFIG,
)

# ========================================
# LOGGING SETUP
# ========================================
log_dir = Path(LOGGING_CONFIG["log_dir"])
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=LOGGING_CONFIG["log_level"],
    format=LOGGING_CONFIG["format"],
    handlers=[
        logging.FileHandler(log_dir / f"phase1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ========================================
# DATABASE UTILITIES
# ========================================
class DatabaseConnection:
    """Manages PostgreSQL connections."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.connection = None
    
    def connect(self):
        """Establish connection to PostgreSQL."""
        try:
            self.connection = psycopg2.connect(
                host=self.config["host"],
                port=self.config["port"],
                database=self.config["database"],
                user=self.config["user"],
                password=self.config["password"],
            )
            logger.info("✓ Connected to PostgreSQL")
            return self.connection
        except psycopg2.Error as e:
            logger.error(f"✗ Database connection failed: {e}")
            raise
    
    def close(self):
        """Close connection."""
        if self.connection:
            self.connection.close()
            logger.info("✓ Database connection closed")
    
    def execute_query(self, query: str, params: tuple = None):
        """Execute query and return cursor."""
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query, params or ())
            self.connection.commit()
            return cursor
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"✗ Query execution failed: {e}")
            raise


# ========================================
# FILE UTILITIES
# ========================================
def compute_file_hash(file_path: str) -> str:
    """Compute MD5 hash of file for duplicate detection."""
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def check_file_freshness(db: DatabaseConnection, file_name: str, file_hash: str) -> bool:
    """Check if file has been ingested before."""
    query = """
    SELECT COUNT(*) as count FROM file_integrity_log 
    WHERE file_name = %s AND file_hash = %s
    """
    cursor = db.execute_query(query, (file_name, file_hash))
    result = cursor.fetchone()
    is_new = result["count"] == 0
    cursor.close()
    
    if not is_new:
        logger.warning(f"⚠ File {file_name} has same hash as previous run (no changes)")
    return is_new


# ========================================
# DATA LOADING & PARSING
# ========================================
def load_csv_file(file_path: str, encoding: str = "utf-8") -> Tuple[pd.DataFrame, str]:
    """Load CSV file and compute hash."""
    try:
        df = pd.read_csv(file_path, encoding=encoding)
        file_hash = compute_file_hash(file_path)
        logger.info(f"✓ Loaded {file_path} ({len(df)} rows, {len(df.columns)} columns)")
        return df, file_hash
    except FileNotFoundError:
        logger.error(f"✗ File not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"✗ Error loading {file_path}: {e}")
        raise


def standardize_column_names(df: pd.DataFrame, field_mappings: Dict) -> Tuple[pd.DataFrame, Dict]:
    """
    Standardize column names based on configuration.
    Returns: (standardized_df, quality_report)
    """
    quality_report = {"mapped_columns": {}, "missing_columns": {}, "extra_columns": []}
    new_columns = {}
    
    for output_name, mapping_config in field_mappings.items():
        input_names = mapping_config["input_names"]
        matched = False
        
        # Try to find matching input column
        for input_name in input_names:
            if input_name in df.columns:
                new_columns[input_name] = output_name
                quality_report["mapped_columns"][input_name] = output_name
                matched = True
                break
        
        if not matched:
            if mapping_config["required"]:
                logger.error(f"✗ Required column '{output_name}' not found. Alternatives: {input_names}")
                quality_report["missing_columns"][output_name] = "required"
            else:
                logger.warning(f"⚠ Optional column '{output_name}' not found. Creating empty column.")
                df[output_name] = None
                quality_report["missing_columns"][output_name] = "optional"
    
    # Rename mapped columns
    df = df.rename(columns=new_columns)
    
    # Identify extra columns
    expected_output_cols = list(field_mappings.keys())
    extra_cols = [col for col in df.columns if col not in expected_output_cols]
    quality_report["extra_columns"] = extra_cols
    
    if extra_cols:
        logger.warning(f"⚠ Extra columns found (will be dropped): {extra_cols}")
        df = df[expected_output_cols]
    
    return df, quality_report


# ========================================
# DATA TYPE CONVERSION
# ========================================
def convert_data_types(df: pd.DataFrame, field_mappings: Dict, source_type: str) -> Tuple[pd.DataFrame, Dict]:
    """
    Convert columns to declared data types.
    Returns: (converted_df, conversion_report)
    """
    conversion_report = {
        "successful": 0,
        "failed": 0,
        "errors": [],
    }
    
    for col_name, mapping_config in field_mappings.items():
        data_type = mapping_config["data_type"]
        
        try:
            if data_type == "string":
                df[col_name] = df[col_name].astype(str).str.strip() if mapping_config.get("normalize") else df[col_name].astype(str)
                conversion_report["successful"] += 1
            
            elif data_type == "float":
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
                conversion_report["successful"] += 1
            
            elif data_type == "date":
                date_formats = mapping_config.get("date_formats", ["%Y-%m-%d"])
                for fmt in date_formats:
                    try:
                        df[col_name] = pd.to_datetime(df[col_name], format=fmt)
                        break
                    except:
                        pass
                if not pd.api.types.is_datetime64_any_dtype(df[col_name]):
                    df[col_name] = pd.to_datetime(df[col_name], errors="coerce")
                conversion_report["successful"] += 1
        
        except Exception as e:
            conversion_report["failed"] += 1
            conversion_report["errors"].append({"column": col_name, "error": str(e)})
            logger.warning(f"⚠ Type conversion failed for {col_name}: {e}")
    
    return df, conversion_report


# ========================================
# DATA VALIDATION
# ========================================
def validate_data(df: pd.DataFrame, field_mappings: Dict, source_type: str) -> Tuple[pd.DataFrame, Dict]:
    """
    Apply validation rules and flag problematic rows.
    Returns: (validated_df_with_flags, validation_report)
    """
    df["row_status"] = "clean"
    df["quality_flags"] = df.apply(lambda x: {}, axis=1)
    
    validation_report = {
        "null_values": 0,
        "validation_errors": 0,
        "outliers": 0,
        "flagged_rows": [],
    }
    
    # Check for null values
    for col_name, mapping_config in field_mappings.items():
        if not mapping_config.get("allow_null", True):
            null_mask = df[col_name].isnull()
            if null_mask.any():
                validation_report["null_values"] += null_mask.sum()
                
                # Flag rows with nulls
                for idx in df[null_mask].index:
                    df.at[idx, "row_status"] = "null_found"
                    flags = df.at[idx, "quality_flags"]
                    flags[col_name] = "null_value"
                    df.at[idx, "quality_flags"] = flags
    
    # Apply field-specific validation rules
    for col_name, mapping_config in field_mappings.items():
        if col_name not in VALIDATION_RULES:
            continue
        
        rules = VALIDATION_RULES[col_name]
        
        # String validation
        if mapping_config["data_type"] == "string":
            if "min_length" in rules:
                invalid_mask = df[col_name].apply(lambda x: len(str(x)) < rules["min_length"])
                validation_report["validation_errors"] += invalid_mask.sum()
        
        # Numeric validation
        elif mapping_config["data_type"] == "float":
            if "min_value" in rules and "max_value" in rules:
                invalid_mask = (df[col_name] < rules["min_value"]) | (df[col_name] > rules["max_value"])
                validation_report["validation_errors"] += invalid_mask.sum()
                
                for idx in df[invalid_mask].index:
                    df.at[idx, "row_status"] = "error"
                    flags = df.at[idx, "quality_flags"]
                    flags[col_name] = f"out_of_range ({rules['min_value']}-{rules['max_value']})"
                    df.at[idx, "quality_flags"] = flags
            
            # Outlier detection
            if "outlier_threshold" in rules:
                outlier_mask = df[col_name] > rules["outlier_threshold"]
                validation_report["outliers"] += outlier_mask.sum()
        
        # Date validation
        elif mapping_config["data_type"] == "date":
            if "min_date" in rules and "max_date" in rules:
                min_date = pd.to_datetime(rules["min_date"])
                max_date = pd.to_datetime(rules["max_date"])
                invalid_mask = (df[col_name] < min_date) | (df[col_name] > max_date)
                validation_report["validation_errors"] += invalid_mask.sum()
    
    return df, validation_report


# ========================================
# DEDUPLICATION
# ========================================
def remove_duplicates(df: pd.DataFrame, source_type: str) -> Tuple[pd.DataFrame, Dict]:
    """Remove duplicates based on source type."""
    dedup_report = {
        "initial_rows": len(df),
        "duplicates_removed": 0,
        "final_rows": 0,
        "unique_on": [],
    }
    
    if source_type == "risk_assessment":
        dedup_report["unique_on"] = ["supplier_name"]
        df = df.drop_duplicates(subset=["supplier_name"], keep="first")
    
    elif source_type == "transactions":
        dedup_report["unique_on"] = ["supplier_name", "invoice_number", "transaction_date"]
        df = df.drop_duplicates(subset=["supplier_name", "invoice_number", "transaction_date"], keep="first")
    
    elif source_type == "kraljic":
        dedup_report["unique_on"] = ["supplier_name"]
        df = df.drop_duplicates(subset=["supplier_name"], keep="first")
    
    dedup_report["duplicates_removed"] = dedup_report["initial_rows"] - len(df)
    dedup_report["final_rows"] = len(df)
    
    if dedup_report["duplicates_removed"] > 0:
        logger.info(f"✓ Removed {dedup_report['duplicates_removed']} duplicates from {source_type}")
    
    return df, dedup_report


# ========================================
# DATABASE INSERTION
# ========================================
def insert_into_database(
    db: DatabaseConnection,
    df: pd.DataFrame,
    table_name: str,
    source_file_name: str,
    file_hash: str,
    run_id: uuid.UUID,
) -> Dict:
    """Insert cleaned data into PostgreSQL."""
    insert_report = {
        "rows_inserted": 0,
        "rows_failed": 0,
        "errors": [],
    }
    
    # Convert DataFrame to records
    records = df.to_dict("records")
    
    # Add metadata
    for record in records:
        record["source_file_name"] = source_file_name
        record["source_file_hash"] = file_hash
        record["ingestion_timestamp"] = datetime.now()
    
    # Build INSERT query
    if len(records) == 0:
        logger.warning(f"⚠ No records to insert into {table_name}")
        return insert_report
    
    columns = list(records[0].keys())
    placeholders = ",".join(["%s"] * len(columns))
    column_names = ",".join(columns)
    
    insert_query = f"""
    INSERT INTO {table_name} ({column_names})
    VALUES ({placeholders})
    ON CONFLICT DO NOTHING
    """
    
    try:
        cursor = db.connection.cursor()
        for record in records:
            values = tuple(record.get(col) for col in columns)
            try:
                cursor.execute(insert_query, values)
            except psycopg2.IntegrityError as e:
                insert_report["rows_failed"] += 1
                insert_report["errors"].append(str(e))
        
        db.connection.commit()
        insert_report["rows_inserted"] = len(records) - insert_report["rows_failed"]
        cursor.close()
        logger.info(f"✓ Inserted {insert_report['rows_inserted']} rows into {table_name}")
    
    except Exception as e:
        db.connection.rollback()
        logger.error(f"✗ Insert failed for {table_name}: {e}")
        raise
    
    return insert_report


# ========================================
# MAIN PIPELINE
# ========================================
def run_phase1_ingestion():
    """Main Phase 1 ingestion pipeline."""
    run_id = uuid.uuid4()
    start_time = datetime.now()
    
    logger.info("=" * 60)
    logger.info(f"PHASE 1: DATA INGESTION & NORMALIZATION")
    logger.info(f"Run ID: {run_id}")
    logger.info(f"Start Time: {start_time}")
    logger.info("=" * 60)
    
    # Initialize database connection
    db = DatabaseConnection(DATABASE_CONFIG)
    db.connect()
    
    # Log pipeline run start
    run_log_query = """
    INSERT INTO pipeline_run_log 
    (run_id, pipeline_phase, status, start_time, files_processed)
    VALUES (%s, %s, %s, %s, %s)
    """
    db.execute_query(run_log_query, (str(run_id), "Phase 1", "in_progress", start_time, 0))
    
    # Track metrics
    all_metrics = {
        "risk_assessment": {},
        "transactions": {},
        "kraljic": {},
    }
    
    # Process each source file
    for source_type, file_config in SOURCE_FILES.items():
        logger.info(f"\n--- Processing {source_type} ---")
        file_path = file_config["file_path"]
        table_name = file_config["table_name"]
        
        try:
            # Step 1: Load file
            df, file_hash = load_csv_file(file_path, file_config["encoding"])
            
            # Check file freshness
            if not check_file_freshness(db, Path(file_path).name, file_hash):
                logger.info(f"Skipping {source_type} (no changes from previous run)")
                all_metrics[source_type]["status"] = "skipped"
                continue
            
            # Step 2: Standardize column names
            field_mappings = COLUMN_MAPPINGS[source_type]
            df, col_mapping_report = standardize_column_names(df, field_mappings)
            logger.info(f"✓ Column mapping: {len(col_mapping_report['mapped_columns'])} columns standardized")
            
            # Step 3: Convert data types
            df, conversion_report = convert_data_types(df, field_mappings, source_type)
            logger.info(f"✓ Type conversion: {conversion_report['successful']} successful, {conversion_report['failed']} failed")
            
            # Step 4: Validate data
            df, validation_report = validate_data(df, field_mappings, source_type)
            logger.info(f"✓ Validation: {validation_report['null_values']} nulls, {validation_report['validation_errors']} errors, {validation_report['outliers']} outliers")
            
            # Step 5: Remove duplicates
            df, dedup_report = remove_duplicates(df, source_type)
            logger.info(f"✓ Deduplication: {dedup_report['duplicates_removed']} removed, {dedup_report['final_rows']} final rows")
            
            # Step 6: Insert into database
            insert_report = insert_into_database(db, df, table_name, Path(file_path).name, file_hash, run_id)
            
            # Store metrics
            all_metrics[source_type] = {
                "status": "success",
                "initial_rows": dedup_report["initial_rows"],
                "final_rows": dedup_report["final_rows"],
                "rows_inserted": insert_report["rows_inserted"],
                "validation_errors": validation_report["validation_errors"],
                "file_hash": file_hash,
            }
        
        except Exception as e:
            logger.error(f"✗ Failed to process {source_type}: {e}")
            all_metrics[source_type]["status"] = "failed"
            all_metrics[source_type]["error"] = str(e)
    
    # Generate quality report
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("\n" + "=" * 60)
    logger.info("PHASE 1 QUALITY REPORT")
    logger.info("=" * 60)
    
    for source_type, metrics in all_metrics.items():
        logger.info(f"\n{source_type.upper()}:")
        for key, value in metrics.items():
            logger.info(f"  {key}: {value}")
    
    # Update pipeline run log with final status
    status = "success" if all(m.get("status") in ["success", "skipped"] for m in all_metrics.values()) else "failed"
    
    update_run_log = """
    UPDATE pipeline_run_log SET
      status = %s,
      rows_ingested_risk_assessment = %s,
      rows_ingested_transactions = %s,
      rows_ingested_kraljic = %s,
      rows_with_errors = %s,
      end_time = %s,
      duration_seconds = %s,
      ready_for_next_phase = %s
    WHERE run_id = %s
    """
    
    db.execute_query(
        update_run_log,
        (
            status,
            all_metrics.get("risk_assessment", {}).get("rows_inserted", 0),
            all_metrics.get("transactions", {}).get("rows_inserted", 0),
            all_metrics.get("kraljic", {}).get("rows_inserted", 0),
            sum(m.get("validation_errors", 0) for m in all_metrics.values()),
            end_time,
            int(duration),
            status == "success",
            str(run_id),
        ),
    )
    
    db.close()
    
    logger.info("=" * 60)
    logger.info(f"PHASE 1 COMPLETE | Status: {status} | Duration: {duration:.1f}s")
    logger.info("=" * 60)
    
    return status, all_metrics


if __name__ == "__main__":
    run_phase1_ingestion()
