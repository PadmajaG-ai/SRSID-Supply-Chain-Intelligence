# Phase 1: Data Ingestion & Normalization Setup Guide

## Overview

This guide walks you through setting up and running the **Phase 1 Data Ingestion** pipeline for the Supplier Risk & Spend Intelligence Dashboard (SRSID).

**Phase 1** performs:
- CSV data loading and validation
- Column name standardization
- Data type conversion
- Quality checks and deduplication
- PostgreSQL ingestion
- Automatic daily scheduling

---

## 📋 Prerequisites

### 1. PostgreSQL Database
- PostgreSQL 12+ installed and running
- Database user with CREATE/INSERT/UPDATE permissions
- Recommended: Local instance for development

**Quick Start (macOS with Homebrew):**
```bash
brew install postgresql
brew services start postgresql
createdb supplier_risk_db
```

**Quick Start (Windows/Linux):**
- Download from https://www.postgresql.org/download/
- Create database: `createdb supplier_risk_db`

### 2. Python Environment
- Python 3.8+
- Virtual environment (recommended)

### 3. Source CSV Files
Place these files in `data/raw/` directory:
- `supplier_risk_assessment.csv` (~1,800 rows)
- `us_supply_chain_risk.csv` (~1,000 rows)
- `kraljic_matrix.csv` (~1,800 rows)

---

## 🚀 Quick Start (5 Steps)

### Step 1: Clone/Download Files
```bash
mkdir -p srsid_project/data/raw
mkdir -p srsid_project/logs
cd srsid_project

# Copy all Phase 1 files into this directory:
# - phase1_schema.sql
# - config_phase1.py
# - phase1_ingestion.py
# - scheduler.py
# - requirements.txt
```

### Step 2: Setup PostgreSQL Database
```bash
# Connect to PostgreSQL
psql -U postgres

# Create database (if not already created)
CREATE DATABASE supplier_risk_db;

# Exit psql
\q

# Run schema script
psql -U postgres -d supplier_risk_db -f phase1_schema.sql
```

**Verify schema was created:**
```bash
psql -U postgres -d supplier_risk_db

# Inside psql:
\dt  # List tables (should show raw_supplier_risk_assessment, etc.)
\q   # Exit
```

### Step 3: Create Python Virtual Environment
```bash
python3 -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate

# On Windows:
venv\Scripts\activate
```

### Step 4: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 5: Configure Database Connection
Edit `config_phase1.py` and update:

```python
DATABASE_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "supplier_risk_db",
    "user": "your_postgres_username",      # ← Change this
    "password": "your_postgres_password",  # ← Change this
}
```

**Example:**
```python
DATABASE_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "supplier_risk_db",
    "user": "postgres",
    "password": "mypassword",
}
```

Also update CSV file paths if needed:
```python
SOURCE_FILES = {
    "risk_assessment": {
        "file_path": "data/raw/supplier_risk_assessment.csv",  # ← Verify path
        ...
    },
    ...
}
```

---

## 🧪 Test Phase 1 (Manual Run)

Before setting up daily scheduler, test Phase 1 manually:

```bash
python phase1_ingestion.py
```

**Expected Output:**
```
============================================================
PHASE 1: DATA INGESTION & NORMALIZATION
Run ID: abc-123-def-456
Start Time: 2025-02-12 14:30:45.123456
============================================================

--- Processing risk_assessment ---
✓ Loaded data/raw/supplier_risk_assessment.csv (1800 rows, 5 columns)
✓ Column mapping: 4 columns standardized
✓ Type conversion: 4 successful, 0 failed
✓ Validation: 0 nulls, 0 errors, 5 outliers
✓ Removed 3 duplicates from risk_assessment, 1797 final rows
✓ Inserted 1797 rows into raw_supplier_risk_assessment

--- Processing transactions ---
✓ Loaded data/raw/us_supply_chain_risk.csv (1000 rows, 7 columns)
...

============================================================
PHASE 1 QUALITY REPORT
============================================================

risk_assessment:
  status: success
  initial_rows: 1800
  final_rows: 1797
  rows_inserted: 1797
  validation_errors: 0
  file_hash: a1b2c3d4e5f6...

transactions:
  status: success
  ...

kraljic:
  status: success
  ...

============================================================
PHASE 1 COMPLETE | Status: success | Duration: 12.3s
============================================================
```

**Verify data in PostgreSQL:**
```bash
psql -U postgres -d supplier_risk_db

# Inside psql:
SELECT COUNT(*) FROM raw_supplier_risk_assessment;
SELECT COUNT(*) FROM raw_supply_chain_transactions;
SELECT COUNT(*) FROM raw_kraljic_matrix;

# Check pipeline log
SELECT * FROM pipeline_run_log ORDER BY run_timestamp DESC LIMIT 1;

\q
```

---

## ⏰ Setup Daily Scheduler

### Option A: Using APScheduler (Background)

Run scheduler in background:

```bash
python scheduler.py &
```

Check if running:
```bash
ps aux | grep scheduler.py
```

Stop scheduler:
```bash
pkill -f scheduler.py
```

### Option B: Using Cron (Linux/macOS)

Edit crontab:
```bash
crontab -e
```

Add this line (runs Phase 1 at 2:00 AM UTC every day):
```cron
0 2 * * * cd /path/to/srsid_project && /path/to/venv/bin/python phase1_ingestion.py >> logs/cron.log 2>&1
```

### Option C: Using Task Scheduler (Windows)

1. Open Task Scheduler
2. Create Basic Task:
   - Name: "SRSID Phase 1 Daily"
   - Trigger: Daily at 2:00 AM
   - Action: Start a program
   - Program: `C:\path\to\venv\Scripts\python.exe`
   - Arguments: `phase1_ingestion.py`
   - Start in: `C:\path\to\srsid_project`

---

## 📊 Monitoring & Logging

### Log Files

All logs stored in `logs/` directory:

- **Daily ingestion logs**: `logs/phase1_YYYYMMDD_HHMMSS.log`
- **Scheduler logs**: `logs/scheduler.log`
- **Error logs**: Same files (errors logged with `✗` prefix)

**View latest log:**
```bash
tail -f logs/phase1_*.log
```

### Database Monitoring

Check pipeline execution history:
```bash
psql -U postgres -d supplier_risk_db

# View all pipeline runs
SELECT run_id, pipeline_phase, status, duration_seconds, ready_for_next_phase
FROM pipeline_run_log
ORDER BY run_timestamp DESC
LIMIT 10;

# View data quality issues
SELECT source_table, issue_type, COUNT(*) as count
FROM data_quality_log
WHERE severity = 'error'
GROUP BY source_table, issue_type;

\q
```

### Email Notifications

To enable email alerts, update `config_phase1.py`:

```python
SCHEDULER_CONFIG = {
    ...
    "notifications": {
        "on_success": {
            "email_to": ["procurement-head@company.com", "supply-chain-risk@company.com"],
            "include_report": True,
        },
        "on_failure": {
            "email_to": ["data-team@company.com"],
            "include_logs": True,
        },
    },
}
```

**Note:** Current implementation logs notifications (doesn't actually send email). To enable actual email sending, update `scheduler.py` SMTP configuration.

---

## 🔧 Configuration Guide

### Column Mappings

If your CSV columns have different names, update `COLUMN_MAPPINGS` in `config_phase1.py`:

**Example:** If your CSV uses "Vendor Name" instead of "Supplier Name"

```python
COLUMN_MAPPINGS = {
    "risk_assessment": {
        "supplier_name": {
            "input_names": ["Supplier Name", "Vendor Name", "supplier_name"],  # ← Add "Vendor Name"
            "output_name": "supplier_name",
            ...
        },
        ...
    },
}
```

### Validation Rules

Customize validation thresholds in `VALIDATION_RULES`:

```python
VALIDATION_RULES = {
    "transaction_amount": {
        "allow_null": False,
        "min_value": 0.0,
        "max_value": 1000000000.0,  # Change 1B limit if needed
    },
    "transaction_date": {
        "min_date": datetime(2020, 1, 1),  # Change lookback start date
        "max_date": datetime.now(),
    },
}
```

### Error Tolerance

Adjust how strict Phase 1 is:

```python
ERROR_TOLERANCE = {
    "max_null_pct": 5.0,  # Warn if >5% nulls
    "max_duplicate_pct": 2.0,  # Warn if >2% duplicates
    "max_error_pct": 3.0,  # Fail if >3% errors (set to 100 to never fail)
}
```

---

## 🐛 Troubleshooting

### Error: "Connection refused" / Database not found

**Solution:**
```bash
# Verify PostgreSQL is running
psql -U postgres -c "SELECT 1"

# If not running:
# macOS:
brew services start postgresql

# Linux (Ubuntu):
sudo systemctl start postgresql

# Windows: Start PostgreSQL from Services
```

### Error: "permission denied" on CSV files

**Solution:**
```bash
# Check file permissions
ls -la data/raw/

# Make readable
chmod 644 data/raw/*.csv
```

### Error: "Column 'supplier_name' not found"

**Solution:**
1. Check actual CSV column names:
```bash
head -1 data/raw/supplier_risk_assessment.csv
```

2. Update `COLUMN_MAPPINGS` in `config_phase1.py` to match

### Pipeline runs but inserts 0 rows

**Likely causes:**
1. CSV is empty or has no data rows (only header)
2. All rows failed validation
3. Database permissions issue

**Debugging:**
```python
# Add to phase1_ingestion.py before insert:
print(f"DataFrame shape: {df.shape}")
print(f"Row statuses: {df['row_status'].value_counts()}")
print(f"Sample rows:\n{df.head()}")
```

### "UNIQUE constraint violation"

This is normal if re-running same data twice. Phase 1 logs this but continues.

**To re-ingest data:**
```bash
# Delete previous ingestion
psql -U postgres -d supplier_risk_db

DELETE FROM raw_supplier_risk_assessment WHERE ingestion_timestamp > '2025-02-12';
DELETE FROM file_integrity_log WHERE file_name = 'supplier_risk_assessment.csv';

\q

# Re-run Phase 1
python phase1_ingestion.py
```

---

## ✅ Validation Checklist

- [ ] PostgreSQL installed and running
- [ ] Database `supplier_risk_db` created
- [ ] Schema imported (7 tables created)
- [ ] Python virtual environment activated
- [ ] Dependencies installed (`pip install -r requirements.txt`)
- [ ] Database credentials updated in `config_phase1.py`
- [ ] CSV files in `data/raw/` directory
- [ ] Manual Phase 1 run successful (0 errors)
- [ ] Data appears in PostgreSQL tables
- [ ] Scheduler configured (cron/Task Scheduler/APScheduler)
- [ ] Log directory exists (`logs/`)

---

## 📈 Next Steps

Once Phase 1 is running successfully:

**→ Proceed to Phase 2: Entity Resolution & Consolidation**
- Deduplicates suppliers across datasets
- Creates unified supplier master table
- Maps supplier variants to parent entities

Files: `phase2_schema.py`, `phase2_consolidation.py`

---

## 📞 Support

For issues:
1. Check logs: `logs/phase1_*.log`
2. Review troubleshooting section above
3. Check database: `psql -U postgres -d supplier_risk_db`
4. Verify config: `config_phase1.py`

---

## 📝 Summary

You now have:
✓ PostgreSQL database with Phase 1 schema  
✓ Python ingestion script that loads & validates data  
✓ Daily scheduler that runs Phase 1 automatically  
✓ Comprehensive logging and error tracking  
✓ Email notifications (configurable)  

**Phase 1 will run daily at 2:00 AM UTC** and populate the raw data tables.

Ready for Phase 2? 🚀
