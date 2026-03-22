# Vendor Synthesis with Guardian News Integration
## Complete Guide to Creating SAP-Compliant Datasets

---

## Overview

This system synthesizes realistic vendor data from 25 real Ariba suppliers, generates variants for testing fuzzy matching, and enriches the dataset with real news from The Guardian API.

### What You Get

✅ **25 real vendor names** from Ariba network  
✅ **~120-150 vendors total** (with realistic variants)  
✅ **SAP-compliant columns** (LIFNR, ORT01, Land1, etc.)  
✅ **Synthesized metrics** (performance scores, risk categories)  
✅ **Real news data** from Guardian API about vendors  
✅ **Disruption events** (strikes, recalls, shortages, lawsuits)  
✅ **Phase 1 ready** CSV files for direct import  

---

## Architecture

```
Real Vendors (25)
    ↓
[synthesize_vendors.py]
    • Generate variants (3-5 per vendor)
    • Add synthetic metrics
    • SAP column mapping
    ↓
~150 Vendors + SAP Columns
    ↓
[guardian_news_integration.py]
    • Fetch real news from Guardian API
    • Extract disruption events
    • Match to vendors
    ↓
Vendors + News + Disruptions
    ↓
[integrated_dataset_builder.py]
    • Combine all data
    • Create Phase 1 tables
    • Ready for import
    ↓
Phase 1 CSV Files (ready for PostgreSQL)
```

---

## Prerequisites

### 1. Python Libraries

```bash
pip install pandas requests numpy
```

### 2. Guardian API Key (FREE)

1. Go to: https://open-platform.theguardian.com/
2. Sign up (free account)
3. Get API key
4. Set environment variable:

```bash
# macOS/Linux:
export GUARDIAN_API_KEY='your_api_key_here'

# Windows (PowerShell):
$env:GUARDIAN_API_KEY='your_api_key_here'

# Windows (Command Prompt):
set GUARDIAN_API_KEY=your_api_key_here
```

### 3. Virtual Environment (Optional but Recommended)

```bash
python -m venv venv
source venv/bin/activate  # macOS/Linux
# OR
venv\Scripts\activate     # Windows
```

---

## Step-by-Step Execution

### Step 1: Synthesize Vendors

Generate synthetic vendor data with variants and SAP columns.

```bash
python synthesize_vendors.py
```

**Output:**
```
======================================================================
VENDOR SYNTHESIS ENGINE
======================================================================

[STEP 1] Creating master vendor records...
Created 25 master vendors

[STEP 2] Generating vendor variants...
Generated 125 variant vendors

[STEP 3] Combining master + variants...
Total unique vendors: 150

[STEP 4] Adding synthetic metrics...

[STEP 5] Adding SAP-compliant columns...

======================================================================
VENDOR SYNTHESIS COMPLETE
======================================================================
Master vendors: 25
Total vendors (with variants): 150
Output files:
  - vendors_sap_compliant.csv
  - vendors_variants.csv
```

**Files Created:**
- `vendors_sap_compliant.csv` - Master vendors (25 records)
- `vendors_variants.csv` - All vendors with variants (150 records)

**Sample Data:**
```
LIFNR,NAME1,ORT01,Land1,PERF_SCORE,FIN_STAB,RISC,KVADRANT
001000,Rich's,Los Angeles,US,85,82,Medium,Tactical
001001,Rich's EMEA,Los Angeles,US,82,80,Medium,Tactical
001002,Rich's Inc,Los Angeles,US,81,79,Medium,Tactical
...
```

---

### Step 2: Fetch News from Guardian API

Fetch real news about vendors and extract disruption events.

```bash
python guardian_news_integration.py
```

**Output:**
```
======================================================================
GUARDIAN API NEWS INTEGRATION
======================================================================

[STEP 1] Validating Guardian API key...
✓ API key configured

[STEP 2] Loading vendor data...
Loaded 150 vendors

[STEP 3] Fetching vendor news from Guardian API...
Searching for: Rich's
Searching for: Indo Autotech Limited
...
Fetched 247 articles total

[STEP 4] Building vendor + news dataset...

[STEP 5] Extracting disruption events...

[STEP 6] Saving to CSV files...
Saved vendor news to vendors_with_news.csv
Saved disruption events to disruption_events.csv

======================================================================
GUARDIAN API INTEGRATION COMPLETE
======================================================================
Vendors with news: 145
Disruption events found: 32

Sample disruptions:
      Vendor_Name Disruption_Type Event_Date
0    Apple             earnings    2024-01-15
1    Cisco       supply shortage  2024-01-10
...
```

**Files Created:**
- `vendors_with_news.csv` - Vendors matched with news
- `disruption_events.csv` - Extracted disruption events

**Sample Disruption Data:**
```
LIFNR,Vendor_Name,Country,Disruption_Type,Event_Date,Headline
001000,Apple,US,earnings,2024-01-15,Apple reports strong earnings
001001,Cisco,US,supply shortage,2024-01-10,Chip shortage impacts Cisco
```

---

### Step 3: Build Integrated Dataset

Combine vendors, news, and disruptions into Phase 1 table format.

```bash
python integrated_dataset_builder.py
```

**Output:**
```
======================================================================
INTEGRATED DATASET BUILDER
======================================================================

[STEP 1] Loading data...

[STEP 2] Building Phase 1 tables...
Building raw_supplier_risk_assessment...
Created 150 risk assessment records

Building raw_supply_chain_transactions...
Created 425 transaction records

Building raw_kraljic_matrix...
Created 150 Kraljic matrix records

[STEP 3] Generating summary...

======================================================================
INTEGRATED DATASET SUMMARY
======================================================================

Vendor Data:
  Master vendors: 25
  Total vendors (with variants): 150
  Countries: 8
  Industries: 14

Risk Distribution:
  Low: 45 vendors
  Medium: 85 vendors
  High: 20 vendors

Kraljic Distribution:
  Strategic: 32 vendors
  Leverage: 38 vendors
  Bottleneck: 28 vendors
  Tactical: 52 vendors

Disruption Events:
  Total events: 32
  strike: 5 events
  shortage: 12 events
  earnings: 8 events
  lawsuit: 7 events

======================================================================
✓ DATASET INTEGRATION COMPLETE
======================================================================

Ready for Phase 1 import!
Files created:
  - phase1_supplier_risk_assessment.csv
  - phase1_supply_chain_transactions.csv
  - phase1_kraljic_matrix.csv
```

**Files Created:**
- `phase1_supplier_risk_assessment.csv` - Risk assessment data
- `phase1_supply_chain_transactions.csv` - Transactions + disruptions
- `phase1_kraljic_matrix.csv` - Strategic positioning

---

## SAP Column Reference

### Standard SAP Columns Generated

| Column | Type | Example | Description |
|--------|------|---------|-------------|
| LIFNR | String | 001000 | Vendor ID (SAP) |
| NAME1 | String | Apple Inc | Vendor name (35 chars max) |
| NAME2 | String | US Division | Supplement name |
| ORT01 | String | Cupertino | City |
| Land1 | String | US | Country code |
| REGIO | String | US | Region |
| PSTLZ | String | 95014 | Postal code |
| KTOKK | String | Z001 | Account group |
| ZTERM | String | NET30 | Payment terms |
| INCO1 | String | FOB | Incoterms |
| NACE | String | 2620 | Industry code |
| NACE_DESC | String | Electronics | Industry description |

### Custom Risk Columns

| Column | Range | Example | Description |
|--------|-------|---------|-------------|
| PERF_SCORE | 0-100 | 85 | Performance score |
| FIN_STAB | 0-100 | 82 | Financial stability |
| PERF_NORM | 0-1 | 0.85 | Normalized performance |
| FIN_STAB_NORM | 0-1 | 0.82 | Normalized financial |
| RISC | Low/Med/High | Medium | Risk category |
| SupplyRiskScore | 0-1 | 0.25 | Supply risk (Kraljic) |
| ProfitImpactScore | 0-1 | 0.65 | Profit impact (Kraljic) |
| KVADRANT | String | Tactical | Kraljic quadrant |

---

## Phase 1 CSV Format

### raw_supplier_risk_assessment.csv

```csv
supplier_name,financial_stability_score,delivery_performance,historical_risk_category,ingestion_timestamp,ariba_supplier_id,supplier_country,supplier_city,industry
Apple Inc,0.92,0.95,Low,2024-01-20 10:30:00,001000,US,Cupertino,Electronics
Apple EMEA,0.91,0.94,Low,2024-01-20 10:30:00,001001,US,Cupertino,Electronics
...
```

### raw_supply_chain_transactions.csv

```csv
supplier,amount,transaction_date,disruption_type,procurement_category,invoice_number,po_number,ingestion_timestamp,ariba_supplier_id
Apple Inc,150000,2024-01-15,None,Electronics,INV-001000,PO-001000,2024-01-20 10:30:00,001000
Apple Inc,200000,2023-12-10,None,Electronics,INV-001001,PO-001001,2024-01-20 10:30:00,001000
Cisco,0,2024-01-10,shortage,Networking,DISRUPTION-001100,DISP-001100,2024-01-20 10:30:00,001100
...
```

### raw_kraljic_matrix.csv

```csv
supplier_name,supply_risk_score,profit_impact_score,kraljic_quadrant,ingestion_timestamp,ariba_supplier_id
Apple Inc,0.05,0.90,Strategic,2024-01-20 10:30:00,001000
Cisco,0.25,0.75,Strategic,2024-01-20 10:30:00,001100
...
```

---

## Guardian API News Data

### What News Gets Captured

The Guardian API searches for articles about each vendor that mention:

```
- Supply chain disruptions
- Strikes or labor disputes
- Shortages or supply constraints
- Product recalls
- Lawsuits or legal issues
- Bankruptcies
- Earnings reports
- Facility closures or expansions
```

### Example News Events Captured

```
Apple - "Apple announces earnings beat" → earnings event
Cisco - "Chip shortage impacts networking equipment" → shortage
Intel - "Intel faces production delays" → logistics_delay
AstraZeneca - "Drug recall announced" → recall
Siemens - "Strike at German manufacturing plant" → strike
```

### Disruption Type Mapping

The system extracts these disruption types from news:

| Type | Keywords | Severity |
|------|----------|----------|
| strike | strike, labor dispute, worker action | Medium |
| shortage | shortage, supply constraint, chip shortage | Medium |
| logistics_delay | delay, supply chain, shipping, customs | Medium |
| recall | recall, product recall, defect | High |
| lawsuit | lawsuit, legal, court, settlement | High |
| bankruptcy | bankruptcy, insolvency | High |
| sanction | sanction, tariff, trade war, export ban | High |

---

## Verification & Quality Checks

### Check 1: Vendor Count

```bash
wc -l vendors_variants.csv
# Should show ~151 (150 vendors + header)

wc -l phase1_supplier_risk_assessment.csv
# Should show ~151 (150 suppliers + header)
```

### Check 2: Data Distribution

```bash
# Check risk distribution
tail -n +2 phase1_supplier_risk_assessment.csv | cut -d, -f4 | sort | uniq -c

# Check Kraljic distribution
tail -n +2 phase1_kraljic_matrix.csv | cut -d, -f4 | sort | uniq -c
```

### Check 3: News Coverage

```bash
# How many disruption events?
wc -l disruption_events.csv

# What disruption types?
tail -n +2 disruption_events.csv | cut -d, -f3 | sort | uniq -c
```

### Check 4: SAP Column Validation

```bash
# Verify SAP columns exist
head -1 vendors_variants.csv | tr ',' '\n' | grep -E "LIFNR|NAME1|ORT01|Land1"
```

---

## Next: Import to Phase 1

Once you have the CSV files, import them to Phase 1 raw tables:

```bash
# From SRSID project root:
python phase1_ingestion.py \
  --risk-file phase1_supplier_risk_assessment.csv \
  --transaction-file phase1_supply_chain_transactions.csv \
  --kraljic-file phase1_kraljic_matrix.csv
```

---

## Troubleshooting

### Issue 1: "Guardian API key not configured"

**Solution:**
```bash
# Set API key
export GUARDIAN_API_KEY='your_key_here'

# Verify it's set
echo $GUARDIAN_API_KEY

# Run again
python guardian_news_integration.py
```

### Issue 2: "File not found"

**Solution:**
- Make sure you ran scripts in order:
  1. `synthesize_vendors.py` (creates vendors_variants.csv)
  2. `guardian_news_integration.py` (uses vendors_variants.csv)
  3. `integrated_dataset_builder.py` (uses both)

### Issue 3: "No disruption events found"

**Possible causes:**
- Guardian API returns fewer articles for some vendors
- News articles don't contain keywords we're searching for
- This is normal - not all vendors have recent disruptions

**Check manually:**
```bash
grep -i "strike\|recall\|shortage" disruption_events.csv
```

### Issue 4: "API rate limit exceeded"

**Solution:**
- Guardian free tier allows ~100 requests/day
- Script adds delays between requests
- Wait 24 hours and run again

---

## Data Characteristics

### Vendor Distribution

- **25 real vendors** from Ariba network
- **~150 total vendors** (with variants)
- **8 countries**: US, Germany, UK, France, Switzerland, Canada, India
- **14 industries**: Electronics, Pharmaceuticals, Manufacturing, etc.

### Performance Scores

- **Range**: 65-98 (realistic variation)
- **Industry-based**: Pharma (high), Chemicals (lower)
- **Variant variance**: ±3 points from parent

### Risk Categories

- **Low**: ~30% (high performers)
- **Medium**: ~55% (mixed)
- **High**: ~15% (risky suppliers)

### Disruption Events

- **32 events** from real news
- **Mix of types**: Strikes, shortages, recalls, earnings
- **Date range**: Past 365 days
- **Real source**: The Guardian

---

## Use Cases

### 1. Testing Phase 2 Consolidation

The variants are designed to test fuzzy matching:
- "Apple" vs "Apple USA" vs "Apple Inc"
- Will consolidate to single unified supplier
- Tests your Phase 2 fuzzy matching algorithm

### 2. Testing Risk Assessment

Real news events create disruptions:
- Risk scores reflect industry patterns
- News disruptions create realistic scenarios
- Can test risk trending over time

### 3. Supply Chain Analysis

Realistic spend distribution:
- Large vendors have more spend
- Country-based spending patterns
- Multi-country operations

---

## Summary

✅ **Real vendors** (25)  
✅ **Realistic variants** (~150 total)  
✅ **SAP columns** (LIFNR, ORT01, Land1)  
✅ **Real news** (Guardian API)  
✅ **Disruptions** (32 events)  
✅ **Phase 1 ready** (CSV files)  

All ready for Phase 1 import! 🚀

