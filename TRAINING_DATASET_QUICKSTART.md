# Combined Training Dataset: Quick Start (5 Minutes)

---

## What You're Combining

```
Phase 1 Synthetic (1,800)
    +
Real Vendors (150)
    +
Kaggle (3 files with hundreds of records)
    +
Guardian News (32+ disruptions)
    =
~2,000-2,500 suppliers total
~3,000+ transactions
Ready for training & Phase 2 consolidation
```

---

## Prerequisites

✓ Have these files ready:
```
Phase 1 original:
  - phase1_supplier_risk_assessment_original.csv
  - phase1_supply_chain_transactions_original.csv
  - phase1_kraljic_matrix_original.csv

Real vendors (from synthesis):
  - phase1_supplier_risk_assessment.csv
  - phase1_supply_chain_transactions.csv
  - phase1_kraljic_matrix.csv

Kaggle:
  - us_supply_chain_risk.csv
  - supplier_risk_assessment.csv
  - realistic_kraljic_dataset.csv

Guardian:
  - disruption_events.csv
```

---

## Run (1 Command)

```bash
python combine_training_datasets.py
```

**That's it!** 🎉

---

## What Happens (2-3 minutes)

```
[STEP 1] Loading Phase 1 Synthetic Data...
  ✓ 1800 risk + 1000 transactions + 16 Kraljic

[STEP 2] Loading Real Vendor Data...
  ✓ 150 risk + 425 transactions + 150 Kraljic

[STEP 3] Loading Kaggle Data...
  ✓ 95 risk + 450 transactions + 75 Kraljic

[STEP 4] Loading Guardian News Disruptions...
  ✓ 32 news events

[STEP 5] Combining All Data Sources...
  ✓ 4 risk sources → 2,045 rows
  ✓ 4 transaction sources → 2,900 rows
  ✓ 3 Kraljic sources → 241 rows

[STEP 6] Adding Source Tracking...
  ✓ Each row marked with origin source

[STEP 7] Validating Data Quality...
  ✓ 2,045 risk records valid
  ✓ 2,900 transaction records valid
  ✓ 241 Kraljic records valid

[STEP 8] Saving Training Datasets...
  ✓ training_supplier_risk_assessment.csv
  ✓ training_supply_chain_transactions.csv
  ✓ training_kraljic_matrix.csv

[STEP 9] Generating Reports...
  ✓ training_dataset_manifest.json
```

---

## Output Files (3 CSVs)

```
training_datasets/
├── training_supplier_risk_assessment.csv       (2,045 suppliers)
├── training_supply_chain_transactions.csv      (2,900 transactions)
├── training_kraljic_matrix.csv                 (241 Kraljic records)
└── training_dataset_manifest.json              (metadata)
```

---

## Verify Success

```bash
# Check files created
ls -la training_datasets/

# Check row counts
wc -l training_datasets/training_*.csv

# Should see ~2,000+ suppliers, ~2,900 transactions
```

---

## What Makes It Special

✅ **Keep Separate** - Not pre-merged (preserves variants)  
✅ **Source Tracked** - Each row marked with origin  
✅ **Maximum Volume** - All sources combined (~2,000 suppliers)  
✅ **Real + Synthetic** - Best of both worlds  
✅ **Phase 2 Ready** - Fuzzy matching will consolidate  

---

## Next: Phase 2

```bash
# Now run Phase 2 consolidation
python phase2_consolidation.py

# This will:
# - Find duplicates across sources
# - Consolidate to ~1,500 unique suppliers
# - Create unified_supplier_master
```

---

## Data Breakdown

**Sources:**
```
phase1_synthetic:  1,800 suppliers (baseline)
real_vendors:      150 suppliers (your Ariba vendors)
kaggle:            100+ suppliers (real patterns)
guardian_news:     32+ disruption events (real news)
─────────────────────────────────────────────────
TOTAL:             ~2,000+ unique suppliers
```

**Disruptions Found:**
```
shortage:    12 events
strike:      5 events
earnings:    8 events
recall:      7 events
```

---

## Why This Approach

| Aspect | Benefit |
|--------|---------|
| **Keep Separate** | Preserves all variants for fuzzy matching |
| **Source Tracking** | Can see which suppliers are from which source |
| **Maximum Data** | All sources combined for better training |
| **Real News** | Actual disruption events from Guardian |
| **Phase 2 Ready** | Consolidation will merge intelligently |

---

## One More Thing

⚠️ **Important:** After combining, **do NOT import to Phase 1 tables yet**

Instead:
1. ✅ Run `combine_training_datasets.py` (creates training CSVs)
2. ✅ Verify output files in `training_datasets/` folder
3. ✅ Run `phase2_consolidation.py` (uses training CSVs)
4. ✅ Check `unified_supplier_master` (final consolidated table)

---

## That's It! 🚀

You now have:
- ✓ ~2,000 suppliers (combined from all sources)
- ✓ ~2,900 transactions
- ✓ Real news disruptions
- ✓ Source tracking for analysis
- ✓ Ready for Phase 2 consolidation

**Next:** Run Phase 2 for fuzzy matching consolidation!

