# Spend Analytics: Quick Start Guide

## 🎯 What We're Adding

Three spend management KPIs to Phase 1:

1. **Spend Under Management (SUM)** - % of spend covered by contracts
2. **Maverick Spend** - Off-contract, unauthorized, or emergency spending
3. **Concentration Risk** - Over-dependence on specific suppliers

---

## 📋 Updated Phase 1 Workflow

### **Before (Phase 1 only):**
```bash
python fetch_gdelt_disruptions.py
python fetch_newsapi_disruptions.py
python combine_disruption_sources.py
python analyze_kaggle_patterns.py
python assign_vendor_industries.py
python map_vendors_to_disruptions.py
python synthesize_vendor_risk_metrics.py
python build_phase1_tables.py
```

### **After (Phase 1 + Spend Analytics):**
```bash
python fetch_gdelt_disruptions.py
python fetch_newsapi_disruptions.py
python combine_disruption_sources.py
python analyze_kaggle_patterns.py
python assign_vendor_industries.py
python map_vendors_to_disruptions.py
python synthesize_vendor_risk_metrics.py
python build_phase1_tables.py

# NEW: Add spend analytics
python calculate_spend_analytics.py  ✅ NEW
```

**Total time: ~10 minutes + 2 minutes for spend analytics = ~12 minutes**

---

## 📊 Output Files

### **Before (3 files):**
```
phase1_tables/
├─ raw_supplier_risk_assessment.csv
├─ raw_supply_chain_transactions.csv
└─ raw_kraljic_matrix.csv
```

### **After (6 files + 2 reports):**
```
phase1_tables/
├─ raw_supplier_risk_assessment.csv (ENHANCED with spend columns)
├─ raw_supply_chain_transactions.csv (ENHANCED with spend columns)
├─ raw_kraljic_matrix.csv
├─ raw_spend_analytics.csv (NEW)
├─ raw_concentration_analysis.csv (NEW)
├─ raw_maverick_spend_summary.csv (NEW)
├─ phase1_quality_report.json
└─ phase1_spend_intelligence_report.json (NEW)
```

---

## 🚀 How to Run

### **Step 1: Ensure Phase 1 is Complete**

Run standard Phase 1 scripts first:
```bash
python build_phase1_tables.py  # Last Phase 1 script
```

Should generate:
```
phase1_tables/
├─ raw_supplier_risk_assessment.csv
├─ raw_supply_chain_transactions.csv
└─ raw_kraljic_matrix.csv
```

### **Step 2: Run Spend Analytics**

```bash
python calculate_spend_analytics.py
```

This will:
1. ✅ Load Phase 1 tables
2. ✅ Calculate SUM per supplier
3. ✅ Identify maverick spend
4. ✅ Analyze concentration
5. ✅ Enhance existing tables
6. ✅ Generate new outputs
7. ✅ Create spend report

### **Step 3: Verify Outputs**

Check that new files were created:
```bash
ls -la phase1_tables/

# Should show:
# raw_spend_analytics.csv (NEW)
# raw_concentration_analysis.csv (NEW)
# phase1_spend_intelligence_report.json (NEW)
# + enhanced versions of existing files
```

---

## 📊 Expected Output Example

### **raw_spend_analytics.csv (Per Supplier)**

```
supplier_name,industry,total_spend,spend_under_contract,sum_percentage,maverick_spend,maverick_percentage,spend_concentration_percentage,concentration_risk_level,diversification_needed,estimated_savings_opportunity

Apple,Electronics & Semiconductors,4500000,3500000,77.78,675000,15.0,0.9,LOW,false,225000
BASF,Chemicals,3500000,2100000,60.0,525000,15.0,0.7,LOW,true,175000
Intel,Electronics & Semiconductors,4000000,2800000,70.0,600000,15.0,0.8,LOW,false,200000
```

### **phase1_spend_intelligence_report.json**

```json
{
  "timestamp": "2026-03-18T...",
  "spend_intelligence": {
    "total_enterprise_spend": 500000000,
    "total_contract_spend": 400000000,
    "total_maverick_spend": 65000000,
    "enterprise_sum_percentage": 80.0,
    "enterprise_maverick_percentage": 13.0
  },
  "concentration_analysis": {
    "suppliers_high_risk": 8,
    "suppliers_medium_risk": 15,
    "suppliers_low_risk": 175,
    "top_supplier_concentration": 0.9
  },
  "opportunity_analysis": {
    "total_savings_opportunity": 18000000,
    "suppliers_needing_diversification": 23,
    "suppliers_below_sum_target": 45
  }
}
```

---

## 💡 Key Metrics Explained

### **Spend Under Management (SUM)**

```
SUM % = (Spend Under Contract / Total Spend) × 100

Example - Apple:
├─ Total Spend: $4,500,000
├─ Contract Spend: $3,500,000
└─ SUM: 77.8%

Target: >80%
Status: Below target (need contracts for $1M spend)
```

### **Maverick Spend**

```
Types:
├─ Off-Contract: 54% - Spending with approved vendor outside contract
├─ Unauthorized: 31% - Spending with unapproved vendors
├─ Emergency: 12% - Legitimate emergency at high cost
└─ Non-Compliant: 3% - Violates policies

Enterprise Maverick:
├─ Total: $65M out of $500M
├─ %: 13%
└─ Target: <10%
```

### **Concentration Risk**

```
By Supplier:
├─ Apple: 0.9% of enterprise spend
├─ Samsung: 0.8%
├─ Intel: 0.7%
└─ Risk Classification:
   └─ HIGH: >15% of enterprise
   └─ MEDIUM: 10-15%
   └─ LOW: <10%

Target: Top supplier <10%, HHI <1500
```

---

## 📈 Benefits for Phase 2+

Now when Phase 2 consolidates suppliers, it has:

✅ **Spend visibility** - Know total spend per supplier
✅ **Maverick detection** - Find off-contract spending to recover
✅ **Concentration risk** - Identify suppliers needing diversification
✅ **Savings opportunity** - Quantify consolidation benefits
✅ **Strategic insights** - Data for procurement strategy

---

## 🔧 Configuration (If You Need to Adjust)

Edit `spend_analytics_config.py` to change thresholds:

```python
# Change SUM target
SUM_CONFIG["enterprise_sum_target"] = 0.85  # 85% instead of 80%

# Change concentration threshold
CONCENTRATION_CONFIG["enterprise_thresholds"]["high_risk"] = 0.20  # 20% instead of 15%

# Change maverick rate
MAVERICK_CONFIG["maverick_baseline_rate"] = 0.10  # 10% instead of 15%
```

Then run: `python calculate_spend_analytics.py`

---

## ✅ Execution Checklist

```
□ Phase 1 scripts completed (all 8)
□ phase1_tables/ directory created with 3 CSV files
□ spend_analytics_config.py in project root
□ calculate_spend_analytics.py in project root
□ Run: python calculate_spend_analytics.py
□ Verify: New files created in phase1_tables/
□ Check: phase1_spend_intelligence_report.json generated
□ Ready for Phase 2!
```

---

## 🎯 Next: Phase 2

Phase 2 now has full context:

```
Raw Supplier Data:
├─ Supplier quality (risk metrics)
├─ Disruption history
├─ Spend patterns
├─ Concentration risk
└─ Maverick spend

= Better fuzzy matching
= Smarter consolidation decisions
= Higher quality golden record
```

---

## 📞 Troubleshooting

### **Issue: "FileNotFoundError: phase1_risk_assessment.csv"**

**Solution:** Run Phase 1 scripts first
```bash
python build_phase1_tables.py
# Wait for completion, then:
python calculate_spend_analytics.py
```

### **Issue: "No new files created"**

**Check:**
1. Look at logs: `tail -100 logs/phase1_hybrid.log`
2. Check phase1_tables/ directory exists
3. Verify raw_supplier_risk_assessment.csv has data

### **Issue: Thresholds not matching**

**Check:**
1. Edit spend_analytics_config.py
2. Verify values are correct
3. Run: `python spend_analytics_config.py` to validate
4. Re-run: `python calculate_spend_analytics.py`

---

## 🚀 Summary

**What we added:**
- SUM calculation (contract coverage)
- Maverick spend detection (off-contract)
- Concentration risk analysis (supplier dependency)

**How long:** ~2 minutes execution time

**Output:** 3 new files + 2 new reports + enhanced existing files

**Next:** Phase 2 consolidation with full spend intelligence context

---

**Phase 1 Enhanced: Risk + Disruptions + Spend Intelligence** ✅
