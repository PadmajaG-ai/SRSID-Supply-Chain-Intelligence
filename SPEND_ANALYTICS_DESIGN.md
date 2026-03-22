# Phase 1 Enhancement: Spend Analytics Design
## Spend Under Management + Maverick Spend + Concentration Risk

---

## 🎯 Overview

Adding 3 spend management KPIs to Phase 1 to bridge spend intelligence and supplier risk management:

```
Phase 1 (Before):
├─ Supplier Risk Data
├─ Disruption History
└─ Risk Metrics (Kraljic)

Phase 1 (After - Enhanced):
├─ Supplier Risk Data
├─ Disruption History
├─ Risk Metrics (Kraljic)
└─ Spend Analytics KPIs ✅ NEW
    ├─ Spend Under Management (SUM)
    ├─ Maverick Spend Detection
    └─ Spend Concentration Risk
```

---

## 📋 Component 1: Spend Under Management (SUM)

### **Definition**
Percentage of total spend that is covered by active contracts/agreements with suppliers.

### **Formula**
```
SUM % = (Spend Under Contract / Total Spend) × 100
```

### **Calculation Approach**

**Step 1: Contract Classification**
- Mark each transaction as "Contract" or "Non-Contract"
- Use heuristic rules (supplier + category based)
- Assumptions:
  - Regular/baseline transactions = likely under contract (80%)
  - Emergency/high-variance = likely not under contract (20%)
  - Disruption-related = non-contract (0% under contract during disruptions)

**Step 2: Per-Supplier SUM**
```
For each supplier:
├─ Sum all transactions = Total Spend
├─ Sum contract transactions = Spend Under Contract
└─ SUM % = (Contract / Total) × 100

Example - Apple:
├─ Total Spend: $4,500,000
├─ Contract Spend: $3,500,000 (77.8%)
└─ Non-Contract Spend: $1,000,000 (22.2%)
```

**Step 3: Enterprise SUM**
```
Aggregate all suppliers:
├─ Total Enterprise Spend: $500M
├─ Total Contract Spend: $400M
└─ Enterprise SUM: 80%
```

### **Output Metrics**
```
Per Supplier:
├─ supplier_name
├─ total_spend
├─ spend_under_contract
├─ sum_percentage
├─ contract_gaps
└─ contract_gap_percentage

Enterprise:
├─ total_enterprise_spend
├─ total_enterprise_contract_spend
├─ enterprise_sum_percentage
├─ top_contract_gaps (by amount)
└─ contract_opportunity_dollars
```

### **Rules for Heuristic Classification**

```python
def is_transaction_under_contract(transaction, supplier):
    """
    Classify if transaction is under contract.
    
    Rules (in order):
    1. If disruption_type != "None" → NOT under contract (0%)
    2. If is_baseline_spending → Likely contract (80%)
    3. If is_emergency_purchase → NOT contract (0%)
    4. Default → Likely contract (75%)
    
    Result: confidence_percentage (0-100%)
    """
    
    if transaction.disruption_type != "None":
        return False, 0  # During disruptions, assumed non-contract
    
    if transaction.amount > supplier.avg_spend * 3:
        return False, 0  # Emergency/unusual spending
    
    if is_baseline_spending(transaction):
        return True, 80  # Regular baseline = likely contract
    
    return True, 75  # Default assumption
```

---

## 📋 Component 2: Maverick Spend

### **Definition**
Spending that bypasses established contracts/processes - unauthorized, off-contract, or with unapproved vendors.

### **Types of Maverick Spend**
```
1. Off-Contract
   ├─ With approved vendor BUT outside contract terms
   ├─ Example: Buy from Apple at premium when contract exists
   └─ Risk: No volume discounts, compliance issue

2. Unauthorized Vendor
   ├─ With vendor not on approved list
   ├─ Example: Emergency buy from unknown supplier
   └─ Risk: Quality, compliance, audit trail

3. Emergency High-Cost
   ├─ Legitimate emergency BUT at high cost
   ├─ Example: Air freight when rail possible
   └─ Risk: Unnecessary cost

4. Non-Compliant
   ├─ Violates procurement policies
   ├─ Example: Bypassed approval authority
   └─ Risk: Governance, audit, fraud
```

### **Calculation Approach**

**Step 1: Identify Maverick Transactions**
```python
def classify_maverick_spend(transaction, supplier, category):
    """
    Classify transaction as maverick spend.
    
    Check in order:
    1. Is supplier approved? → If no → UNAUTHORIZED_VENDOR
    2. Is under contract? → If no → OFF_CONTRACT
    3. Is emergency high-cost? → If yes → EMERGENCY_HIGH_COST
    4. Is non-compliant? → If yes → NON_COMPLIANT
    5. Otherwise → COMPLIANT
    """
    
    if not is_approved_vendor(supplier):
        return "UNAUTHORIZED_VENDOR"
    
    if not is_under_contract(transaction):
        return "OFF_CONTRACT"
    
    if is_emergency_high_cost(transaction):
        return "EMERGENCY_HIGH_COST"
    
    if violates_policy(transaction):
        return "NON_COMPLIANT"
    
    return "COMPLIANT"
```

**Step 2: Per-Supplier Maverick**
```
For each supplier:
├─ Total Maverick Spend: $X
├─ Maverick %, by type:
│  ├─ Off-Contract: X%
│  ├─ Unauthorized: X%
│  ├─ Emergency High-Cost: X%
│  └─ Non-Compliant: X%
└─ Savings Opportunity: $(contract_rate × maverick_qty)
```

**Step 3: Enterprise Maverick**
```
Aggregate:
├─ Total Enterprise Spend: $500M
├─ Total Maverick Spend: $65M
├─ Maverick %: 13%
├─ By Type:
│  ├─ Off-Contract: $35M (54%)
│  ├─ Unauthorized: $20M (31%)
│  ├─ Emergency: $8M (12%)
│  └─ Non-Compliant: $2M (3%)
└─ Total Savings Opportunity: $18-22M
```

### **Output Metrics**
```
Per Supplier:
├─ supplier_name
├─ total_spend
├─ maverick_spend
├─ maverick_percentage
├─ maverick_by_type (breakdown)
└─ estimated_savings_opportunity

Transaction Level:
├─ supplier
├─ amount
├─ is_maverick (true/false)
├─ maverick_type (if maverick)
└─ estimated_savings

Enterprise:
├─ total_enterprise_maverick
├─ maverick_percentage
├─ by_type_breakdown
└─ total_savings_opportunity
```

### **Heuristic Rules**

```python
def calculate_maverick_spend():
    """
    For Phase 1, mark ~15% of transactions as maverick.
    
    Distribution:
    ├─ Off-Contract: 54% of maverick (8.1% of total)
    ├─ Unauthorized: 31% of maverick (4.65% of total)
    ├─ Emergency: 12% of maverick (1.8% of total)
    └─ Non-Compliant: 3% of maverick (0.45% of total)
    
    Rule: Mark transactions where:
    ├─ disruption_type != "None" → Off-Contract (30% of disruptions)
    ├─ amount > 2x avg + random → Emergency
    ├─ supplier not in approved → Unauthorized
    └─ random selection (5%) → Non-Compliant
    """
    
    maverick_rate = 0.15  # 15% of transactions
    
    for transaction in all_transactions:
        if should_mark_maverick(transaction, maverick_rate):
            transaction.is_maverick = True
            transaction.maverick_type = determine_type(transaction)
            transaction.savings_opportunity = calculate_savings(transaction)
```

---

## 📋 Component 3: Spend Concentration Risk

### **Definition**
Percentage of total spend concentrated with top suppliers - identifies over-dependence and diversification needs.

### **Metrics**
```
1. Concentration by Supplier
   ├─ % of total spend with each supplier
   └─ Identifies single-supplier dependency

2. Concentration by Category
   ├─ % of category spend with each supplier
   └─ Identifies category concentration

3. Top N Suppliers
   ├─ Top 1, 5, 10 suppliers cumulative %
   └─ Healthy: <10%, <40%, <65%

4. Herfindahl-Hirschman Index (HHI)
   ├─ Economic concentration measure
   ├─ Range: 0 (perfect competition) to 10,000 (monopoly)
   └─ <1500 = Healthy, 1500-2500 = Moderate, >2500 = High
```

### **Calculation Approach**

**Step 1: Rank Suppliers by Spend**
```
Supplier Ranking (descending by spend):
1. Apple: $50M (10.0%)
2. Samsung: $45M (9.0%)
3. Intel: $40M (8.0%)
4. BASF: $35M (7.0%)
5. Siemens: $30M (6.0%)
... (rest)
```

**Step 2: Calculate Concentration Metrics**
```
Per Supplier:
├─ Total Spend: $X
├─ % of Total Enterprise Spend: Y%
├─ Rank: N
├─ Cumulative Top N %: Z%
└─ Concentration Risk Level: Low/Medium/High

Per Category:
├─ Category Spend: $X
├─ Supplier % of Category: Y%
└─ Category Concentration Risk: Low/Medium/High
```

**Step 3: Calculate HHI**
```
HHI = Σ(market_share%)²

Example:
├─ Apple: (10%)² = 100
├─ Samsung: (9%)² = 81
├─ Intel: (8%)² = 64
├─ ... (rest)
└─ HHI = 1,850 (Moderate concentration)
```

**Step 4: Identify Diversification Needs**
```
For each high-concentration supplier:
├─ Current Spend: $X
├─ Target Concentration: <15%
├─ Reduction Needed: $Y
├─ Alternative Suppliers Available: N
└─ Diversification Priority: High/Medium/Low
```

### **Output Metrics**
```
Per Supplier:
├─ supplier_name
├─ total_spend
├─ spend_percentage_of_enterprise
├─ spend_percentage_of_category
├─ concentration_risk (Low/Medium/High)
├─ rank
├─ cumulative_top_n_percentage
└─ diversification_needed (true/false)

Category-Level:
├─ category
├─ total_spend
├─ top_3_suppliers (% of category)
├─ concentration_index (HHI)
└─ diversification_priority

Enterprise:
├─ hhi_index
├─ top_1_concentration
├─ top_5_concentration
├─ top_10_concentration
└─ diversification_opportunities (list)
```

### **Risk Classification Rules**

```python
def classify_concentration_risk(supplier_spend_pct, category_spend_pct):
    """
    Classify concentration risk level.
    
    Thresholds:
    ├─ HIGH: >15% enterprise OR >20% category
    ├─ MEDIUM: 10-15% enterprise OR 10-20% category
    └─ LOW: <10% enterprise AND <10% category
    """
    
    if supplier_spend_pct > 0.15 or category_spend_pct > 0.20:
        return "HIGH"
    elif supplier_spend_pct > 0.10 or category_spend_pct > 0.10:
        return "MEDIUM"
    else:
        return "LOW"


def calculate_hhi_index(suppliers):
    """
    Calculate Herfindahl-Hirschman Index.
    
    Interpretation:
    ├─ <0.15 (1500 points) = Healthy
    ├─ 0.15-0.25 = Moderate concentration
    └─ >0.25 = High concentration
    """
    
    total_spend = sum(s.spend for s in suppliers)
    hhi = 0
    
    for supplier in suppliers:
        market_share = (supplier.spend / total_spend) * 100
        hhi += market_share ** 2
    
    return hhi / 10000  # Normalize to 0-1
```

---

## 🔄 Integration with Existing Phase 1

### **Existing Phase 1 Tables (Unchanged Structure)**

```
raw_supplier_risk_assessment.csv
├─ supplier_name
├─ financial_stability_score
├─ delivery_performance
├─ historical_risk_category
├─ industry
└─ (other existing columns)

raw_supply_chain_transactions.csv
├─ supplier
├─ amount
├─ transaction_date
├─ disruption_type
├─ procurement_category
└─ (other existing columns)

raw_kraljic_matrix.csv
├─ supplier_name
├─ supply_risk_score
├─ profit_impact_score
├─ kraljic_quadrant
└─ (other existing columns)
```

### **Enhanced Phase 1 Tables (Add Columns)**

```
raw_supplier_risk_assessment.csv (ADD):
├─ total_spend
├─ spend_under_contract
├─ sum_percentage
├─ maverick_spend
├─ maverick_percentage
├─ spend_concentration_percentage
├─ concentration_risk_level
├─ diversification_priority
└─ savings_opportunity

raw_supply_chain_transactions.csv (ADD):
├─ is_under_contract (true/false)
├─ is_maverick (true/false)
├─ maverick_type (if applicable)
├─ estimated_savings
└─ spend_category
```

### **NEW Phase 1 Tables**

```
raw_spend_analytics.csv
├─ supplier_name
├─ category
├─ total_spend
├─ spend_under_contract
├─ sum_percentage
├─ maverick_spend
├─ maverick_percentage
├─ spend_concentration_pct_enterprise
├─ spend_concentration_pct_category
├─ concentration_risk_level
├─ rank
├─ cumulative_top_n_pct
├─ diversification_needed
└─ savings_opportunity

raw_concentration_analysis.csv
├─ category
├─ supplier_name
├─ spend
├─ pct_of_category
├─ pct_of_enterprise
├─ rank_in_category
├─ hhi_contribution
└─ alternatives_available

raw_maverick_spend_summary.csv
├─ supplier_name
├─ total_maverick_spend
├─ off_contract_spend
├─ unauthorized_vendor_spend
├─ emergency_high_cost_spend
├─ non_compliant_spend
├─ maverick_percentage
└─ estimated_savings_opportunity
```

---

## 📊 Data Flow (Enhanced Phase 1)

```
PHASE 1 INPUT:
├─ 56 vendors from CSV
├─ Real disruptions (GDELT + NewsAPI)
└─ Baseline transaction data (country/industry based)

PHASE 1 PROCESSING:

Existing Steps:
├─ Assign industries
├─ Create variants (3-5 per vendor)
├─ Fetch real disruptions
├─ Synthesize risk metrics
└─ Build Kraljic positioning

NEW Steps:
├─ Classify transactions (Contract/Non-Contract)
├─ Identify maverick transactions
├─ Calculate per-supplier SUM %
├─ Calculate per-supplier maverick spend
├─ Calculate concentration metrics
├─ Generate spend analytics

PHASE 1 OUTPUT (8 files total):

Risk Management (3):
├─ raw_supplier_risk_assessment.csv (enhanced)
├─ raw_supply_chain_transactions.csv (enhanced)
└─ raw_kraljic_matrix.csv (unchanged)

Spend Analytics (3 NEW):
├─ raw_spend_analytics.csv
├─ raw_concentration_analysis.csv
└─ raw_maverick_spend_summary.csv

Quality Reporting (2):
├─ phase1_quality_report.json (enhanced with spend metrics)
└─ phase1_spend_intelligence_report.json (NEW)
```

---

## 🎯 Thresholds & Rules (Recommended)

### **SUM Targets**
```
Excellent (>85%): >85% spend under contract
Good (80-85%): 80-85% spend under contract
Fair (75-80%): 75-80% spend under contract
Poor (<75%): <75% spend under contract

Target: Enterprise SUM >80%
```

### **Maverick Spend**
```
For Phase 1 baseline: 15% of transactions marked as maverick
Distribution:
├─ Off-Contract: 54% (8.1% of total)
├─ Unauthorized: 31% (4.65% of total)
├─ Emergency: 12% (1.8% of total)
└─ Non-Compliant: 3% (0.45% of total)

Target: <10% enterprise maverick spend
```

### **Concentration Risk**
```
By Supplier (% of total enterprise spend):
├─ HIGH RISK: >15%
├─ MEDIUM RISK: 10-15%
└─ LOW RISK: <10%

By Category (% of category spend):
├─ HIGH RISK: >20%
├─ MEDIUM RISK: 10-20%
└─ LOW RISK: <10%

HHI Index:
├─ <1500: Healthy
├─ 1500-2500: Moderate
└─ >2500: High concentration

Target: HHI <1500, Top supplier <10%, Top 5 <40%
```

---

## 📈 Expected Phase 1 Spend Analytics Output

### **Supplier Example - Apple**
```
raw_supplier_risk_assessment.csv (rows for Apple):
├─ supplier_name: Apple
├─ total_spend: $4,500,000
├─ spend_under_contract: $3,500,000
├─ sum_percentage: 77.8%
├─ maverick_spend: $675,000
├─ maverick_percentage: 15%
├─ spend_concentration_percentage: 0.9% (of enterprise)
├─ concentration_risk_level: LOW
├─ diversification_priority: LOW
└─ savings_opportunity: $225,000
```

### **Concentration Example**
```
raw_concentration_analysis.csv (top 5 suppliers):
1. Apple: 0.9% of enterprise, HIGH concentration risk
2. Samsung: 0.8%
3. Intel: 0.7%
4. BASF: 0.7%
5. Siemens: 0.6%
... (rest below 0.5%)

Enterprise HHI: 1,250 (Healthy)
Top 1: 0.9% (Low)
Top 5: 3.7% (Very Low)
```

### **Maverick Example**
```
raw_maverick_spend_summary.csv:
├─ total_enterprise_maverick: $65,000,000
├─ maverick_percentage: 13%
├─ off_contract_spend: $35,100,000 (54%)
├─ unauthorized_vendor_spend: $20,150,000 (31%)
├─ emergency_high_cost_spend: $7,800,000 (12%)
├─ non_compliant_spend: $1,950,000 (3%)
└─ estimated_savings_opportunity: $18,000,000
```

---

## ✅ Benefits of Enhanced Phase 1

✅ **Cost Savings Visibility** - Identify $X M maverick spend opportunity
✅ **Contract Gap Analysis** - See which suppliers need contracts
✅ **Concentration Risk Mitigation** - Know which suppliers are over-concentrated
✅ **Negotiation Leverage** - Identify high-spend suppliers for better terms
✅ **Diversification Strategy** - Data-driven alternative sourcing
✅ **Governance** - Complete spend visibility for compliance
✅ **Phase 2 Readiness** - Consolidation with spend context

---

## 🚀 Next: Generate Implementation Scripts

Once approved, I'll create:

1. **spend_analytics_config.py** - Configuration for thresholds/rules
2. **calculate_sum.py** - Spend Under Management calculator
3. **identify_maverick_spend.py** - Maverick transaction identifier
4. **analyze_concentration.py** - Concentration risk analyzer
5. **enhance_phase1_tables.py** - Add columns to existing tables
6. **generate_spend_outputs.py** - Create new spend analytics tables
7. **spend_intelligence_report.py** - Generate spend insights report

**Ready to proceed with implementation?** 🚀
