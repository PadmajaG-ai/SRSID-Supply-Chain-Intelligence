# fetch_vendor_news_integrated.py - Guide

## 🎯 What It Does

**Single script that:**
- ✅ Fetches news from **NewsAPI, GDELT, Guardian** for each vendor in your CSV
- ✅ Extracts **disruption events** from articles
- ✅ **Auto-discovers new vendors** mentioned in articles
- ✅ **Auto-adds** new vendors to an enriched vendor list
- ✅ Outputs **vendor-disruption mapping**

---

## 📊 Input

**Required:** `vendors_list_simplified.csv`
```csv
Vendor Name,Industry
Apple,Electronics & Semiconductors
Tesla,Electronics & Vehicles
BASF,Chemicals
```

---

## 📤 Outputs

**1. vendor_disruption_mapping.csv** (Disruptions found)
```csv
Vendor_Name,Disruption_Type,Headline,Date,Source,URL
Apple,supply_chain,Apple faces chip shortage,2025-08-15,NewsAPI,https://...
Tesla,geopolitical,Tesla hit by trade tariffs,2025-08-10,NewsAPI,https://...
```

**2. new_vendors_discovered.csv** (Auto-added vendors)
```csv
name,industry,source,date_added,confidence,disruption,article
Intel,Electronics & Semiconductors,Auto-discovered,2025-03-19,95%,supply_chain,Intel warns of chip shortage
TSMC,Electronics & Semiconductors,Auto-discovered,2025-03-19,92%,shortage,TSMC faces production issues
Samsung Electronics,Electronics & Semiconductors,Auto-discovered,2025-03-19,88%,supply_chain,Samsung supply woes
```

**3. vendors_list_enriched.csv** (Updated vendor list)
```csv
Vendor Name,Industry,Source,DateAdded
Apple,Electronics & Semiconductors,CSV,2025-03-19
Tesla,Electronics & Vehicles,CSV,2025-03-19
BASF,Chemicals,CSV,2025-03-19
Intel,Electronics & Semiconductors,Auto-discovered,2025-03-19
TSMC,Electronics & Semiconductors,Auto-discovered,2025-03-19
```

---

## 🔄 Workflow

```
1. Load vendors from vendors_list_simplified.csv
   ├─ Apple, Tesla, BASF, ... (58 vendors)
   
2. For EACH vendor:
   ├─ Search NewsAPI
   ├─ Search GDELT
   ├─ Search Guardian
   │
   ├─ For EACH article found:
   │  ├─ Extract headline + text
   │  ├─ Detect disruption type
   │  └─ Save disruption (if found)
   │
   └─ Extract company names from articles
      ├─ Fuzzy match against known companies
      ├─ Check if exists in CSV
      └─ Auto-add if NEW

3. Save outputs:
   ├─ vendor_disruption_mapping.csv
   ├─ new_vendors_discovered.csv
   └─ vendors_list_enriched.csv
```

---

## 🚀 How to Use

### **Step 1: Set API Keys (Optional)**

```bash
# NewsAPI (free tier - included)
export NEWSAPI_KEY="your_key"

# Guardian (free tier)
export GUARDIAN_API_KEY="your_key"
```

### **Step 2: Run Script**

```bash
python fetch_vendor_news_integrated.py
```

### **Step 3: Check Outputs**

```bash
# View disruptions found
head vendor_disruption_mapping.csv

# View new vendors discovered
head new_vendors_discovered.csv

# View enriched vendor list
head vendors_list_enriched.csv
```

---

## 📊 Example Output

```
[INTEGRATED VENDOR NEWS FETCHER]

Processing 58 vendors from CSV...

[1/58] Apple
  NewsAPI: Apple → 12 articles
  GDELT: Apple → 0 articles
  Guardian: Apple → 3 articles
  ✨ New vendor discovered: Intel
  ✨ New vendor discovered: TSMC

[2/58] Tesla
  NewsAPI: Tesla → 8 articles
  GDELT: Tesla → 0 articles
  Guardian: Tesla → 2 articles
  ✨ New vendor discovered: Volkswagen

...

[58/58] Valero Energy Corporation
  NewsAPI: Valero → 2 articles

✓ Found 12 new vendors
✓ Found 245 disruptions

SAVING RESULTS
✓ Saved 245 disruptions to vendor_disruption_mapping.csv
✓ Saved 12 new vendors to new_vendors_discovered.csv
✓ Saved 70 vendors to vendors_list_enriched.csv (58 CSV + 12 new)

SUMMARY
CSV Vendors: 58
New Vendors Discovered: 12
Total Vendors: 70
Disruptions Found: 245

New Vendors:
  - Intel (Electronics & Semiconductors)
  - TSMC (Electronics & Semiconductors)
  - Samsung Electronics (Electronics & Semiconductors)
  - Volkswagen (Electronics & Vehicles)
  - ...

✅ Processing complete!
```

---

## 🔍 Key Features

### **1. Company Extraction**
- Searches for 100+ known companies in articles
- Uses fuzzy matching (95% confidence threshold)
- Avoids false positives (e.g., "Apple" the fruit)

### **2. Auto-Add Logic**
```
Extract company from article
  ↓
Is it in CSV vendors? → YES → Skip
  ↓ NO
Is it already in new vendors? → YES → Skip
  ↓ NO
Auto-detect industry
  ↓
Add to new_vendors_discovered.csv
```

### **3. Disruption Detection**
- Detects 8 disruption types:
  - supply_chain
  - bankruptcy
  - labor_strike
  - geopolitical
  - natural_disaster
  - recall
  - shortage
  - logistics_delay

### **4. Fuzzy Vendor Matching**
- 85%+ similarity threshold
- Handles: "Apple" vs "Apple Inc"
- Prevents duplicate entries

---

## ⚙️ Configuration

All in the script, easy to customize:

```python
DISRUPTION_TYPES = {
    "supply_chain": [...],  # Add more keywords
    "bankruptcy": [...],
    ...
}

KNOWN_COMPANIES = [
    "Apple", "Microsoft", ...  # Add more companies
]
```

---

## 📈 Integration with Phase 1

**New Phase 1 Order:**

```
1. synthesize_vendors.py
   ↓ (outputs vendors_variants.csv)

2. fetch_vendor_news_integrated.py  ← NEW (SINGLE SCRIPT FOR ALL 3 APIs)
   ├─ Fetches news for each vendor
   ├─ Auto-adds new vendors
   └─ Outputs vendor-disruption mapping

3. combine_disruption_sources.py
   ├─ Merge with GDELT/NewsAPI generic disruptions
   └─ Create master disruption list

4. (Continue with Phase 1...)
```

---

## 🎯 Benefits

✅ **Single Script** - No 3 separate files
✅ **All 3 APIs** - NewsAPI + GDELT + Guardian
✅ **Auto-Discovery** - Finds competitors & alternates
✅ **Deduplication** - No duplicate vendors
✅ **Fuzzy Matching** - Handles name variations
✅ **Confidence Scores** - Know quality of discovery
✅ **Industry Auto-Detection** - Guess industry for new vendors
✅ **Disruption-Linked** - Know why vendor was added
✅ **Enriched Dataset** - 70+ vendors instead of 58

---

## ⚠️ Limitations

- GDELT returns empty (would need complex setup)
- Guardian requires API key (free tier available)
- NewsAPI has free tier limits (100 requests/day)
- Company extraction limited to known ~120 companies
- Industry auto-detection is heuristic-based

---

## 🚀 What's Next?

1. **Run this script** to get vendor-disruption mapping
2. **Use enriched vendor list** for Phase 1
3. **Combine with GDELT/NewsAPI** generic disruptions
4. **Proceed with Phase 1 pipeline**

---

## 📝 Files in This Script

| Component | Purpose |
|-----------|---------|
| `load_vendors_from_csv()` | Load vendors from CSV |
| `extract_companies_from_text()` | Find company names in text |
| `vendor_exists()` | Check for fuzzy match in existing vendors |
| `detect_disruption_type()` | Classify disruptions |
| `get_industry_for_vendor()` | Auto-assign industry |
| `NewsAPIClient` | Fetch from NewsAPI |
| `GDELTClient` | Fetch from GDELT |
| `GuardianClient` | Fetch from Guardian |
| `VendorNewsProcessor` | Main processor |

---

**Single script, all features, zero redundancy!** ✅
