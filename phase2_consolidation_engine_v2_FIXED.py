"""
Phase 2 v2: Entity Resolution & Consolidation Engine with Regional Variant Detection
Consolidate duplicate vendors into unified suppliers - FIXED COLUMN NAMES
"""

import pandas as pd
import numpy as np
import logging
import sys
import os
from typing import List, Dict, Tuple, Set, Optional
from datetime import datetime
from difflib import SequenceMatcher
import json
import re

# ============================================================
# LOGGING SETUP
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/phase2_consolidation_v2.log', encoding='utf-8'),
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

# Phase 1 input files
PHASE1_FILES = {
    "vendors": "phase1_tables/vendors_list_enriched.csv",
    "risk_assessment": "phase1_tables/raw_supplier_risk_assessment.csv",
    "transactions": "phase1_tables/raw_supply_chain_transactions.csv",
    "disruptions": "phase1_tables/vendor_disruption_mapping.csv",
}

# Phase 2 output files
PHASE2_FILES = {
    "unified_master": "phase2_unified_supplier_master_v2.csv",
    "consolidation_mapping": "phase2_supplier_consolidation_mapping_v2.csv",
    "risk_assessment": "phase2_consolidated_risk_assessment_v2.csv",
    "quality_report": "phase2_quality_report_v2.json",
    "regional_variants": "phase2_regional_variants_detected.csv",
}

# Fuzzy matching thresholds
FUZZY_THRESHOLDS = {
    "exact": 0.99,           # Definite duplicate
    "high": 0.95,            # Very likely duplicate
    "medium": 0.85,          # Likely duplicate (for same-base vendors)
    "review": 0.75,          # Manual review
}

# Vendor suffixes to strip (EXCLUDING regional identifiers)
VENDOR_SUFFIXES = [
    " Inc", " Inc.", " Ltd", " Ltd.", " GmbH", " GmbH.",
    " S.A.", " S.A", " Corporation", " Corp", " Corp.",
    " Solutions", " Business Services", " Global"
]

# Regional identifiers (DO NOT STRIP - use for variant detection)
REGIONAL_KEYWORDS = {
    # Countries
    "USA": ["usa", "u.s.a", "us"],
    "FRANCE": ["france", "fr"],
    "GERMANY": ["germany", "de", "deutschland"],
    "UK": ["uk", "united kingdom", "england"],
    "JAPAN": ["japan", "jp"],
    "CHINA": ["china", "cn"],
    "INDIA": ["india", "in"],
    "BRAZIL": ["brazil", "br"],
    "CANADA": ["canada", "ca"],
    "MEXICO": ["mexico", "mx"],
    "AUSTRALIA": ["australia", "au"],
    "SINGAPORE": ["singapore", "sg"],
    "HONG KONG": ["hong kong", "hk"],
    "SOUTH KOREA": ["south korea", "korea", "kr"],
    "NETHERLANDS": ["netherlands", "nl"],
    "BELGIUM": ["belgium", "be"],
    "SPAIN": ["spain", "es"],
    "ITALY": ["italy", "it"],
    "SWEDEN": ["sweden", "se"],
    "NORWAY": ["norway", "no"],
    
    # Regions
    "EMEA": ["emea"],
    "EUROPE": ["europe"],
    "APAC": ["apac", "asia-pacific", "asia pacific"],
    "AMERICAS": ["americas", "amer"],
    "ASIA": ["asia"],
    "PACIFIC": ["pacific"],
    "MIDDLE EAST": ["middle east", "mena"],
    "AFRICA": ["africa"],
    "LATIN AMERICA": ["latin", "latam"],
    
    # Geographic descriptors
    "NORTH": ["north"],
    "SOUTH": ["south"],
    "EAST": ["east"],
    "WEST": ["west"],
    "CENTRAL": ["central"],
}

# ============================================================
# STEP 1: LOAD PHASE 1 DATA (FIXED COLUMN HANDLING)
# ============================================================

class Phase1DataLoader:
    """Load and combine all Phase 1 outputs with flexible column handling."""
    
    def __init__(self, file_paths: Dict[str, str]):
        self.file_paths = file_paths
        self.vendors = None
        self.risk_data = None
        self.transactions = None
        self.disruptions = None
        self.combined_data = None
    
    def load_all_data(self) -> pd.DataFrame:
        """Load and combine all Phase 1 data."""
        logger.info("\n" + "="*70)
        logger.info("PHASE 2 v2: ENTITY RESOLUTION WITH REGIONAL VARIANT DETECTION")
        logger.info("="*70)
        
        logger.info("\n[STEP 1] Loading Phase 1 Data...")
        
        # Load vendors
        logger.info("  Loading vendors...")
        self.vendors = self._load_file(self.file_paths.get("vendors"))
        logger.info(f"    ✓ {len(self.vendors)} vendors")
        
        # Load risk assessment
        logger.info("  Loading risk assessment...")
        self.risk_data = self._load_file(self.file_paths.get("risk_assessment"))
        logger.info(f"    ✓ {len(self.risk_data)} risk records")
        
        # Load transactions
        logger.info("  Loading transactions...")
        self.transactions = self._load_file(self.file_paths.get("transactions"))
        logger.info(f"    ✓ {len(self.transactions)} transactions")
        
        # Load disruptions
        logger.info("  Loading disruptions...")
        self.disruptions = self._load_file(self.file_paths.get("disruptions"))
        logger.info(f"    ✓ {len(self.disruptions)} disruptions")
        
        # Combine data
        logger.info("  Combining data...")
        self.combined_data = self._combine_data()
        logger.info(f"    ✓ {len(self.combined_data)} combined records")
        
        return self.combined_data
    
    def _load_file(self, file_path: str) -> pd.DataFrame:
        """Load a CSV file safely."""
        try:
            if not os.path.exists(file_path):
                logger.warning(f"    ⚠ File not found: {file_path} - using empty dataframe")
                return pd.DataFrame()
            
            return pd.read_csv(file_path)
        
        except Exception as e:
            logger.error(f"    ❌ Error loading {file_path}: {e}")
            return pd.DataFrame()
    
    def _combine_data(self) -> pd.DataFrame:
        """Combine all data into single dataframe with flexible column handling."""
        df = self.vendors.copy()
        
        # Normalize vendor column name (handle both "Vendor Name" and "NAME1")
        vendor_col = self._find_vendor_column(df)
        if vendor_col and vendor_col != "Vendor Name":
            df = df.rename(columns={vendor_col: "Vendor Name"})
        
        # Merge risk data
        if not self.risk_data.empty:
            risk_data = self.risk_data.copy()
            risk_vendor_col = self._find_vendor_column(risk_data)
            
            if risk_vendor_col:
                if risk_vendor_col != "Vendor Name":
                    risk_data = risk_data.rename(columns={risk_vendor_col: "Vendor Name"})
                
                df = df.merge(risk_data, on="Vendor Name", how="left")
                logger.info(f"    Merged risk data ({len(risk_data)} records)")
        
        # Merge transaction counts
        if not self.transactions.empty:
            trans_data = self.transactions.copy()
            trans_vendor_col = self._find_vendor_column(trans_data)
            
            if trans_vendor_col:
                if trans_vendor_col != "Vendor Name":
                    trans_data = trans_data.rename(columns={trans_vendor_col: "Vendor Name"})
                
                trans_counts = trans_data.groupby("Vendor Name").size().reset_index(name="transaction_count")
                df = df.merge(trans_counts, on="Vendor Name", how="left")
                df["transaction_count"] = df["transaction_count"].fillna(0).astype(int)
                logger.info(f"    Merged transaction data ({len(trans_counts)} vendors)")
        
        # Merge disruption counts
        if not self.disruptions.empty:
            disruption_data = self.disruptions.copy()
            disruption_vendor_col = self._find_vendor_column(disruption_data)
            
            if disruption_vendor_col:
                if disruption_vendor_col != "Vendor Name":
                    disruption_data = disruption_data.rename(columns={disruption_vendor_col: "Vendor Name"})
                
                disruption_counts = disruption_data.groupby("Vendor Name").size().reset_index(name="disruption_count")
                df = df.merge(disruption_counts, on="Vendor Name", how="left")
                df["disruption_count"] = df["disruption_count"].fillna(0).astype(int)
                logger.info(f"    Merged disruption data ({len(disruption_counts)} vendors)")
        
        return df
    
    @staticmethod
    def _find_vendor_column(df: pd.DataFrame) -> Optional[str]:
        """Find the vendor/supplier name column in dataframe."""
        possible_cols = [
            "Vendor Name", "vendor_name", "NAME1", "name",
            "supplier_name", "Supplier Name", "company_name"
        ]
        
        for col in possible_cols:
            if col in df.columns:
                return col
        
        return None

# ============================================================
# STEP 2: REGIONAL VARIANT DETECTION
# ============================================================

class RegionalVariantDetector:
    """Detect regional variants vs true duplicates."""
    
    @staticmethod
    def extract_regional_keywords(name: str) -> List[str]:
        """Extract regional keywords from vendor name."""
        name_lower = name.lower()
        found_regions = []
        
        for region_code, keywords in REGIONAL_KEYWORDS.items():
            for keyword in keywords:
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, name_lower):
                    found_regions.append(region_code)
                    break
        
        return found_regions
    
    @staticmethod
    def extract_base_name(name: str) -> str:
        """Extract base name without regional keywords."""
        name_lower = name.lower()
        
        for region_code, keywords in REGIONAL_KEYWORDS.items():
            for keyword in keywords:
                pattern = r'\s*\b' + re.escape(keyword) + r'\b\s*'
                name_lower = re.sub(pattern, ' ', name_lower, flags=re.IGNORECASE)
        
        for suffix in VENDOR_SUFFIXES:
            if name_lower.endswith(suffix.lower()):
                name_lower = name_lower[:-len(suffix)].strip()
        
        name_lower = " ".join(name_lower.split())
        
        return name_lower.strip()
    
    @staticmethod
    def classify_relationship(name1: str, name2: str) -> Tuple[str, Dict]:
        """Classify relationship between two vendor names."""
        regions1 = RegionalVariantDetector.extract_regional_keywords(name1)
        regions2 = RegionalVariantDetector.extract_regional_keywords(name2)
        base1 = RegionalVariantDetector.extract_base_name(name1)
        base2 = RegionalVariantDetector.extract_base_name(name2)
        
        if not regions1 and not regions2:
            if base1 == base2:
                return ("TRUE_DUPLICATE", {
                    "base1": base1,
                    "base2": base2,
                    "regions1": regions1,
                    "regions2": regions2,
                    "reason": "Same base name, no regional variants"
                })
        
        if base1 == base2 and (regions1 or regions2):
            if regions1 != regions2:
                return ("REGIONAL_VARIANT", {
                    "base1": base1,
                    "base2": base2,
                    "regions1": regions1,
                    "regions2": regions2,
                    "reason": f"Same base '{base1}' but different regions: {regions1} vs {regions2}"
                })
            else:
                return ("SAME_REGIONAL_ENTITY", {
                    "base1": base1,
                    "base2": base2,
                    "regions1": regions1,
                    "regions2": regions2,
                    "reason": f"Same base and same region(s): {regions1}"
                })
        
        if base1 == base2 and bool(regions1) != bool(regions2):
            return ("REGIONAL_SUBSIDIARY", {
                "base1": base1,
                "base2": base2,
                "regions1": regions1,
                "regions2": regions2,
                "reason": f"Same company '{base1}', one is global and one is regional"
            })
        
        return ("DIFFERENT_COMPANY", {
            "base1": base1,
            "base2": base2,
            "regions1": regions1,
            "regions2": regions2,
            "reason": f"Different base names: '{base1}' vs '{base2}'"
        })

# ============================================================
# STEP 3: SMART FUZZY MATCHING
# ============================================================

class SmartFuzzyMatcher:
    """Fuzzy matching that respects regional variants."""
    
    @staticmethod
    def jaro_winkler_similarity(s1: str, s2: str) -> float:
        """Calculate Jaro-Winkler similarity."""
        matcher = SequenceMatcher(None, s1.lower(), s2.lower())
        ratio = matcher.ratio()
        
        if s1.lower()[:4] == s2.lower()[:4]:
            ratio = ratio + (0.1 * (1 - ratio))
        
        return round(ratio, 4)
    
    @staticmethod
    def find_duplicates(vendors: List[str], threshold: float = 0.85) -> List[Dict]:
        """Find duplicate vendor pairs, considering regional variants."""
        duplicates = []
        detector = RegionalVariantDetector()
        
        for i in range(len(vendors)):
            for j in range(i + 1, len(vendors)):
                vendor1 = vendors[i]
                vendor2 = vendors[j]
                
                relationship, details = detector.classify_relationship(vendor1, vendor2)
                
                if relationship in ["REGIONAL_VARIANT", "DIFFERENT_COMPANY"]:
                    continue
                
                similarity = SmartFuzzyMatcher.jaro_winkler_similarity(vendor1, vendor2)
                
                if similarity >= threshold:
                    duplicates.append({
                        "vendor1": vendor1,
                        "vendor2": vendor2,
                        "similarity": similarity,
                        "relationship": relationship,
                        "details": details,
                    })
        
        return duplicates

# ============================================================
# STEP 4: BUILD CLUSTERS
# ============================================================

class ClusterBuilder:
    """Build clusters of duplicate vendors."""
    
    def __init__(self):
        self.clusters = {}
        self.vendor_to_cluster = {}
        self.cluster_id_counter = 0
    
    def build_clusters(self, duplicate_pairs: List[Dict]) -> Dict:
        """Build clusters from duplicate pairs."""
        logger.info("\n[STEP 4] Building Clusters...")
        
        for dup in duplicate_pairs:
            vendor1 = dup["vendor1"]
            vendor2 = dup["vendor2"]
            self._add_to_cluster(vendor1, vendor2)
        
        logger.info(f"  ✓ Created {len(self.clusters)} clusters")
        
        return self.clusters
    
    def _add_to_cluster(self, vendor1: str, vendor2: str):
        """Add vendors to appropriate cluster."""
        cluster1 = self.vendor_to_cluster.get(vendor1)
        cluster2 = self.vendor_to_cluster.get(vendor2)
        
        if cluster1 and cluster2:
            if cluster1 != cluster2:
                self.clusters[cluster1].extend(self.clusters[cluster2])
                for vendor in self.clusters[cluster2]:
                    self.vendor_to_cluster[vendor] = cluster1
                del self.clusters[cluster2]
        
        elif cluster1:
            self.clusters[cluster1].append(vendor2)
            self.vendor_to_cluster[vendor2] = cluster1
        
        elif cluster2:
            self.clusters[cluster2].append(vendor1)
            self.vendor_to_cluster[vendor1] = cluster2
        
        else:
            cluster_id = f"CLUSTER_{self.cluster_id_counter}"
            self.cluster_id_counter += 1
            self.clusters[cluster_id] = [vendor1, vendor2]
            self.vendor_to_cluster[vendor1] = cluster_id
            self.vendor_to_cluster[vendor2] = cluster_id
    
    def add_singletons(self, all_vendors: List[str]):
        """Add vendors with no duplicates as single-vendor clusters."""
        for vendor in all_vendors:
            if vendor not in self.vendor_to_cluster:
                cluster_id = f"CLUSTER_{self.cluster_id_counter}"
                self.cluster_id_counter += 1
                self.clusters[cluster_id] = [vendor]
                self.vendor_to_cluster[vendor] = cluster_id

# ============================================================
# STEP 5: SELECT MASTER RECORDS
# ============================================================

class MasterRecordSelector:
    """Select master record for each cluster."""
    
    def __init__(self, combined_data: pd.DataFrame):
        self.combined_data = combined_data
        self.masters = {}
    
    def select_masters(self, clusters: Dict) -> Dict[str, str]:
        """Select master record for each cluster."""
        logger.info("\n[STEP 5] Selecting Master Records...")
        
        for cluster_id, vendors in clusters.items():
            master = self._select_best_vendor(vendors)
            self.masters[cluster_id] = master
        
        logger.info(f"  ✓ Selected {len(self.masters)} masters")
        
        return self.masters
    
    def _select_best_vendor(self, vendors: List[str]) -> str:
        """Select best vendor from cluster."""
        candidates = []
        
        for vendor_name in vendors:
            vendor_data = self.combined_data[
                self.combined_data["Vendor Name"] == vendor_name
            ]
            
            if vendor_data.empty:
                continue
            
            score = self._calculate_score(vendor_data.iloc[0], vendor_name)
            candidates.append((vendor_name, score))
        
        if not candidates:
            return vendors[0]
        
        return max(candidates, key=lambda x: x[1])[0]
    
    def _calculate_score(self, row: pd.Series, vendor_name: str) -> float:
        """Calculate master record selection score."""
        score = 0.0
        
        source = row.get("Source", "")
        if source == "CSV" or "csv" in str(source).lower():
            score += 100
        
        trans_count = row.get("transaction_count", 0)
        if pd.notna(trans_count):
            score += float(trans_count) * 5
        
        confidence = row.get("Confidence", row.get("confidence", 0))
        if pd.notna(confidence):
            if isinstance(confidence, str) and "%" in str(confidence):
                confidence = float(str(confidence).replace("%", ""))
            score += float(confidence) * 10
        
        name_length = len(vendor_name)
        score += max(0, 100 - name_length)
        
        return score

# ============================================================
# STEP 6: MERGE METRICS
# ============================================================

class MetricsMerger:
    """Merge metrics from variant vendors."""
    
    def __init__(self, combined_data: pd.DataFrame):
        self.combined_data = combined_data
    
    def merge_variant_metrics(self, master_name: str, variant_names: List[str]) -> Dict:
        """Merge metrics from variants into master."""
        all_names = [master_name] + [v for v in variant_names if v != master_name]
        records = []
        
        for name in all_names:
            record = self.combined_data[self.combined_data["Vendor Name"] == name]
            if not record.empty:
                records.append(record.iloc[0])
        
        if not records:
            return {}
        
        master = records[0].to_dict()
        
        perf_scores = []
        for record in records:
            perf = record.get("delivery_performance", record.get("Perf_Score"))
            if pd.notna(perf):
                perf_scores.append(float(perf))
        
        if perf_scores:
            master["Perf_Score"] = round(sum(perf_scores) / len(perf_scores), 2)
        
        fin_stabs = []
        for record in records:
            fin = record.get("financial_stability_score", record.get("Fin_Stab"))
            if pd.notna(fin):
                fin_stabs.append(float(fin))
        
        if fin_stabs:
            master["Fin_Stab"] = round(sum(fin_stabs) / len(fin_stabs), 2)
        
        total_trans = 0
        for record in records:
            trans = record.get("transaction_count", 0)
            if pd.notna(trans):
                total_trans += float(trans)
        master["TotalTransactions"] = int(total_trans)
        
        total_disruptions = 0
        for record in records:
            disr = record.get("disruption_count", 0)
            if pd.notna(disr):
                total_disruptions += float(disr)
        master["UniqueDisruptions"] = int(total_disruptions)
        
        perf_score = master.get("Perf_Score")
        if perf_score and perf_score >= 0.75:
            master["Risk_Category"] = "Low"
        elif perf_score and perf_score >= 0.5:
            master["Risk_Category"] = "Medium"
        else:
            master["Risk_Category"] = "High"
        
        master["VariantCount"] = len(variant_names)
        
        return master

# ============================================================
# STEP 7: DETECT & REPORT REGIONAL VARIANTS
# ============================================================

class RegionalVariantReporter:
    """Track and report regional variants that were kept separate."""
    
    def __init__(self, combined_data: pd.DataFrame):
        self.combined_data = combined_data
        self.regional_variants = []
    
    def report_regional_variants(self, all_vendors: List[str]) -> pd.DataFrame:
        """Report all detected regional variants."""
        logger.info("\n[STEP 7] Detecting Regional Variants...")
        
        detector = RegionalVariantDetector()
        vendor_groups = {}
        
        for vendor in all_vendors:
            base_name = detector.extract_base_name(vendor)
            regions = detector.extract_regional_keywords(vendor)
            
            if base_name not in vendor_groups:
                vendor_groups[base_name] = []
            
            vendor_groups[base_name].append({
                "vendor_name": vendor,
                "base_name": base_name,
                "regions": regions,
                "region_str": ", ".join(regions) if regions else "Global",
            })
        
        for base_name, vendors in vendor_groups.items():
            if len(vendors) > 1:
                regions_set = [tuple(v["regions"]) for v in vendors]
                if len(set(regions_set)) > 1:
                    for v in vendors:
                        self.regional_variants.append({
                            "Vendor_Name": v["vendor_name"],
                            "Base_Name": base_name,
                            "Regions": v["region_str"],
                            "Status": "KEPT_SEPARATE_AS_REGIONAL_VARIANT",
                        })
        
        logger.info(f"  ✓ Detected {len(self.regional_variants)} regional variants")
        
        return pd.DataFrame(self.regional_variants)

# ============================================================
# STEP 8: CREATE CONSOLIDATION MAPPING
# ============================================================

class ConsolidationMapper:
    """Create mapping of duplicates to masters."""
    
    def __init__(self, combined_data: pd.DataFrame):
        self.combined_data = combined_data
        self.mappings = []
    
    def create_mappings(self, clusters: Dict, masters: Dict, duplicates: List[Dict]) -> pd.DataFrame:
        """Create consolidation mapping table."""
        logger.info("\n[STEP 8] Creating Consolidation Mapping...")
        
        matcher = SmartFuzzyMatcher()
        
        for cluster_id, vendors in clusters.items():
            master_name = masters[cluster_id]
            master_record = self.combined_data[
                self.combined_data["Vendor Name"] == master_name
            ]
            
            if master_record.empty:
                continue
            
            master_lifnr = master_record.iloc[0].get("LIFNR", "")
            
            for vendor_name in vendors:
                if vendor_name == master_name:
                    continue
                
                variant_record = self.combined_data[
                    self.combined_data["Vendor Name"] == vendor_name
                ]
                
                if variant_record.empty:
                    continue
                
                variant_lifnr = variant_record.iloc[0].get("LIFNR", "")
                similarity = matcher.jaro_winkler_similarity(master_name, vendor_name)
                
                relationship = "DUPLICATE"
                for dup in duplicates:
                    if {dup["vendor1"], dup["vendor2"]} == {master_name, vendor_name}:
                        relationship = dup["relationship"]
                        break
                
                mapping = {
                    "MasterLIFNR": master_lifnr,
                    "MasterName": master_name,
                    "VariantLIFNR": variant_lifnr,
                    "VariantName": vendor_name,
                    "MergeType": relationship,
                    "Similarity": similarity,
                }
                
                self.mappings.append(mapping)
        
        logger.info(f"  ✓ Created {len(self.mappings)} mappings")
        
        return pd.DataFrame(self.mappings)

# ============================================================
# STEP 9: BUILD TABLES
# ============================================================

class Phase2TableBuilder:
    """Build Phase 2 output tables."""
    
    def __init__(self, combined_data: pd.DataFrame, clusters: Dict, 
                 masters: Dict, consolidated_records: Dict):
        self.combined_data = combined_data
        self.clusters = clusters
        self.masters = masters
        self.consolidated_records = consolidated_records
    
    def build_unified_master_table(self) -> pd.DataFrame:
        """Build unified supplier master table."""
        logger.info("\n[STEP 9] Building Output Tables...")
        
        records = []
        
        for cluster_id, master_name in self.masters.items():
            consolidated = self.consolidated_records.get(cluster_id, {})
            
            record = {
                "MasterName": master_name,
                "Industry": consolidated.get("industry"),
                "Country": consolidated.get("supplier_country"),
                "Perf_Score": consolidated.get("Perf_Score"),
                "Fin_Stab": consolidated.get("Fin_Stab"),
                "Risk_Category": consolidated.get("Risk_Category"),
                "TotalTransactions": consolidated.get("TotalTransactions", 0),
                "UniqueDisruptions": consolidated.get("UniqueDisruptions", 0),
                "VariantCount": consolidated.get("VariantCount", 1),
            }
            
            records.append(record)
        
        df = pd.DataFrame(records)
        logger.info(f"  ✓ Built unified master: {len(df)} records")
        
        return df

# ============================================================
# STEP 10: VALIDATE
# ============================================================

class Phase2Validator:
    """Validate Phase 2 consolidation."""
    
    def validate(self, clusters: Dict, masters: Dict, unified_table: pd.DataFrame) -> Dict:
        """Run validation checks."""
        logger.info("\n[STEP 10] Validating Consolidation...")
        
        errors = []
        warnings = []
        
        master_names = list(masters.values())
        if len(master_names) != len(set(master_names)):
            errors.append("Duplicate master names found!")
        
        total_clusters = len(clusters)
        unified_count = len(unified_table)
        
        if total_clusters != unified_count:
            errors.append(f"Cluster count ({total_clusters}) != unified count ({unified_count})")
        
        logger.info(f"  ✓ Validation complete")
        
        return {
            "errors": errors,
            "warnings": warnings,
            "quality": "PASS" if not errors else "FAIL",
        }

# ============================================================
# STEP 11: GENERATE REPORT
# ============================================================

def generate_quality_report(original_count: int, unified_count: int, 
                           regional_variants_count: int, clusters: Dict, 
                           validation: Dict, mapping_df: pd.DataFrame) -> Dict:
    """Generate Phase 2 quality report."""
    logger.info("\n[STEP 11] Generating Quality Report...")
    
    report = {
        "phase": "Phase 2 v2: Entity Resolution with Regional Variant Detection",
        "timestamp": datetime.now().isoformat(),
        "improvement": "Distinguishes between TRUE DUPLICATES and REGIONAL VARIANTS",
        "input": {
            "total_vendors": original_count,
        },
        "process": {
            "regional_variant_detection": "ENABLED",
            "regional_variants_kept_separate": regional_variants_count,
            "clustering": {
                "clusters_created": len(clusters),
                "pairs_consolidated": len(mapping_df),
            }
        },
        "output": {
            "unified_suppliers": unified_count,
            "regional_variants_as_separate_entities": regional_variants_count,
            "consolidation_ratio": round(original_count / max(unified_count, 1), 2),
            "quality": validation.get("quality"),
            "errors": len(validation.get("errors", [])),
        },
        "files_created": [
            "phase2_unified_supplier_master_v2.csv",
            "phase2_supplier_consolidation_mapping_v2.csv",
            "phase2_consolidated_risk_assessment_v2.csv",
            "phase2_regional_variants_detected.csv",
            "phase2_quality_report_v2.json",
        ]
    }
    
    logger.info(f"  ✓ Report generated")
    
    return report

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Main execution."""
    try:
        # STEP 1: Load Phase 1 data
        loader = Phase1DataLoader(PHASE1_FILES)
        combined_data = loader.load_all_data()
        
        if combined_data.empty:
            logger.error("❌ No data loaded!")
            return 1
        
        # Get unique vendor names
        all_vendors = combined_data["Vendor Name"].unique().tolist()
        logger.info(f"\n  Total unique vendors: {len(all_vendors)}")
        
        # STEP 2-3: Regional variant detection + smart fuzzy matching
        logger.info("\n[STEP 2-3] Regional Variant Detection & Smart Fuzzy Matching...")
        duplicates = SmartFuzzyMatcher.find_duplicates(all_vendors, threshold=FUZZY_THRESHOLDS["medium"])
        logger.info(f"  ✓ Found {len(duplicates)} duplicate pairs (excluding regional variants)")
        
        for i, dup in enumerate(duplicates[:5]):
            logger.info(f"    - {dup['vendor1']} ≈ {dup['vendor2']} ({dup['relationship']}, {dup['similarity']})")
        
        # STEP 4: Build clusters
        cluster_builder = ClusterBuilder()
        clusters = cluster_builder.build_clusters(duplicates)
        cluster_builder.add_singletons(all_vendors)
        
        # STEP 5: Select masters
        selector = MasterRecordSelector(combined_data)
        masters = selector.select_masters(clusters)
        
        # STEP 6: Merge metrics
        logger.info("\n[STEP 6] Merging Variant Metrics...")
        merger = MetricsMerger(combined_data)
        consolidated_records = {}
        
        for cluster_id, vendors in clusters.items():
            master = masters[cluster_id]
            consolidated = merger.merge_variant_metrics(master, vendors)
            consolidated_records[cluster_id] = consolidated
        
        logger.info(f"  ✓ Merged metrics for {len(consolidated_records)} clusters")
        
        # STEP 7: Detect regional variants
        regional_reporter = RegionalVariantReporter(combined_data)
        regional_variants_df = regional_reporter.report_regional_variants(all_vendors)
        
        # STEP 8: Create mapping
        mapper = ConsolidationMapper(combined_data)
        mapping_df = mapper.create_mappings(clusters, masters, duplicates)
        
        # STEP 9: Build tables
        table_builder = Phase2TableBuilder(combined_data, clusters, masters, consolidated_records)
        unified_table = table_builder.build_unified_master_table()
        
        # STEP 10: Validate
        validator = Phase2Validator()
        validation = validator.validate(clusters, masters, unified_table)
        
        # STEP 11: Generate report
        report = generate_quality_report(len(all_vendors), len(unified_table), 
                                        len(regional_variants_df), clusters, 
                                        validation, mapping_df)
        
        # Save outputs
        logger.info("\n[SAVING] Writing output files...")
        
        os.makedirs(os.path.dirname(PHASE2_FILES["unified_master"]) or ".", exist_ok=True)
        
        unified_table.to_csv(PHASE2_FILES["unified_master"], index=False)
        logger.info(f"  ✓ {PHASE2_FILES['unified_master']}")
        
        mapping_df.to_csv(PHASE2_FILES["consolidation_mapping"], index=False)
        logger.info(f"  ✓ {PHASE2_FILES['consolidation_mapping']}")
        
        unified_table.to_csv(PHASE2_FILES["risk_assessment"], index=False)
        logger.info(f"  ✓ {PHASE2_FILES['risk_assessment']}")
        
        if not regional_variants_df.empty:
            regional_variants_df.to_csv(PHASE2_FILES["regional_variants"], index=False)
            logger.info(f"  ✓ {PHASE2_FILES['regional_variants']}")
        
        with open(PHASE2_FILES["quality_report"], "w") as f:
            json.dump(report, f, indent=2)
        logger.info(f"  ✓ {PHASE2_FILES['quality_report']}")
        
        # Summary
        logger.info("\n" + "="*70)
        logger.info("PHASE 2 v2 COMPLETE (WITH REGIONAL VARIANT DETECTION)")
        logger.info("="*70)
        logger.info(f"Original vendors: {len(all_vendors)}")
        logger.info(f"Unified suppliers: {len(unified_table)}")
        logger.info(f"Regional variants (kept separate): {len(regional_variants_df)}")
        logger.info(f"Consolidation ratio: {report['output']['consolidation_ratio']}x")
        logger.info(f"Validation: {validation['quality']}")
        
        if len(regional_variants_df) > 0:
            logger.info(f"\n✨ Regional Variants Detected (Kept Separate):")
            for idx, row in regional_variants_df.head(10).iterrows():
                logger.info(f"    - {row['Vendor_Name']} ({row['Regions']})")
        
        if validation.get("errors"):
            logger.info(f"\n⚠ Errors found: {len(validation['errors'])}")
            for error in validation['errors'][:5]:
                logger.info(f"  - {error}")
        else:
            logger.info("\n✅ No validation errors!")
        
        logger.info("\n✅ Phase 2 v2 consolidation complete!")
        
        return 0
    
    except Exception as e:
        logger.error(f"❌ Phase 2 v2 failed: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    sys.exit(main())
