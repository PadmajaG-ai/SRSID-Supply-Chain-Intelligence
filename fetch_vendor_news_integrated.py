"""
Integrated Vendor News Fetcher
Fetch news from NewsAPI, GDELT, and Guardian for vendors in CSV
Auto-add new vendors discovered in disruption articles
Outputs: vendor-disruption mapping + enriched vendor list
"""

import requests
import pandas as pd
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import List, Dict, Set, Tuple, Optional
import json
import time
from difflib import SequenceMatcher

# ============================================================
# LOGGING SETUP
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/vendor_news_integrated.log', encoding='utf-8'),
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

VENDORS_CSV = "vendors_list_simplified.csv"

OUTPUT_FILES = {
    "vendor_disruptions": "vendor_disruption_mapping.csv",
    "enriched_vendors": "vendors_list_enriched.csv",
    "new_vendors_discovered": "new_vendors_discovered.csv",
}

# Disruption keywords
DISRUPTION_TYPES = {
    "supply_chain": ["supply chain", "supply disruption", "supply constraint", "supply problem"],
    "bankruptcy": ["bankruptcy", "bankrupt", "insolvency", "financial crisis"],
    "labor_strike": ["strike", "labor dispute", "worker protest", "union action"],
    "geopolitical": ["sanction", "tariff", "trade war", "export ban", "import ban"],
    "natural_disaster": ["earthquake", "flood", "hurricane", "storm", "weather disaster"],
    "recall": ["recall", "product recall", "defect", "safety recall"],
    "shortage": ["shortage", "chip shortage", "semiconductor shortage", "material shortage"],
    "logistics_delay": ["delay", "logistics delay", "port delay", "shipping delay"],
}

# Known vendors for company extraction
KNOWN_COMPANIES = [
    # Tech
    "Apple", "Microsoft", "Google", "Amazon", "Meta", "Tesla", "Intel", "NVIDIA", "AMD", 
    "IBM", "Samsung", "TSMC", "Qualcomm", "Broadcom", "Cisco", "Dell", "HP", "Lenovo", "ASUS",
    "Sony", "Nintendo", "Razer", "MSI", "Corsair", "Western Digital", "Seagate",
    
    # Automotive
    "Toyota", "Volkswagen", "BMW", "Mercedes", "Ford", "GM", "Hyundai", "Kia", "Volvo", "Audi",
    "Porsche", "Lamborghini", "Ferrari", "Bugatti", "Rolls-Royce", "Bentley", "Jaguar", "Tesla",
    
    # Chemicals & Energy
    "BASF", "Dow", "DuPont", "Eastman", "Lyondell", "Shell", "ExxonMobil", "Chevron", 
    "Saudi Aramco", "Gazprom", "BP", "Total", "ConocoPhillips", "Valero",
    
    # Pharma & Healthcare
    "Johnson & Johnson", "Pfizer", "Merck", "AstraZeneca", "Novartis", "Roche", "Eli Lilly",
    "Bristol Myers", "Amgen", "Sanofi", "GlaxoSmithKline", "Moderna", "BioNTech",
    
    # Food & Beverage
    "Nestlé", "Unilever", "Procter & Gamble", "Coca-Cola", "PepsiCo", "Kraft Heinz", "Mondelez",
    "Kellogg", "General Mills", "Danone", "Lactalis", "Dairygold",
    
    # Retail & Distribution
    "Walmart", "Amazon", "Costco", "Target", "Best Buy", "Home Depot", "Lowe's", "Alibaba",
    "JD.com", "Taobao", "eBay", "Shopify", "Etsy",
    
    # Logistics & Transport
    "DHL", "FedEx", "UPS", "Maersk", "Hapag-Lloyd", "MSC", "CMA CGM", "Evergreen", "ONE",
    "DB Schenker", "XPO", "J.B. Hunt", "Werner", "Schneider National",
    
    # Industrial & Manufacturing
    "Siemens", "ABB", "Schneider Electric", "Eaton", "Rockwell", "Honeywell", "Parker Hannifin",
    "Emerson", "SPX Corporation", "Flowserve", "Roper Technologies",
    
    # Semiconductors & Electronics
    "Qualcomm", "Broadcom", "MediaTek", "Nvidia", "AMD", "ARM", "Marvell", "Microchip", 
    "STMicroelectronics", "NXP", "Infineon", "SanDisk", "Micron", "SK Hynix", "Kioxia",
    
    # Others
    "Cirque du Soleil", "Rich's", "Indo Autotech", "Zentis", "Flint Group", "Fisher Scientific",
    "Eppendorf", "Grainger", "Qiagen", "CDW", "Manutan", "RS Components", "Graybar",
    "Office Depot", "Air Liquide", "Binder", "Saint-Gobain",
]

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def load_vendors_from_csv(csv_path: str = VENDORS_CSV) -> List[Dict]:
    """Load vendors from CSV file."""
    try:
        if not os.path.exists(csv_path):
            logger.error(f"❌ {csv_path} not found!")
            return []
        
        df = pd.read_csv(csv_path)
        vendors = []
        
        for idx, row in df.iterrows():
            vendor_name = str(row.get("Vendor Name", "")).strip()
            if not vendor_name or vendor_name == "nan":
                continue
            
            industry = str(row.get("Industry", "")).strip()
            if pd.isna(row.get("Industry")) or industry == "nan" or industry == "":
                industry = None
            
            vendor = {
                "name": vendor_name,
                "industry": industry,
                "source": "CSV",
                "date_added": row.get("DateAdded", str(datetime.now().date())),
                "confidence": 100,
            }
            vendors.append(vendor)
        
        logger.info(f"✓ Loaded {len(vendors)} vendors from CSV")
        return vendors
    
    except Exception as e:
        logger.error(f"❌ Error loading vendors: {e}")
        return []

def extract_companies_from_text(text: str, min_confidence: float = 0.8) -> List[Tuple[str, float]]:
    """
    Extract company names from text using fuzzy matching.
    
    Returns:
        List of (company_name, confidence_score) tuples
    """
    companies = []
    text_lower = text.lower()
    
    for company in KNOWN_COMPANIES:
        if company.lower() in text_lower:
            # Simple confidence: longer company names = higher confidence
            confidence = min(0.95, 0.5 + (len(company) / 50))
            companies.append((company, confidence))
    
    # Remove duplicates, keep highest confidence
    seen = {}
    for company, confidence in companies:
        key = company.lower()
        if key not in seen or confidence > seen[key][1]:
            seen[key] = (company, confidence)
    
    return [v for v in seen.values() if v[1] >= min_confidence]

def vendor_exists(vendor_name: str, vendors: List[Dict], threshold: float = 0.85) -> Optional[str]:
    """
    Check if vendor exists using fuzzy matching.
    
    Returns:
        Existing vendor name if found, else None
    """
    vendor_lower = vendor_name.lower()
    
    for vendor in vendors:
        existing_lower = vendor["name"].lower()
        similarity = SequenceMatcher(None, vendor_lower, existing_lower).ratio()
        
        if similarity >= threshold:
            return vendor["name"]
    
    return None

def detect_disruption_type(text: str) -> Optional[str]:
    """Detect disruption type from text."""
    text_lower = text.lower()
    
    for disruption_type, keywords in DISRUPTION_TYPES.items():
        for keyword in keywords:
            if keyword.lower() in text_lower:
                return disruption_type
    
    return None

def get_industry_for_vendor(company_name: str) -> str:
    """Try to guess industry for new vendor."""
    tech_keywords = ["apple", "microsoft", "google", "amazon", "intel", "nvidia", "amd", "cisco"]
    auto_keywords = ["toyota", "volkswagen", "bmw", "ford", "gm", "tesla"]
    pharma_keywords = ["pfizer", "merck", "johnson", "astrazeneca", "novartis", "roche"]
    energy_keywords = ["shell", "exxon", "chevron", "bp", "valero"]
    
    name_lower = company_name.lower()
    
    if any(kw in name_lower for kw in tech_keywords):
        return "Electronics & Semiconductors"
    elif any(kw in name_lower for kw in auto_keywords):
        return "Electronics & Vehicles"
    elif any(kw in name_lower for kw in pharma_keywords):
        return "Pharmaceuticals"
    elif any(kw in name_lower for kw in energy_keywords):
        return "Energy & Oil"
    else:
        return "Other"

# ============================================================
# NEWS API CLIENTS
# ============================================================

class NewsAPIClient:
    """Fetch news from NewsAPI."""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("NEWSAPI_KEY", "c2ce506d781e4c7f87f3305da07d3430")
        self.base_url = "https://newsapi.org/v2/everything"
        self.session = requests.Session()
    
    def search_vendor(self, vendor_name: str, days_back: int = 30) -> List[Dict]:
        """Search for vendor news."""
        try:
            from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
            
            params = {
                "q": f'"{vendor_name}" (supply OR disruption OR shortage OR recall OR strike)',
                "from": from_date,
                "sortBy": "publishedAt",
                "apiKey": self.api_key,
                "pageSize": 50,
                "language": "en",
            }
            
            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            articles = response.json().get("articles", [])
            logger.info(f"  NewsAPI: {vendor_name} → {len(articles)} articles")
            
            return articles
        
        except Exception as e:
            logger.warning(f"  NewsAPI error for {vendor_name}: {e}")
            return []

class GDELTClient:
    """Fetch events from GDELT (using sample data)."""
    
    def search_vendor(self, vendor_name: str) -> List[Dict]:
        """Search for vendor events."""
        # GDELT is complex; using sample approach
        logger.info(f"  GDELT: {vendor_name} → searching...")
        
        # In production, would query: https://api.gdeltproject.org/api/v2/
        # For now, return empty (phase can be extended)
        return []

class GuardianClient:
    """Fetch news from Guardian API."""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("GUARDIAN_API_KEY", "bb0b336f-6c59-4e4c-96b4-3955a6b07ba8")
        self.base_url = "https://open-platform.theguardian.com/search"
        self.session = requests.Session()
    
    def search_vendor(self, vendor_name: str, days_back: int = 365) -> List[Dict]:
        """Search for vendor news."""
        if not self.api_key or self.api_key == "":
            logger.warning(f"  Guardian: API key not configured, skipping")
            return []
        
        try:
            from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
            
            params = {
                "q": f'"{vendor_name}" (supply OR disruption OR shortage OR recall OR strike)',
                "api-key": self.api_key,
                "format": "json",
                "show-fields": "headline,trailText,lastModified",
                "page-size": 50,
                "order-by": "newest",
                "from-date": from_date,
            }
            
            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            articles = response.json().get("response", {}).get("results", [])
            logger.info(f"  Guardian: {vendor_name} → {len(articles)} articles")
            
            return articles
        
        except Exception as e:
            logger.warning(f"  Guardian error for {vendor_name}: {e}")
            return []

# ============================================================
# MAIN PROCESSOR
# ============================================================

class VendorNewsProcessor:
    """Process vendor news and auto-add new vendors."""
    
    def __init__(self, vendors: List[Dict]):
        self.vendors = vendors
        self.new_vendors = []
        self.disruptions = []
        
        self.newsapi = NewsAPIClient()
        self.gdelt = GDELTClient()
        self.guardian = GuardianClient()
    
    def process_all_vendors(self) -> Tuple[List[Dict], List[Dict]]:
        """
        Fetch news for all vendors and auto-add discovered vendors.
        
        Returns:
            (disruptions, new_vendors)
        """
        logger.info("\n" + "="*70)
        logger.info("INTEGRATED VENDOR NEWS FETCHER")
        logger.info("="*70)
        
        logger.info(f"\nProcessing {len(self.vendors)} vendors from CSV...")
        
        for idx, vendor in enumerate(self.vendors, 1):
            logger.info(f"\n[{idx}/{len(self.vendors)}] {vendor['name']}")
            
            # Search all 3 APIs
            newsapi_articles = self.newsapi.search_vendor(vendor['name'])
            gdelt_articles = self.gdelt.search_vendor(vendor['name'])
            guardian_articles = self.guardian.search_vendor(vendor['name'])
            
            # Combine articles
            all_articles = newsapi_articles + gdelt_articles + guardian_articles
            
            # Process articles
            self._process_articles(
                articles=all_articles,
                vendor_name=vendor['name'],
                vendor_industry=vendor.get('industry'),
                source_counts={
                    'NewsAPI': len(newsapi_articles),
                    'GDELT': len(gdelt_articles),
                    'Guardian': len(guardian_articles),
                }
            )
        
        logger.info(f"\n✓ Found {len(self.new_vendors)} new vendors")
        logger.info(f"✓ Found {len(self.disruptions)} disruptions")
        
        return self.disruptions, self.new_vendors
    
    def _process_articles(self, articles: List[Dict], vendor_name: str, vendor_industry: str, 
                         source_counts: Dict):
        """Process articles for a vendor."""
        
        for article in articles:
            # Get text
            headline = article.get('title') or article.get('fields', {}).get('headline', '')
            text = article.get('description') or article.get('fields', {}).get('trailText', '') or ''
            full_text = f"{headline} {text}".lower()
            
            # Detect disruption
            disruption_type = detect_disruption_type(full_text)
            if not disruption_type:
                continue
            
            # Record disruption
            disruption = {
                'Vendor_Name': vendor_name,
                'Disruption_Type': disruption_type,
                'Headline': headline,
                'Date': article.get('publishedAt', '')[:10] if article.get('publishedAt') else '',
                'Source': article.get('source', {}).get('name', 'Unknown') if isinstance(article.get('source'), dict) else 'News',
                'URL': article.get('url') or article.get('webUrl', ''),
            }
            self.disruptions.append(disruption)
            
            # Extract new vendors from article
            extracted_companies = extract_companies_from_text(full_text)
            
            for company_name, confidence in extracted_companies:
                # Check if already in CSV vendors
                existing = vendor_exists(company_name, self.vendors)
                if existing:
                    continue
                
                # Check if already discovered
                already_discovered = any(
                    v['name'].lower() == company_name.lower() for v in self.new_vendors
                )
                if already_discovered:
                    continue
                
                # Add as new vendor
                new_vendor = {
                    'name': company_name,
                    'industry': get_industry_for_vendor(company_name),
                    'source': 'Auto-discovered',
                    'date_added': str(datetime.now().date()),
                    'confidence': f"{int(confidence*100)}%",
                    'disruption': disruption_type,
                    'article': headline,
                }
                
                self.new_vendors.append(new_vendor)
                logger.info(f"  ✨ New vendor discovered: {company_name}")
    
    def save_results(self, vendors_csv: str = VENDORS_CSV):
        """Save disruptions and update vendor CSV."""
        
        logger.info("\n" + "="*70)
        logger.info("SAVING RESULTS")
        logger.info("="*70)
        
        # Save disruption mapping
        disruptions_df = pd.DataFrame(self.disruptions)
        disruptions_df.to_csv(OUTPUT_FILES["vendor_disruptions"], index=False)
        logger.info(f"✓ Saved {len(disruptions_df)} disruptions to {OUTPUT_FILES['vendor_disruptions']}")
        
        # Save new vendors discovered
        if self.new_vendors:
            new_vendors_df = pd.DataFrame(self.new_vendors)
            new_vendors_df.to_csv(OUTPUT_FILES["new_vendors_discovered"], index=False)
            logger.info(f"✓ Saved {len(new_vendors_df)} new vendors to {OUTPUT_FILES['new_vendors_discovered']}")
            
            # Update enriched vendor list (CSV vendors + new vendors)
            enriched_df = pd.DataFrame([
                {
                    'Vendor Name': v['name'],
                    'Industry': v.get('industry'),
                    'Source': v.get('source'),
                    'DateAdded': v.get('date_added'),
                }
                for v in (self.vendors + self.new_vendors)
            ])
            
            enriched_df.to_csv(OUTPUT_FILES["enriched_vendors"], index=False)
            logger.info(f"✓ Saved {len(enriched_df)} vendors to {OUTPUT_FILES['enriched_vendors']}")
        else:
            logger.info("ℹ No new vendors discovered")

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Main execution."""
    try:
        # Load vendors
        vendors = load_vendors_from_csv(VENDORS_CSV)
        if not vendors:
            logger.error("❌ No vendors loaded!")
            return 1
        
        # Process news
        processor = VendorNewsProcessor(vendors)
        disruptions, new_vendors = processor.process_all_vendors()
        
        # Save results
        processor.save_results(VENDORS_CSV)
        
        # Summary
        logger.info("\n" + "="*70)
        logger.info("SUMMARY")
        logger.info("="*70)
        logger.info(f"CSV Vendors: {len(vendors)}")
        logger.info(f"New Vendors Discovered: {len(new_vendors)}")
        logger.info(f"Total Vendors: {len(vendors) + len(new_vendors)}")
        logger.info(f"Disruptions Found: {len(disruptions)}")
        
        if new_vendors:
            logger.info("\nNew Vendors:")
            for v in new_vendors[:10]:
                logger.info(f"  - {v['name']} ({v['industry']})")
        
        logger.info("\n✅ Processing complete!")
        
        return 0
    
    except Exception as e:
        logger.error(f"❌ Failed: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    sys.exit(main())
