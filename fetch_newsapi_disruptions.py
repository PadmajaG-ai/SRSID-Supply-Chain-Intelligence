"""
Fetch NewsAPI Disruptions
Real supply chain disruption news articles from NewsAPI
"""

import requests
import pandas as pd
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import List, Dict

from hybrid_phase1_config import (
    NEWSAPI_CONFIG,
    DISRUPTION_TYPES,
    OUTPUT_FILES,
    PATHS,
    LOGGING,
)

# ============================================================
# LOGGING SETUP
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format=LOGGING["format"],
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOGGING["log_file"], encoding="utf-8"),
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# NEWSAPI CLIENT
# ============================================================

class NewsAPIClient:
    """Fetch disruption news articles from NewsAPI."""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or NEWSAPI_CONFIG["api_key"]
        self.base_url = NEWSAPI_CONFIG["base_url"]
        self.endpoint = NEWSAPI_CONFIG["endpoint"]
        self.session = requests.Session()
    
    def search_articles(self, keyword: str, days_back: int = None) -> List[Dict]:
        """
        Search NewsAPI for articles.
        
        Args:
            keyword: Search keyword
            days_back: How far back to search
        
        Returns:
            List of articles
        """
        if days_back is None:
            days_back = NEWSAPI_CONFIG["lookback_days"]
        
        try:
            from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
            
            url = f"{self.base_url}{self.endpoint}"
            params = {
                "q": keyword,
                "from": from_date,
                "sortBy": "publishedAt",
                "apiKey": self.api_key,
                "pageSize": 100,
                "language": "en",
            }
            
            logger.info(f"Searching NewsAPI for: {keyword}")
            
            response = self.session.get(url, params=params, timeout=NEWSAPI_CONFIG["timeout"])
            response.raise_for_status()
            
            data = response.json()
            articles = data.get("articles", [])
            
            logger.info(f"Found {len(articles)} articles for: {keyword}")
            
            return articles
        
        except requests.exceptions.RequestException as e:
            logger.error(f"NewsAPI request failed for '{keyword}': {e}")
            return []
    
    def fetch_all_disruptions(self) -> Dict[str, List[Dict]]:
        """
        Fetch all disruption types from NewsAPI.
        
        Returns:
            Dictionary mapping disruption type to articles
        """
        all_articles = {}
        
        for disruption_type, keywords in DISRUPTION_TYPES.items():
            articles = []
            for keyword in keywords:
                found_articles = self.search_articles(keyword)
                articles.extend(found_articles)
            
            all_articles[disruption_type] = articles
        
        return all_articles

# ============================================================
# ARTICLE PARSER
# ============================================================

class ArticleParser:
    """Parse and format NewsAPI articles."""
    
    @staticmethod
    def extract_companies(text: str) -> List[str]:
        """
        Extract company names from text.
        Simple heuristic-based approach.
        """
        companies = []
        
        # List of vendors to match
        known_companies = [
            "Apple", "Microsoft", "Google", "Amazon", "Tesla",
            "Intel", "NVIDIA", "AMD", "IBM", "Samsung",
            "TSMC", "Qualcomm", "Broadcom", "Cisco", "Dell",
            "HP", "Lenovo", "ASUS", "Sony", "Nintendo",
            "Toyota", "Volkswagen", "BMW", "Mercedes", "Ford",
            "General Motors", "Tesla", "Hyundai", "Kia",
            "BASF", "Dow", "DuPont", "Eastman", "Lyondell",
            "Unilever", "Procter & Gamble", "Nestlé", "Coca-Cola", "Pepsi",
            "Shell", "ExxonMobil", "Chevron", "Saudi Aramco", "Gazprom",
            "Siemens", "ABB", "Schneider", "Eaton", "Rockwell",
            "Johnson & Johnson", "Merck", "Pfizer", "AstraZeneca", "Novartis",
            "Roche", "Eli Lilly", "Bristol Myers", "Amgen",
            "Walmart", "Costco", "Target", "Best Buy", "Home Depot",
            "DHL", "FedEx", "UPS", "Maersk", "Hapag-Lloyd",
            "Sanofi", "GlaxoSmithKline", "Teva",
        ]
        
        for company in known_companies:
            if company.lower() in text.lower():
                if company not in companies:
                    companies.append(company)
        
        return companies
    
    @staticmethod
    def create_disruption_record(
        article: Dict,
        disruption_type: str,
    ) -> Dict:
        """Create a standardized disruption record from article."""
        
        companies = ArticleParser.extract_companies(
            f"{article.get('title', '')} {article.get('description', '')}"
        )
        
        return {
            "event_id": f"newsapi_{article.get('publishedAt', '').replace('-', '').replace('T', '').replace(':', '')}",
            "event_date": article.get("publishedAt", "")[:10],  # YYYY-MM-DD
            "disruption_type": disruption_type,
            "title": article.get("title", ""),
            "description": article.get("description", ""),
            "actors": "; ".join(companies) if companies else "",
            "location": "",  # NewsAPI doesn't provide location
            "source": article.get("source", {}).get("name", "NewsAPI"),
            "url": article.get("url", ""),
            "source_file": "newsapi",
            "fetch_timestamp": datetime.now().isoformat(),
        }

# ============================================================
# ALTERNATIVE: USE SAMPLE DATA
# ============================================================

class NewsAPISampleData:
    """
    Generate sample NewsAPI disruption data for demonstration.
    """
    
    @staticmethod
    def generate_samples() -> List[Dict]:
        """Generate realistic sample disruption articles."""
        
        samples = [
            {
                "event_id": "newsapi_20250920_001",
                "event_date": "2025-09-20",
                "disruption_type": "supply_chain",
                "title": "Global Supply Chain Faces New Disruptions",
                "description": "Intel and TSMC warn of continued supply chain challenges",
                "actors": "Intel; TSMC; Samsung",
                "location": "Asia-Pacific",
                "source": "Reuters",
                "url": "https://reuters.com/...",
                "source_file": "newsapi",
                "fetch_timestamp": datetime.now().isoformat(),
            },
            {
                "event_id": "newsapi_20250915_002",
                "event_date": "2025-09-15",
                "disruption_type": "recall",
                "title": "Major Pharmaceutical Recall Announced",
                "description": "Johnson & Johnson issues recall for contaminated batch",
                "actors": "Johnson & Johnson",
                "location": "North America",
                "source": "Bloomberg",
                "url": "https://bloomberg.com/...",
                "source_file": "newsapi",
                "fetch_timestamp": datetime.now().isoformat(),
            },
            {
                "event_id": "newsapi_20250910_003",
                "event_date": "2025-09-10",
                "disruption_type": "labor_strike",
                "title": "Workers Strike at Major Manufacturer",
                "description": "Labor dispute halts production at Siemens facility",
                "actors": "Siemens",
                "location": "Germany",
                "source": "Financial Times",
                "url": "https://ft.com/...",
                "source_file": "newsapi",
                "fetch_timestamp": datetime.now().isoformat(),
            },
            {
                "event_id": "newsapi_20250905_004",
                "event_date": "2025-09-05",
                "disruption_type": "shortage",
                "title": "Semiconductor Shortage Impacts Automotive",
                "description": "Chip shortage continues to affect car manufacturers globally",
                "actors": "Intel; TSMC; Toyota; Volkswagen; BMW",
                "location": "Global",
                "source": "MarketWatch",
                "url": "https://marketwatch.com/...",
                "source_file": "newsapi",
                "fetch_timestamp": datetime.now().isoformat(),
            },
            {
                "event_id": "newsapi_20250901_005",
                "event_date": "2025-09-01",
                "disruption_type": "geopolitical",
                "title": "Trade Tensions Rise, Supply Chain at Risk",
                "description": "New tariffs threaten global supply chains",
                "actors": "Multiple Industries",
                "location": "Global",
                "source": "CNBC",
                "url": "https://cnbc.com/...",
                "source_file": "newsapi",
                "fetch_timestamp": datetime.now().isoformat(),
            },
            {
                "event_id": "newsapi_20250825_006",
                "event_date": "2025-08-25",
                "disruption_type": "bankruptcy",
                "title": "Retail Chain Files for Bankruptcy",
                "description": "Economic downturn forces major retailer to restructure",
                "actors": "Walmart; Costco",
                "location": "United States",
                "source": "Wall Street Journal",
                "url": "https://wsj.com/...",
                "source_file": "newsapi",
                "fetch_timestamp": datetime.now().isoformat(),
            },
            {
                "event_id": "newsapi_20250820_007",
                "event_date": "2025-08-20",
                "disruption_type": "logistics_delay",
                "title": "Port Congestion Causes Shipping Delays",
                "description": "FedEx and DHL report significant logistics delays",
                "actors": "FedEx; DHL; Maersk",
                "location": "Europe",
                "source": "Shipping Times",
                "url": "https://shippingtimes.com/...",
                "source_file": "newsapi",
                "fetch_timestamp": datetime.now().isoformat(),
            },
        ]
        
        return samples

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Main execution."""
    try:
        logger.info("=" * 70)
        logger.info("NEWSAPI DISRUPTIONS FETCHER")
        logger.info("=" * 70)
        
        # Step 1: Check API key
        logger.info("\n[STEP 1] Validating NewsAPI key...")
        if NEWSAPI_CONFIG["api_key"] == "YOUR_NEWSAPI_KEY_HERE":
            logger.warning("NewsAPI key not configured. Using sample data.")
            logger.info("Get free key from: https://newsapi.org/")
            use_sample = True
        else:
            logger.info("✓ API key configured")
            use_sample = False
        
        # Step 2: Fetch articles
        logger.info("\n[STEP 2] Fetching disruption articles...")
        
        if use_sample:
            logger.info("Using sample data (for demonstration)")
            articles_list = NewsAPISampleData.generate_samples()
        else:
            logger.info("Querying NewsAPI...")
            client = NewsAPIClient()
            all_articles = client.fetch_all_disruptions()
            
            # Convert to records
            articles_list = []
            for disruption_type, articles in all_articles.items():
                for article in articles:
                    record = ArticleParser.create_disruption_record(article, disruption_type)
                    articles_list.append(record)
        
        logger.info(f"Total articles: {len(articles_list)}")
        
        # Step 3: Convert to DataFrame
        logger.info("\n[STEP 3] Converting to DataFrame...")
        df = pd.DataFrame(articles_list)
        
        # Step 4: Save to CSV
        logger.info("\n[STEP 4] Saving to CSV...")
        os.makedirs(os.path.dirname(OUTPUT_FILES["newsapi_disruptions"]), exist_ok=True)
        df.to_csv(OUTPUT_FILES["newsapi_disruptions"], index=False)
        logger.info(f"Saved to {OUTPUT_FILES['newsapi_disruptions']}")
        
        # Summary
        logger.info("\n" + "=" * 70)
        logger.info("NEWSAPI DISRUPTIONS FETCH COMPLETE")
        logger.info("=" * 70)
        logger.info(f"Total disruptions: {len(df)}")
        logger.info(f"\nDisruption types found:")
        for dtype, count in df["disruption_type"].value_counts().items():
            logger.info(f"  {dtype}: {count}")
        
        return 0
    
    except Exception as e:
        logger.error(f"Failed to fetch NewsAPI disruptions: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
