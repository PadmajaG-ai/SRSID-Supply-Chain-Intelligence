"""
Guardian API News Integration
Fetch real news about vendors and extract disruption events
"""

import requests
import pandas as pd
import logging
import sys
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time

from vendor_synthesis_config import (
    REAL_VENDORS,
    GUARDIAN_API,
    OUTPUT_FILES,
    validate_guardian_api_key,
)

# ============================================================
# LOGGING SETUP
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/guardian_news.log', encoding='utf-8'),
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# GUARDIAN API CLIENT
# ============================================================

class GuardianAPIClient:
    """Fetch news from The Guardian API."""
    
    def __init__(self, api_key: str):
        """Initialize Guardian API client."""
        self.api_key = api_key
        self.base_url = GUARDIAN_API["base_url"]
        self.session = requests.Session()
        self.request_count = 0
        self.rate_limit_delay = 0.5  # 500ms between requests
    
    def search_vendor_news(self, vendor_name: str, days_back: int = 365) -> List[Dict]:
        """
        Search for news about a vendor.
        
        Args:
            vendor_name: Name of vendor to search
            days_back: How far back to search (days)
        
        Returns:
            List of news articles
        """
        try:
            # Build query
            query = f'"{vendor_name}" (supply OR disruption OR shortage OR recall OR strike OR earnings)'
            
            # Calculate date range
            from_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            
            # Build request parameters
            params = {
                'q': query,
                'api-key': self.api_key,
                'format': GUARDIAN_API["search_config"]["format"],
                'show-fields': GUARDIAN_API["search_config"]["show-fields"],
                'page-size': GUARDIAN_API["search_config"]["page-size"],
                'order-by': GUARDIAN_API["search_config"]["order-by"],
                'from-date': from_date,
            }
            
            logger.info(f"Searching for: {vendor_name}")
            
            # Make request
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            # Rate limiting
            self.request_count += 1
            if self.request_count % 5 == 0:
                time.sleep(self.rate_limit_delay)
            
            # Parse response
            data = response.json()
            articles = data.get('response', {}).get('results', [])
            
            logger.info(f"Found {len(articles)} articles about {vendor_name}")
            
            return articles
        
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {vendor_name}: {e}")
            return []
    
    def fetch_all_vendor_news(self) -> Dict[str, List[Dict]]:
        """
        Fetch news for all vendors.
        
        Returns:
            Dictionary mapping vendor name to list of articles
        """
        all_news = {}
        
        for vendor in REAL_VENDORS:
            vendor_name = vendor["name"]
            articles = self.search_vendor_news(vendor_name)
            all_news[vendor_name] = articles
        
        return all_news

# ============================================================
# DISRUPTION EXTRACTION
# ============================================================

class DisruptionExtractor:
    """Extract disruption events from news articles."""
    
    def __init__(self):
        self.disruption_keywords = GUARDIAN_API["disruption_type_mapping"]
    
    def extract_disruptions(self, articles: List[Dict]) -> List[Dict]:
        """
        Extract disruption events from articles.
        
        Args:
            articles: List of news articles
        
        Returns:
            List of disruption events
        """
        disruptions = []
        
        for article in articles:
            # Get article text
            headline = article.get('fields', {}).get('headline', '')
            trail = article.get('fields', {}).get('trailText', '')
            text = f"{headline} {trail}".lower()
            
            # Check for disruption keywords
            disruption_type = self._detect_disruption_type(text)
            
            if disruption_type:
                disruption = {
                    'headline': headline,
                    'disruption_type': disruption_type,
                    'date': article.get('webPublicationDate', '')[:10],
                    'url': article.get('webUrl', ''),
                    'source': 'Guardian',
                    'confidence': 'High' if disruption_type in text else 'Medium',
                }
                disruptions.append(disruption)
        
        return disruptions
    
    def _detect_disruption_type(self, text: str) -> Optional[str]:
        """
        Detect disruption type from text.
        
        Args:
            text: Text to analyze
        
        Returns:
            Disruption type or None
        """
        for disruption_type, keywords in self.disruption_keywords.items():
            for keyword in keywords:
                if keyword.lower() in text:
                    return disruption_type
        
        return None

# ============================================================
# NEWS DATA BUILDER
# ============================================================

class NewsDataBuilder:
    """Combine vendor data with news information."""
    
    def __init__(self):
        self.extractor = DisruptionExtractor()
    
    def build_vendor_news_dataset(self, vendor_df: pd.DataFrame, all_news: Dict) -> pd.DataFrame:
        """
        Combine vendor data with news information.
        
        Args:
            vendor_df: Vendor master data
            all_news: Dictionary of news by vendor
        
        Returns:
            DataFrame with vendor + news data
        """
        records = []
        
        for idx, vendor in vendor_df.iterrows():
            vendor_name = vendor.get("NAME1", "")
            
            # Find matching news for this vendor
            # Check both exact and variant names
            news_articles = []
            
            # Check if master vendor has news
            for real_vendor in REAL_VENDORS:
                if real_vendor["name"].lower() in vendor_name.lower() or \
                   vendor_name.lower() in real_vendor["name"].lower():
                    news_articles.extend(all_news.get(real_vendor["name"], []))
                    break
            
            if news_articles:
                # Extract disruptions
                disruptions = self.extractor.extract_disruptions(news_articles)
                
                # Create record for each disruption
                for disruption in disruptions[:3]:  # Limit to 3 per vendor
                    record = {
                        'LIFNR': vendor.get('LIFNR'),
                        'Vendor_Name': vendor_name,
                        'Country': vendor.get('Land1'),
                        'Disruption_Type': disruption.get('disruption_type'),
                        'News_Headline': disruption.get('headline'),
                        'Event_Date': disruption.get('date'),
                        'News_Source': disruption.get('source'),
                        'Confidence': disruption.get('confidence'),
                        'News_URL': disruption.get('url'),
                    }
                    records.append(record)
                
                # Add clean record if no disruptions found but has news
                if not records:
                    record = {
                        'LIFNR': vendor.get('LIFNR'),
                        'Vendor_Name': vendor_name,
                        'Country': vendor.get('Land1'),
                        'Disruption_Type': 'None',
                        'News_Headline': f"{len(news_articles)} articles found",
                        'Event_Date': None,
                        'News_Source': 'Guardian',
                        'Confidence': 'Low',
                        'News_URL': '',
                    }
                    records.append(record)
        
        return pd.DataFrame(records)
    
    def build_disruption_events(self, vendor_df: pd.DataFrame, all_news: Dict) -> pd.DataFrame:
        """
        Build list of disruption events for supply chain.
        
        Args:
            vendor_df: Vendor master data
            all_news: Dictionary of news by vendor
        
        Returns:
            DataFrame of disruption events
        """
        disruptions = []
        
        for real_vendor in REAL_VENDORS:
            news_articles = all_news.get(real_vendor["name"], [])
            
            extracted = self.extractor.extract_disruptions(news_articles)
            
            for disruption in extracted:
                # Find matching vendor in vendor_df
                matching_vendor = vendor_df[
                    vendor_df["NAME1"].str.contains(
                        real_vendor["name"].split()[0],
                        case=False,
                        na=False
                    )
                ]
                
                if not matching_vendor.empty:
                    record = {
                        'LIFNR': matching_vendor.iloc[0].get('LIFNR'),
                        'Vendor_Name': real_vendor["name"],
                        'Country': real_vendor["country"],
                        'Disruption_Type': disruption.get('disruption_type'),
                        'Event_Date': disruption.get('date'),
                        'Headline': disruption.get('headline'),
                        'URL': disruption.get('url'),
                        'Source': disruption.get('source'),
                        'Severity': self._estimate_severity(disruption.get('disruption_type')),
                    }
                    disruptions.append(record)
        
        return pd.DataFrame(disruptions)
    
    @staticmethod
    def _estimate_severity(disruption_type: str) -> str:
        """Estimate severity of disruption."""
        high_severity = ['bankruptcy', 'lawsuit', 'recall', 'sanction']
        medium_severity = ['strike', 'shortage', 'logistics_delay']
        
        if disruption_type in high_severity:
            return 'High'
        elif disruption_type in medium_severity:
            return 'Medium'
        else:
            return 'Low'

# ============================================================
# MAIN EXECUTION
# ============================================================

def main():
    """Main execution."""
    try:
        logger.info("=" * 70)
        logger.info("GUARDIAN API NEWS INTEGRATION")
        logger.info("=" * 70)
        
        # Validate API key
        logger.info("\n[STEP 1] Validating Guardian API key...")
        if not validate_guardian_api_key():
            logger.error("Guardian API key not configured!")
            logger.info("Get free key from: https://open-platform.theguardian.com/")
            return 1
        
        logger.info("✓ API key configured")
        
        # Load vendor data
        logger.info("\n[STEP 2] Loading vendor data...")
        try:
            vendor_df = pd.read_csv(OUTPUT_FILES["vendor_variants"])
            logger.info(f"Loaded {len(vendor_df)} vendors")
        except FileNotFoundError:
            logger.error(f"Vendor file not found: {OUTPUT_FILES['vendor_variants']}")
            logger.info("Run synthesize_vendors.py first")
            return 1
        
        # Fetch news
        logger.info("\n[STEP 3] Fetching vendor news from Guardian API...")
        client = GuardianAPIClient(GUARDIAN_API["api_key"])
        all_news = client.fetch_all_vendor_news()
        
        total_articles = sum(len(articles) for articles in all_news.values())
        logger.info(f"Fetched {total_articles} articles total")
        
        # Build news dataset
        logger.info("\n[STEP 4] Building vendor + news dataset...")
        builder = NewsDataBuilder()
        vendor_news_df = builder.build_vendor_news_dataset(vendor_df, all_news)
        
        # Build disruption events
        logger.info("\n[STEP 5] Extracting disruption events...")
        disruptions_df = builder.build_disruption_events(vendor_df, all_news)
        
        # Save to CSV
        logger.info("\n[STEP 6] Saving to CSV files...")
        vendor_news_df.to_csv(OUTPUT_FILES["vendor_with_news"], index=False)
        logger.info(f"Saved vendor news to {OUTPUT_FILES['vendor_with_news']}")
        
        disruptions_df.to_csv(OUTPUT_FILES["disruption_events"], index=False)
        logger.info(f"Saved disruption events to {OUTPUT_FILES['disruption_events']}")
        
        # Summary
        logger.info("\n" + "=" * 70)
        logger.info("GUARDIAN API INTEGRATION COMPLETE")
        logger.info("=" * 70)
        logger.info(f"Vendors with news: {len(vendor_news_df)}")
        logger.info(f"Disruption events found: {len(disruptions_df)}")
        
        if len(disruptions_df) > 0:
            logger.info("\nSample disruptions:")
            logger.info(disruptions_df[['Vendor_Name', 'Disruption_Type', 'Event_Date']].head(10).to_string())
        
        return 0
    
    except Exception as e:
        logger.error(f"News integration failed: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    import os
    os.makedirs("logs", exist_ok=True)
    sys.exit(main())
