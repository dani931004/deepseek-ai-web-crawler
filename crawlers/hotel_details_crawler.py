import os
import asyncio
import json
import time
import random
import logging
from typing import List, Dict, Any, Optional, Type
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from crawl4ai.async_configs import BrowserConfig
from bs4 import BeautifulSoup
from config import dari_tour_config, get_browser_config
from models.hotel_details_model import HotelDetails
from utils.data_utils import save_to_json, sanitize_filename
import pandas as pd
import urllib.parse
from .base_crawler import BaseCrawler


class HotelDetailsCrawler(BaseCrawler):
    def __init__(self):
        super().__init__(session_id="hotel_details_crawl_session")
        self.config = dari_tour_config
        self.hotel_details_dir = self.config.HOTEL_DETAILS_DIR
        os.makedirs(self.hotel_details_dir, exist_ok=True)
        self.load_existing_data(self.hotel_details_dir)

    def load_existing_data(self, dirpath: str):
        if os.path.exists(dirpath):
            for filename in os.listdir(dirpath):
                if filename.endswith(".json"):
                    self.seen_items.add(filename.replace(".json", ""))

    async def get_urls_to_crawl(self) -> List[Any]:
        csv_filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', self.config.FILES_DIR, 'complete_offers.csv'))
        if not os.path.exists(csv_filepath):
            print(f"Error: The file '{csv_filepath}' was not found.")
            return []

        offers_df = pd.read_csv(csv_filepath)
        hotels_to_process = []
        for index, row in offers_df.iterrows():
            offer_name = row['name']
            offer_slug = offer_name.lower().replace(' ', '-')
            detailed_offer_path = os.path.join(self.config.DETAILS_DIR, f"{offer_slug}.json")
            
            if os.path.exists(detailed_offer_path):
                with open(detailed_offer_path, 'r', encoding='utf-8') as f:
                    detailed_offer_data = json.load(f)
                
                if 'hotels' in detailed_offer_data:
                    for hotel in detailed_offer_data['hotels']:
                        if 'link' in hotel and hotel['link']:
                            hotel_name = hotel['name']
                            hotel_slug = sanitize_filename(hotel_name.lower().replace(' ', '-'))
                            
                            if hotel_slug not in self.seen_items:
                                hotels_to_process.append({
                                    'hotel_name': hotel_name,
                                    'hotel_link': hotel['link'],
                                    'offer_title': offer_name
                                })
                            else:
                                print(f"Skipping hotel {hotel_name} as its details have already been processed.")

        if not hotels_to_process:
            print("All hotel details have already been processed or no hotel links found.")
            return []
        
        return hotels_to_process

    async def process_item(self, item: Any) -> Optional[Dict[str, Any]]:
        hotel_info = item
        hotel_name = hotel_info['hotel_name']
        hotel_link = hotel_info['hotel_link']
        offer_title = hotel_info['offer_title']
        hotel_slug = sanitize_filename(hotel_name.lower().replace(' ', '-'))
        output_path = os.path.join(self.hotel_details_dir, f"{hotel_slug}.json")

        print(f"Processing hotel: {hotel_name} from offer: {offer_title}")
        print(f"URL: {hotel_link}")

        config = CrawlerRunConfig(
            url=hotel_link,
            cache_mode=self.cache_mode,
        )

        result = await self.crawler.arun(hotel_link, config=config)

        if result.html:
            soup = BeautifulSoup(result.html, 'html.parser')
            
            google_map_link = None
            iframe_element = soup.select_one(CSS_SELECTOR_HOTEL_MAP_IFRAME)
            if iframe_element and 'src' in iframe_element.attrs:
                embed_url = iframe_element['src']
                parsed_url = urllib.parse.urlparse(embed_url)
                query_params = urllib.parse.parse_qs(parsed_url.query)
                
                if 'q' in query_params and query_params['q']:
                    location_query = query_params['q'][0]
                    google_map_link = f"https://www.google.com/maps/search/?api=1&query={urllib.parse.quote_plus(location_query)}"
                else:
                    google_map_link = embed_url
            
            description = None
            description_div = soup.select_one(CSS_SELECTOR_HOTEL_DESCRIPTION_BOX)
            if description_div:
                description = description_div.get_text(strip=True)
            
            hotel_details_data = HotelDetails(
                google_map_link=google_map_link,
                description=description,
                offer_title=offer_title,
                hotel_name=hotel_name,
                hotel_link=hotel_link
            )
            
            return {"data": hotel_details_data.model_dump(), "path": output_path}
        else:
            print(f"No HTML content retrieved for {hotel_link}")
            return None

    def is_duplicate(self, item: Dict[str, Any]) -> bool:
        hotel_slug = sanitize_filename(item['hotel_name'].lower().strip())
        return hotel_slug in self.seen_items

    def is_complete(self, item: Dict[str, Any]) -> bool:
        return all(key in item['data'] for key in ['google_map_link', 'description', 'offer_title', 'hotel_name', 'hotel_link'])


async def crawl_hotel_details():
    crawler = HotelDetailsCrawler()
    await crawler.crawl()
