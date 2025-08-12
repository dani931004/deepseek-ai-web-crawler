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
from config import dari_tour_config, get_browser_config, CSS_SELECTOR_HOTEL_MAP_IFRAME, CSS_SELECTOR_HOTEL_DESCRIPTION_BOX
from models.hotel_details_model import HotelDetails
from utils.data_utils import save_to_json, slugify
import pandas as pd
import urllib.parse
from .base_crawler import BaseCrawler


class HotelDetailsCrawler(BaseCrawler):
    """
    A crawler for extracting detailed hotel information from individual hotel pages.
    Inherits from BaseCrawler to leverage common crawling functionalities.
    """
    def __init__(self, session_id: str, config: Type, model_class: Type):
        """
        Initializes the HotelDetailsCrawler with a session ID and sets up
        output directories and loads existing data.
        """
        super().__init__(
            session_id=session_id,
            config=config,
            model_class=model_class,
            output_file_type='json',
            key_fields=['hotel_name'] # Using 'hotel_name' as key field for duplicate checking
        )

    def load_existing_data(self, dirpath: str):
        """
        Loads existing hotel details data from the specified directory to avoid re-processing.

        Args:
            dirpath (str): The path to the directory containing existing hotel details JSON files.
        """
        if os.path.exists(dirpath):
            for filename in os.listdir(dirpath):
                if filename.endswith(".json"):
                    # Add the sanitized filename (without extension) to seen_items to mark it as processed.
                    self.seen_items.add(filename.replace(".json", ""))

    async def get_urls_to_crawl(self, max_items: Optional[int] = None) -> List[Any]:
        """
        Determines the list of hotel URLs to crawl by reading the complete offers CSV
        and checking against already processed hotel details.

        Returns:
            List[Any]: A list of dictionaries, each containing 'hotel_name', 'hotel_link',
                       and 'offer_title' for hotels that need to be crawled.
        """
        # Construct the absolute path to the complete offers CSV file.
        csv_filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', self.config.FILES_DIR, 'complete_offers.csv'))
        if not os.path.exists(csv_filepath):
            print(f"Error: The file '{csv_filepath}' was not found.")
            return []

        offers_df = pd.read_csv(csv_filepath)
        hotels_to_process = []
        for index, row in offers_df.iterrows():
            offer_name = row['name']
            # Create a slug from the offer name for file naming consistency.
            offer_slug = offer_name.lower().replace(' ', '-')
            detailed_offer_path = os.path.join(self.config.DETAILS_DIR, f"{offer_slug}.json")
            
            if os.path.exists(detailed_offer_path):
                with open(detailed_offer_path, 'r', encoding='utf-8') as f:
                    detailed_offer_data = json.load(f)
                
                # Check if the detailed offer data contains hotel information.
                if 'hotels' in detailed_offer_data:
                    for hotel in detailed_offer_data['hotels']:
                        # Ensure the hotel entry has a valid link.
                        if 'link' in hotel and hotel['link']:
                            hotel_name = hotel['name']
                            # Sanitize the hotel name to create a valid filename slug.
                            hotel_slug = slugify(hotel_name.lower().replace(' ', '-'))
                            
                            # Only add to the processing list if the hotel details haven't been seen before.
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
        
        if max_items:
            return hotels_to_process[:max_items]
        return hotels_to_process

    async def process_item(self, item: Any, seen_items: set) -> Optional[Dict[str, Any]]:
        """
        Processes a single hotel item by crawling its link and extracting relevant details.

        Args:
            item (Any): A dictionary containing 'hotel_name', 'hotel_link', and 'offer_title'.

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing the extracted hotel details and
                                      the output path, or None if processing fails.
        """
        hotel_info = item
        hotel_name = hotel_info['hotel_name']
        hotel_link = hotel_info['hotel_link']
        offer_title = hotel_info['offer_title']
        # Generate a sanitized slug for the hotel name to use as a filename.
        hotel_slug = slugify(hotel_name.lower().replace(' ', '-'))
        output_path = os.path.join(self.hotel_details_dir, f"{hotel_slug}.json")

        print(f"Processing hotel: {hotel_name} from offer: {offer_title}")
        print(f"URL: {hotel_link}")

        config = CrawlerRunConfig(
            url=hotel_link,
            cache_mode=self.cache_mode,
        )

        # Execute the crawl for the hotel link.
        result = await self._run_crawler_with_retries(hotel_link, config=config, description="fetching hotel details")

        if result.html:
            soup = BeautifulSoup(result.html, 'html.parser')
            
            google_map_link = None
            # Find the iframe element containing the Google Maps embed URL.
            iframe_element = soup.select_one(CSS_SELECTOR_HOTEL_MAP_IFRAME)
            if iframe_element and 'src' in iframe_element.attrs:
                embed_url = iframe_element['src']
                parsed_url = urllib.parse.urlparse(embed_url)
                query_params = urllib.parse.parse_qs(parsed_url.query)
                
                # Extract the 'q' parameter from the embed URL for the location query.
                if 'q' in query_params and query_params['q']:
                    location_query = query_params['q'][0]
                    # Construct a Google Maps search URL.
                    google_map_link = f"https://www.google.com/maps/search/?api=1&query={urllib.parse.quote_plus(location_query)}"
                else:
                    # If 'q' parameter is not found, use the embed URL directly.
                    google_map_link = embed_url
            
            description = None
            # Find the div containing the hotel description.
            description_div = soup.select_one(CSS_SELECTOR_HOTEL_DESCRIPTION_BOX)
            if description_div:
                description = description_div.get_text(strip=True)
            
            # Create a HotelDetails object with the extracted information.
            hotel_details_data = HotelDetails(
                google_map_link=google_map_link,
                description=description,
                offer_title=offer_title,
                hotel_name=hotel_name,
                hotel_link=hotel_link
            )
            
            # Return the model dump and the intended output path.
            return {"data": hotel_details_data.model_dump(), "path": output_path}
        else:
            print(f"No HTML content retrieved for {hotel_link}")
            return None

    def is_duplicate(self, item: Dict[str, Any]) -> bool:
        """
        Checks if a hotel item has already been processed based on its sanitized name.

        Args:
            item (Dict[str, Any]): The hotel item dictionary.

        Returns:
            bool: True if the hotel is a duplicate (already processed), False otherwise.
        """
        hotel_slug = slugify(item['hotel_name'].lower().strip())
        return hotel_slug in self.seen_items

    def is_complete(self, item: Dict[str, Any]) -> bool:
        """
        Checks if the extracted hotel details are complete (i.e., all required fields are present).

        Args:
            item (Dict[str, Any]): The dictionary containing the extracted hotel details.

        Returns:
            bool: True if all required fields are present, False otherwise.
        """
        return all(key in item['data'] for key in ['google_map_link', 'description', 'offer_title', 'hotel_name', 'hotel_link'])

    def save_data(self):
        """
        Saves the collected hotel details data to JSON files.
        """
        for item in self.all_items:
            self._save_data_json(item["data"], item["path"])


async def crawl_hotel_details():
    """
    Asynchronously initiates the hotel details crawling process.
    """
    crawler = HotelDetailsCrawler()
    await crawler.crawl()