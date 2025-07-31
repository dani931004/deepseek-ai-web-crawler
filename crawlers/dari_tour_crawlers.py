import os
import asyncio
import json
import time
import random
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Type
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig
from config import dari_tour_config, get_browser_config

from bs4 import BeautifulSoup
from config import CSS_SELECTOR_DARI_TOUR_DETAIL_OFFER_NAME, CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_ELEMENTS, CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_NAME, CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_PRICE, CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_COUNTRY, CSS_SELECTOR_DARI_TOUR_DETAIL_PROGRAM, CSS_SELECTOR_DARI_TOUR_DETAIL_INCLUDED_SERVICES, CSS_SELECTOR_DARI_TOUR_DETAIL_EXCLUDED_SERVICES, CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_ITEM_LINK, CSS_SELECTOR_OFFER_ITEM_TITLE
from utils.data_utils import (
    save_offers_to_csv,
)
from utils.scraper_utils import (
    fetch_and_process_page,
    get_llm_strategy,
    process_page_content,
)
import urllib.parse
import re
from models.dari_tour_models import DariTourOffer
from models.dari_tour_detailed_models import OfferDetails, Hotel
from utils.data_utils import save_to_json
import pandas as pd
from .base_crawler import BaseCrawler


class DariTourCrawler(BaseCrawler):
    def __init__(self):
        super().__init__(
            session_id="offer_crawl_session",
            required_keys=dari_tour_config.required_keys,
            key_fields=['name', 'link']
        )
        self.config = dari_tour_config
        self.filepath = os.path.join(
            self.config.FILES_DIR,
            "complete_offers.csv",
        )
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        self._load_existing_data_csv(self.filepath, self.key_fields)

    async def get_urls_to_crawl(self) -> List[Any]:
        url = f"{self.config.base_url}?page=1"
        print(f"Loading page 1...")
        
        config = CrawlerRunConfig(
            cache_mode=self.cache_mode,
            session_id=f"{self.session_id}_page1",
            extraction_strategy=None,
            scan_full_page=False,
            wait_for_images=False,
            remove_overlay_elements=True,
            verbose=True,
            page_timeout=120000,
            delay_before_return_html=2.0,
            wait_until="domcontentloaded",
            wait_for=self.config.css_selector,
            only_text=False,
            remove_forms=True,
            prettiify=True,
            ignore_body_visibility=True,
            js_only=False,
            magic=True,
            screenshot=False,
            pdf=False
        )
        
        result = await self.crawler.arun(url, config=config)
        
        if not result or not result.html:
            print(f"Failed to load page: {url}")
            return []
            
        soup = BeautifulSoup(result.html, 'html.parser')
        offer_elements = soup.select(self.config.css_selector)
        
        if not offer_elements:
            print(f"No offer items found on {url}")
            return []
            
        print(f"Found {len(offer_elements)} offer items to process...")
        return offer_elements

    async def process_item(self, item: Any) -> Optional[Dict[str, Any]]:
        offer_element = item
        actual_url = None
        offer_name = ""
        if offer_element.name == 'a' and 'href' in offer_element.attrs:
            href = offer_element['href']
            if href.startswith('http'):
                actual_url = href
            else:
                actual_url = urllib.parse.urljoin(self.config.base_url, href)
            actual_url = actual_url.split('?')[0].split('#')[0]
            
            name_el = offer_element.select_one(CSS_SELECTOR_OFFER_ITEM_TITLE)
            if name_el:
                offer_name = name_el.get_text(strip=True)

        normalized_offer_name = offer_name.lower().strip()
        normalized_actual_url = actual_url.lower().strip() if actual_url else ""
        if (normalized_offer_name, normalized_actual_url) in self.seen_items:
            print(f"Skipping already processed offer: {offer_name} ({actual_url})")
            return None

        try:
            import tempfile
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as f:
                f.write(f'''
                <!DOCTYPE html>
                <html>
                <head>
                    <meta charset="UTF-8">
                    <title>Offer</title>
                    <base href="https://dari-tour.com/">
                </head>
                <body>
                    {str(offer_element)}
                    <!-- Actual URL: {actual_url} -->
                </body>
                </html>
                ''')
                temp_file_path = f.name
            
            try:
                offer_config = CrawlerRunConfig(
                    cache_mode=self.cache_mode,
                    session_id=f"{self.session_id}_offer",
                    extraction_strategy=self.llm_strategy,
                    scan_full_page=False,
                    wait_for_images=False,
                    remove_overlay_elements=True,
                    verbose=False,
                    page_timeout=120000,
                    delay_before_return_html=2.0,
                    only_text=False,
                    remove_forms=True,
                    prettiify=True,
                    ignore_body_visibility=True,
                    js_only=True,
                    magic=False
                )
                
                file_url = f"file://{temp_file_path}"
                offer_result = await self.crawler.arun(
                    file_url,
                    config=offer_config
                )
            
                if offer_result and offer_result.extracted_content:
                    extracted_content = self._parse_extracted_content(offer_result.extracted_content)
                    
                    if isinstance(extracted_content, list):
                        for offer in extracted_content:
                            if not self.is_duplicate(offer) and self.is_complete(offer):
                                offer['link'] = actual_url
                                self.all_items.append(offer)
                                self.seen_items.add(tuple(offer.get(k, '').lower().strip() for k in self.key_fields))
                                print(f"Successfully extracted and added new offer: {offer['name']}")
                            else:
                                print(f"Skipping duplicate or incomplete offer: {offer.get('name', 'N/A')}")
                    elif isinstance(extracted_content, dict):
                        if not self.is_duplicate(extracted_content) and self.is_complete(extracted_content):
                            extracted_content['link'] = actual_url
                            self.all_items.append(extracted_content)
                            self.seen_items.add(tuple(extracted_content.get(k, '').lower().strip() for k in self.key_fields))
                            print(f"Successfully extracted and added new offer: {extracted_content['name']}")
                        else:
                            print(f"Skipping duplicate or incomplete offer: {extracted_content.get('name', 'N/A')}")

            finally:
                if os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)

        except Exception as e:
            print(f"Error processing offer: {str(e)}")

        return None

    def _parse_extracted_content(self, content: Any) -> Any:
        if isinstance(content, str) and (content.startswith('[') or content.startswith('{')):
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                pass
        return content

    def save_data(self):
        self._save_data_csv(self.filepath, DariTourOffer)


class DariTourDetailedCrawler(BaseCrawler):
    def __init__(self):
        super().__init__(
            session_id="dari_tour_detailed_offer_crawl_session",
            key_fields=['offer_name'] # Using 'offer_name' as key field for duplicate checking
        )
        self.config = dari_tour_config
        self.output_dir = self.config.DETAILS_DIR
        os.makedirs(self.output_dir, exist_ok=True)
        self._load_existing_data_json(self.output_dir)

    async def get_urls_to_crawl(self) -> List[Any]:
        csv_filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', self.config.FILES_DIR, 'complete_offers.csv'))
        if not os.path.exists(csv_filepath):
            print(f"Error: The file '{csv_filepath}' was not found.")
            return []

        offers_df = pd.read_csv(csv_filepath)
        offers_to_process = []
        for index, row in offers_df.iterrows():
            offer_name = row['name']
            offer_slug = offer_name.lower().replace(' ', '-')
            if offer_slug not in self.seen_items:
                offers_to_process.append(row)
            else:
                print(f"Skipping {offer_name} as it has already been processed.")
        
        if not offers_to_process:
            print("All detailed offers have already been processed.")
            return []

        return offers_to_process

    async def process_item(self, item: Any) -> Optional[Dict[str, Any]]:
        offer_url = item['link']
        offer_name = item['name']
        offer_slug = offer_name.lower().replace(' ', '-')
        output_path = os.path.join(self.output_dir, f"{offer_slug}.json")

        print(f"Processing offer: {offer_name}")
        print(f"URL: {offer_url}")

        config = CrawlerRunConfig(
            url=offer_url,
            cache_mode=self.cache_mode,
        )

        result = await self.crawler.arun(offer_url, config=config)

        if result.html:
            detailed_offer_data = await self._parse_detailed_offer(result.html)
            if detailed_offer_data and self.is_complete(detailed_offer_data):
                return {"data": detailed_offer_data.model_dump(), "path": output_path}
            else:
                print(f"No detailed data extracted or incomplete for {offer_url}")
        else:
            print(f"No HTML content retrieved for {offer_url}")
        
        return None

    async def _parse_detailed_offer(self, html_content: str) -> Optional[OfferDetails]:
        soup = BeautifulSoup(html_content, 'html.parser')

        offer_name_element = soup.select_one(CSS_SELECTOR_DARI_TOUR_DETAIL_OFFER_NAME)
        offer_name = offer_name_element.get_text(strip=True) if offer_name_element else ""

        hotels_data = []
        hotel_elements = soup.select(CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_ELEMENTS)
        for hotel_el in hotel_elements:
            name_el = hotel_el.select_one(CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_NAME)
            price_el = hotel_el.select_one(CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_PRICE)
            country_el = hotel_el.select_one(CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_COUNTRY)
            link_el = hotel_el.select_one(CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_ITEM_LINK)

            hotel_name = name_el.get_text(strip=True) if name_el else ""
            hotel_price = price_el.get_text(strip=True) if price_el else ""
            hotel_country = country_el.get_text(strip=True) if country_el else ""
            hotel_link = None
            if link_el and 'href' in link_el.attrs:
                relative_url = link_el['href']
                hotel_link = urllib.parse.urljoin("https://dari-tour.com/", relative_url)
            
            if hotel_name and hotel_price and hotel_country:
                hotels_data.append(Hotel(name=hotel_name, price=hotel_price, country=hotel_country, link=hotel_link))

        program_element = soup.select_one(CSS_SELECTOR_DARI_TOUR_DETAIL_PROGRAM)
        program = program_element.get_text(strip=True) if program_element else ""

        included_services = []
        included_elements = soup.select(CSS_SELECTOR_DARI_TOUR_DETAIL_INCLUDED_SERVICES)
        for li in included_elements:
            service = li.get_text(strip=True)
            if service:
                included_services.append(service)

        excluded_services = []
        excluded_elements = soup.select(CSS_SELECTOR_DARI_TOUR_DETAIL_EXCLUDED_SERVICES)
        for li in excluded_elements:
            service = li.get_text(strip=True)
            if service:
                excluded_services.append(service)

        if offer_name:
            return OfferDetails(
                offer_name=offer_name,
                hotels=hotels_data,
                program=program,
                included_services=included_services,
                excluded_services=excluded_services
            )
        return None

    def save_data(self):
        for item in self.all_items:
            self._save_data_json(item["data"], item["path"])




    async def get_urls_to_crawl(self) -> List[Any]:
        csv_filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', self.config.FILES_DIR, 'complete_offers.csv'))
        if not os.path.exists(csv_filepath):
            print(f"Error: The file '{csv_filepath}' was not found.")
            return []

        offers_df = pd.read_csv(csv_filepath)
        offers_to_process = []
        for index, row in offers_df.iterrows():
            offer_name = row['name']
            offer_slug = offer_name.lower().replace(' ', '-')
            if offer_slug not in self.seen_items:
                offers_to_process.append(row)
            else:
                print(f"Skipping {offer_name} as it has already been processed.")
        
        if not offers_to_process:
            print("All detailed offers have already been processed.")
            return []

        return offers_to_process

    async def process_item(self, item: Any) -> Optional[Dict[str, Any]]:
        offer_url = item['link']
        offer_name = item['name']
        offer_slug = offer_name.lower().replace(' ', '-')
        output_path = os.path.join(self.output_dir, f"{offer_slug}.json")

        print(f"Processing offer: {offer_name}")
        print(f"URL: {offer_url}")

        config = CrawlerRunConfig(
            url=offer_url,
            cache_mode=self.cache_mode,
        )

        result = await self.crawler.arun(offer_url, config=config)

        if result.html:
            detailed_offer_data = await self._parse_detailed_offer(result.html)
            if detailed_offer_data and self.is_complete(detailed_offer_data):
                return {"data": detailed_offer_data.model_dump(), "path": output_path}
            else:
                print(f"No detailed data extracted or incomplete for {offer_url}")
        else:
            print(f"No HTML content retrieved for {offer_url}")
        
        return None

    async def _parse_detailed_offer(self, html_content: str) -> Optional[OfferDetails]:
        soup = BeautifulSoup(html_content, 'html.parser')

        offer_name_element = soup.select_one(CSS_SELECTOR_DARI_TOUR_DETAIL_OFFER_NAME)
        offer_name = offer_name_element.get_text(strip=True) if offer_name_element else ""

        hotels_data = []
        hotel_elements = soup.select(CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_ELEMENTS)
        for hotel_el in hotel_elements:
            name_el = hotel_el.select_one(CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_NAME)
            price_el = hotel_el.select_one(CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_PRICE)
            country_el = hotel_el.select_one(CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_COUNTRY)
            link_el = hotel_el.select_one(CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_ITEM_LINK)

            hotel_name = name_el.get_text(strip=True) if name_el else ""
            hotel_price = price_el.get_text(strip=True) if price_el else ""
            hotel_country = country_el.get_text(strip=True) if country_el else ""
            hotel_link = None
            if link_el and 'href' in link_el.attrs:
                relative_url = link_el['href']
                hotel_link = urllib.parse.urljoin("https://dari-tour.com/", relative_url)
            
            if hotel_name and hotel_price and hotel_country:
                hotels_data.append(Hotel(name=hotel_name, price=hotel_price, country=hotel_country, link=hotel_link))

        program_element = soup.select_one(CSS_SELECTOR_DARI_TOUR_DETAIL_PROGRAM)
        program = program_element.get_text(strip=True) if program_element else ""

        included_services = []
        included_elements = soup.select(CSS_SELECTOR_DARI_TOUR_DETAIL_INCLUDED_SERVICES)
        for li in included_elements:
            service = li.get_text(strip=True)
            if service:
                included_services.append(service)

        excluded_services = []
        excluded_elements = soup.select(CSS_SELECTOR_DARI_TOUR_DETAIL_EXCLUDED_SERVICES)
        for li in excluded_elements:
            service = li.get_text(strip=True)
            if service:
                excluded_services.append(service)

        if offer_name:
            return OfferDetails(
                offer_name=offer_name,
                hotels=hotels_data,
                program=program,
                included_services=included_services,
                excluded_services=excluded_services
            )
        return None

    def save_data(self):
        for item in self.all_items:
            self._save_data_json(item["data"], item["path"])



