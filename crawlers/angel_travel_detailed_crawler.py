import os
import asyncio
import json
import time
import random
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Type
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig

from bs4 import BeautifulSoup
from config import angel_travel_config, CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_PROGRAM, CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_INCLUDED_SERVICES, CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_EXCLUDED_SERVICES, CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_ELEMENTS, CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_NAME, CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_PRICE, CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_COUNTRY, CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_ITEM_LINK
from utils.data_utils import save_to_json
import urllib.parse
import re
from models.angel_travel_models import AngelTravelOffer
from models.angel_travel_detailed_models import AngelTravelDetailedOffer # Assuming a new detailed model
import pandas as pd
from .base_crawler import BaseCrawler


class AngelTravelDetailedCrawler(BaseCrawler):
    def __init__(self):
        super().__init__(
            session_id="angel_travel_detailed_offer_crawl_session",
            key_fields=['offer_name'] # Using 'offer_name' as key field for duplicate checking
        )
        self.config = angel_travel_config
        self.output_dir = os.path.join(self.config.FILES_DIR, "detailed_offers")
        os.makedirs(self.output_dir, exist_ok=True)
        self.seen_items = set() # Clear seen_items before loading existing data
        self._load_existing_data_json(self.output_dir)

    async def get_urls_to_crawl(self) -> List[Any]:
        csv_filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', self.config.FILES_DIR, 'complete_offers.csv'))
        if not os.path.exists(csv_filepath):
            print(f"Error: The file '{csv_filepath}' was not found.")
            return []

        offers_df = pd.read_csv(csv_filepath)
        offers_to_process = []
        for index, row in offers_df.iterrows():
            offer_name = row['title'] # Assuming 'title' is the name in complete_offers.csv
            offer_link = row['link']
            full_offer_url = urllib.parse.urljoin(self.config.base_url, offer_link)
            offer_slug = self._slugify(offer_name)
            # Only process links that contain 'exotic-destinations' and do not contain 'programa.php'
            if "exotic-destinations" in full_offer_url and "programa.php" not in full_offer_url and offer_slug not in self.seen_items:
                offers_to_process.append(row)
            else:
                print(f"Skipping {offer_name} (link: {offer_link}) as it has already been processed or is not an exotic-destinations link.")
        
        if not offers_to_process:
            print("All detailed offers have already been processed.")
            return []

        return offers_to_process

    async def process_item(self, item: Any) -> Optional[Dict[str, Any]]:
        offer_url = urllib.parse.urljoin(self.config.base_url, item['link'])
        offer_name = item['title']
        offer_slug = self._slugify(offer_name)
        output_path = os.path.join(self.output_dir, f"{offer_slug}.json")

        print(f"Processing offer: {offer_name}")
        print(f"URL: {offer_url}")

        config = CrawlerRunConfig(
            url=offer_url,
            cache_mode=self.cache_mode
        )

        result = await self.crawler.arun(offer_url, config=config)

        if result.html:
            soup = BeautifulSoup(result.html, 'html.parser')
            iframe_element = soup.select_one('iframe[src*="peakview.bg"]')
            
            if iframe_element and 'src' in iframe_element.attrs:
                iframe_src = iframe_element['src']
                # Ensure the iframe URL is absolute
                if not iframe_src.startswith('http'):
                    iframe_url = urllib.parse.urljoin(offer_url, iframe_src)
                else:
                    iframe_url = iframe_src

                print(f"Fetching iframe content from: {iframe_url}")
                iframe_config = CrawlerRunConfig(url=iframe_url, cache_mode=self.cache_mode)
                iframe_result = await self.crawler.arun(iframe_url, config=iframe_config)

                if iframe_result.html:
                    # Find the 'Виж повече' link within the iframe content
                    iframe_soup = BeautifulSoup(iframe_result.html, 'html.parser')
                    detailed_link_element = iframe_soup.find('a', class_='but', string='Виж повече')

                    detailed_offer_url = None
                    if detailed_link_element and 'href' in detailed_link_element.attrs:
                        relative_detailed_link = detailed_link_element['href']
                        # Construct absolute URL for the detailed offer page
                        detailed_offer_url = urllib.parse.urljoin(iframe_url, relative_detailed_link)
                        print(f"Found detailed offer link: {detailed_offer_url}")

                        # Fetch the detailed offer page content
                        detailed_config = CrawlerRunConfig(url=detailed_offer_url, cache_mode=self.cache_mode)
                        detailed_result = await self.crawler.arun(detailed_offer_url, config=detailed_config)

                        if detailed_result.html:
                            detailed_offer_data = await self._parse_detailed_offer_content(detailed_result.html, offer_name, detailed_offer_url)
                            if detailed_offer_data and self.is_complete(detailed_offer_data.model_dump()):
                                processed_item = {"data": detailed_offer_data.model_dump(), "path": output_path}
                                self.all_items.append(processed_item)
                                return processed_item
                            else:
                                print(f"No detailed data extracted or incomplete from detailed page for {detailed_offer_url}")
                        else:
                            print(f"No HTML content retrieved from detailed page for {detailed_offer_url}")
                    else:
                        print(f"No 'Виж повече' link found in iframe for {offer_url}")

                    # If no detailed link was found or processed, still save the basic info if available
                    if not detailed_offer_url:
                        detailed_offer_data = await self._parse_detailed_offer_content(iframe_result.html, offer_name, offer_url)
                        if detailed_offer_data and self.is_complete(detailed_offer_data.model_dump()):
                            processed_item = {"data": detailed_offer_data.model_dump(), "path": output_path}
                            self.all_items.append(processed_item)
                            return processed_item
                        else:
                            print(f"No detailed data extracted or incomplete from iframe for {offer_url}")
                else:
                    print(f"No HTML content retrieved from iframe for {iframe_url}")
            else:
                print(f"No iframe found or iframe src missing for {offer_url}")
        else:
            print(f"No HTML content retrieved for {offer_url}")
        
        return None

    async def _parse_detailed_offer_content(self, html_content: str, offer_name: str, detailed_offer_link: Optional[str] = None) -> Optional[AngelTravelDetailedOffer]:
        soup = BeautifulSoup(html_content, 'html.parser')

        program = ""
        included_services = []
        excluded_services = []

        # Extract program
        program_li = soup.find('li', string=re.compile(r'ПРОГРАМА', re.IGNORECASE))
        print(f"DEBUG: program_li: {program_li}")
        if program_li and 'aria-controls' in program_li.attrs:
            program_id = program_li['aria-controls']
            print(f"DEBUG: program_id: {program_id}")
            program_div = soup.find('div', class_='resp-tab-content', id=program_id)
            print(f"DEBUG: program_div: {program_div}")
            if program_div:
                program = program_div.get_text(separator='\n', strip=True)
                print(f"DEBUG: Extracted program: {program}")

        # Extract included services
        included_li = soup.find('li', string=re.compile(r'ЦЕНАТА ВКЛЮЧВА', re.IGNORECASE))
        print(f"DEBUG: included_li: {included_li}")
        if included_li and 'aria-controls' in included_li.attrs:
            included_id = included_li['aria-controls']
            print(f"DEBUG: included_id: {included_id}")
            included_div = soup.find('div', class_='resp-tab-content', id=included_id)
            print(f"DEBUG: included_div: {included_div}")
            if included_div:
                included_services = [item.strip() for item in included_div.get_text(separator='\n', strip=True).split('\n') if item.strip()]
                print(f"DEBUG: Extracted included_services: {included_services}")

        # Extract excluded services
        excluded_li = soup.find('li', string=re.compile(r'ЦЕНАТА НЕ ВКЛЮЧВА', re.IGNORECASE))
        print(f"DEBUG: excluded_li: {excluded_li}")
        if excluded_li and 'aria-controls' in excluded_li.attrs:
            excluded_id = excluded_li['aria-controls']
            print(f"DEBUG: excluded_id: {excluded_id}")
            excluded_div = soup.find('div', class_='resp-tab-content', id=excluded_id)
            print(f"DEBUG: excluded_div: {excluded_div}")
            if excluded_div:
                excluded_services = [item.strip() for item in excluded_div.get_text(separator='\n', strip=True).split('\n') if item.strip()]
                print(f"DEBUG: Extracted excluded_services: {excluded_services}")

        if offer_name:
            detailed_offer = AngelTravelDetailedOffer(
                offer_name=offer_name,
                program=program,
                included_services=included_services,
                excluded_services=excluded_services,
                detailed_offer_link=detailed_offer_link
            )
            return detailed_offer
        return None

    def _slugify(self, text: str) -> str:
        # Convert to lowercase
        text = text.lower()
        # Replace Cyrillic characters with their Latin equivalents (basic transliteration)
        # This is a simplified transliteration and might need to be expanded for full accuracy
        cyrillic_to_latin = {
            'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ж': 'zh', 'з': 'z',
            'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o', 'п': 'p',
            'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch',
            'ш': 'sh', 'щ': 'sht', 'ъ': 'a', 'ь': 'y', 'ю': 'yu', 'я': 'ya',
            '': '' # Handle the special character for 'амалфийска-ривиера.json'
        }
        for cyr, lat in cyrillic_to_latin.items():
            text = text.replace(cyr, lat)

        # Replace non-alphanumeric (excluding hyphens) characters with hyphens
        text = re.sub(r'[^a-z0-9-]+', '-', text)
        # Remove leading/trailing hyphens
        text = text.strip('-')
        # Replace multiple hyphens with a single hyphen
        text = re.sub(r'-+', '-', text)
        return text

    def save_data(self):
        for item in self.all_items:
            self._save_data_json(item["data"], item["path"])

async def crawl_angel_travel_detailed_offers():
    crawler = AngelTravelDetailedCrawler()
    await crawler.crawl()
