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
            cache_mode=self.cache_mode,
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
        program_h2 = soup.find('h2', string='ПРОГРАМА')
        if program_h2:
            program_div = program_h2.find_next_sibling('div', class_='resp-tab-content hor_1')
            if program_div:
                program = program_div.get_text(separator='\n', strip=True)

        # Extract included services
        included_h2 = soup.find('h2', string='ЦЕНАТА ВКЛЮЧВА')
        if included_h2:
            included_div = included_h2.find_next_sibling('div', class_='resp-tab-content hor_1')
            if included_div:
                included_services = [li.get_text(strip=True) for li in included_div.find_all('li') if li.get_text(strip=True)]

        # Extract excluded services
        excluded_h2 = soup.find('h2', string='ЦЕНАТА НЕ ВКЛЮЧВА')
        if excluded_h2:
            excluded_div = excluded_h2.find_next_sibling('div', class_='resp-tab-content hor_1')
            if excluded_div:
                excluded_services = [li.get_text(strip=True) for li in excluded_div.find_all('li') if li.get_text(strip=True)]

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
        text = text.lower()
        text = re.sub(r'[\n\w\s-]', '', text)
        text = re.sub(r'[-\s]+', '-', text).strip('-')
        return text

    def save_data(self):
        for item in self.all_items:
            self._save_data_json(item["data"], item["path"])

async def crawl_angel_travel_detailed_offers():
    crawler = AngelTravelDetailedCrawler()
    await crawler.crawl()
