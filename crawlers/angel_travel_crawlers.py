import os
import asyncio
import json
import time
import random
import logging
from datetime import datetime

from typing import List, Dict, Any, Optional, Type, Tuple
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig
from config import angel_travel_config, get_browser_config, PAGE_TIMEOUT

from bs4 import BeautifulSoup
from config import CSS_SELECTOR_OFFER_ITEM_TITLE
from utils.data_utils import (
    save_offers_to_csv,
    slugify
)

from utils.scraper_utils.llm_strategy import get_llm_strategy
import urllib.parse
import re
from models.angel_travel_models import AngelTravelOffer
import pandas as pd
from .base_crawler import BaseCrawler
from utils.enums import OutputType


class AngelTravelCrawler(BaseCrawler):
    def __init__(self, session_id: str, config: Type, model_class: Type, output_file_type: OutputType = OutputType.CSV):
        super().__init__(
            session_id=session_id,
            config=config,
            model_class=model_class,
            required_keys=config.required_keys,
            key_fields=['title', 'link'],
            output_file_type=OutputType.CSV
        )
        self.llm_strategy = get_llm_strategy(AngelTravelOffer)
        self.processed_destinations = set()

    async def get_urls_to_crawl(self, max_items: Optional[int] = None) -> List[Any]:
        logging.info("Step 1: Fetching destination links...")
        all_destination_links = await self._get_destination_links()
        if not all_destination_links:
            logging.info("No destination links found. Exiting.")
            return []

        # Filter out already processed destinations
        destination_links = []
        for dest_url, dest_name in all_destination_links:
            if dest_url not in self.processed_destinations:
                destination_links.append((dest_url, dest_name))
            else:
                logging.info(f"Skipping already processed destination: {dest_name} ({dest_url})")

        if not destination_links:
            logging.info("All destination links have already been processed. Exiting.")
            return []

        logging.info(f"Found {len(destination_links)} new destination links to process.")
        return destination_links

    async def _get_destination_links(self) -> List[tuple[str, str]]:
        url = self.config.base_url
        config = CrawlerRunConfig(
            cache_mode=self.cache_mode,
            session_id=f"{self.session_id}_main_page",
            extraction_strategy=None,
            verbose=True,
            page_timeout=PAGE_TIMEOUT,
        )
        result = await self._run_crawler_with_retries(url, config=config, description="fetching destination links")
        if not result or not result.html:
            logging.error(f"Failed to load main page: {url}")
            return []

        soup = BeautifulSoup(result.html, 'html.parser')
        offer_elements = soup.select(self.config.css_selector)
        
        destination_links = []
        for element in offer_elements:
            a_tag = element.find('a', class_='accordeonck')
            if a_tag and 'href' in a_tag.attrs:
                href = a_tag['href']
                full_url = urllib.parse.urljoin(self.config.base_url, href)
                name = a_tag.get_text(strip=True)
                destination_links.append((full_url, name))
                
        return destination_links

    async def process_item(self, item: Any, seen_items: set) -> Optional[Dict[str, Any]]:
        dest_url, dest_name = item
        logging.info(f"\nProcessing destination: {dest_name} ({dest_url})")
        
        try:
            offer_elements, iframe_src = await self._crawl_destination_page(dest_url)
            if not offer_elements:
                logging.info(f"No offers found on {dest_url}")
                return None

            logging.info(f"Found {len(offer_elements)} offer elements on {dest_name}")

            total_offers_on_page = len(offer_elements) # Get total offers on this page

            for i, offer_element in enumerate(offer_elements, 1):
                logging.info(f"Processing offer {i}/{total_offers_on_page} for destination: {dest_name})")
                if self.config.max_offers_to_crawl and len(self.all_items) >= self.config.max_offers_to_crawl:
                    logging.info(f"Reached max_items limit of {max_items}. Stopping processing offer elements.")
                    break
                try:
                    # Manually extract data using BeautifulSoup
                    title_el = offer_element.find('h2')
                    title = title_el.get_text(strip=True) if title_el else ""

                    dates_el = offer_element.find('font', class_='date')
                    dates = dates_el.get_text(strip=True) if dates_el else ""

                    price_el = offer_element.find('font', class_='price')
                    price = price_el.get_text(strip=True) if price_el else ""

                    link_el = offer_element.find('a', class_='read-more')
                    link = urllib.parse.urljoin(dest_url, link_el['href']) if link_el and 'href' in link_el.attrs else ""

                    # Create a dictionary for the offer
                    offer_data = {
                        'title': title,
                        'dates': dates,
                        'price': price,
                        'transport_type': 'N/A', # Transport type is not directly available in the iframe content
                        'link': link,
                        'main_page_link': dest_url
                    }

                    if self.is_complete(offer_data): # is_duplicate check will be handled by _append_item_to_csv
                        self._append_item_to_csv(offer_data, self.filepath, self.model_class, self.key_fields)
                        logging.info(f"Successfully extracted and added new offer: {offer_data['title']}")
                    else:
                        logging.info(f"Skipping incomplete offer: {offer_data.get('title', 'N/A')}")

                except Exception as e:
                    logging.error(f"Error processing offer element {i} on {dest_url}: {e}")

        except Exception as e:
            logging.error(f"Error crawling destination {dest_url}: {e}")
        
        self.processed_destinations.add(dest_url)
        return None

    async def _crawl_destination_page(self, dest_url: str) -> Tuple[List[Any], str]:
        config = CrawlerRunConfig(
            cache_mode=self.cache_mode,
            session_id=f"{self.session_id}_{slugify(dest_url)}",
            extraction_strategy=None,
            verbose=True,
            page_timeout=PAGE_TIMEOUT,
        )
        result = await self._run_crawler_with_retries(dest_url, config=config, description=f"fetching destination page {dest_url}")
        if not result or not result.html:
            logging.error(f"Failed to load destination page: {dest_url}")
            return [], ""

        soup = BeautifulSoup(result.html, 'html.parser')
        iframe_tag = soup.find('iframe', src=re.compile(r'iframe\.peakview\.bg'))
        if not iframe_tag or not iframe_tag.get('src'):
            logging.error(f"Could not find iframe with peakview.bg src on {dest_url}")
            return [], ""

        iframe_src = iframe_tag['src']
        if iframe_src.startswith('//'):
            iframe_src = "https:" + iframe_src
        elif iframe_src.startswith('/'):
            iframe_src = urllib.parse.urljoin(dest_url, iframe_src)

        iframe_config = CrawlerRunConfig(
            cache_mode=self.cache_mode,
            session_id=f"{self.session_id}_{slugify(iframe_src)}",
            extraction_strategy=None,
            verbose=True,
            page_timeout=PAGE_TIMEOUT,
        )
        iframe_result = await self._run_crawler_with_retries(iframe_src, config=iframe_config, description=f"fetching iframe content from {iframe_src}")
        if not iframe_result or not iframe_result.html:
            logging.error(f"Failed to load iframe content from {iframe_src}")
            return [], ""

        iframe_soup = BeautifulSoup(iframe_result.html, 'html.parser')
        offer_elements = iframe_soup.select('div.program_once') # This selector needs to be confirmed based on actual iframe content
        return offer_elements, iframe_src

    async def crawl(self, max_items: Optional[int] = None):
        # Call the base crawler's crawl method to load existing data
        await super().crawl(max_items=max_items)

        # Populate processed_destinations after existing data has been loaded
        self.processed_destinations = set()
        for item in self.all_items:
            if 'main_page_link' in item:
                self.processed_destinations.add(item['main_page_link'])


async def crawl_angel_travel_offers(max_offers: Optional[int] = None):
    crawler = AngelTravelCrawler(session_id="angel_travel_session", config=angel_travel_config, model_class=AngelTravelOffer)
    await crawler.crawl(max_items=max_offers)

    


    
