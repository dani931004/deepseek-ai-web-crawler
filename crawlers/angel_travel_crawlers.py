import os
import asyncio
import json
import time
import random
import logging
from datetime import datetime

from typing import List, Dict, Any, Optional, Type
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig
from config import angel_travel_config, get_browser_config

from bs4 import BeautifulSoup
from config import CSS_SELECTOR_OFFER_ITEM_TITLE
from utils.data_utils import (
    save_offers_to_csv,
)

from utils.scraper_utils.llm_strategy import get_llm_strategy
import urllib.parse
import re
from models.angel_travel_models import AngelTravelOffer
import pandas as pd
from .base_crawler import BaseCrawler


class AngelTravelCrawler(BaseCrawler):
    def __init__(self, session_id: str, config: Type, model_class: Type):
        super().__init__(
            session_id=session_id,
            config=config,
            model_class=model_class,
            required_keys=config.required_keys,
            key_fields=['title', 'link'],
            output_file_type='csv'
        )
        self.llm_strategy = get_llm_strategy(AngelTravelOffer)

    async def get_urls_to_crawl(self, max_items: Optional[int] = None) -> List[Any]:
        print("Step 1: Fetching destination links...")
        destination_links = await self._get_destination_links(max_items)
        if not destination_links:
            print("No destination links found. Exiting.")
            return []
        print(f"Found {len(destination_links)} destination links to process.")
        return destination_links

    async def _get_destination_links(self, max_items: Optional[int] = None) -> List[tuple[str, str]]:
        url = self.config.base_url
        config = CrawlerRunConfig(
            cache_mode=self.cache_mode,
            session_id=f"{self.session_id}_main_page",
            extraction_strategy=None,
            verbose=True,
        )
        result = await self._run_crawler_with_retries(url, config=config, description="fetching destination links")
        if not result or not result.html:
            print(f"Failed to load main page: {url}")
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
                if max_items and len(destination_links) >= max_items:
                    break
                
        return destination_links

    async def process_item(self, item: Any, max_items: Optional[int] = None) -> Optional[Dict[str, Any]]:
        dest_url, dest_name = item
        print(f"\nProcessing destination: {dest_name} ({dest_url})")
        
        try:
            offer_elements, iframe_src = await self._crawl_destination_page(dest_url)
            if not offer_elements:
                print(f"No offers found on {dest_url}")
                return None

            print(f"Found {len(offer_elements)} offer elements on {dest_name}")

            for i, offer_element in enumerate(offer_elements, 1):
                if max_items and len(self.all_items) >= max_items:
                    print(f"Reached max_items limit of {max_items}. Stopping processing offer elements.")
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

                    if not self.is_duplicate(offer_data) and self.is_complete(offer_data):
                        self.all_items.append(offer_data)
                        self.seen_items.add(tuple(offer_data.get(k, '').lower().strip() for k in self.key_fields))
                        print(f"Successfully extracted and added new offer: {offer_data['title']}")
                    else:
                        print(f"Skipping duplicate or incomplete offer: {offer_data.get('title', 'N/A')}")

                except Exception as e:
                    print(f"Error processing offer element {i} on {dest_url}: {e}")

        except Exception as e:
            print(f"Error crawling destination {dest_url}: {e}")
        
        return None

    async def _crawl_destination_page(self, url: str, save_html: bool = False) -> tuple[List[Any], Optional[str]]:
        config = CrawlerRunConfig(
            cache_mode=self.cache_mode,
            session_id=f"{self.session_id}_destination_page",
            extraction_strategy=None,
            verbose=True,
            page_timeout=120000,
            delay_before_return_html=2.0
        )
        try:
            result = await self._run_crawler_with_retries(url, config=config, description="crawling destination page")
            if not result or not result.html:
                print(f"Failed to load destination page: {url}")
                return []

            if save_html:
                with open('debug_destination_page.html', 'w', encoding='utf-8') as f:
                    f.write(result.html)

            soup = BeautifulSoup(result.html, 'html.parser')
            iframe = soup.find('iframe')
            if iframe and 'src' in iframe.attrs:
                iframe_src = iframe['src']
                if not iframe_src.startswith('http'):
                    iframe_src = urllib.parse.urljoin(url, iframe_src)
                
                print(f"Found iframe with src: {iframe_src}")
                iframe_result = await self.crawler.arun(iframe_src, config=config)
                if iframe_result and iframe_result.html:
                    iframe_soup = BeautifulSoup(iframe_result.html, 'html.parser')
                    return iframe_soup.select('div.once_offer'), iframe_src

            return soup.select('a.offer-item'), None
        except Exception as e:
            print(f"An exception occurred while crawling {url}: {e}")
            print("Attempting to restart the browser...")
            await self.crawler.__aexit__(None, None, None)
            await self.crawler.__aenter__()
            return []


async def crawl_angel_travel_offers(max_offers: Optional[int] = None):
    crawler = AngelTravelCrawler()
    await crawler.crawl(max_items=max_offers)
