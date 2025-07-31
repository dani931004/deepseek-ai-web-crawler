
import os
import asyncio
import json
import time
import random
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Type
from abc import ABC, abstractmethod

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig
from config import get_browser_config
from utils.data_utils import save_offers_to_csv, save_to_json
from utils.scraper_utils.llm_strategy import get_llm_strategy
import pandas as pd

class BaseCrawler(ABC):
    def __init__(
        self,
        session_id: str,
        cache_mode: CacheMode = CacheMode.BYPASS,
        max_retries: int = 3,
        required_keys: Optional[List[str]] = None,
        key_fields: Optional[List[str]] = None,
    ):
        self.session_id = session_id
        self.cache_mode = cache_mode
        self.max_retries = max_retries
        self.required_keys = required_keys if required_keys is not None else []
        self.key_fields = key_fields if key_fields is not None else []
        
        self.browser_config = get_browser_config()
        self.crawler = AsyncWebCrawler(config=self.browser_config)
        self.llm_strategy = None
        self.seen_items = set()
        self.all_items = []

    def _load_existing_data_csv(self, filepath: str, key_fields: List[str]):
        if os.path.exists(filepath):
            existing_df = pd.read_csv(filepath, dtype={k: str for k in key_fields})
            for _, row in existing_df.iterrows():
                normalized_keys = tuple(str(row[k]).lower().strip() for k in key_fields)
                self.seen_items.add(normalized_keys)
            self.all_items.extend(existing_df.to_dict(orient='records'))
            print(f"Loaded {len(self.seen_items)} existing items from {filepath}")

    def _load_existing_data_json(self, dirpath: str):
        self.seen_items = set() # Ensure seen_items is empty before loading
        if os.path.exists(dirpath):
            for filename in os.listdir(dirpath):
                if filename.endswith(".json"):
                    slugified_name = filename.replace(".json", "")
                    if slugified_name: # Only add if not empty
                        self.seen_items.add(slugified_name)
                        print(f"DEBUG: Added {slugified_name} to seen_items.")
            print(f"Loaded {len(self.seen_items)} existing items from {dirpath}")

    @abstractmethod
    async def get_urls_to_crawl(self) -> List[Any]:
        pass

    @abstractmethod
    async def process_item(self, item: Any) -> Optional[Dict[str, Any]]:
        pass

    def is_duplicate(self, item: Dict[str, Any]) -> bool:
        if not self.key_fields:
            return False
        normalized_keys = tuple(item.get(k, '').lower().strip() for k in self.key_fields)
        return normalized_keys in self.seen_items

    def is_complete(self, item: Dict[str, Any]) -> bool:
        if not self.required_keys:
            return True
        return all(key in item for key in self.required_keys)

    def _save_data_csv(self, filepath: str, model_class: Type):
        if self.all_items:
            save_offers_to_csv(self.all_items, filepath, model_class)
            print(f"Saved {len(self.all_items)} total offers to '{filepath}'.")
        else:
            print("No offers were found during the entire crawl.")

    def _save_data_json(self, data: Dict[str, Any], filepath: str):
        save_to_json(data, filepath)
        print(f"Saved detailed offer to {filepath}")

    async def crawl(self, max_items: Optional[int] = None):
        await self.crawler.__aenter__()
        self.seen_items.clear() # Clear seen items at the beginning of each crawl
        self._load_existing_data_json(self.output_dir) # Reload existing data
        
        try:
            urls_to_crawl = await self.get_urls_to_crawl()
            
            for i, item in enumerate(urls_to_crawl):
                if max_items and len(self.all_items) >= max_items:
                    print(f"Reached max_items limit of {max_items}. Stopping.")
                    break
                
                processed_item = await self.process_item(item)
                if processed_item:
                    self.all_items.append(processed_item)

                # Add delay between requests
                if i < len(urls_to_crawl) - 1:
                    delay = random.uniform(5, 15)
                    print(f"Waiting {delay:.1f} seconds before next request...")
                    await asyncio.sleep(delay)

        except Exception as e:
            print(f"An error occurred during the crawling process: {e}")
        finally:
            await self.crawler.__aexit__(None, None, None)
            self.save_data()
            if self.llm_strategy:
                self.llm_strategy.show_usage()

