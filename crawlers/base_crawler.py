import os
import asyncio
import json
import time
import random
from datetime import datetime
import logging
from typing import List, Dict, Any, Optional, Type
from abc import ABC, abstractmethod
import signal

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig
from config import get_browser_config, MIN_DELAY_SECONDS, MAX_DELAY_SECONDS
from utils.data_utils import save_offers_to_csv, save_to_json, slugify
from utils.scraper_utils.llm_strategy import get_llm_strategy
from utils.enums import OutputType
import pandas as pd

class BaseCrawler(ABC):
    """
    Abstract base class for web crawlers. Provides common functionalities like session management,
    caching, retry mechanisms, and data handling. Subclasses must implement specific crawling
    logic through abstract methods.
    """
    def __init__(
        self,
        session_id: str,
        config: Type,
        model_class: Type,
        cache_mode: CacheMode = CacheMode.BYPASS,
        max_retries: int = 3,
        required_keys: Optional[List[str]] = None,
        key_fields: Optional[List[str]] = None,
        output_file_type: OutputType = OutputType.CSV,
    ):
        """
        Initializes the BaseCrawler with session-specific and crawling parameters.

        Args:
            session_id (str): A unique identifier for the crawling session.
            config (Type): The configuration object for the crawler (e.g., CrawlerConfig).
            model_class (Type): The Pydantic model class for data validation and serialization.
            cache_mode (CacheMode): Determines how caching is handled (e.g., BYPASS, CACHE_ONLY).
            max_retries (int): Maximum number of retries for failed requests.
            required_keys (Optional[List[str]]): List of keys that must be present in extracted data for it to be considered complete.
            key_fields (Optional[List[str]]): Fields used to identify unique items for duplicate checking.
            output_file_type (OutputType): Indicates the type of output file (e.g., OutputType.CSV, OutputType.JSON).
        """
        self.session_id = session_id
        self.config = config
        self.model_class = model_class
        self.cache_mode = cache_mode
        self.max_retries = max_retries
        self.required_keys = required_keys if required_keys is not None else []
        self.key_fields = key_fields if key_fields is not None else []
        self.output_file_type = output_file_type
        
        # Initialize output_dir and filepath based on config and output_file_type
        if self.output_file_type == OutputType.CSV:
            self.output_dir = self.config.FILES_DIR
            self.filepath = os.path.join(self.output_dir, "complete_offers.csv")
        elif self.output_file_type == OutputType.JSON:
            self.output_dir = self.config.DETAILS_DIR
            self.filepath = None # For JSON, filepath is dynamic per item
        else:
            self.output_dir = None
            self.filepath = None

        # Ensure output directory exists
        if self.output_dir:
            os.makedirs(self.output_dir, exist_ok=True)
        
        # Initialize the AsyncWebCrawler with browser configuration.
        self.browser_config = get_browser_config()
        self.crawler = AsyncWebCrawler(config=self.browser_config)
        self.llm_strategy = None  # Placeholder for LLM strategy, if used.
        self.seen_items = set()  # Stores identifiers of already processed items to avoid duplicates.
        self.all_items = []  # Accumulates all successfully processed items.
        self.stop_event = asyncio.Event() # Event to signal graceful shutdown.

    def _signal_handler(self, signum, frame):
        logging.info("Ctrl+C detected. Initiating forceful shutdown...")
        self.stop_event.set()
        
        # Forceful exit after a short delay to allow some cleanup
        # This is a last resort to ensure the process terminates.
        asyncio.get_event_loop().call_later(0.1, os._exit, 1)

    async def _run_crawler_with_retries(self, url: str, config: CrawlerRunConfig, description: str = "crawling") -> Any:
        """
        Executes a crawling operation with retry mechanism and exponential backoff.

        Args:
            url (str): The URL to crawl.
            config (CrawlerRunConfig): The configuration for the crawler run.
            description (str): A description of the crawling operation for logging.

        Returns:
            Any: The result of the crawling operation.

        Raises:
            Exception: If the crawling operation fails after all retries.
        """
        for attempt in range(self.max_retries):
            # Check for graceful shutdown before attempting to crawl
            if self.stop_event.is_set():
                logging.info(f"Graceful shutdown initiated. Skipping {description} {url}.")
                raise asyncio.CancelledError("Crawling cancelled due to graceful shutdown.")

            try:
                logging.info(f"Attempt {attempt + 1}/{self.max_retries} to {description} {url}")
                result = await self.crawler.arun(url, config=config)
                if result and (result.html or result.extracted_content):
                    return result
                elif attempt == self.max_retries - 1:
                    raise Exception(f"Failed to get content for {url} after {self.max_retries} attempts.")
            except Exception as e:
                logging.error(f"Error during {description} {url} (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    retry_delay = 2 ** attempt + random.uniform(0, 1)
                    logging.warning(f"Retrying in {retry_delay:.2f} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    raise
        return None

    def _load_existing_data_csv(self, filepath: str, key_fields: List[str]):
        """
        Loads existing data from a CSV file into `seen_items` and `all_items`.
        This prevents reprocessing items that have already been crawled.

        Args:
            filepath (str): The path to the CSV file.
            key_fields (List[str]): A list of keys to uniquely identify each row in the CSV.
        """
        if os.path.exists(filepath):
            # Read the CSV, ensuring key fields are treated as strings to prevent data type issues.
            existing_df = pd.read_csv(filepath, dtype={k: str for k in key_fields})
            for _, row in existing_df.iterrows():
                # Create a normalized tuple of key field values for duplicate checking.
                normalized_keys = tuple(str(row[k]).lower().strip() for k in key_fields)
                self.seen_items.add(normalized_keys)
            self.all_items.extend(existing_df.to_dict(orient='records'))
            logging.info(f"Loaded {len(self.seen_items)} existing items from {filepath}")

    def _load_existing_data_json(self, dirpath: str):
        """
        Loads existing data from JSON files within a directory into `seen_items`.
        It reads the 'offer_name' from each JSON file and slugifies it to add to `seen_items`.

        Args:
            dirpath (str): The path to the directory containing JSON files.
        """
        self.seen_items = set() # Clear existing seen_items to ensure a fresh load.
        if os.path.exists(dirpath):
            for filename in os.listdir(dirpath):
                if filename.endswith(".json"):
                    filepath = os.path.join(dirpath, filename)
                    try:
                        with open(filepath, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            if 'offer_name' in data:
                                offer_name_slug = slugify(data['offer_name'])
                                self.seen_items.add(offer_name_slug)
                    except json.JSONDecodeError as e:
                        logging.error(f"Error decoding JSON from {filepath}: {e}")
                    except Exception as e:
                        logging.error(f"Error loading {filepath}: {e}")
            logging.info(f"Loaded {len(self.seen_items)} existing items from {dirpath}")

    @abstractmethod
    async def get_urls_to_crawl(self, max_items: Optional[int] = None) -> List[Any]:
        """
        Abstract method to be implemented by subclasses. This method should return
        a list of URLs or items that need to be crawled.

        Returns:
            List[Any]: A list of items (e.g., URLs, dictionaries) to be processed.
        """
        pass

    @abstractmethod
    async def process_item(self, item: Any, seen_items: set) -> Optional[Dict[str, Any]]:
        """
        Abstract method to be implemented by subclasses. This method defines how
        a single item (e.g., a URL) is processed and its data extracted.

        Args:
            item (Any): The item to be processed.

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing the processed data, or None if processing fails.
        """
        pass

    def is_duplicate(self, item: Dict[str, Any]) -> bool:
        """
        Checks if an item is a duplicate based on its key fields.

        Args:
            item (Dict[str, Any]): The item to check for duplication.

        Returns:
            bool: True if the item is a duplicate, False otherwise.
        """
        if not self.key_fields:
            return False  # If no key fields are defined, no duplication check is performed.
        # Normalize the key field values for consistent comparison.
        normalized_keys = tuple(item.get(k, '').lower().strip() for k in self.key_fields)
        return normalized_keys in self.seen_items

    def is_complete(self, item: Dict[str, Any]) -> bool:
        """
        Checks if an item contains all the required keys.

        Args:
            item (Dict[str, Any]): The item to check for completeness.

        Returns:
            bool: True if all required keys are present, False otherwise.
        """
        if not self.required_keys:
            return True  # If no required keys are defined, the item is always considered complete.
        # Check if all specified required keys exist in the item.
        missing_keys = [key for key in self.required_keys if key not in item]
        if missing_keys:
            logging.warning(f"Item is incomplete. Missing keys: {', '.join(missing_keys)}. Item: {item}")
            return False
        return True

    def _save_data_csv(self, filepath: str, model_class: Type):
        """
        Saves collected data to a CSV file, merging with existing data and removing duplicates.

        Args:
            filepath (str): The path where the CSV file will be saved.
            model_class (Type): The Pydantic model class used for data validation and serialization.
        """
        if not self.all_items:
            logging.info("No new offers to save in this crawl.")
            return

        new_df = pd.DataFrame(self.all_items)
        
        if os.path.exists(filepath):
            existing_df = pd.read_csv(filepath, dtype={k: str for k in self.key_fields})
            combined_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=self.key_fields).reset_index(drop=True)
        else:
            combined_df = new_df

        # Ensure all columns from the model are present, filling missing with None or empty string
        # This is important if new_df doesn't have all columns from the model
        fieldnames = list(model_class.model_fields.keys())
        for col in fieldnames:
            if col not in combined_df.columns:
                combined_df[col] = None # Or '' depending on desired default

        # Reorder columns to match model_class field order
        combined_df = combined_df[fieldnames]

        combined_df.to_csv(filepath, index=False, encoding="utf-8")
        logging.info(f"Saved {len(combined_df)} unique offers to '{filepath}'.")

    def _save_data_json(self, data: Dict[str, Any], filepath: str):
        """
        Saves a single data item to a JSON file.

        Args:
            data (Dict[str, Any]): The data to be saved.
            filepath (str): The path where the JSON file will be saved.
        """
        save_to_json(data, filepath)
        logging.info(f"Saved detailed offer to {filepath}")

    def _get_detailed_item_filepath(self, item: Dict[str, Any]) -> Optional[str]:
        """
        Generates the expected file path for a detailed item based on its name.
        Assumes the item has a 'name' key that can be slugified.
        """
        if "name" in item and self.output_file_type == 'json':
            slugified_name = slugify(item["name"])
            return os.path.join(self.config.DETAILS_DIR, f"{slugified_name}.json")
        return None

    def _load_detailed_item_from_file(self, filepath: str) -> Optional[Dict[str, Any]]:
        """
        Loads a detailed item from its JSON file.
        """
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError as e:
                    logging.error(f"Error decoding JSON from {filepath}: {e}")
        return None

    def save_data(self):
        """
        Saves the collected data based on the configured output file type.
        """
        if self.output_file_type == OutputType.CSV:
            self._save_data_csv(self.filepath, self.model_class)
        elif self.output_file_type == OutputType.JSON:
            # For JSON, all_items will contain dictionaries with 'data' and 'path'
            for item in self.all_items:
                self._save_data_json(item["data"], item["path"])
        else:
            logging.warning(f"Unknown output file type: {self.output_file_type}. Data not saved.")

    async def crawl(self, max_items: Optional[int] = None):
        """
        Orchestrates the crawling process. This method initializes the crawler,
        loads existing data, fetches URLs to crawl, processes each item, and saves the results.

        Args:
            max_items (Optional[int]): An optional limit on the number of items to process.
        """
        # Register the signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)

        # Enter the asynchronous context for the crawler.
        try:
            await self.crawler.__aenter__()
        except Exception as e:
            logging.error(f"Failed to initialize crawler: {type(e).__name__}: {e}")
            # Re-raise the exception to stop the crawl if initialization fails
            raise
        # Load existing data based on the configured output file type.
        if self.output_file_type == OutputType.CSV:
            self._load_existing_data_csv(self.filepath, self.key_fields)
        elif self.output_file_type == OutputType.JSON:
            self._load_existing_data_json(self.output_dir)
        
        try:
            # Retrieve the list of URLs or items that need to be crawled.
            urls_to_crawl = await self.get_urls_to_crawl(max_items=max_items)
            
            # Iterate through each item to be crawled.
            for i, item in enumerate(urls_to_crawl):
                # Check if the maximum item limit has been reached.
                if max_items is not None and len(self.all_items) >= max_items:
                    logging.info(f"Reached max_items limit of {max_items}. Stopping.")
                    break
                
                # Check if graceful shutdown has been initiated
                if self.stop_event.is_set():
                    logging.info("Graceful shutdown initiated. Stopping crawling.")
                    break

                # Process the current item.
                processed_item = await self.process_item(item, self.seen_items)
                if processed_item:
                    self.all_items.append(processed_item) # Add successfully processed item to the list.

                # Introduce a random delay between requests to avoid overwhelming the server.
                if i < len(urls_to_crawl) - 1:
                    delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
                    logging.info(f"Waiting {delay:.1f} seconds before next request...")
                    # Wait for the delay or until stop_event is set
                    try:
                        await asyncio.wait_for(self.stop_event.wait(), timeout=delay)
                        logging.info("Delay interrupted by graceful shutdown signal.")
                        break # Break the loop if signal received during delay
                    except asyncio.TimeoutError:
                        pass # Delay completed without interruption

        except asyncio.CancelledError:
            logging.info("Crawling task cancelled. Performing cleanup.")
        except Exception as e:
            # Log any errors that occur during the crawling process.
            logging.error(f"An error occurred during the crawling process: {e}")
        finally:
            # Exit the asynchronous context for the crawler.
            try:
                await self.crawler.__aexit__(None, None, None)
            except Exception as e:
                # Catch any exception during cleanup, as it's expected during graceful shutdown
                # when Playwright might try to close an already closed browser/context,
                # or when the event loop is closing.
                logging.warning(f"Error during crawler cleanup (expected during shutdown): {type(e).__name__}: {e}")
            
            self.save_data() # Save all collected data.
            if self.llm_strategy:
                self.llm_strategy.show_usage() # Display LLM usage if an LLM strategy is present.