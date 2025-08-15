import os
import asyncio
import json
import logging
from typing import List, Dict, Any, Optional, Type
from bs4 import BeautifulSoup
import urllib.parse
import pandas as pd

from crawl4ai import CrawlerRunConfig, CacheMode
from config import dari_tour_excursions_config, CSS_SELECTOR_OFFER_ITEM_TITLE, PAGE_TIMEOUT
from utils.scraper_utils.llm_strategy import get_llm_strategy
from .base_crawler import BaseCrawler
from utils.enums import OutputType
from models.dari_tour_excursions_models import DariTourExcursionOffer


class DariTourExcursionsCrawler(BaseCrawler):
    """
    A crawler for Dari Tour website to extract general excursion offer information.
    It extends the BaseCrawler to utilize shared crawling infrastructure.
    """
    def __init__(self, session_id: str, config: Type, model_class: Type, output_file_type: OutputType = OutputType.CSV):
        """
        Initializes the DariTourExcursionsCrawler with session ID, config, and model class.
        """
        super().__init__(
            session_id=session_id,
            config=config,
            model_class=model_class,
            output_file_type=OutputType.CSV,
            required_keys=config.required_keys,
            key_fields=['name', 'link'] # Define key fields for duplicate checking.
        )
        self.llm_strategy = get_llm_strategy(model=model_class)
        self.processed_destination_urls_filepath = os.path.join(self.output_dir, "processed_general_excursion_urls.csv")
        self.processed_destination_urls = self._load_processed_destination_urls()

    def _load_processed_destination_urls(self) -> set:
        """
        Loads previously processed destination URLs from a CSV file.
        """
        if os.path.exists(self.processed_destination_urls_filepath):
            try:
                df = pd.read_csv(self.processed_destination_urls_filepath)
                if 'url' in df.columns:
                    return set(df['url'].tolist())
            except Exception as e:
                logging.warning(f"Could not load processed destination URLs from {self.processed_destination_urls_filepath}: {e}")
        return set()

    def _save_processed_destination_urls(self):
        """
        Saves the set of processed destination URLs to a CSV file.
        """
        df = pd.DataFrame(list(self.processed_destination_urls), columns=['url'])
        df.to_csv(self.processed_destination_urls_filepath, index=False)
        logging.info(f"Saved {len(self.processed_destination_urls)} processed destination URLs to {self.processed_destination_urls_filepath}")

    async def get_urls_to_crawl(self, max_items: Optional[int] = None) -> List[Any]:
        """
        Fetches the main excursions page, extracts destination links, and then crawls
        each destination page to identify individual excursion offers.

        Returns:
            List[Any]: A list of dictionaries, each representing an offer element with its URL and name.
        """
        main_excursions_url = self.config.base_url # https://dari-tour.com/
        logging.info(f"Loading main excursions page: {main_excursions_url}")

        # Configure crawler for the main page to get destination links
        main_page_config = CrawlerRunConfig(
            cache_mode=self.cache_mode,
            session_id=f"{self.session_id}_main_excursions",
            extraction_strategy=None,
            scan_full_page=False,
            wait_for_images=False,
            remove_overlay_elements=True,
            verbose=True,
            page_timeout=PAGE_TIMEOUT,
            delay_before_return_html=2.0,
            wait_until="domcontentloaded",
            wait_for="body", # Wait for the body to load, then parse for links
            only_text=False,
            remove_forms=True,
            prettiify=True,
            ignore_body_visibility=True,
            js_only=False,
            magic=True,
            screenshot=False,
            pdf=False
        )

        main_page_result = await self._run_crawler_with_retries(
            main_excursions_url,
            config=main_page_config,
            description="fetching main excursions page"
        )

        if not main_page_result or not main_page_result.html:
            logging.error(f"Failed to load main excursions page: {main_excursions_url}")
            return []

        soup = BeautifulSoup(main_page_result.html, 'html.parser')
        destination_links = soup.select("ul.clearfix.three-col li a")

        total_destinations = len(destination_links)
        logging.info(f"Found {total_destinations} potential destination links.")

        all_offers_to_process = []
        for i, link_element in enumerate(destination_links):
            relative_path = link_element.get('href')
            if relative_path and not relative_path.startswith('javascript'):
                destination_url = urllib.parse.urljoin(self.config.base_url, relative_path)
                destination_name = link_element.get_text(strip=True)

                logging.info(f"\033[1;36mProcessing destination {i+1}/{total_destinations}: {destination_name} ({destination_url})\033[0m")

                if destination_url in self.processed_destination_urls:
                    logging.info(f"Skipping destination {destination_name} as it has already been processed.")
                    continue

                # Now crawl each destination page for offers
                destination_page_config = CrawlerRunConfig(
                    cache_mode=self.cache_mode,
                    session_id=f"{self.session_id}_{destination_name.replace(' ', '_')}",
                    extraction_strategy=None,
                    scan_full_page=False,
                    wait_for_images=False,
                    remove_overlay_elements=True,
                    verbose=True,
                    page_timeout=PAGE_TIMEOUT,
                    delay_before_return_html=2.0,
                    wait_until="domcontentloaded",
                    wait_for=self.config.css_selector, # Selector for individual offer items
                    only_text=False,
                    remove_forms=True,
                    prettiify=True,
                    ignore_body_visibility=True,
                    js_only=False,
                    magic=True,
                    screenshot=False,
                    pdf=False
                )

                destination_page_result = await self._run_crawler_with_retries(
                    destination_url,
                    config=destination_page_config,
                    description=f"fetching offers from {destination_name} page"
                )

                if not destination_page_result or not destination_page_result.html:
                    logging.error(f"Failed to load destination page: {destination_url}")
                    continue

                dest_soup = BeautifulSoup(destination_page_result.html, 'html.parser')
                offer_elements = dest_soup.select(self.config.css_selector)

                if not offer_elements:
                    logging.info(f"No offer items found on {destination_url}")
                    continue

                for offer_element in offer_elements:
                    actual_url = None
                    offer_title = ""
                    # The offer link is the href of the a.offer-item itself
                    if offer_element.name == 'a' and 'href' in offer_element.attrs:
                        href = offer_element['href']
                        if href.startswith('http'):
                            actual_url = href
                        else:
                            actual_url = urllib.parse.urljoin(self.config.base_url, href)
                        actual_url = actual_url.split('?')[0].split('#')[0]
                        
                        # The title is within a div.title inside the offer_element
                        title_el = offer_element.select_one("div.title")
                        if title_el:
                            offer_title = title_el.get_text(strip=True)

                    normalized_offer_title = offer_title.lower().strip()
                    normalized_actual_url = actual_url.lower().strip() if actual_url else ""

                    # Check for duplicates before adding to the list of items to process
                    # Note: self.seen_items is populated by _load_existing_data_csv at the start of crawl()
                    if (normalized_offer_title, normalized_actual_url) not in self.seen_items:
                        all_offers_to_process.append({
                            'offer_element': offer_element,
                            'actual_url': actual_url,
                            'offer_name': offer_title # Use offer_title as offer_name for consistency
                        })
                    else:
                        logging.info(f"Skipping {offer_title} ({actual_url}) from initial crawl list as it has already been processed.")
                
                # Mark the destination URL as processed after all its offers have been considered
                self.processed_destination_urls.add(destination_url)
                self._save_processed_destination_urls()

        logging.info(f"Found {len(all_offers_to_process)} new excursion offers to process.")
        if max_items:
            return all_offers_to_process[:max_items]
        return all_offers_to_process

    async def process_item(self, item: Any, seen_items: set) -> Optional[Dict[str, Any]]:
        """
        Processes a single excursion offer item extracted from the listing page.
        This involves extracting the offer name and link, and then using an LLM strategy
        to extract structured data from the offer element.

        Args:
            item (Any): A dictionary containing the BeautifulSoup tag object, actual URL, and offer name.

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing the extracted offer data, or None if processing fails or the item is a duplicate.
        """
        offer_element = item['offer_element']
        actual_url = item['actual_url']
        offer_name = item['offer_name']

        # Check if the offer has already been processed.
        # The is_duplicate method in BaseCrawler uses self.key_fields to check against self.seen_items
        if self.is_duplicate(item):
            logging.info(f"Skipping already processed offer: {offer_name} ({actual_url})")
            return None

        try:
            import tempfile
            
            # Create a temporary HTML file to feed the offer element to the crawler.
            # This is done because the crawler expects a URL or file path.
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
                # Configure the crawler to extract content using an LLM strategy.
                offer_config = CrawlerRunConfig(
                    cache_mode=self.cache_mode,
                    session_id=f"{self.session_id}_excursion_offer",
                    extraction_strategy=self.llm_strategy,
                    scan_full_page=False,
                    wait_for_images=False,
                    remove_overlay_elements=True,
                    verbose=False,
                    page_timeout=PAGE_TIMEOUT,
                    delay_before_return_html=2.0,
                    only_text=False,
                    remove_forms=True,
                    prettiify=True,
                    ignore_body_visibility=True,
                    js_only=True,
                    magic=False
                )
                
                # Construct a file URL for the temporary HTML file.
                file_url = f"file://{temp_file_path}"
                # Run the crawler on the temporary file to extract data.
                offer_result = await self._run_crawler_with_retries(
                    file_url,
                    config=offer_config,
                    description="extracting excursion offer details from temporary file"
                )
                logging.debug(f"DEBUG: HTML snippet sent to LLM: {str(offer_element)}")
                logging.debug(f"DEBUG: Raw LLM extracted content: {offer_result.extracted_content}")
                if offer_result and offer_result.extracted_content:
                    extracted_content = self._parse_extracted_content(offer_result.extracted_content)
                    logging.debug(f"DEBUG: Extracted content: {extracted_content}")
                    logging.debug(f"DEBUG: Type of extracted_content: {type(extracted_content)}")
                    
                    if extracted_content is None:
                        logging.warning(f"Skipping offer due to unparseable LLM content: {offer_result.extracted_content}")
                        return None

                    # Handle cases where extracted content is a list or a single dictionary.
                    if isinstance(extracted_content, list):
                        for offer in extracted_content:
                            offer['link'] = actual_url # Assign link before checking completeness
                            logging.debug(f"DEBUG: Processing offer in list: {offer}")
                            logging.debug(f"DEBUG: Is complete? {self.is_complete(offer)}") # is_duplicate check will be handled by _append_item_to_csv
                            # Check for completeness before adding to all_items.
                            if self.is_complete(offer) and not offer.get('error', False):
                                if 'error' in offer: # Remove the 'error' key if present
                                    del offer['error']
                                self._append_item_to_csv(offer, self.filepath, self.model_class, self.key_fields)
                                logging.info(f"Successfully extracted and added new offer: {offer['name']}")
                                await asyncio.sleep(15) # Add delay after successful LLM call
                                return offer # Return after processing the first valid offer in the list
                            else:
                                logging.info(f"Skipping incomplete or error offer: {offer.get('name', 'N/A')}")
                    elif isinstance(extracted_content, dict):
                        extracted_content['link'] = actual_url # Assign link before checking completeness
                        logging.debug(f"DEBUG: Processing offer as dict: {extracted_content}")
                        logging.debug(f"DEBUG: Is duplicate? {self.is_duplicate(extracted_content)}")
                        logging.debug(f"DEBUG: Is complete? {self.is_complete(extracted_content)}")
                        if self.is_complete(extracted_content) and not extracted_content.get('error', False): # is_duplicate check will be handled by _append_item_to_csv
                            if 'error' in extracted_content: # Remove the 'error' key if present
                                del extracted_content['error']
                            
                            self._append_item_to_csv(extracted_content, self.filepath, self.model_class, self.key_fields)
                            logging.info(f"Successfully extracted and added new offer: {extracted_content['name']}")
                            await asyncio.sleep(15) # Add delay after successful LLM call
                        else:
                            logging.info(f"Skipping incomplete or error offer: {extracted_content.get('name', 'N/A')}")

            finally:
                # Ensure the temporary file is deleted after processing.
                if os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)

        except Exception as e:
            logging.error(f"Error processing offer: {str(e)}")

        return None
