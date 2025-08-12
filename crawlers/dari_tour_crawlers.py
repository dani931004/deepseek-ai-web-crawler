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
    """
    A crawler for Dari Tour website to extract general offer information.
    It extends the BaseCrawler to utilize shared crawling infrastructure.
    """
    def __init__(self, session_id: str, config: Type, model_class: Type):
        """
        Initializes the DariTourCrawler with session ID, config, and model class.
        """
        super().__init__(
            session_id=session_id,
            config=config,
            model_class=model_class,
            output_file_type='csv',
            required_keys=config.required_keys,
            key_fields=['name', 'link'] # Define key fields for duplicate checking.
        )
        self.llm_strategy = get_llm_strategy(model=model_class)

    async def get_urls_to_crawl(self, max_items: Optional[int] = None) -> List[Any]:
        """
        Fetches the initial page to identify offer elements to crawl.
        This method is responsible for navigating to the starting URL and extracting
        the initial set of items (e.g., links to individual offers).

        Returns:
            List[Any]: A list of BeautifulSoup tag objects, each representing an offer element.
        """
        url = f"{self.config.base_url}?page=1" # Construct the URL for the first page.
        print(f"Loading page 1...")
        
        # Configure the crawler for the initial page load.
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
        
        # Execute the crawl operation for the initial page.
        result = await self._run_crawler_with_retries(url, config=config, description="fetching initial page")
        
        # Check if HTML content was successfully retrieved.
        if not result or not result.html:
            print(f"Failed to load page: {url}")
            return []
            
        # Parse the HTML content using BeautifulSoup.
        soup = BeautifulSoup(result.html, 'html.parser')
        # Select all offer elements based on the configured CSS selector.
        offer_elements = soup.select(self.config.css_selector)
        
        # If no offer elements are found, log a message and return an empty list.
        if not offer_elements:
            print(f"No offer items found on {url}")
            return []
            
        print(f"Found {len(offer_elements)} offer items to process...")
        if max_items:
            return offer_elements[:max_items]
        return offer_elements

    async def process_item(self, item: Any) -> Optional[Dict[str, Any]]:
        """
        Processes a single offer item extracted from the initial page.
        This involves extracting the offer name and link, and then using an LLM strategy
        to extract structured data from the offer element.

        Args:
            item (Any): A BeautifulSoup tag object representing an offer element.

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing the extracted offer data, or None if processing fails or the item is a duplicate.
        """
        offer_element = item
        actual_url = None
        offer_name = ""
        # Extract the offer URL and name from the BeautifulSoup tag.
        if offer_element.name == 'a' and 'href' in offer_element.attrs:
            href = offer_element['href']
            # Handle both absolute and relative URLs.
            if href.startswith('http'):
                actual_url = href
            else:
                actual_url = urllib.parse.urljoin(self.config.base_url, href)
            # Clean up the URL by removing query parameters and fragments.
            actual_url = actual_url.split('?')[0].split('#')[0]
            
            # Extract the offer name using a specific CSS selector.
            name_el = offer_element.select_one(CSS_SELECTOR_OFFER_ITEM_TITLE)
            if name_el:
                offer_name = name_el.get_text(strip=True)

        # Normalize offer name and URL for duplicate checking.
        normalized_offer_name = offer_name.lower().strip()
        normalized_actual_url = actual_url.lower().strip() if actual_url else ""
        # Check if the offer has already been processed.
        if (normalized_offer_name, normalized_actual_url) in self.seen_items:
            print(f"Skipping already processed offer: {offer_name} ({actual_url})")
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
                
                # Construct a file URL for the temporary HTML file.
                file_url = f"file://{temp_file_path}"
                # Run the crawler on the temporary file to extract data.
                offer_result = await self._run_crawler_with_retries(
                    file_url,
                    config=offer_config,
                    description="extracting offer details from temporary file"
                )
                print(f"DEBUG: offer_result: {offer_result}")
                if offer_result and offer_result.extracted_content:
                    extracted_content = self._parse_extracted_content(offer_result.extracted_content)
                    print(f"DEBUG: Extracted content: {extracted_content}")
                    
                    # Handle cases where extracted content is a list or a single dictionary.
                    if isinstance(extracted_content, list):
                        for offer in extracted_content:
                            print(f"DEBUG: Processing offer in list: {offer}")
                            print(f"DEBUG: Is duplicate? {self.is_duplicate(offer)}")
                            print(f"DEBUG: Is complete? {self.is_complete(offer)}")
                            # Check for duplicates and completeness before adding to all_items.
                            if not self.is_duplicate(offer) and self.is_complete(offer):
                                offer['link'] = actual_url
                                self.seen_items.add(tuple(offer.get(k, '').lower().strip() for k in self.key_fields))
                                print(f"Successfully extracted and added new offer: {offer['name']}")
                                return offer
                            else:
                                print(f"Skipping duplicate or incomplete offer: {offer.get('name', 'N/A')}")
                    elif isinstance(extracted_content, dict):
                        print(f"DEBUG: Processing offer as dict: {extracted_content}")
                        print(f"DEBUG: Is duplicate? {self.is_duplicate(extracted_content)}")
                        print(f"DEBUG: Is complete? {self.is_complete(extracted_content)}")
                        if not self.is_duplicate(extracted_content) and self.is_complete(extracted_content):
                            extracted_content['link'] = actual_url
                            self.seen_items.add(tuple(extracted_content.get(k, '').lower().strip() for k in self.key_fields))
                            print(f"Successfully extracted and added new offer: {extracted_content['name']}")
                            return extracted_content
                        else:
                            print(f"Skipping duplicate or incomplete offer: {extracted_content.get('name', 'N/A')}")

            finally:
                # Ensure the temporary file is deleted after processing.
                if os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)

        except Exception as e:
            print(f"Error processing offer: {str(e)}")

        return None

    def _parse_extracted_content(self, content: Any) -> Any:
        """
        Parses the extracted content, attempting to load it as JSON if it's a string.

        Args:
            content (Any): The content extracted by the LLM.

        Returns:
            Any: The parsed content (e.g., dictionary, list) or the original content if not JSON.
        """
        # If the content is a string and looks like JSON, attempt to parse it.
        if isinstance(content, str) and (content.startswith('[') or content.startswith('{')):
            try:
                return json.loads(content)
            except json.JSONDecodeError:
                pass # If JSON decoding fails, treat as plain text.
        return content

    


class DariTourDetailedCrawler(BaseCrawler):
    """
    A crawler for Dari Tour website to extract detailed offer information.
    It extends the BaseCrawler to utilize shared crawling infrastructure.
    """
    def __init__(self, session_id: str, config: Type, model_class: Type):
        """
        Initializes the DariTourDetailedCrawler with session ID, config, and model class.
        """
        super().__init__(
            session_id=session_id,
            config=config,
            model_class=model_class,
            output_file_type='json',
            key_fields=['offer_name'] # Using 'offer_name' as key field for duplicate checking.
        )

    async def get_urls_to_crawl(self, max_items: Optional[int] = None) -> List[Any]:
        """
        Retrieves a list of URLs for detailed offers to crawl from a CSV file.
        It filters out offers that have already been processed.

        Returns:
            List[Any]: A list of dictionaries, each representing an offer to be processed.
        """
        # Construct the absolute path to the CSV file containing complete offers.
        csv_filepath = os.path.join(self.config.FILES_DIR, "complete_offers.csv")
        # Check if the CSV file exists before proceeding.
        if not os.path.exists(csv_filepath):
            print(f"Error: The file '{csv_filepath}' was not found.")
            return []

        # Read the complete offers from the CSV file into a Pandas DataFrame.
        offers_df = pd.read_csv(csv_filepath)
        offers_to_process = []
        # Iterate through each row (offer) in the DataFrame.
        for index, row in offers_df.iterrows():
            offer_name = row['name']
            # Generate a slug from the offer name for consistent file naming and duplicate checking.
            offer_slug = offer_name.lower().replace(' ', '-')
            # Check if this offer has already been processed.
            if offer_slug not in self.seen_items:
                offers_to_process.append(row)
            else:
                print(f"Skipping {offer_name} as it has already been processed.")
        
        # If no new offers are found, inform the user.
        if not offers_to_process:
            print("All detailed offers have already been processed.")
            return []

        return offers_to_process

    async def process_item(self, item: Any) -> Optional[Dict[str, Any]]:
        """
        Processes a single detailed offer item by crawling its page and extracting information.

        Args:
            item (Any): A dictionary containing 'link' and 'name' for the offer.

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing the extracted data and output path if successful, else None.
        """
        offer_url = item['link']
        offer_name = item['name']
        # Generate a slug for the offer name to use in the output filename.
        offer_slug = offer_name.lower().replace(' ', '-')
        output_path = self._get_detailed_item_filepath({"name": offer_name})

        # Check if skipping is enabled and the file already exists
        if self.config.skip_existing_detailed_offers and output_path and os.path.exists(output_path):
            # Check if the item is already marked as seen (from _load_existing_data_json)
            if offer_slug in self.seen_items:
                print(f"Skipping detailed offer processing for {offer_name} as it already exists and skip_existing_detailed_offers is True.")
                # Load the existing data and return it in the expected format
                existing_data = self._load_detailed_item_from_file(output_path)
                if existing_data:
                    return {"data": existing_data, "path": output_path}
                else:
                    print(f"Warning: Could not load existing data from {output_path}. Re-processing.")
            else:
                print(f"Warning: File {output_path} exists, but {offer_name} not in seen_items. Re-processing.")

        print(f"Processing offer: {offer_name}")
        print(f"URL: {offer_url}")
        print(f"DEBUG: Item received by process_item: {item}")
        print(f"DEBUG: Generated output_path: {output_path}")

        # Configure the crawler to fetch the detailed offer page.
        config = CrawlerRunConfig(
            url=offer_url,
            cache_mode=self.cache_mode,
        )

        # Execute the crawl operation.
        result = await self.crawler.arun(offer_url, config=config)

        # Check if HTML content was successfully retrieved.
        if result.html:
            # Parse the HTML content to extract detailed offer data.
            detailed_offer_data = await self._parse_detailed_offer(result.html)
            # Check if data was extracted and is complete before returning.
            if detailed_offer_data and self.is_complete(detailed_offer_data):
                return {"data": detailed_offer_data.model_dump(), "path": output_path}
            else:
                print(f"No detailed data extracted or incomplete for {offer_url}")
        else:
            print(f"No HTML content retrieved for {offer_url}")
        
        return None

    async def _parse_detailed_offer(self, html_content: str) -> Optional[OfferDetails]:
        """
        Parses the HTML content of a detailed offer page to extract specific information
        such as offer name, hotel details, program, included, and excluded services.

        Args:
            html_content (str): The HTML content of the page.

        Returns:
            Optional[OfferDetails]: An instance of OfferDetails with extracted data, or None if parsing fails.
        """
        # Initialize BeautifulSoup to parse the HTML content.
        soup = BeautifulSoup(html_content, 'html.parser')

        # Extract offer name.
        offer_name_element = soup.select_one(CSS_SELECTOR_DARI_TOUR_DETAIL_OFFER_NAME)
        offer_name = offer_name_element.get_text(strip=True) if offer_name_element else ""

        hotels_data = []
        # Find all hotel elements using the defined CSS selector.
        hotel_elements = soup.select(CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_ELEMENTS)
        for hotel_el in hotel_elements:
            # Extract hotel details: name, price, country, and link.
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
                # Construct the absolute URL for the hotel link.
                hotel_link = urllib.parse.urljoin("https://dari-tour.com/", relative_url)
            
            # If essential hotel data is present, create a Hotel object and add it to the list.
            if hotel_name and hotel_price and hotel_country:
                hotels_data.append(Hotel(name=hotel_name, price=hotel_price, country=hotel_country, link=hotel_link))

        # Extract program details.
        program_element = soup.select_one(CSS_SELECTOR_DARI_TOUR_DETAIL_PROGRAM)
        program = program_element.get_text(strip=True) if program_element else ""

        included_services = []
        # Extract included services by iterating through list items.
        included_elements = soup.select(CSS_SELECTOR_DARI_TOUR_DETAIL_INCLUDED_SERVICES)
        for li in included_elements:
            service = li.get_text(strip=True)
            if service:
                included_services.append(service)

        excluded_services = []
        # Extract excluded services by iterating through list items.
        excluded_elements = soup.select(CSS_SELECTOR_DARI_TOUR_DETAIL_EXCLUDED_SERVICES)
        for li in excluded_elements:
            service = li.get_text(strip=True)
            if service:
                excluded_services.append(service)

        # If the offer name is available, construct and return the OfferDetails object.
        if offer_name:
            return OfferDetails(
                offer_name=offer_name,
                hotels=hotels_data,
                program=program,
                included_services=included_services,
                excluded_services=excluded_services
            )
        return None

    