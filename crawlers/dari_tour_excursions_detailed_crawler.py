import os
import asyncio
import json
import logging
from typing import List, Dict, Any, Optional, Type
from bs4 import BeautifulSoup
import urllib.parse
import pandas as pd
import re

from utils.data_utils import slugify

from crawl4ai import CrawlerRunConfig, CacheMode
from config import (
    dari_tour_excursions_config,
    PAGE_TIMEOUT,
    TAB_LABEL_PROGRAM,
    TAB_LABEL_INCLUDED_SERVICES,
    TAB_LABEL_EXCLUDED_SERVICES,
    TAB_LABEL_ADDITIONAL_EXCURSIONS
)
from utils.scraper_utils.llm_strategy import get_llm_strategy
from .base_crawler import BaseCrawler
from utils.enums import OutputType
from models.dari_tour_excursions_detailed_models import DariTourExcursionDetailedOffer


class DariTourExcursionsDetailedCrawler(BaseCrawler):
    """
    A crawler for Dari Tour website to extract detailed excursion offer information.
    It extends the BaseCrawler to utilize shared crawling infrastructure.
    """
    def __init__(self, session_id: str, config: Type, model_class: Type, output_file_type: OutputType = OutputType.JSON):
        """
        Initializes the DariTourExcursionsDetailedCrawler with session ID, config, and model class.
        """
        super().__init__(
            session_id=session_id,
            config=config,
            model_class=model_class,
            output_file_type=OutputType.JSON,
            key_fields=["link"] # Using "link" as key field for duplicate checking for detailed offers.
        )
        self.llm_strategy = get_llm_strategy(model=model_class)

    async def get_urls_to_crawl(self, max_items: Optional[int] = None) -> List[Any]:
        """
        Retrieves a list of URLs for detailed offers to crawl from the complete_excursions.csv file.
        It filters out offers that have already been processed.

        Returns:
            List[Any]: A list of dictionaries, each representing an offer to be processed.
        """
        # Construct the absolute path to the CSV file containing complete offers.
        csv_filepath = os.path.join(self.config.FILES_DIR, "complete_offers.csv")
        # Check if the CSV file exists before proceeding.
        if not os.path.exists(csv_filepath):
            logging.error(f"Error: The file '{csv_filepath}' was not found. Run DariTourExcursionsCrawler first.")
            return []

        # Read the complete offers from the CSV file into a Pandas DataFrame.
        offers_df = pd.read_csv(csv_filepath)
        offers_to_process = []
        # Iterate through each row (offer) in the DataFrame.
        for index, row in offers_df.iterrows():
            offer_link = row['link']
            # Check if this offer has already been processed.
            if offer_link not in self.processed_urls_cache:
                offers_to_process.append(row.to_dict()) # Convert row to dict for consistency
            else:
                logging.info(f"Skipping {offer_link} as it has already been processed.")
        
        # If no new offers are found, inform the user.
        if not offers_to_process:
            logging.info("All detailed excursion offers have already been processed.")
            return []

        if max_items:
            return offers_to_process[:max_items]
        return offers_to_process

    async def process_item(self, item: Any, seen_items: set) -> Optional[Dict[str, Any]]:
        """
        Processes a single detailed excursion offer item by crawling its page and extracting information.

        Args:
            item (Any): A dictionary containing 'link' and 'name' for the offer.

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing the extracted data and output path if successful, else None.
        """
        offer_url = item['link']
        offer_name = item['name']
        # Generate a slug for the offer name to use in the output filename.
        offer_slug = slugify(offer_name)
        output_path = self._get_detailed_item_filepath({"name": offer_name}) # Use offer_name for slug

        # Check if the output file already exists
        if output_path and os.path.exists(output_path):
            logging.info(f"Skipping detailed offer processing for {offer_name} as its file already exists: {output_path}")
            self._add_processed_url(offer_url, offer_name) # Ensure it's marked as processed
            return None

        logging.info(f"Processing detailed excursion offer: {offer_name}")
        logging.info(f"URL: {offer_url}")
        logging.debug(f"DEBUG: Item received by process_item: {item}")
        logging.debug(f"DEBUG: Generated output_path: {output_path}")

        # Configure the crawler to fetch the detailed offer page.
        config = CrawlerRunConfig(
            url=offer_url,
            cache_mode=self.cache_mode,
            page_timeout=PAGE_TIMEOUT,
            wait_until="domcontentloaded",
        )

        # Execute the crawl operation.
        result = await self._run_crawler_with_retries(offer_url, config=config, description="fetching detailed excursion page")

        # Check if HTML content was successfully retrieved.
        if result.html:
            # Parse the HTML content to extract detailed offer data.
            detailed_offer_data = await self._parse_detailed_excursion_offer(result.html, offer_name)
            # Check if data was extracted and is complete before returning.
            if detailed_offer_data and self.is_complete(detailed_offer_data.model_dump()):
                self._save_data_json(detailed_offer_data.model_dump(), output_path)
                self._add_processed_url(offer_url, offer_name) # Mark as processed after successful save
                return {"data": detailed_offer_data.model_dump(), "path": output_path}
            else:
                logging.error(f"No detailed data extracted or incomplete for {offer_url}")
        else:
            logging.error(f"No HTML content retrieved for {offer_url}")
        
        return None

    async def _parse_detailed_excursion_offer(self, html_content: str, offer_name: str) -> Optional[DariTourExcursionDetailedOffer]:
        """
        Parses the HTML content of a detailed excursion offer page to extract specific information.

        Args:
            html_content (str): The HTML content of the page.
            offer_name (str): The name of the offer, passed from the general crawler.

        Returns:
            Optional[DariTourExcursionDetailedOffer]: An instance of DariTourExcursionDetailedOffer with extracted data, or None if parsing fails.
        """
        soup = BeautifulSoup(html_content, 'html.parser')

        # Dynamically find the aria-labelledby for each tab
        tab_map = {}
        tabs_list = soup.select_one("ul.resp-tabs-list.hor_1")
        if tabs_list:
            for li in tabs_list.find_all('li', class_='resp-tab-item'):
                a_tag = li.find('a')
                if a_tag and 'aria-controls' in li.attrs:
                    tab_map[a_tag.get_text(strip=True)] = li['aria-controls']

        program_content = ""
        included_services = []
        excluded_services = []
        additional_excursions_content = ""

        # Get Program content
        program_tab_id = tab_map.get(TAB_LABEL_PROGRAM)
        if program_tab_id:
            program_element = soup.select_one(f"div.resp-tab-content[aria-labelledby='{program_tab_id}']")
            program_content = str(program_element) if program_element else ""

            if program_element:
                # Extract included services
                included_heading = program_element.find('strong', string=lambda text: text and "1. В ЦЕНАТА СА ВКЛЮЧЕНИ:" in text)
                if included_heading:
                    ul_tag = included_heading.find_next('ul')
                    if ul_tag:
                        for li in ul_tag.find_all('li'):
                            service = li.get_text(strip=True)
                            if service:
                                included_services.append(service)

                # Extract excluded services
                excluded_heading = program_element.find('strong', string=lambda text: text and "2. В ЦЕНАТА НЕ СА ВКЛЮЧЕНИ:" in text)
                if excluded_heading:
                    ul_tag = excluded_heading.find_next('ul')
                    if ul_tag:
                        for li in ul_tag.find_all('li'):
                            service = li.get_text(strip=True)
                            if service:
                                excluded_services.append(service)

        # Get Additional Excursions content
        additional_excursions_tab_id = tab_map.get(TAB_LABEL_ADDITIONAL_EXCURSIONS)
        if additional_excursions_tab_id:
            additional_excursions_element = soup.select_one(f"div.resp-tab-content[aria-labelledby='{additional_excursions_tab_id}']")
            additional_excursions_content = additional_excursions_element.get_text(strip=True) if additional_excursions_element else ""

        if offer_name:
            return DariTourExcursionDetailedOffer(
                offer_name=offer_name,
                program=program_content,
                included_services=included_services,
                excluded_services=excluded_services,
                additional_excursions=additional_excursions_content
            )
        return None
