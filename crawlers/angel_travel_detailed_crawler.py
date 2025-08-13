import os
import asyncio
import json
import time
import random
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Type, Tuple
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig


from bs4 import BeautifulSoup
from config import angel_travel_config, CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_PROGRAM, CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_INCLUDED_SERVICES, CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_EXCLUDED_SERVICES, CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_ELEMENTS, CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_NAME, CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_PRICE, CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_COUNTRY, CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_ITEM_LINK
from utils.data_utils import save_to_json, slugify
import urllib.parse
import re
from models.angel_travel_models import AngelTravelOffer
from models.angel_travel_detailed_models import AngelTravelDetailedOffer # Assuming a new detailed model
import pandas as pd
from .base_crawler import BaseCrawler
from utils.enums import OutputType


class AngelTravelDetailedCrawler(BaseCrawler):
    """
    A crawler specifically designed to extract detailed offer information from Angel Travel.
    It extends the BaseCrawler to leverage common crawling functionalities.
    """
    def __init__(self, session_id: str, config: Type, model_class: Type, output_file_type: OutputType = OutputType.JSON):
        """
        Initializes the AngelTravelDetailedCrawler with a specific session ID and key fields.
        Sets up the configuration and output directory for detailed offers.
        """
        super().__init__(
            session_id=session_id,
            config=config,
            model_class=model_class,
            output_file_type=OutputType.JSON,
            key_fields=['offer_name'], # Using 'offer_name' as key field for duplicate checking
            
        )

    async def get_urls_to_crawl(self, max_items: Optional[int] = None) -> List[Any]:
        """
        Retrieves a list of URLs for detailed offers to crawl.
        It reads from a CSV file containing complete offers and filters for detailed offer links.
        Only offers not yet processed (based on 'offer_name' slug) are included.

        Returns:
            List[Any]: A list of dictionaries, each representing an offer to be processed.
        """
        # Construct the absolute path to the CSV file containing complete offers.
        csv_filepath = os.path.join(self.config.FILES_DIR, 'complete_offers.csv')
        # Check if the CSV file exists before proceeding, with retries.
        max_retries = 5
        retry_delay = 1  # seconds
        for i in range(max_retries):
            if os.path.exists(csv_filepath):
                break
            logging.info(f"Waiting for {csv_filepath} to appear... Attempt {i+1}/{max_retries}")
            await asyncio.sleep(retry_delay)
        else: # This else block executes if the loop completes without a 'break'
            logging.error(f"Error: The file '{csv_filepath}' was not found after multiple attempts.")
            return []

        # Read the complete offers from the CSV file into a Pandas DataFrame.
        offers_df = pd.read_csv(csv_filepath)
        offers_to_process = []
        # Iterate through each row (offer) in the DataFrame.
        for index, row in offers_df.iterrows():
            offer_name = str(row['title']) if pd.notna(row['title']) else ""
            offer_link = str(row['link']) if pd.notna(row['link']) else ""
            main_page_link = str(row['main_page_link']) if pd.notna(row['main_page_link']) else ""
            # Only process links that are identified as detailed offer pages.
            # if "programa.php" in offer_link: # Only process detailed offer links
            # Generate a slug from the offer name for consistent file naming and duplicate checking.
            offer_slug = slugify(offer_name)
            # Check if this offer has already been processed in previous runs.
            if offer_slug not in self.seen_items:
                offers_to_process.append({'title': offer_name, 'link': offer_link, 'main_page_link': main_page_link})
            else:
                logging.info(f"Skipping {offer_name} as it has already been processed.")
        # If no new offers are found, inform the user.
        if not offers_to_process:
            logging.info("All detailed offers have already been processed.")
            return []

        if max_items:
            return offers_to_process[:max_items]
        return offers_to_process

    async def process_item(self, item: Any, seen_items: set) -> Optional[Dict[str, Any]]:
        """
        Processes a single offer item by crawling its detailed page and extracting information.

        Args:
            item (Any): A dictionary containing 'link', 'title', and 'main_page_link' for the offer.

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing the extracted data and output path if successful, else None.
        """
        offer_name = item['title']
        main_page_url = item['main_page_link']
        programa_php_url = item['link']

        offer_slug = slugify(offer_name)
        output_path = self._get_detailed_item_filepath({"name": offer_name})

        # Check if the output file already exists
        if output_path and os.path.exists(output_path):
            logging.info(f"Skipping detailed offer processing for {offer_name} as its file already exists: {output_path}")
            return None

        logging.info(f"Processing offer: {offer_name}")
        logging.info(f"Main Page URL: {main_page_url}")
        logging.info(f"Programa.php URL: {programa_php_url}")

        main_page_html, program_page_html, tabs_page_html = await self._get_main_and_program_html(main_page_url, programa_php_url)

        if not main_page_html or not program_page_html:
            logging.error(f"Failed to get required HTML content for {offer_name}")
            return None

        logging.debug(f"DEBUG: Length of main_page_html: {len(main_page_html)}")
        logging.debug(f"DEBUG: Length of program_page_html: {len(program_page_html)}")
        if tabs_page_html:
            logging.debug(f"DEBUG: Length of tabs_page_html: {len(tabs_page_html)}")

        # Save the detailed page HTML for debugging
        with open(f"/home/dani/Desktop/Crawl4AI/data/debug/debug_program_page_html_{offer_slug}.html", "w", encoding="utf-8") as f:
            f.write(program_page_html)
        with open(f"/home/dani/Desktop/Crawl4AI/data/debug/debug_main_page_html_{offer_slug}.html", "w", encoding="utf-8") as f:
            f.write(main_page_html)
        if tabs_page_html:
            with open(f"/home/dani/Desktop/Crawl4AI/data/debug/debug_tabs_page_html_{offer_slug}.html", "w", encoding="utf-8") as f:
                f.write(tabs_page_html)

        detailed_offer_data = await self._parse_detailed_offer_content(main_page_html, program_page_html, tabs_page_html, offer_name, main_page_url)
        if detailed_offer_data:
            return {"data": detailed_offer_data.model_dump(), "path": output_path}
        else:
            logging.error(f"No detailed data extracted or incomplete for {main_page_url}")
        
        return None

    async def _get_main_and_program_html(self, main_page_url: str, programa_php_url: str) -> Optional[Tuple[str, str, str]]:
        """
        Navigates to the main page, extracts the iframe src, and then crawls the iframe src to get the program HTML.
        Returns a tuple of (main_page_html, program_page_html, detailed_program_page_html).
        """
        try:
            # Step 1: Crawl the main page to get its HTML
            main_page_config = CrawlerRunConfig(
                url=main_page_url,
                verbose=True,
                wait_until="networkidle"
            )
            main_page_result = await self.crawler.arun(main_page_url, config=main_page_config)

            if not main_page_result or not main_page_result.html:
                logging.error(f"Failed to get main page HTML for {main_page_url}")
                return None, None, None

            main_page_html = main_page_result.html
            main_page_soup = BeautifulSoup(main_page_html, 'html.parser')

            # Step 2: Find the first iframe and extract its src attribute (programa.php - list of offers)
            iframe_tag = main_page_soup.find('iframe', src=re.compile(r'iframe\.peakview\.bg'))
            if not iframe_tag or not iframe_tag.get('src'):
                logging.error(f"Could not find first iframe with peakview.bg src on {main_page_url}")
                return None, None, None

            iframe_src = iframe_tag['src']
            # Ensure the iframe_src is a complete URL
            if iframe_src.startswith('//'):
                iframe_src = "https:" + iframe_src
            elif iframe_src.startswith('/'):
                iframe_src = urllib.parse.urljoin(main_page_url, iframe_src)

            # Step 3: Crawl the first iframe_src to get the HTML of the list of offers
            iframe_config = CrawlerRunConfig(
                url=iframe_src,
                verbose=True,
                wait_until="networkidle"
            )
            iframe_result = await self.crawler.arun(iframe_src, config=iframe_config)
            await asyncio.sleep(3)

            if not iframe_result or not iframe_result.html:
                logging.error(f"Failed to get HTML from first iframe src (list of offers): {iframe_src}")
                return None, None, None

            program_page_html = iframe_result.html # This is the HTML of the list of offers
            program_page_soup = BeautifulSoup(program_page_html, 'html.parser')

            # Step 4: Find the link to the detailed programa.php within the list of offers
            # The link is within a div with class 'but-wrap' and has class 'but'
            detailed_offer_link_tag = program_page_soup.find('a', class_='but')
            if not detailed_offer_link_tag or not detailed_offer_link_tag.get('href'):
                logging.warning(f"Could not find detailed offer link within {iframe_src}")
                return main_page_html, program_page_html, None

            detailed_programa_php_url = detailed_offer_link_tag['href']
            # Ensure the detailed_programa_php_url is a complete URL
            if not detailed_programa_php_url.startswith('http://') and not detailed_programa_php_url.startswith('https://'):
                if detailed_programa_php_url.startswith('//'):
                    detailed_programa_php_url = "https:" + detailed_programa_php_url
                else:
                    # The base URL for the detailed programa.php is the first iframe's URL
                    detailed_programa_php_url = urllib.parse.urljoin(iframe_src, detailed_programa_php_url)

            # Step 5: Crawl the detailed programa.php URL to get the HTML containing the tabs
            detailed_program_config = CrawlerRunConfig(
                url=detailed_programa_php_url,
                verbose=True,
                wait_until="networkidle"
            )
            detailed_program_result = await self.crawler.arun(detailed_programa_php_url, config=detailed_program_config)
            await asyncio.sleep(3) # Add a delay for the detailed program page as well

            if detailed_program_result and detailed_program_result.html:
                detailed_program_page_html = detailed_program_result.html
                return main_page_html, program_page_html, detailed_program_page_html
            else:
                logging.error(f"Failed to get detailed program page HTML from {detailed_programa_php_url}")
                return main_page_html, program_page_html, None

        except Exception as e:
            logging.error(f"Error in _get_main_and_program_html: {e}")
            return None, None, None

    async def _parse_detailed_offer_content(self, main_page_html: str, program_page_html: str, tabs_page_html: str, offer_name: str, detailed_offer_link: Optional[str]) -> Optional[AngelTravelDetailedOffer]:
        """
        Parses the HTML content of a detailed offer page to extract specific information.

        Args:
            html_content (str): The HTML content of the page.
            offer_name (str): The name of the offer.
            detailed_offer_link (Optional[str]): The URL of the detailed offer page.

        Returns:
            Optional[AngelTravelDetailedOffer]: An instance of AngelTravelDetailedOffer with extracted data, or None if parsing fails.
        """
        # Initialize BeautifulSoup to parse the HTML content.
        main_page_soup = BeautifulSoup(main_page_html, 'html.parser')
        program_page_soup = BeautifulSoup(program_page_html, 'html.parser')
        tabs_page_soup = BeautifulSoup(tabs_page_html, 'html.parser')

        program = ""
        included_services = []
        excluded_services = []

        # Attempt to extract the program details using a predefined CSS selector.
        program_element = program_page_soup.find('div', class_='ofcontent')
        program_text_parts = []
        if program_element:
            for content in program_element.contents:
                if isinstance(content, str):  # It\\'s a text node
                    stripped_text = content.strip()
                    if stripped_text:
                        program_text_parts.append(stripped_text)
                elif content.name == 'font': # Handle font tags like dates and prices
                    stripped_text = content.get_text(strip=True)
                    if stripped_text:
                        program_text_parts.append(stripped_text)
                elif content.name == 'br': # Add newline for <br> tags
                    program_text_parts.append(os.linesep)
                elif content.name == 'div' and 'but-wrap' in content.get('class', []):
                    # Skip the button wrap div
                    continue
            program = " ".join(program_text_parts).strip()
            # Replace multiple newlines with a single one for cleaner output
            program = re.sub(r'\\n+', '\\n', program)

        # Find the main tab container
        parent_horizontal_tab = tabs_page_soup.find('div', id='parentHorizontalTab')

        if parent_horizontal_tab:
            # Find the "ЦЕНАТА ВКЛЮЧВА" content
            included_content_div = tabs_page_soup.find('div', attrs={'aria-labelledby': 'hor_1_tab_item-1'})
            if included_content_div:
                for li in included_content_div.find_all('li'):
                    text = li.get_text(strip=True)
                    if text:
                        included_services.append(text)
                # The content seems to be directly in <li> tags, but keeping <p> for robustness
                for p in included_content_div.find_all('p'):
                    text = p.get_text(strip=True)
                    if text:
                        included_services.append(text)

            # Find the "ЦЕНАТА НЕ ВКЛЮЧВА" content
            excluded_content_div = tabs_page_soup.find('div', attrs={'aria-labelledby': 'hor_1_tab_item-2'})
            if excluded_content_div:
                for li in excluded_content_div.find_all('li'):
                    text = li.get_text(strip=True)
                    if text:
                        excluded_services.append(text)
                # The content seems to be directly in <li> tags, but keeping <p> for robustness
                for p in excluded_content_div.find_all('p'):
                    text = p.get_text(strip=True)
                    if text:
                        excluded_services.append(text)

        # If an offer name is provided, create and return an AngelTravelDetailedOffer object.
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

        # If an offer name is provided, create and return an AngelTravelDetailedOffer object.
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

    def save_data(self):
        """
        Saves the collected detailed offer data to JSON files.
        Iterates through all processed items and calls the internal save method.
        """
        # Loop through each item that has been processed and is ready to be saved.
        for item in self.all_items:
            # Call the helper method to save the data to a JSON file at the specified path.
            self._save_data_json(item["data"], item["path"])

async def crawl_angel_travel_detailed_offers():
    """
    Asynchronous function to initiate the crawling process for Angel Travel detailed offers.
    This function creates an instance of AngelTravelDetailedCrawler and starts its crawling operation.
    """
    # Create an instance of the AngelTravelDetailedCrawler.
    crawler = AngelTravelDetailedCrawler()
    # Start the crawling process. The 'crawl' method orchestrates fetching and processing.
    await crawler.crawl()