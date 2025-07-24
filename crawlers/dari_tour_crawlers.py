import os
import asyncio
import json
import time
import random
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Type
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, BrowserConfig
from config import get_browser_config

from bs4 import BeautifulSoup
from config import BASE_URL_DARI_TOUR_OFFERS, CSS_SELECTOR_DARI_TOUR_OFFERS, REQUIRED_KEYS_DARI_TOUR_OFFERS, CSS_SELECTOR_DARI_TOUR_DETAIL_OFFER_NAME, CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_ELEMENTS, CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_NAME, CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_PRICE, CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_COUNTRY, CSS_SELECTOR_DARI_TOUR_DETAIL_PROGRAM, CSS_SELECTOR_DARI_TOUR_DETAIL_INCLUDED_SERVICES, CSS_SELECTOR_DARI_TOUR_DETAIL_EXCLUDED_SERVICES, DARI_TOUR_DETAILS_DIR, CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_ITEM_LINK, CSS_SELECTOR_OFFER_ITEM_TITLE
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



async def crawl_dari_tour_offers():
    import os
    """
    Crawls offers from Dari Tour website and saves them to a CSV file.
    All offers are on a single page, so no pagination is needed.
    """
    filepath = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..",
        "dari_tour_files",
        "complete_offers.csv",
    )
    print(f"DEBUG: Saving CSV to: {filepath}")
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # Initialize configurations
    browser_config = get_browser_config()
    llm_strategy = get_llm_strategy(DariTourOffer)
    session_id = "offer_crawl_session"
    seen_names = set()
    all_offers = []
    processed_count = 0
    max_retries = 3  # Maximum number of retries for rate limiting
    offers = [] # Initialize offers list here

    # Load existing offers if the CSV file exists
    if os.path.exists(filepath):
        existing_offers_df = pd.read_csv(filepath)
        seen_names = set(existing_offers_df['name'].tolist())
        all_offers.extend(existing_offers_df.to_dict(orient='records'))
        print(f"Loaded {len(seen_names)} existing offers from {filepath}")

    try:
        # Initialize the crawler once
        crawler = AsyncWebCrawler(config=browser_config)
        await crawler.__aenter__()  # Manually enter the async context

        # Configure the crawler with the LLM strategy
        crawler.extraction_strategy = llm_strategy
        
        # Process the page
        url = f"{BASE_URL_DARI_TOUR_OFFERS}?page=1"
        print(f"Loading page 1...")
        
        try:
            # First, configure the crawler to get the page with all offers
            config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                session_id=f"{session_id}_page1",
                extraction_strategy=None,  # No extraction yet
                scan_full_page=False,
                wait_for_images=False,
                remove_overlay_elements=True,
                verbose=True,
                page_timeout=120000,
                delay_before_return_html=2.0,
                wait_until="domcontentloaded",
                wait_for=CSS_SELECTOR_DARI_TOUR_OFFERS,
                only_text=False,
                remove_forms=True,
                prettiify=True,
                ignore_body_visibility=True,
                js_only=False,
                magic=True,
                screenshot=False,
                pdf=False
            )
            
            # Get the page content
            result = await crawler.arun(url, config=config)
            
            if not result or not result.html:
                print(f"Failed to load page: {url}")
                return
                
            # Save the full HTML for debugging
            with open('debug_page.html', 'w', encoding='utf-8') as f:
                f.write(result.html)
                
            # Use BeautifulSoup to parse the HTML and get all offer items
            soup = BeautifulSoup(result.html, 'html.parser')
            offer_elements = soup.select(CSS_SELECTOR_DARI_TOUR_OFFERS)
            
            if not offer_elements:
                print(f"No offer items found on {url}")
                return
                
            print(f"Found {len(offer_elements)} offer items to process...")
            
            # Debug: Print the first offer's HTML structure
            if offer_elements:
                with open('debug_offer_element.html', 'w', encoding='utf-8') as f:
                    f.write(str(offer_elements[0]))
                print("Saved first offer's HTML structure to debug_offer_element.html")
            
            # Now process each offer item individually with rate limiting
            processed_count = 0
            max_retries = 3  # Maximum number of retries for rate limiting
            
            for i, offer_element in enumerate(offer_elements[:6], 1): # limit offers to 6
                # Extract the actual offer URL and name from the offer element
                actual_url = None
                offer_name = ""
                if offer_element.name == 'a' and 'href' in offer_element.attrs:
                    href = offer_element['href']
                    if href.startswith('http'):
                        actual_url = href
                    else:
                        actual_url = f"https://dari-tour.com/{href.lstrip('/')}"
                    actual_url = actual_url.split('?')[0].split('#')[0]
                    
                    # Attempt to get the offer name from a common selector within the offer_element
                    name_el = offer_element.select_one(CSS_SELECTOR_OFFER_ITEM_TITLE)
                    if name_el:
                        offer_name = name_el.get_text(strip=True)

                if offer_name and offer_name in seen_names:
                    print(f"Skipping already processed offer: {offer_name}")
                    continue

                try:
                    # Implement exponential backoff with jitter for rate limiting
                    if processed_count > 0:  # No delay for the first request
                        base_delay = 5.0  # Start with 5 seconds base delay
                        max_delay = 30.0  # Maximum delay of 30 seconds
                        jitter = random.uniform(0.8, 1.2)  # Add some randomness
                        
                        # Exponential backoff based on processed count
                        delay = min(base_delay * (2 ** (processed_count // 5)) * jitter, max_delay)
                        
                        # Add additional delay if we're processing many offers
                        if len(offer_elements) > 20:
                            delay = min(delay * 1.5, max_delay)
                            
                        print(f"Waiting {delay:.1f} seconds before processing next offer...")
                        await asyncio.sleep(delay)
                    
                    import tempfile
                    import os
                    import urllib.parse
                    
                    print(f"Processing offer {i}/{len(offer_elements)}...")
                    print(f"Debug - Extracted URL: {actual_url}")

                    # Create a temporary file for the offer HTML
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as f:
                        # Create a complete HTML document with the offer
                        f.write(f"""
                        <!DOCTYPE html>
                        <html>
                        <head>
                            <meta charset="UTF-8">
                            <title>Offer {i}</title>
                            <base href="https://dari-tour.com/">
                        </head>
                        <body>
                            {str(offer_element)}
                            <!-- Actual URL: {actual_url} -->
                        </body>
                        </html>
                        """)
                        temp_file_path = f.name
                    
                    try:
                        # Configure for processing a single offer with more conservative settings
                        offer_config = CrawlerRunConfig(
                            cache_mode=CacheMode.BYPASS,
                            session_id=f"{session_id}_offer_{i}",
                            extraction_strategy=llm_strategy,
                            scan_full_page=False,
                            wait_for_images=False,
                            remove_overlay_elements=True,
                            verbose=False,  # Less verbose for individual offers
                            page_timeout=120000,  # 120 seconds per offer (increased)
                            delay_before_return_html=2.0,  # Increased delay
                            only_text=False,
                            remove_forms=True,
                            prettiify=True,
                            ignore_body_visibility=True,
                            js_only=True,  # No need for full page load for local files
                            magic=False    # Disable magic to reduce complexity
                        )
                        
                        # Process the offer using file:// URL with rate limit handling
                        file_url = f"file://{temp_file_path}"
                        retry_count = 0
                        success = False
                        
                        while retry_count < max_retries and not success:
                            try:
                                offer_result = await crawler.arun(
                                    file_url,
                                    config=offer_config
                                )
                                success = True  # If we get here, the request was successful
                                
                            except Exception as e:
                                if "rate_limit" in str(e).lower() or "429" in str(e):
                                    retry_count += 1
                                    wait_time_to_use = (2 ** retry_count) + random.random()  # Default exponential backoff with jitter

                                    print(f"DEBUG: Exception string: {str(e)}") # Debug line

                                    # Attempt to extract precise wait time from Groq error message
                                    match = re.search(r"Please try again in (\d+\.?\d*)s", str(e))
                                    if match:
                                        try:
                                            precise_wait_time = float(match.group(1))
                                            wait_time_to_use = precise_wait_time # Use the precise time directly
                                            print(f"Groq rate limit requested wait time: {precise_wait_time:.3f}s")
                                        except ValueError:
                                            print("DEBUG: ValueError when parsing precise_wait_time.")
                                            pass # Fallback to default if parsing fails

                                    print(f"DEBUG: Sleeping for {wait_time_to_use:.3f} seconds.")
                                    await asyncio.sleep(wait_time_to_use)
                                else:
                                    raise  # Re-raise if it's not a rate limit error
                        
                        if not success:
                            print(f"Failed to process offer after {max_retries} retries due to rate limits. Skipping...")
                            continue
                    
                        if offer_result and offer_result.extracted_content:
                            try:
                                # Check if the content is a string that looks like JSON
                                if isinstance(offer_result.extracted_content, str) and \
                                   (offer_result.extracted_content.startswith('[') or \
                                    offer_result.extracted_content.startswith('{')):
                                    try:
                                        parsed_content = json.loads(offer_result.extracted_content)
                                        print("Successfully parsed JSON content")
                                        extracted_content = parsed_content
                                    except json.JSONDecodeError as e:
                                        print(f"Failed to parse JSON content: {e}")
                                        extracted_content = offer_result.extracted_content
                                else:
                                    extracted_content = offer_result.extracted_content
                                
                                print("\nDebug - Extracted content type:", type(extracted_content))
                                print("Debug - Required keys:", REQUIRED_KEYS_DARI_TOUR_OFFERS)
                                
                                if isinstance(extracted_content, list):
                                    valid_offers = []
                                    for i, offer in enumerate(extracted_content):
                                        if not isinstance(offer, dict):
                                            print(f"Debug - Offer {i} is not a dictionary:", offer)
                                            continue
                                        missing_keys = [key for key in REQUIRED_KEYS_DARI_TOUR_OFFERS if key not in offer]
                                        if missing_keys:
                                            print(f"Debug - Offer {i} is missing keys: {missing_keys}")
                                            print(f"Debug - Available keys: {list(offer.keys())}")
                                        else:
                                            valid_offers.append(offer)
                                    
                                    if valid_offers:
                                        # Add the actual URL to each offer
                                        for offer in valid_offers:
                                            if actual_url:
                                                offer['link'] = actual_url
                                                print(f"Debug - Set link to: {actual_url}")
                                            else:
                                                print("Warning: No URL found for offer")
                                        
                                        # Check if the offer is already in seen_names before adding
                                        for offer_item in valid_offers:
                                            if offer_item['name'] not in seen_names:
                                                offers.append(offer_item)
                                                seen_names.add(offer_item['name'])
                                                processed_count += 1
                                                print(f"Successfully extracted and added new offer: {offer_item['name']}")
                                            else:
                                                print(f"Skipping already processed offer: {offer_item['name']}")
                                    else:
                                        print("No valid offers found in the list after validation")
                                
                                elif isinstance(extracted_content, dict):
                                    missing_keys = [key for key in REQUIRED_KEYS_DARI_TOUR_OFFERS if key not in extracted_content]
                                    if missing_keys:
                                        print(f"Debug - Single offer is missing keys: {missing_keys}")
                                        print(f"Debug - Available keys: {list(extracted_content.keys())}")
                                    else:
                                        if extracted_content['name'] not in seen_names:
                                            offers.append(extracted_content)
                                            seen_names.add(extracted_content['name'])
                                            processed_count += 1
                                            print(f"Successfully extracted and added new offer: {extracted_content['name']}")
                                        else:
                                            print(f"Skipping already processed offer: {extracted_content['name']}")
                                
                                else:
                                    print(f"Debug - Unexpected extracted content type: {type(extracted_content)}")
                                    print(f"Content: {extracted_content}")
                            
                            except Exception as e:
                                print(f"Error processing extracted content: {str(e)}")
                                print(f"Content type: {type(offer_result.extracted_content)}")
                                print(f"Content: {offer_result.extracted_content}")
                        
                    except Exception as e:
                        print(f"Error processing offer {i}: {str(e)}")
                        
                        # More aggressive backoff on error
                        error_base_delay = 10.0  # Start with 10 seconds on error
                        error_jitter = random.uniform(0.9, 1.1)
                        error_delay = min(error_base_delay * (2 ** (i // 3)) * error_jitter, 60.0)  # Cap at 60 seconds
                        
                        print(f"Waiting {error_delay:.1f} seconds after error...")
                        await asyncio.sleep(error_delay)
                        
                        # If we've had multiple errors, consider taking a longer break
                        if i > 0 and i % 5 == 0:
                            long_break = random.uniform(30, 60)
                            print(f"Taking a longer break of {long_break:.1f} seconds after multiple errors...")
                            await asyncio.sleep(long_break)
                    finally:
                        # Clean up the temporary file
                        try:
                            if 'temp_file_path' in locals() and os.path.exists(temp_file_path):
                                os.unlink(temp_file_path)
                        except Exception as e:
                            print(f"Warning: Could not delete temporary file: {str(e)}")
                
                except Exception as e:
                    print(f"Critical error in offer processing loop: {str(e)}")
                    
                    # Even longer backoff for critical errors
                    critical_delay = random.uniform(15, 30)
                    print(f"Critical error encountered. Waiting {critical_delay:.1f} seconds before continuing...")
                    await asyncio.sleep(critical_delay)
                    
                    # If we've had multiple critical errors, consider aborting
                    if i > 10 and i % 5 == 0:
                        print("Multiple critical errors encountered. Consider checking your API key and rate limits.")
                        print("You may want to wait before trying again or upgrade your Groq API plan.")
                        break
                        
                    continue
            
            if not offers:
                print("No valid offers could be extracted")
                return
                
            print(f"Successfully processed {len(offers)} offers")
            all_offers.extend(offers)
        except Exception as e:
            print(f"Error during extraction: {str(e)}")
            return
        
        if not all_offers:
            print("No offers were found on the page.")
            return
            
    except Exception as e:
        print(f"An error occurred during crawling: {str(e)}")
        return
        
    finally:
        # Ensure the crawler is properly closed
        if 'crawler' in locals():
            await crawler.__aexit__(None, None, None)

    # Save the collected offers to a CSV file
    filepath = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..",
        "dari_tour_files",
        "complete_offers.csv",
    )
    print(f"DEBUG: Saving CSV to: {filepath}")
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    if all_offers:
        save_offers_to_csv(all_offers, filepath, DariTourOffer)
        print(f"Saved {len(all_offers)} offers to '{filepath}'.")
    else:
        print("No offers were found during the crawl.")

    # Display usage statistics for the LLM strategy
    llm_strategy.show_usage()


async def parse_detailed_offer(html_content: str) -> Optional[OfferDetails]:
    soup = BeautifulSoup(html_content, 'html.parser')

    offer_name_element = soup.select_one(CSS_SELECTOR_DARI_TOUR_DETAIL_OFFER_NAME)
    offer_name = offer_name_element.get_text(strip=True) if offer_name_element else ""

    hotels_data = []
    hotel_elements = soup.select(CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_ELEMENTS)
    for hotel_el in hotel_elements:
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
            hotel_link = urllib.parse.urljoin("https://dari-tour.com/", relative_url)
        
        if hotel_name and hotel_price and hotel_country:
            hotels_data.append(Hotel(name=hotel_name, price=hotel_price, country=hotel_country, link=hotel_link))

    program_element = soup.select_one(CSS_SELECTOR_DARI_TOUR_DETAIL_PROGRAM)
    program = program_element.get_text(strip=True) if program_element else ""

    included_services = []
    included_elements = soup.select(CSS_SELECTOR_DARI_TOUR_DETAIL_INCLUDED_SERVICES)
    for li in included_elements:
        service = li.get_text(strip=True)
        if service:
            included_services.append(service)

    excluded_services = []
    excluded_elements = soup.select(CSS_SELECTOR_DARI_TOUR_DETAIL_EXCLUDED_SERVICES)
    for li in excluded_elements:
        service = li.get_text(strip=True)
        if service:
            excluded_services.append(service)

    if offer_name:
        return OfferDetails(
            offer_name=offer_name,
            hotels=hotels_data,
            program=program,
            included_services=included_services,
            excluded_services=excluded_services
        )
    return None

async def crawl_dari_tour_detailed_offers():
    csv_filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dari_tour_files', 'complete_offers.csv'))
    print(f"DEBUG: Attempting to read CSV from: {csv_filepath}")
    if not os.path.exists(csv_filepath):
        print(f"Error: The file '{csv_filepath}' was not found. Please ensure the initial offer crawl has been run successfully.")
        return

    # Read the CSV file with offers
    offers_df = pd.read_csv(csv_filepath)

    # Ensure the output directory exists
    output_dir = DARI_TOUR_DETAILS_DIR
    os.makedirs(output_dir, exist_ok=True)

    # Get a list of already processed detailed offer slugs
    processed_slugs = set()
    if os.path.exists(output_dir):
        for filename in os.listdir(output_dir):
            if filename.endswith(".json"):
                processed_slugs.add(filename.replace(".json", ""))

    # Filter out offers that have already been processed
    offers_to_process = []
    for index, row in offers_df.iterrows():
        offer_name = row['name']
        offer_slug = offer_name.lower().replace(' ', '-')
        if offer_slug not in processed_slugs:
            offers_to_process.append(row)
        else:
            print(f"Skipping {offer_name} as it has already been processed.")

    if not offers_to_process:
        print("All detailed offers have already been processed.")
        return

    browser_config = get_browser_config()
    browser_config.headers = {
        "Accept-Language": "bg-BG,bg;q=0.9"
    }
    async with AsyncWebCrawler(config=browser_config) as crawler:

        # Process each offer
        for row in offers_to_process:
            offer_url = row['link']
            offer_name = row['name']
            # Create a slug from the offer name to use as a filename
            offer_slug = offer_name.lower().replace(' ', '-')

            # Define the path for the output JSON file
            output_path = os.path.join(output_dir, f"{offer_slug}.json")

            print(f"Processing offer: {offer_name}")
            print(f"URL: {offer_url}")

            # Configure the crawler run
            config = CrawlerRunConfig(
                url=offer_url,
                cache_mode=CacheMode.BYPASS,
                )

            # Run the crawler
            result = await crawler.arun(offer_url, config=config)

            # Manually parse the HTML content
            if result.html:
                detailed_offer_data = await parse_detailed_offer(result.html)
                if detailed_offer_data:
                    save_to_json(detailed_offer_data.model_dump(), output_path)
                    print(f"Saved detailed offer to {output_path}")
                else:
                    print(f"No detailed data extracted for {offer_url}")
            else:
                print(f"No HTML content retrieved for {offer_url}")