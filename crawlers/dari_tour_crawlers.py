import asyncio
import json
import os
import time
import random
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Type
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from bs4 import BeautifulSoup
from config import BASE_URL_DARI_TOUR_OFFERS, CSS_SELECTOR_DARI_TOUR_OFFERS, REQUIRED_KEYS_DARI_TOUR_OFFERS
from utils.data_utils import (
    save_offers_to_csv,
)
from utils.scraper_utils import (
    fetch_and_process_page,
    get_browser_config,
    get_llm_strategy,
    process_page_content,
)
import os
from models.dari_tour_models import DariTourOffer



async def crawl_dari_tour_offers():
    """
    Crawls offers from Dari Tour website and saves them to a CSV file.
    All offers are on a single page, so no pagination is needed.
    """
    # Initialize configurations
    browser_config = get_browser_config()
    llm_strategy = get_llm_strategy(DariTourOffer)
    session_id = "offer_crawl_session"
    seen_names = set()
    all_offers = []

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
            offers = []
            total_offers = min(5, len(offer_elements))  # Only process 3 offers for testing
            processed_count = 0
            max_retries = 3  # Maximum number of retries for rate limiting
            
            for i, offer_element in enumerate(offer_elements[:total_offers], 1):
                try:
                    print(f"Processing offer {i}/{total_offers}...")
                    
                    # Implement exponential backoff with jitter for rate limiting
                    if processed_count > 0:  # No delay for the first request
                        base_delay = 5.0  # Start with 5 seconds base delay
                        max_delay = 30.0  # Maximum delay of 30 seconds
                        jitter = random.uniform(0.8, 1.2)  # Add some randomness
                        
                        # Exponential backoff based on processed count
                        delay = min(base_delay * (2 ** (processed_count // 5)) * jitter, max_delay)
                        
                        # Add additional delay if we're processing many offers
                        if total_offers > 20:
                            delay = min(delay * 1.5, max_delay)
                            
                        print(f"Waiting {delay:.1f} seconds before processing next offer...")
                        await asyncio.sleep(delay)
                    
                    import tempfile
                    import os
                    import urllib.parse
                    
                    # Extract the actual offer URL from the offer element
                    # The offer element itself is an <a> tag with class 'offer-item'
                    actual_url = None
                    if offer_element.name == 'a' and 'href' in offer_element.attrs:
                        href = offer_element['href']
                        if href.startswith('http'):
                            actual_url = href
                        else:
                            # Handle relative URLs
                            actual_url = f"https://dari-tour.com/{href.lstrip('/')}"
                        
                        # Clean up the URL by removing any query parameters or fragments
                        actual_url = actual_url.split('?')[0].split('#')[0]
                        print(f"Debug - Extracted URL: {actual_url}")
                    else:
                        print("Warning: Could not find URL in offer element")
                    
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
                                    wait_time = (2 ** retry_count) + random.random()  # Exponential backoff with jitter
                                    print(f"Rate limit hit. Waiting {wait_time:.1f} seconds before retry {retry_count}/{max_retries}...")
                                    await asyncio.sleep(wait_time)
                                else:
                                    raise  # Re-raise if it's not a rate limit error
                        
                        if not success:
                            print(f"Failed to process offer after {max_retries} retries due to rate limits. Skipping...")
                            continue
                    
                        if offer_result and offer_result.extracted_content:
                            try:
                                # Check if the content is a string that looks like JSON
                                if isinstance(offer_result.extracted_content, str) and \
                                   (offer_result.extracted_content.startswith('[') or 
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
                                        
                                        offers.extend(valid_offers)
                                        processed_count += 1
                                        print(f"Successfully extracted {len(valid_offers)} valid offers from this item")
                                    else:
                                        print("No valid offers found in the list after validation")
                                
                                elif isinstance(extracted_content, dict):
                                    missing_keys = [key for key in REQUIRED_KEYS_DARI_TOUR_OFFERS if key not in extracted_content]
                                    if missing_keys:
                                        print(f"Debug - Single offer is missing keys: {missing_keys}")
                                        print(f"Debug - Available keys: {list(extracted_content.keys())}")
                                    else:
                                        offers.append(extracted_content)
                                        processed_count += 1
                                        print("Successfully extracted 1 valid offer")
                                
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
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "dari_tour_files",
        "complete_offers.csv",
    )
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    if all_offers:
        save_offers_to_csv(all_offers, filepath, DariTourOffer)
        print(f"Saved {len(all_offers)} offers to '{filepath}'.")
    else:
        print("No offers were found during the crawl.")

    # Display usage statistics for the LLM strategy
    llm_strategy.show_usage()