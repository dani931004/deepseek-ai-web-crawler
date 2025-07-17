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
                
            # Use BeautifulSoup to parse the HTML and get all offer items
            soup = BeautifulSoup(result.html, 'html.parser')
            offer_elements = soup.select(CSS_SELECTOR_DARI_TOUR_OFFERS)
            
            if not offer_elements:
                print(f"No offer items found on {url}")
                return
            print(f"Found {len(offer_elements)} offer items to process...")
            
            # Now process each offer item individually
            offers = []
            for i, offer_element in enumerate(offer_elements, 1):
                try:
                    print(f"Processing offer {i}/{len(offer_elements)}...")
                    
                    import tempfile
                    import os
                    import urllib.parse
                    
                    # Create a temporary file for the offer HTML
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as f:
                        # Create a complete HTML document with the offer
                        f.write(f"""
                        <!DOCTYPE html>
                        <html>
                        <head>
                            <meta charset="UTF-8">
                            <title>Offer {i}</title>
                        </head>
                        <body>
                            {str(offer_element)}
                        </body>
                        </html>
                        """)
                        temp_file_path = f.name
                    
                    try:
                        # Configure for processing a single offer
                        offer_config = CrawlerRunConfig(
                            cache_mode=CacheMode.BYPASS,
                            session_id=f"{session_id}_offer_{i}",
                            extraction_strategy=llm_strategy,
                            scan_full_page=False,
                            wait_for_images=False,
                            remove_overlay_elements=True,
                            verbose=False,  # Less verbose for individual offers
                            page_timeout=30000,  # 30 seconds per offer
                            delay_before_return_html=1.0,
                            only_text=False,
                            remove_forms=True,
                            prettiify=True,
                            ignore_body_visibility=True,
                            js_only=True,  # No need for full page load for local files
                            magic=True
                        )
                        
                        # Process the offer using file:// URL
                        file_url = f"file://{temp_file_path}"
                        offer_result = await crawler.arun(
                            file_url,
                            config=offer_config
                        )
                    
                        if offer_result and offer_result.extracted_content:
                            if isinstance(offer_result.extracted_content, list):
                                offers.extend(offer_result.extracted_content)
                            else:
                                offers.append(offer_result.extracted_content)
                        
                    except Exception as e:
                        print(f"Error processing offer {i}: {str(e)}")
                    finally:
                        # Clean up the temporary file
                        try:
                            if os.path.exists(temp_file_path):
                                os.unlink(temp_file_path)
                        except Exception as e:
                            print(f"Warning: Could not delete temporary file {temp_file_path}: {str(e)}")
                
                except Exception as e:
                    print(f"Error in offer processing loop: {str(e)}")
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