from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
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
            # Use the crawler's arun method with the CSS selector and extraction strategy
            result = await crawler.arun(
                url=url,
                config=CrawlerRunConfig(
                    cache_mode=CacheMode.BYPASS,
                    session_id=f"{session_id}_page1",
                    css_selector=CSS_SELECTOR_DARI_TOUR_OFFERS,
                    extraction_strategy=llm_strategy
                ),
            )
            
            if not result.success:
                print(f"Error fetching page: {result.error_message}")
                return
                
            # The result should already contain the extracted data
            if hasattr(result, 'extracted_content') and result.extracted_content:
                # Convert the extracted content to a list of offers
                if isinstance(result.extracted_content, list):
                    all_offers.extend(result.extracted_content)
                else:
                    all_offers.append(result.extracted_content)
            else:
                print("No content was extracted from the page.")
                
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
    all_offers = offers
    if all_offers:
        save_offers_to_csv(all_offers, filepath, DariTourOffer)
        print(f"Saved {len(all_offers)} offers to '{filepath}'.")
    else:
        print("No offers were found during the crawl.")

    # Display usage statistics for the LLM strategy
    llm_strategy.show_usage()