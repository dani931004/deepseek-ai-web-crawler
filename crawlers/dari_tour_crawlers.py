from crawl4ai import AsyncWebCrawler
from config import BASE_URL_DARI_TOUR_OFFERS, CSS_SELECTOR_DARI_TOUR_OFFERS, REQUIRED_KEYS_DARI_TOUR_OFFERS
from utils.data_utils import (
    save_offers_to_csv,
)
from utils.scraper_utils import (
    fetch_and_process_page,
    get_browser_config,
    get_llm_strategy,
)
import os
import asyncio
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

    # Start the web crawler context
    async with AsyncWebCrawler(config=browser_config) as crawler:
        # Fetch and process the single page of offers
        offers, _ = await fetch_and_process_page(
            crawler,
            page_number=1,  # Only one page to process
            base_url=BASE_URL_DARI_TOUR_OFFERS,
            css_selector=CSS_SELECTOR_DARI_TOUR_OFFERS,
            llm_strategy=llm_strategy,
            session_id=session_id,
            required_keys=REQUIRED_KEYS_DARI_TOUR_OFFERS,
            seen_names=seen_names,
        )

        if not offers:
            print("No offers were found on the page.")
            return

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