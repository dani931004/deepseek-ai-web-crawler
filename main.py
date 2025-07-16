#!/home/dani/Desktop/Crawl4AI/deepseek-crawler/venv/bin/python3
import asyncio

from crawl4ai import AsyncWebCrawler
from dotenv import load_dotenv

from config import BASE_URL, CSS_SELECTOR, REQUIRED_KEYS
from utils.data_utils import (
    save_offers_to_csv,
)
from utils.scraper_utils import (
    fetch_and_process_page,
    get_browser_config,
    get_llm_strategy,
)

load_dotenv()


async def crawl_offers():
    """
    Main function to crawl offer data from the website.
    """
    # Initialize configurations
    browser_config = get_browser_config()
    llm_strategy = get_llm_strategy()
    session_id = "offer_crawl_session"

    # Initialize state variables
    page_number = 1
    all_offers = []
    seen_names = set()

    # Start the web crawler context
    async with AsyncWebCrawler(config=browser_config) as crawler:
        while True:
            # Fetch and process data from the current page
            offers, no_results_found = await fetch_and_process_page(
                crawler,
                page_number,
                BASE_URL,
                CSS_SELECTOR,
                llm_strategy,
                session_id,
                REQUIRED_KEYS,
                seen_names,
            )

            if no_results_found:
                print("No more offers found. Ending crawl.")
                break  # Stop crawling when "No Results Found" message appears

            if not offers:
                print(f"No offers extracted from page {page_number}.")
                break  # Stop if no offers are extracted

            # Add the offers from this page to the total list
            all_offers.extend(offers)
            page_number += 1  # Move to the next page

            # Pause between requests to be polite and avoid rate limits
            await asyncio.sleep(2)  # Adjust sleep time as needed

    # Save the collected offers to a CSV file
    if all_offers:
        save_offers_to_csv(all_offers, "complete_offers.csv")
        print(f"Saved {len(all_offers)} offers to 'complete_offers.csv'.")
    else:
        print("No offers were found during the crawl.")

    # Display usage statistics for the LLM strategy
    llm_strategy.show_usage()


async def main():
    """
    Entry point of the script.
    """
    await crawl_offers()


if __name__ == "__main__":
    asyncio.run(main())
