#!/usr/bin/env /home/dani/Desktop/Crawl4AI/deepseek-crawler/venv/bin/python3
import asyncio

from dotenv import load_dotenv

from crawlers.dari_tour_crawlers import crawl_dari_tour_offers
from crawlers.dari_tour_crawlers import crawl_dari_tour_detailed_offers
from crawlers.hotel_details_crawler import crawl_hotel_details


load_dotenv()

async def main():
    """
    Entry point of the script.
    """
    # Crawl Dari Tour Offers
    await crawl_dari_tour_offers()
    await asyncio.sleep(1) # Add a small delay to ensure file is written

    # Crawl Dari Tour Detailed Offers
    await crawl_dari_tour_detailed_offers()
    await asyncio.sleep(1) # Add a small delay to ensure file is written

    # Crawl Hotel Details
    await crawl_hotel_details()


if __name__ == "__main__":
    asyncio.run(main())
