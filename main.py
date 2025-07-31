#!/usr/bin/env /home/dani/Desktop/Crawl4AI/deepseek-crawler/venv/bin/python3
import asyncio

from dotenv import load_dotenv

from crawlers.dari_tour_crawlers import DariTourCrawler, DariTourDetailedCrawler
from crawlers.hotel_details_crawler import HotelDetailsCrawler
from crawlers.angel_travel_crawlers import AngelTravelCrawler


load_dotenv()

async def main():
    """
    Entry point of the script.
    """
    # Crawl Dari Tour Offers
    # dari_tour_crawler = DariTourCrawler()
    # await dari_tour_crawler.crawl(max_items=20)
    # await asyncio.sleep(1) # Add a small delay to ensure file is written

    # # Crawl Dari Tour Detailed Offers
    # dari_tour_detailed_crawler = DariTourDetailedCrawler()
    # await dari_tour_detailed_crawler.crawl()
    # await asyncio.sleep(1) # Add a small delay to ensure file is written

    # # Crawl Hotel Details
    # hotel_details_crawler = HotelDetailsCrawler()
    # await hotel_details_crawler.crawl()

    # Crawl Angel Travel Offers
    angel_travel_crawler = AngelTravelCrawler()
    await angel_travel_crawler.crawl()
    await asyncio.sleep(1) # Add a small delay to ensure file is written


if __name__ == "__main__":
    asyncio.run(main())
