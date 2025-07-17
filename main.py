#!/home/dani/Desktop/Crawl4AI/deepseek-crawler/venv/bin/python3
import asyncio

from dotenv import load_dotenv

from crawlers.dari_tour_crawlers import crawl_dari_tour_offers


load_dotenv()

async def main():
    """
    Entry point of the script.
    """
    # Crawl Dari Tour Offers
    await crawl_dari_tour_offers()


if __name__ == "__main__":
    asyncio.run(main())
