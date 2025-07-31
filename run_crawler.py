import asyncio
from crawlers.angel_travel_detailed_crawler import AngelTravelDetailedCrawler

async def main():
    crawler = AngelTravelDetailedCrawler()
    await crawler.crawl()

if __name__ == "__main__":
    asyncio.run(main())