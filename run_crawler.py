import asyncio
from crawlers.angel_travel_detailed_crawler import AngelTravelDetailedCrawler
from config import angel_travel_config
from models.angel_travel_detailed_models import AngelTravelDetailedOffer

async def main():
    """
    Main asynchronous function to run the AngelTravelDetailedCrawler.
    This function initializes the crawler and starts the crawling process.
    """
    # Instantiate the AngelTravelDetailedCrawler.
    # This prepares the crawler with its configuration and initial state.
    crawler = AngelTravelDetailedCrawler(session_id="angel_travel_session", config=angel_travel_config, model_class=AngelTravelDetailedOffer)
    # Start the crawling process.
    # The 'crawl' method handles the navigation, data extraction, and saving.
    await crawler.crawl()

if __name__ == "__main__":
    # This block ensures that the 'main' function is called only when the script is executed directly.
    # asyncio.run() is used to run the asynchronous 'main' function.
    asyncio.run(main())