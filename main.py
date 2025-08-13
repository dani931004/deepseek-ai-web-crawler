import asyncio
from datetime import datetime
import logging

from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

from crawlers.dari_tour_crawlers import DariTourCrawler, DariTourDetailedCrawler
from crawlers.hotel_details_crawler import HotelDetailsCrawler
from crawlers.angel_travel_crawlers import AngelTravelCrawler
from crawlers.angel_travel_detailed_crawler import AngelTravelDetailedCrawler
from config import angel_travel_config, dari_tour_config
from models.angel_travel_detailed_models import AngelTravelDetailedOffer
from models.angel_travel_models import AngelTravelOffer
from models.dari_tour_models import DariTourOffer
from models.dari_tour_detailed_models import OfferDetails
from utils.enums import OutputType


load_dotenv()

async def main():
    """
    Main asynchronous function to orchestrate the crawling process.
    This function initializes and runs various crawlers to collect data from different sources.
    The use of `async` and `await` allows for efficient handling of I/O-bound operations,
    such as network requests during crawling, without blocking the main thread.
    """
    session_id = datetime.now().strftime("%Y%m%d%H%M%S")

    # First, run the Angel Travel Crawler to populate the complete_offers.csv
    angel_travel_crawler = AngelTravelCrawler(session_id=session_id, config=angel_travel_config, model_class=AngelTravelOffer, output_file_type=OutputType.CSV)
    await angel_travel_crawler.crawl() # Process all offers
    await asyncio.sleep(1) # Give the file system a moment to catch up

    # Then, run the Angel Travel Detailed Crawler
    angel_travel_detailed_crawler = AngelTravelDetailedCrawler(session_id=session_id, config=angel_travel_config, model_class=AngelTravelDetailedOffer, output_file_type=OutputType.JSON)
    await angel_travel_detailed_crawler.crawl() # Process all offers

    # Then, run the Dari Tour Crawler
    dari_tour_crawler = DariTourCrawler(session_id=session_id, config=dari_tour_config, model_class=DariTourOffer, output_file_type=OutputType.CSV)
    await dari_tour_crawler.crawl() # Process all offers
    await asyncio.sleep(1) # Give the file system a moment to catch up

    # Then, run the Dari Tour Detailed Crawler
    dari_tour_detailed_crawler = DariTourDetailedCrawler(session_id=session_id, config=dari_tour_config, model_class=OfferDetails, output_file_type=OutputType.JSON)
    await dari_tour_detailed_crawler.crawl() # Process all offers


if __name__ == "__main__":
    # Entry point for the script execution.
    # `asyncio.run()` is used to run the main asynchronous function.
    # This ensures that the asynchronous operations within `main()` are properly managed.
    asyncio.run(main())

