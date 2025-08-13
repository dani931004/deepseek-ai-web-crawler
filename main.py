import asyncio
import os
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv

# Configure logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Function to clean up old log files
def cleanup_old_logs(log_dir, days_old=3):
    now = datetime.now()
    for filename in os.listdir(log_dir):
        filepath = os.path.join(log_dir, filename)
        if os.path.isfile(filepath):
            file_mod_time = datetime.fromtimestamp(os.path.getmtime(filepath))
            if (now - file_mod_time) > timedelta(days=days_old):
                os.remove(filepath)
                logging.info(f"Cleaned up old log file: {filename}")

# Set up logging to file with rotation and console output
log_filename = datetime.now().strftime("%Y%m%d_%H%M%S.log")
log_filepath = os.path.join(LOG_DIR, log_filename)

# Create a logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create a formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Create a rotating file handler
# MaxBytes is set to approximately 2000 lines (assuming 100 chars/line * 2000 lines = 200KB)
# backupCount=5 means it will keep current log file + 5 backup files
file_handler = RotatingFileHandler(log_filepath, maxBytes=200 * 1024, backupCount=5)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Create a console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


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
    # Clean up old logs at the start of the program
    cleanup_old_logs(LOG_DIR, days_old=3)

    session_id = datetime.now().strftime("%Y%m%d%H%M%S")

    # First, run the Angel Travel Crawler to populate the complete_offers.csv
    angel_travel_crawler = AngelTravelCrawler(session_id=session_id, config=angel_travel_config, model_class=AngelTravelOffer, output_file_type=OutputType.CSV)
    await angel_travel_crawler.crawl() # Process all offers

    # Then, run the Angel Travel Detailed Crawler
    angel_travel_detailed_crawler = AngelTravelDetailedCrawler(session_id=session_id, config=angel_travel_config, model_class=AngelTravelDetailedOffer, output_file_type=OutputType.JSON)
    await angel_travel_detailed_crawler.crawl() # Process all offers

    # Then, run the Dari Tour Crawler
    dari_tour_crawler = DariTourCrawler(session_id=session_id, config=dari_tour_config, model_class=DariTourOffer, output_file_type=OutputType.CSV)
    await dari_tour_crawler.crawl() # Process all offers

    # Then, run the Dari Tour Detailed Crawler
    dari_tour_detailed_crawler = DariTourDetailedCrawler(session_id=session_id, config=dari_tour_config, model_class=OfferDetails, output_file_type=OutputType.JSON)
    await dari_tour_detailed_crawler.crawl() # Process all offers


if __name__ == "__main__":
    # Entry point for the script execution.
    # `asyncio.run()` is used to run the main asynchronous function.
    # This ensures that the asynchronous operations within `main()` are properly managed.
    asyncio.run(main())