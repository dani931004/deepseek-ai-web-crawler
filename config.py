import os
from pathlib import Path
from crawl4ai import BrowserConfig
from typing import Optional

PAGE_TIMEOUT = 120000

# Delay constants for crawling
MIN_DELAY_SECONDS = 10
MAX_DELAY_SECONDS = 30

class CrawlerConfig:
    """
    Configuration class for defining crawler-specific settings.

    This class encapsulates all necessary parameters for a crawler,
    including its name, base URL, CSS selector for identifying offer items,
    and a list of required keys for data extraction. It also manages
    the directory structure for storing crawled data, ensuring that
    output directories are created if they don't already exist.
    """

    def __init__(self, name: str, base_url: str, css_selector: str, required_keys: list, skip_existing_offers: bool = True, skip_existing_detailed_offers: bool = True, max_offers_to_crawl: Optional[int] = None):
        """
        Initializes a new CrawlerConfig instance.

        Args:
            name (str): The unique name of the crawler (e.g., "dari_tour", "angel_travel").
                        This name is used to create dedicated directories for storing crawled data.
            base_url (str): The starting URL for the crawler.
            css_selector (str): The CSS selector used to identify individual offer items on the page.
                                This selector helps the crawler locate the main data blocks to process.
            required_keys (list): A list of strings representing the essential data fields
                                  that must be extracted for each offer. This helps in data validation.
        """
        self.name = name
        self.base_url = base_url
        self.css_selector = css_selector
        self.required_keys = required_keys
        self.skip_existing_offers = skip_existing_offers
        self.skip_existing_detailed_offers = skip_existing_detailed_offers
        self.max_offers_to_crawl = max_offers_to_crawl

        # Define base directory for the current file to construct absolute paths.
        self.BASE_DIR = Path(__file__).parent
        # Construct the path for storing files specific to this crawler.
        self.FILES_DIR = self.BASE_DIR / f"{name}_files"
        # Construct the path for storing detailed offer information.
        self.DETAILS_DIR = self.FILES_DIR / "detailed_offers"
        # Construct the path for storing hotel-specific details within detailed offers.
        self.HOTEL_DETAILS_DIR = self.DETAILS_DIR / "hotel_details"

        # Ensure all necessary directories exist. If they don't, create them.
        # `parents=True` allows creating parent directories as needed.
        # `exist_ok=True` prevents an error if the directory already exists.
        for directory in [self.FILES_DIR, self.DETAILS_DIR, self.HOTEL_DETAILS_DIR]:
            print(f"Creating directory: {directory}")
            directory.mkdir(parents=True, exist_ok=True)


dari_tour_config = CrawlerConfig(
    name="dari_tour",
    base_url="https://dari-tour.com/lyato-2025",
    css_selector=".offer-item",
    required_keys=["name", "date", "price", "transport_type", "link"],
    max_offers_to_crawl=5,
)

angel_travel_config = CrawlerConfig(
    name="angel_travel",
    base_url="https://www.angeltravel.bg/exotic-destinations",
    css_selector="ul#accordeonck629 li.accordeonck",
    required_keys=["title", "dates", "price", "transport_type", "link"],
    max_offers_to_crawl=None,
)


def get_browser_config() -> BrowserConfig:
    """
    Returns a BrowserConfig object with predefined settings for Playwright.

    These settings are optimized for web scraping, including headless mode,
    viewport dimensions, user agent, and error handling preferences.
    The `extra_args` are crucial for running Chromium in a containerized or
    restricted environment, disabling sandboxing and GPU usage for stability.

    Returns:
        BrowserConfig: An object containing browser configuration parameters.
    """
    return BrowserConfig(
        browser_type="chromium",  # Specify the browser type to use (e.g., "chromium", "firefox", "webkit").
        headless=True,  # Run the browser in headless mode (without a visible UI).
        viewport_width=1920,  # Set the width of the browser viewport.
        viewport_height=1080,  # Set the height of the browser viewport.
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",  # Custom user agent string to mimic a standard browser.
        ignore_https_errors=True,  # Ignore HTTPS errors, useful for sites with self-signed certificates.
        java_script_enabled=True,  # Enable JavaScript execution within the browser.
        verbose=True,  # Enable verbose logging for debugging purposes.
        extra_args=[  # Additional command-line arguments passed to the browser instance.
            "--no-sandbox",  # Disable the sandbox, necessary in some environments (e.g., Docker).
            "--disable-setuid-sandbox",  # Disable the setuid sandbox, often used with --no-sandbox.
            "--disable-dev-shm-usage",  # Overcome limited /dev/shm resources in some environments.
            "--disable-accelerated-2d-canvas",  # Disable hardware acceleration for 2D canvas.
            "--no-first-run",  # Skip the first-run experience.
            "--no-zygote",  # Disable the zygote process, relevant for Linux sandboxing.
            "--disable-gpu"  # Disable GPU hardware acceleration.
        ],
    )

# General CSS Selectors used across different crawlers for common elements.
CSS_SELECTOR_OFFER_ITEM_TITLE = ".title"  # Selector for the title of an offer item.
CSS_SELECTOR_HOTEL_MAP_IFRAME = 'iframe[data-src*="maps.google.com"]'  # Selector for Google Maps iframes.
CSS_SELECTOR_HOTEL_DESCRIPTION_BOX = 'div.details-box'  # Selector for a div containing hotel details.

# CSS Selectors specific to Dari Tour for extracting detailed offer information.
CSS_SELECTOR_DARI_TOUR_DETAIL_OFFER_NAME = "h1.antetka-2"  # Selector for the main offer name on a detail page.
CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_ELEMENTS = "div.resp-tab-content[aria-labelledby='hor_1_tab_item-0'] div.col-hotel"  # Selector for individual hotel elements within a detailed offer.
CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_NAME = "div.title"  # Selector for the hotel name within a hotel element.
CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_PRICE = "div.price"  # Selector for the hotel price within a hotel element.
CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_COUNTRY = "div.info div.country"  # Selector for the hotel country within a hotel element.
CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_LINK = "a"  # Selector for the link to the hotel's detail page.
CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_ITEM_LINK = "a.hotel-item"  # Another selector for a hotel item link.
CSS_SELECTOR_DARI_TOUR_DETAIL_PROGRAM = "div.resp-tab-content[aria-labelledby='hor_1_tab_item-1']"  # Selector for the program/itinerary section.
CSS_SELECTOR_DARI_TOUR_DETAIL_INCLUDED_SERVICES = "div.resp-tab-content[aria-labelledby='hor_1_tab_item-2'] ul li"  # Selector for included services list items.
CSS_SELECTOR_DARI_TOUR_DETAIL_EXCLUDED_SERVICES = "div.resp-tab-content[aria-labelledby='hor_1_tab_item-3'] ul li"  # Selector for excluded services list items.

# CSS Selectors specific to Angel Travel for extracting detailed offer information.
# Note: Some selectors are duplicated or overridden below due to specific page structures.



# Specific overrides/refinements for Angel Travel Detailed offers due to unique page structure.
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_OFFER_NAME = "div.program_once h2 a"  # More specific selector for the offer name.
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_PROGRAM = "div.ofcontent"  # More specific selector for the program content.
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_INCLUDED_SERVICES = "div.antetka div.antetka-inner ul li"  # More specific selector for included services.
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_EXCLUDED_SERVICES = "div.antetka div.antetka-inner ul li"  # More specific selector for excluded services.
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_ELEMENTS = "div.once_offer"  # More specific selector for hotel elements.
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_NAME = "div.program_once h2 a"  # More specific selector for hotel name.
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_PRICE = "font.price"  # More specific selector for hotel price.
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_COUNTRY = "div.ofcontent"  # More specific selector for hotel country (often within general content).
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_ITEM_LINK = "a.but"  # More specific selector for hotel item link.