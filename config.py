# config.py
import os
from pathlib import Path

# Base directories
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "dari_tour_files"
DETAILS_DIR = DATA_DIR / "detailed_offers"
HOTEL_DETAILS_DIR = DETAILS_DIR / "hotel_details"

# Ensure directories exist
for directory in [DATA_DIR, DETAILS_DIR, HOTEL_DETAILS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Dari Tour Offers
BASE_URL_DARI_TOUR_OFFERS = "https://dari-tour.com/lyato-2025"
CSS_SELECTOR_DARI_TOUR_OFFERS = ".offer-item"
REQUIRED_KEYS_DARI_TOUR_OFFERS = [
    "name",  # Changed from "title" to match the model
    "date",
    "price",
    "transport_type",
    "link",
]

# Angel travel offers
BASE_URL_ANGEL_TRAVEL_OFFERS = "https://angeltravel.com/lyato-2025"
CSS_SELECTOR_ANGEL_TRAVEL_OFFERS = "[class^='col-xl-3 col-lg-3 col-md-4 col-sm-6 col-12 col-offer']"
REQUIRED_KEYS_ANGEL_TRAVEL_OFFERS = [
    "title",
    "date",
    "price",
    "transport_type",
    "link",
]

# Detailed offers configuration
DARI_TOUR_DETAILS_DIR = str(DETAILS_DIR)

# CSS Selectors for Detailed Dari Tour Offers
CSS_SELECTOR_DARI_TOUR_DETAIL_OFFER_NAME = "h1.antetka-2"
CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_ELEMENTS = "div.resp-tab-content[aria-labelledby='hor_1_tab_item-0'] div.col-hotel"
CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_NAME = "div.title"
CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_PRICE = "div.price"
CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_COUNTRY = "div.info div.country"
CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_LINK = "a"
CSS_SELECTOR_DARI_TOUR_DETAIL_HOTEL_ITEM_LINK = "a.hotel-item"
CSS_SELECTOR_DARI_TOUR_DETAIL_PROGRAM = "div.resp-tab-content[aria-labelledby='hor_1_tab_item-1']"
CSS_SELECTOR_DARI_TOUR_DETAIL_INCLUDED_SERVICES = "div.resp-tab-content[aria-labelledby='hor_1_tab_item-2'] ul li"
CSS_SELECTOR_DARI_TOUR_DETAIL_EXCLUDED_SERVICES = "div.resp-tab-content[aria-labelledby='hor_1_tab_item-3'] ul li"

# General CSS Selectors
CSS_SELECTOR_OFFER_ITEM_TITLE = ".title"
CSS_SELECTOR_HOTEL_MAP_IFRAME = 'iframe[data-src*="maps.google.com"]'
CSS_SELECTOR_HOTEL_DESCRIPTION_BOX = 'div.details-box'

def ensure_directory_exists(directory: str):
    """Ensure that a directory exists, create it if it doesn't.
    
    Args:
        directory: Path to the directory
    """
    os.makedirs(directory, exist_ok=True)


def get_browser_config():
    """Get the browser configuration for the crawler.
    
    Returns:
        dict: Browser configuration
    """
    return {
        "browser_type": "chromium",
        "headless": True,
        "viewport_width": 1920,
        "viewport_height": 1080,
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "ignore_https_errors": True,
        "java_script_enabled": True,
        "verbose": True,
        "extra_args": [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-accelerated-2d-canvas",
            "--no-first-run",
            "--no-zygote",
            "--single-process",
            "--disable-gpu",
            "--disable-software-rasterizer",
            "--disable-web-security",
            "--disable-features=IsolateOrigins,site-per-process",
            "--disable-blink-features=AutomationControlled",
        ],
    }
