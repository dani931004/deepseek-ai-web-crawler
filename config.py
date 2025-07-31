import os
from pathlib import Path
from crawl4ai import BrowserConfig

class CrawlerConfig:
    def __init__(self, name: str, base_url: str, css_selector: str, required_keys: list):
        self.name = name
        self.base_url = base_url
        self.css_selector = css_selector
        self.required_keys = required_keys

        self.BASE_DIR = Path(__file__).parent
        self.FILES_DIR = self.BASE_DIR / f"{name}_files"
        self.DETAILS_DIR = self.FILES_DIR / "detailed_offers"
        self.HOTEL_DETAILS_DIR = self.DETAILS_DIR / "hotel_details"

        for directory in [self.FILES_DIR, self.DETAILS_DIR, self.HOTEL_DETAILS_DIR]:
            directory.mkdir(parents=True, exist_ok=True)

dari_tour_config = CrawlerConfig(
    name="dari_tour",
    base_url="https://dari-tour.com/lyato-2025",
    css_selector=".offer-item",
    required_keys=["name", "date", "price", "transport_type", "link"],
)

angel_travel_config = CrawlerConfig(
    name="angel_travel",
    base_url="https://www.angeltravel.bg/exotic-destinations",
    css_selector="ul#accordeonck629 li.accordeonck",
    required_keys=["title", "dates", "price", "transport_type", "link"],
)


def get_browser_config() -> BrowserConfig:
    return BrowserConfig(
        browser_type="chromium",
        headless=True,
        viewport_width=1920,
        viewport_height=1080,
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
        ignore_https_errors=True,
        java_script_enabled=True,
        verbose=True,
        extra_args=[
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-accelerated-2d-canvas",
            "--no-first-run",
            "--no-zygote",
            "--disable-gpu"
        ],
    )

# General CSS Selectors
CSS_SELECTOR_OFFER_ITEM_TITLE = ".title"
CSS_SELECTOR_HOTEL_MAP_IFRAME = 'iframe[data-src*="maps.google.com"]'
CSS_SELECTOR_HOTEL_DESCRIPTION_BOX = 'div.details-box'

# Detailed offers configuration
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

CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_OFFER_NAME = "h1.antetka-inner"
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_ELEMENTS = "div.resp-tab-content[aria-labelledby='hor_1_tab_item-0'] div.col-hotel"
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_NAME = "div.title"
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_PRICE = "div.price"
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_COUNTRY = "div.info div.country"
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_LINK = "a"
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_ITEM_LINK = "a.hotel-item"
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_PROGRAM = "div.resp-tab-content[aria-labelledby='hor_1_tab_item-1']"
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_INCLUDED_SERVICES = "div.resp-tab-content[aria-labelledby='hor_1_tab_item-2'] ul li"
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_EXCLUDED_SERVICES = "div.resp-tab-content[aria-labelledby='hor_1_tab_item-3'] ul li"

# Angel Travel Detailed offers configuration
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_OFFER_NAME = "div.program_once h2 a"
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_PROGRAM = "div.ofcontent"
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_INCLUDED_SERVICES = "div.ofcontent ul li"
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_EXCLUDED_SERVICES = "div.ofcontent ul li"
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_ELEMENTS = "div.once_offer"
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_NAME = "div.program_once h2 a"
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_PRICE = "font.price"
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_COUNTRY = "div.ofcontent"
CSS_SELECTOR_ANGEL_TRAVEL_DETAIL_HOTEL_ITEM_LINK = "a.but"
