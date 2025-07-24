import asyncio
import pandas as pd
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from crawl4ai.async_configs import BrowserConfig
from models.dari_tour_detailed_models import OfferDetails, Hotel
from utils.data_utils import save_to_json
import os
from bs4 import BeautifulSoup
from typing import List, Optional, Dict, Any

async def parse_detailed_offer(html_content: str) -> Optional[OfferDetails]:
    soup = BeautifulSoup(html_content, 'html.parser')

    offer_name_element = soup.select_one("h1.antetka-2")
    offer_name = offer_name_element.get_text(strip=True) if offer_name_element else ""

    hotels_data = []
    hotel_elements = soup.select("div.resp-tab-content[aria-labelledby='hor_1_tab_item-0'] div.col-hotel")
    for hotel_el in hotel_elements:
        name_el = hotel_el.select_one("div.title")
        price_el = hotel_el.select_one("div.price")
        country_el = hotel_el.select_one("div.info div.country")

        hotel_name = name_el.get_text(strip=True) if name_el else ""
        hotel_price = price_el.get_text(strip=True) if price_el else ""
        hotel_country = country_el.get_text(strip=True) if country_el else ""
        
        if hotel_name and hotel_price and hotel_country:
            hotels_data.append(Hotel(name=hotel_name, price=hotel_price, country=hotel_country))

    program_element = soup.select_one("div.resp-tab-content[aria-labelledby='hor_1_tab_item-1']")
    program = program_element.get_text(strip=True) if program_element else ""

    included_services = []
    included_elements = soup.select("div.resp-tab-content[aria-labelledby='hor_1_tab_item-2'] ul li")
    for li in included_elements:
        service = li.get_text(strip=True)
        if service:
            included_services.append(service)

    excluded_services = []
    excluded_elements = soup.select("div.resp-tab-content[aria-labelledby='hor_1_tab_item-3'] ul li")
    for li in excluded_elements:
        service = li.get_text(strip=True)
        if service:
            excluded_services.append(service)

    if offer_name:
        return OfferDetails(
            offer_name=offer_name,
            hotels=hotels_data,
            program=program,
            included_services=included_services,
            excluded_services=excluded_services
        )
    return None

async def crawl_dari_tour_detailed_offers():
    csv_filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dari_tour_files', 'complete_offers.csv'))
    print(f"DEBUG: Attempting to read CSV from: {csv_filepath}")
    if not os.path.exists(csv_filepath):
        print(f"Error: The file '{csv_filepath}' was not found. Please ensure the initial offer crawl has been run successfully.")
        return

    # Read the CSV file with offers
    offers_df = pd.read_csv(csv_filepath)

    # Ensure the output directory exists
    output_dir = 'deepseek-crawler/dari_tour_files/detailed_offers'
    os.makedirs(output_dir, exist_ok=True)

    async with AsyncWebCrawler(config=BrowserConfig(headers={
        "Accept-Language": "bg-BG,bg;q=0.9"
    })) as crawler:

        # Process each offer
        for index, row in offers_df.iterrows():
            offer_url = row['link']
            offer_name = row['name']
            # Create a slug from the offer name to use as a filename
            offer_slug = offer_name.lower().replace(' ', '-')

            # Define the path for the output JSON file
            output_path = os.path.join(output_dir, f"{offer_slug}.json")

            # Skip if the file already exists
            if os.path.exists(output_path):
                print(f"Skipping {offer_url} as it has already been processed.")
                continue

            print(f"Processing offer: {offer_name}")
            print(f"URL: {offer_url}")

            # Configure the crawler run
            config = CrawlerRunConfig(
                url=offer_url,
                cache_mode=CacheMode.BYPASS,
                )

            # Run the crawler
            result = await crawler.arun(offer_url, config=config)

            # Manually parse the HTML content
            if result.html:
                detailed_offer_data = await parse_detailed_offer(result.html)
                if detailed_offer_data:
                    save_to_json(detailed_offer_data.model_dump(), output_path)
                    print(f"Saved detailed offer to {output_path}")
                else:
                    print(f"No detailed data extracted for {offer_url}")
            else:
                print(f"No HTML content retrieved for {offer_url}")

if __name__ == '__main__':
    asyncio.run(crawl_dari_tour_detailed_offers())