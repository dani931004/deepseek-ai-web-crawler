import os
import asyncio
import json
import time
import random
import logging
from typing import List, Dict, Any, Optional, Type
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from crawl4ai.async_configs import BrowserConfig
from bs4 import BeautifulSoup
from config import DARI_TOUR_DETAILS_DIR, HOTEL_DETAILS_DIR
from models.hotel_details_model import HotelDetails
from utils.data_utils import save_to_json
import pandas as pd
import urllib.parse

async def crawl_hotel_details():
    csv_filepath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dari_tour_files', 'complete_offers.csv'))
    print(f"DEBUG: Attempting to read CSV from: {csv_filepath}")
    if not os.path.exists(csv_filepath):
        print(f"Error: The file '{csv_filepath}' was not found. Please ensure the initial offer crawl has been run successfully.")
        return

    offers_df = pd.read_csv(csv_filepath)

    hotel_details_dir = HOTEL_DETAILS_DIR
    os.makedirs(hotel_details_dir, exist_ok=True)

    processed_hotel_slugs = set()
    if os.path.exists(hotel_details_dir):
        for filename in os.listdir(hotel_details_dir):
            if filename.endswith(".json"):
                processed_hotel_slugs.add(filename.replace(".json", ""))

    hotels_to_process = []
    for index, row in offers_df.iterrows():
        offer_name = row['name']
        offer_link = row['link']
        # Assuming hotel links are stored in a column named 'hotel_links' in complete_offers.csv
        # If not, you'll need to adjust how you get the hotel links.
        # For now, I'll assume the hotel links are part of the detailed offer JSONs.
        # So, I need to read the detailed offer JSONs first to get the hotel links.
        
        # For now, let's iterate through the detailed offer JSONs to get hotel links
        offer_slug = offer_name.lower().replace(' ', '-')
        detailed_offer_path = os.path.join(DARI_TOUR_DETAILS_DIR, f"{offer_slug}.json")
        
        if os.path.exists(detailed_offer_path):
            with open(detailed_offer_path, 'r', encoding='utf-8') as f:
                detailed_offer_data = json.load(f)
            
            if 'hotels' in detailed_offer_data:
                for hotel in detailed_offer_data['hotels']:
                    if 'link' in hotel and hotel['link']:
                        hotel_name = hotel['name']
                        hotel_link = hotel['link']
                        hotel_slug = hotel_name.lower().replace(' ', '-')
                        
                        if hotel_slug not in processed_hotel_slugs:
                            hotels_to_process.append({
                                'hotel_name': hotel_name,
                                'hotel_link': hotel_link,
                                'offer_title': offer_name # Pass the offer name to the hotel details
                            })
                        else:
                            print(f"Skipping hotel {hotel_name} as its details have already been processed.")

    if not hotels_to_process:
        print("All hotel details have already been processed or no hotel links found.")
        return

    async with AsyncWebCrawler(config=BrowserConfig(headers={
        "Accept-Language": "bg-BG,bg;q=0.9"
    })) as crawler:
        for hotel_info in hotels_to_process:
            hotel_name = hotel_info['hotel_name']
            hotel_link = hotel_info['hotel_link']
            offer_title = hotel_info['offer_title']
            hotel_slug = hotel_name.lower().replace(' ', '-')
            output_path = os.path.join(hotel_details_dir, f"{hotel_slug}.json")

            print(f"Processing hotel: {hotel_name} from offer: {offer_title}")
            print(f"URL: {hotel_link}")

            config = CrawlerRunConfig(
                url=hotel_link,
                cache_mode=CacheMode.BYPASS,
            )

            result = await crawler.arun(hotel_link, config=config)

            if result.html:
                soup = BeautifulSoup(result.html, 'html.parser')
                
                google_map_link = None
                iframe_element = soup.select_one('iframe[data-src*="maps.google.com"]')
                if iframe_element and 'src' in iframe_element.attrs:
                    embed_url = iframe_element['src']
                    parsed_url = urllib.parse.urlparse(embed_url)
                    query_params = urllib.parse.parse_qs(parsed_url.query)
                    
                    if 'q' in query_params and query_params['q']:
                        location_query = query_params['q'][0]
                        # Construct the direct Google Maps search link
                        google_map_link = f"https://www.google.com/maps/search/?api=1&query={urllib.parse.quote_plus(location_query)}"
                    else:
                        google_map_link = embed_url # Fallback to embed URL if 'q' parameter is not found
                
                description = None
                description_div = soup.select_one('div.details-box')
                if description_div:
                    description = description_div.get_text(strip=True)
                
                # The offer_title is passed from the previous crawl, so we don't need to extract it again
                # from the current page, as it refers to the parent offer.
                
                hotel_details_data = HotelDetails(
                    google_map_link=google_map_link,
                    description=description,
                    offer_title=offer_title, # Use the passed offer_title
                    hotel_name=hotel_name,
                    hotel_link=hotel_link
                )
                
                save_to_json(hotel_details_data.model_dump(), output_path)
                print(f"Saved hotel details to {output_path}")
            else:
                print(f"No HTML content retrieved for {hotel_link}")
