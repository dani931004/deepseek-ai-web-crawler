# Comprehensive Guide to Adapting the Crawler for Other Websites

This guide provides detailed instructions on how to modify and use this web crawler for scraping data from different websites.

## Table of Contents
1. [Understanding the Crawler Structure](#understanding-the-crawler-structure)
2. [Configuring for a New Website](#configuring-for-a-new-website)
3. [Adjusting the Data Model](#adjusting-the-data-model)
4. [Customizing the Extraction Logic](#customizing-the-extraction-logic)
5. [Handling Pagination](#handling-pagination)
6. [Dealing with Anti-Scraping Measures](#dealing-with-anti-scraping-measures)
7. [Running the Crawler](#running-the-crawler)
8. [Troubleshooting Common Issues](#troubleshooting-common-issues)

## Understanding the Crawler Structure

The crawler is built with these main components:

- `main.py`: The entry point that orchestrates the crawling process
- `config.py`: Contains all configuration parameters
- `models/venue.py`: Defines the data structure for venues
- `utils/`: Contains helper functions for data processing and scraping

## Configuring for a New Website

1. **Update `config.py`**
   - `BASE_URL`: Change to the target website's base URL
   - `CSS_SELECTOR`: Update to target the container element that holds each venue listing
   - `REQUIRED_KEYS`: Modify based on the data fields you want to extract

2. **Environment Variables**
   Ensure your `.env` file has the required API keys:
   ```env
   GROQ_API_KEY=your_groq_api_key_here
   # Add any other required API keys
   ```

## Adjusting the Data Model

1. **Update `models/venue.py`**
   - Modify the `Venue` class to match the data structure of your target website
   - Add or remove fields as needed
   - Update field validators if necessary

2. **Example for a Restaurant Website**
   ```python
   from pydantic import BaseModel, HttpUrl, Field
   from typing import Optional, List

   class Restaurant(BaseModel):
       name: str
       address: str
       cuisine: List[str]
       price_range: str
       rating: Optional[float] = None
       phone: Optional[str] = None
       website: Optional[HttpUrl] = None
   ```

## Customizing the Extraction Logic

1. **Update `utils/scraper_utils.py`**
   - Modify the `extract_venue_data` function to parse the HTML structure of your target website
   - Update the CSS selectors to match the target website's structure

2. **Example for a Different Website**
   ```python
   async def extract_venue_data(page, selector: str) -> List[Dict]:
       # Custom extraction logic for the target website
       venues = []
       items = await page.query_selector_all(selector)
       
       for item in items:
           try:
               # Example: Extract data from a restaurant listing
               name = await item.query_selector('h2.restaurant-name')
               name = await name.text_content() if name else ""
               
               # Add more fields as needed
               venue_data = {
                   'name': name.strip(),
                   # Add other fields
               }
               venues.append(venue_data)
           except Exception as e:
               print(f"Error extracting data from item: {e}")
       
       return venues
   ```

## Handling Pagination

1. **Update the pagination logic** in `main.py`:
   - Identify how the target website implements pagination (infinite scroll, numbered pages, "Load More" button)
   - Modify the `crawl_pages` function accordingly

2. **Example for Numbered Pagination**
   ```python
   async def crawl_pages(base_url: str, max_pages: int = 5):
       all_venues = []
       
       for page_num in range(1, max_pages + 1):
           url = f"{base_url}?page={page_num}"
           print(f"Crawling page {page_num}...")
           
           # Your existing crawling logic here
           # ...
   ```

## Dealing with Anti-Scraping Measures

1. **Rate Limiting**
   - Add delays between requests
   - Use rotating user agents
   - Implement retry logic

2. **Example Implementation**
   ```python
   import random
   import time
   from fake_useragent import UserAgent

   # Rotate user agents
   def get_random_user_agent():
       ua = UserAgent()
       return ua.random

   # Add random delay
   def random_delay(min_seconds=1, max_seconds=3):
       time.sleep(random.uniform(min_seconds, max_seconds))
   ```

## Running the Crawler

1. **Basic Usage**
   ```bash
   python main.py
   ```

2. **With Custom Parameters**
   ```bash
   python main.py --max-pages 10 --output custom_output.csv
   ```

3. **Running in Headless Mode**
   Update the browser launch options in `scraper_utils.py`:
   ```python
   browser = await p.chromium.launch(headless=True)
   ```

## Troubleshooting Common Issues

1. **Element Not Found**
   - Verify CSS selectors are correct
   - Check if the page has loaded completely
   - Look for iframes or dynamic content loading

2. **Blocked by Website**
   - Implement delays between requests
   - Use rotating proxies
   - Consider using a headless browser with more human-like behavior

3. **Data Extraction Issues**
   - Verify the HTML structure hasn't changed
   - Add more robust error handling
   - Consider using XPath if CSS selectors are unreliable

4. **Performance Problems**
   - Reduce concurrency
   - Implement proper resource cleanup
   - Use connection pooling

## Best Practices

1. **Respect robots.txt**
   - Always check the website's `robots.txt` file
   - Follow the crawl-delay directives if present

2. **Error Handling**
   - Implement comprehensive error handling
   - Log errors for debugging
   - Save progress to resume if interrupted

3. **Data Validation**
   - Validate extracted data before saving
   - Handle missing or malformed data gracefully
   - Consider using Pydantic models for validation

4. **Performance Optimization**
   - Use async/await for I/O-bound operations
   - Implement proper resource cleanup
   - Consider using a task queue for large-scale crawling

## Example: Scraping a Real Estate Website

1. **Update `config.py`**
   ```python
   BASE_URL = "https://www.example-real-estate.com/listings"
   CSS_SELECTOR = ".property-listing"
   REQUIRED_KEYS = ["title", "price", "address", "bedrooms", "bathrooms"]
   ```

2. **Update the Venue Model**
   ```python
   class Property(BaseModel):
       title: str
       price: str
       address: str
       bedrooms: float
       bathrooms: float
       square_feet: Optional[int] = None
       listing_date: Optional[str] = None
   ```

3. **Update Extraction Logic**
   ```python
   async def extract_property_data(page, selector: str) -> List[Dict]:
       properties = []
       items = await page.query_selector_all(selector)
       
       for item in items:
           try:
               # Custom extraction logic for real estate listings
               title_elem = await item.query_selector('.property-title')
               title = await title_elem.text_content() if title_elem else ""
               
               # Add more fields as needed
               property_data = {
                   'title': title.strip(),
                   # Extract other fields
               }
               properties.append(property_data)
           except Exception as e:
               print(f"Error extracting property data: {e}")
       
       return properties
   ```

## Conclusion

This guide provides a comprehensive overview of how to adapt the crawler for different websites. The key to successful web scraping is understanding the target website's structure and adjusting the selectors and data extraction logic accordingly. Always ensure you're complying with the website's terms of service and implementing proper rate limiting to be a good web citizen.
