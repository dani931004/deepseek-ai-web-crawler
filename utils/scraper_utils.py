import asyncio
import json
import os
from typing import List, Optional, Set, Tuple

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CacheMode,
    CrawlerRunConfig,
    LLMExtractionStrategy,
)


def get_browser_config() -> BrowserConfig:
    """
    Returns the browser configuration for the crawler.

    Returns:
        BrowserConfig: The configuration settings for the browser.
    """
    # https://docs.crawl4ai.com/core/browser-crawler-config/
    return BrowserConfig(
        browser_type="chromium",  # Type of browser to simulate
        headless=False,  # Whether to run in headless mode (no GUI)
        verbose=True,  # Enable verbose logging
    )


def get_llm_strategy(model: type) -> LLMExtractionStrategy:
    """
    Returns the configuration for the language model extraction strategy.
    Implements rate limiting for Groq's 6000 TPM (tokens per minute) limit.

    Returns:
        LLMExtractionStrategy: The settings for how to extract data using LLM.
    """
    # Calculate max tokens per request to stay under 6000 TPM
    # Assuming ~10 requests per minute to be safe (500 tokens per request average)
    max_tokens_per_request = 500
    
    return LLMExtractionStrategy(
        provider="groq/deepseek-r1-distill-llama-70b",  # Name of the LLM provider
        api_token=os.getenv("GROQ_API_KEY"),  # API token for authentication
        schema=model.model_json_schema(),  # JSON schema of the data model
        extraction_type="schema",  # Type of extraction to perform
        instruction=(
            "Extract all offer objects with 'name', 'date', 'price', 'transport_type', "
            "and 'link' from the following content. If there are many offers, return "
            "only the most relevant ones (max 3-4 per page). Be concise in responses."
        ),
        input_format="markdown",  # Format of the input content
        verbose=True,  # Enable verbose logging
        max_tokens=max_tokens_per_request,  # Limit tokens per request
        temperature=0.1,  # Lower temperature for more consistent results
        top_p=0.9,  # Controls diversity
        frequency_penalty=0.1,  # Slightly reduce repetition
        presence_penalty=0.1,  # Slightly encourage new topics
        retry_attempts=3,  # Number of retry attempts on failure
        retry_delay=10,  # Increased delay between retries
        request_timeout=60,  # Timeout in seconds
        rate_limit={
            'tokens_per_minute': 5500,  # Stay under 6000 TPM
            'requests_per_minute': 10,  # Limit requests per minute
            'tokens_per_request': max_tokens_per_request  # Tokens per request
        }
    )


async def check_no_results(
    crawler: AsyncWebCrawler,
    url: str,
    session_id: str,
) -> bool:
    """
    Checks if the "No Results Found" message is present on the page.

    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        url (str): The URL to check.
        session_id (str): The session identifier.

    Returns:
        bool: True if "No Results Found" message is found, False otherwise.
    """
    # Fetch the page without any CSS selector or extraction strategy
    result = await crawler.arun(
        url=url,
        config=CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            session_id=session_id,
        ),
    )

    if result.success:
        if "No Results Found" in result.cleaned_html:
            return True
    else:
        print(
            f"Error fetching page for 'No Results Found' check: {result.error_message}"
        )

    return False


async def fetch_and_process_page(
    crawler: AsyncWebCrawler,
    page_number: int,
    base_url: str,
    css_selector: str,
    llm_strategy: LLMExtractionStrategy,
    session_id: str,
    required_keys: List[str],
    seen_names: Set[str],
) -> Tuple[List[dict], bool]:
    """
    Fetches and processes a single page of offer data with rate limiting and error handling.

    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        page_number (int): The page number to fetch.
        base_url (str): The base URL of the website.
        css_selector (str): The CSS selector to target the content.
        llm_strategy (LLMExtractionStrategy): The LLM extraction strategy.
        session_id (str): The session identifier.
        required_keys (List[str]): List of required keys in the offer data.
        seen_names (Set[str]): Set of offer names that have already been seen.

    Returns:
        Tuple[List[dict], bool]:
            - List[dict]: A list of processed offers from the page.
            - bool: A flag indicating if the "No Results Found" message was encountered.
    """
    url = f"{base_url}?page={page_number}"
    print(f"Loading page {page_number}...")
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            # Add delay between requests to respect rate limits
            if attempt > 0:
                print(f"Retry attempt {attempt + 1}/{max_retries} after {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            
            # Check if "No Results Found" message is present
            no_results = await check_no_results(crawler, url, session_id)
            if no_results:
                print("No more results found. Ending crawl.")
                return [], True  # No more results, signal to stop crawling

            # Configure the crawler with minimal required parameters
            config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                extraction_strategy=llm_strategy,
                css_selector=css_selector,
                session_id=f"{session_id}_page{page_number}"
            )
            
            # Fetch the page with the configured settings
            result = await crawler.arun(url=url, config=config)
            
            if not result.success:
                error_msg = result.error_message or "Unknown error"
                if "rate limit" in error_msg.lower() and attempt < max_retries - 1:
                    print("Rate limited. Waiting before retry...")
                    continue
                print(f"Error fetching page {page_number}: {error_msg}")
                return [], False
                
            # Process the successful result
            return await process_extracted_data(result, required_keys, seen_names)
            
        except Exception as e:
            print(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
            if attempt == max_retries - 1:  # Last attempt
                print(f"Failed to fetch page {page_number} after {max_retries} attempts")
                return [], False
                
    return [], False  # Should never reach here due to max_retries


async def process_extracted_data(result, required_keys: List[str], unique_key: str = 'name', seen_values: Optional[Set[str]] = None) -> Tuple[List[dict], bool]:
    """
    Process the extracted data from the crawler result for any structured model.
    
    Args:
        result: The result object from the crawler
        required_keys: List of required keys for each item
        unique_key: The key to use for detecting duplicates (default: 'name')
        seen_values: Set of already seen unique values to avoid duplicates
        
    Returns:
        Tuple of (list of processed items, no_results_flag)
    """
    seen_values = seen_values or set()
    
    try:
        # Parse the extracted data
        extracted_data = json.loads(result.extracted_content)
        if not isinstance(extracted_data, list):
            extracted_data = [extracted_data]
        
        # Process the extracted items
        processed_items = []
        for item in extracted_data:
            # Skip if item is not a dictionary
            if not isinstance(item, dict):
                continue
                
            # Skip if any required key is missing
            if not all(key in item for key in required_keys):
                continue
                
            # Convert all values to strings and strip whitespace
            processed_item = {
                k: str(v).strip() if v is not None else '' 
                for k, v in item.items()
            }
            
            # Skip if any required field is empty
            if not all(processed_item.get(key) for key in required_keys):
                continue
                
            # Skip if we've seen this unique value before
            unique_value = processed_item.get(unique_key)
            if unique_value in seen_values:
                print(f"Duplicate {unique_key} '{unique_value}' found. Skipping.")
                continue
                
            seen_values.add(unique_value)
            processed_items.append(processed_item)
        
        print(f"Processed {len(processed_items)} items from the page.")
        return processed_items, False
        
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return [], False
    except Exception as e:
        print(f"Error processing extracted data: {str(e)}")
        return [], False
