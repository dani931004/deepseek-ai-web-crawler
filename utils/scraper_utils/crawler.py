"""
Main crawling functionality for the web crawler.
"""
import asyncio
from typing import List, Set, Tuple, Any, Optional
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from .content_processor import process_page_content

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
    llm_strategy: Any,
    session_id: str,
    required_keys: List[str],
    seen_names: Set[str],
    browser_config: Optional[dict] = None,
) -> Tuple[List[dict], bool]:
    """
    Fetches and processes a single page of offer data with rate limiting and error handling.

    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        page_number (int): The page number to fetch.
        base_url (str): The base URL of the website.
        css_selector (str): The CSS selector to target the content.
        llm_strategy: The LLM extraction strategy.
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

            # First, fetch just the raw HTML content
            config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                css_selector=css_selector,
                session_id=f"{session_id}_page{page_number}_fetch"
            )
            
            # Fetch the raw content
            result = await crawler.arun(url=url, config=config)
            
            if not result.success:
                error_msg = result.error_message or "Unknown error"
                if "rate limit" in error_msg.lower() and attempt < max_retries - 1:
                    print("Rate limited. Waiting before retry...")
                    continue
                print(f"Error fetching page {page_number}: {error_msg}")
                return [], False
            
            # Process the content in chunks with rate limiting
            offers = await process_page_content(
                content=result.cleaned_html,
                llm_strategy=llm_strategy,
                required_keys=required_keys,
                seen_names=seen_names,
                base_url=base_url,
                browser_config=browser_config,  # Use the passed browser config
                verbose=True
            )
            
            return offers, False
            
        except Exception as e:
            print(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
            if attempt == max_retries - 1:  # Last attempt
                print(f"Failed to fetch page {page_number} after {max_retries} attempts")
                return [], False
                
    return [], False  # Should never reach here due to max_retries
