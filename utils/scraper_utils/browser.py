"""
Browser configuration utilities for the web crawler.
"""
from crawl4ai import BrowserConfig

def get_browser_config() -> BrowserConfig:
    """
    Returns the browser configuration for the crawler.

    Returns:
        BrowserConfig: The configuration settings for the browser.
    """
    # https://docs.crawl4ai.com/core/browser-crawler-config/
    return BrowserConfig(
        browser_type="chromium",  # Type of browser to simulate
        headless=False,          # Whether to run in headless mode (no GUI)
        verbose=True,            # Enable verbose logging
    )
