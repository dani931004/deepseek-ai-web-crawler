import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
import sys
import os

# Add the parent directory to the sys.path to allow importing crawlers
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import main

@pytest.mark.asyncio
async def test_main_orchestration():
    """
    Tests that the main function correctly orchestrates the crawling process
    by ensuring that the crawl method of each crawler is called.
    """
    with patch('main.AngelTravelDetailedCrawler') as MockAngelTravelDetailedCrawler:
        # Configure the mock instance that AngelTravelDetailedCrawler() will return
        mock_angel_travel_detailed_crawler_instance = MagicMock()
        mock_angel_travel_detailed_crawler_instance.crawl = AsyncMock()
        MockAngelTravelDetailedCrawler.return_value = mock_angel_travel_detailed_crawler_instance

        await main()

        # Assert that the crawl method was called on the mock instance
        mock_angel_travel_detailed_crawler_instance.crawl.assert_called_once_with(max_items=4)