import pytest
import asyncio
import json
from unittest.mock import AsyncMock, patch, MagicMock
import os
import sys

# Fixture to set up sys.path and mock crawler classes
@pytest.fixture
def setup_crawler_cli_mocks(monkeypatch):
    # Add the parent directory to the sys.path to allow importing crawlers
    current_dir = os.path.abspath(os.path.dirname(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    # Mock crawler classes
    mock_dari_tour_crawler = MagicMock(spec=True)
    mock_dari_tour_crawler.crawl = AsyncMock()
    mock_dari_tour_detailed_crawler = MagicMock(spec=True)
    mock_dari_tour_detailed_crawler.crawl = AsyncMock()
    mock_hotel_details_crawler = MagicMock(spec=True)
    mock_hotel_details_crawler.crawl = AsyncMock()
    mock_angel_travel_crawler = MagicMock(spec=True)
    mock_angel_travel_crawler.crawl = AsyncMock()
    mock_angel_travel_detailed_crawler = MagicMock(spec=True)
    mock_angel_travel_detailed_crawler.crawl = AsyncMock()

    monkeypatch.setattr('crawlers.dari_tour_crawlers.DariTourCrawler', MagicMock(return_value=mock_dari_tour_crawler))
    monkeypatch.setattr('crawlers.dari_tour_crawlers.DariTourDetailedCrawler', MagicMock(return_value=mock_dari_tour_detailed_crawler))
    monkeypatch.setattr('crawlers.hotel_details_crawler.HotelDetailsCrawler', MagicMock(return_value=mock_hotel_details_crawler))
    monkeypatch.setattr('crawlers.angel_travel_crawlers.AngelTravelCrawler', MagicMock(return_value=mock_angel_travel_crawler))
    monkeypatch.setattr('crawlers.angel_travel_detailed_crawler.AngelTravelDetailedCrawler', MagicMock(return_value=mock_angel_travel_detailed_crawler))

    # Import crawler_cli after patching
    from crawler_cli import (
        load_crawlers_config,
        save_crawlers_config,
        load_inactive_crawlers_config,
        save_inactive_crawlers_config,
        run_crawler,
        main_cli,
        CRAWLERS_CONFIG_FILE,
        INACTIVE_CRAWLERS_CONFIG_FILE
    )

    # Yield the imported functions and mocks
    yield {
        "load_crawlers_config": load_crawlers_config,
        "save_crawlers_config": save_crawlers_config,
        "load_inactive_crawlers_config": load_inactive_crawlers_config,
        "save_inactive_crawlers_config": save_inactive_crawlers_config,
        "run_crawler": run_crawler,
        "main_cli": main_cli,
        "CRAWLERS_CONFIG_FILE": CRAWLERS_CONFIG_FILE,
        "INACTIVE_CRAWLERS_CONFIG_FILE": INACTIVE_CRAWLERS_CONFIG_FILE,
        "mock_dari_tour_crawler": mock_dari_tour_crawler,
        "mock_dari_tour_detailed_crawler": mock_dari_tour_detailed_crawler,
        "mock_hotel_details_crawler": mock_hotel_details_crawler,
        "mock_angel_travel_crawler": mock_angel_travel_crawler,
        "mock_angel_travel_detailed_crawler": mock_angel_travel_detailed_crawler,
    }

    # Clean up sys.path after tests
    if project_root in sys.path:
        sys.path.remove(project_root)

# Mock crawler classes for testing purposes (these are just for type hinting in the test file itself)
class MockCrawler:
    def __init__(self, session_id="test_session", config=None, model_class=None):
        self.session_id = session_id
        self.config = config
        self.model_class = model_class

    async def crawl(self, **kwargs):
        print(f"MockCrawler {self.session_id} crawling with {kwargs}")
        if kwargs.get("raise_type_error"):
            raise TypeError("Simulated TypeError")
        if kwargs.get("raise_exception"):
            raise Exception("Simulated Exception")

class MockDetailedCrawler(MockCrawler):
    pass

# Fixture to create and clean up dummy config files
@pytest.fixture
def setup_config_files(tmp_path, monkeypatch):
    # Adjust the paths to use the temporary directory
    mock_crawlers_config_file = tmp_path / "crawlers_config.json"
    mock_inactive_crawlers_config_file = tmp_path / "inactive_crawlers.json"

    monkeypatch.setattr('crawler_cli.CRAWLERS_CONFIG_FILE', str(mock_crawlers_config_file))
    monkeypatch.setattr('crawler_cli.INACTIVE_CRAWLERS_CONFIG_FILE', str(mock_inactive_crawlers_config_file))

    # Ensure files are empty or non-existent at the start of each test
    if mock_crawlers_config_file.exists():
        mock_crawlers_config_file.unlink()
    if mock_inactive_crawlers_config_file.exists():
        mock_inactive_crawlers_config_file.unlink()

    yield

    # Clean up after tests
    if mock_crawlers_config_file.exists():
        mock_crawlers_config_file.unlink()
    if mock_inactive_crawlers_config_file.exists():
        mock_inactive_crawlers_config_file.unlink()

# Test cases for config loading and saving
def test_load_crawlers_config_empty(setup_config_files, setup_crawler_cli_mocks):
    config = setup_crawler_cli_mocks["load_crawlers_config"]()
    assert config == {}

def test_save_crawlers_config(setup_config_files, setup_crawler_cli_mocks):
    test_config = {"test_crawler": {"name": "Test Crawler", "module": "test_module", "class_name": "TestCrawlerClass"}}
    setup_crawler_cli_mocks["save_crawlers_config"](test_config)
    loaded_config = setup_crawler_cli_mocks["load_crawlers_config"]()
    assert loaded_config == test_config

def test_load_inactive_crawlers_config_empty(setup_config_files, setup_crawler_cli_mocks):
    config = setup_crawler_cli_mocks["load_inactive_crawlers_config"]()
    assert config == {}

def test_save_inactive_crawlers_config(setup_config_files, setup_crawler_cli_mocks):
    test_config = {"inactive_crawler": {"name": "Inactive Crawler", "module": "inactive_module", "class_name": "InactiveCrawlerClass"}}
    setup_crawler_cli_mocks["save_inactive_crawlers_config"](test_config)
    loaded_config = setup_crawler_cli_mocks["load_inactive_crawlers_config"]()
    assert loaded_config == test_config

# Test cases for run_crawler
@pytest.mark.asyncio
async def test_run_crawler_success(capsys, setup_crawler_cli_mocks):
    mock_crawler = MockCrawler()
    await setup_crawler_cli_mocks["run_crawler"](mock_crawler, test_param="value")
    captured = capsys.readouterr()
    assert "Running MockCrawler with config: {'test_param': 'value'}..." in captured.out
    assert "MockCrawler finished successfully." in captured.out

@pytest.mark.asyncio
async def test_run_crawler_type_error(capsys, setup_crawler_cli_mocks):
    mock_crawler = MockCrawler()
    await setup_crawler_cli_mocks["run_crawler"](mock_crawler, raise_type_error=True)
    captured = capsys.readouterr()
    assert "Error: The crawler's crawl() method does not accept the provided arguments." in captured.out

@pytest.mark.asyncio
async def test_run_crawler_general_exception(capsys, setup_crawler_cli_mocks):
    mock_crawler = MockCrawler()
    await setup_crawler_cli_mocks["run_crawler"](mock_crawler, raise_exception=True)
    captured = capsys.readouterr()
    assert "Error running MockCrawler: Simulated Exception" in captured.out

# Test cases for main_cli (simulating user input)
@pytest.mark.asyncio
async def test_main_cli_exit(monkeypatch, capsys, setup_config_files, setup_crawler_cli_mocks):
    inputs = iter(["0"])
    monkeypatch.setattr('builtins.input', lambda _: next(inputs))
    
    # Mock argparse args
    mock_args = MagicMock()
    mock_args.max_items = None

    await setup_crawler_cli_mocks["main_cli"](mock_args)
    captured = capsys.readouterr()
    assert "Exiting Crawler CLI. Goodbye!" in captured.out

'''
@pytest.mark.asyncio
async def test_main_cli_run_predefined_crawler_default_settings(monkeypatch, capsys, setup_config_files, setup_crawler_cli_mocks):
    inputs = iter(["1", "1", "0"])
    monkeypatch.setattr('builtins.input', lambda _: next(inputs))
    
    # Mock argparse args
    mock_args = MagicMock()
    mock_args.max_items = None

    await setup_crawler_cli_mocks["main_cli"](mock_args)
    captured = capsys.readouterr()
    expected_output = """
--- Crawler CLI Menu ---
1. Dari Tour Offers Crawler
2. Dari Tour Detailed Offers Crawler
3. Hotel Details Crawler
4. Angel Travel Offers Crawler
5. Angel Travel Detailed Offers Crawler
D. Delete Crawler
M. Monitor Last Run
0. Exit
Enter your choice: 1
Selected: Dari Tour Offers Crawler
1. Run with default settings
2. Configure and Run
Enter option: 1
Running Dari Tour Offers Crawler with config: {}...
DariTourCrawler finished successfully.
--- Crawler CLI Menu ---
1. Dari Tour Offers Crawler
2. Dari Tour Detailed Offers Crawler
3. Hotel Details Crawler
4. Angel Travel Offers Crawler
5. Angel Travel Detailed Offers Crawler
D. Delete Crawler
M. Monitor Last Run
0. Exit
Enter your choice: 0
Exiting Crawler CLI. Goodbye!
"""
    assert expected_output.strip() in captured.out.strip()
    setup_crawler_cli_mocks["mock_dari_tour_crawler"].crawl.assert_called_once_with()
'''

'''
@pytest.mark.asyncio
async def test_main_cli_run_predefined_crawler_with_max_items(monkeypatch, capsys, setup_config_files, setup_crawler_cli_mocks):
    inputs = iter(["1", "2", "5", "0"])
    monkeypatch.setattr('builtins.input', lambda _: next(inputs))
    
    # Mock argparse args
    mock_args = MagicMock()
    mock_args.max_items = None

    await setup_crawler_cli_mocks["main_cli"](mock_args)
    captured = capsys.readouterr()
    expected_output = """
--- Crawler CLI Menu ---
1. Dari Tour Offers Crawler
2. Dari Tour Detailed Offers Crawler
3. Hotel Details Crawler
4. Angel Travel Offers Crawler
5. Angel Travel Detailed Offers Crawler
D. Delete Crawler
M. Monitor Last Run
0. Exit
Enter your choice: 1
Selected: Dari Tour Offers Crawler
1. Run with default settings
2. Configure and Run
Enter option: 2
Enter max_items (leave empty for no limit): 5
Running Dari Tour Offers Crawler with config: {'max_items': 5}...
DariTourCrawler finished successfully.
--- Crawler CLI Menu ---
1. Dari Tour Offers Crawler
2. Dari Tour Detailed Offers Crawler
3. Hotel Details Crawler
4. Angel Travel Offers Crawler
5. Angel Travel Detailed Offers Crawler
D. Delete Crawler
M. Monitor Last Run
0. Exit
Enter your choice: 0
Exiting Crawler CLI. Goodbye!
"""
    assert expected_output.strip() in captured.out.strip()
    setup_crawler_cli_mocks["mock_dari_tour_crawler"].crawl.assert_called_once_with(max_items=5)
'''

'''
@pytest.mark.asyncio
async def test_main_cli_run_predefined_crawler_with_cli_max_items(monkeypatch, capsys, setup_config_files, setup_crawler_cli_mocks):
    inputs = iter(["1", "0"])
    monkeypatch.setattr('builtins.input', lambda _: next(inputs))
    
    # Mock argparse args
    mock_args = MagicMock()
    mock_args.max_items = 10

    await setup_crawler_cli_mocks["main_cli"](mock_args)
    captured = capsys.readouterr()
    expected_output = """
--- Crawler CLI Menu ---
1. Dari Tour Offers Crawler
2. Dari Tour Detailed Offers Crawler
3. Hotel Details Crawler
4. Angel Travel Offers Crawler
5. Angel Travel Detailed Offers Crawler
D. Delete Crawler
M. Monitor Last Run
0. Exit
Enter your choice: 1
Selected: Dari Tour Offers Crawler
Running with max_items from command line: 10
Running Dari Tour Offers Crawler with config: {'max_items': 10}...
DariTourCrawler finished successfully.
--- Crawler CLI Menu ---
1. Dari Tour Offers Crawler
2. Dari Tour Detailed Offers Crawler
3. Hotel Details Crawler
4. Angel Travel Offers Crawler
5. Angel Travel Detailed Offers Crawler
D. Delete Crawler
M. Monitor Last Run
0. Exit
Enter your choice: 0
Exiting Crawler CLI. Goodbye!
"""
    assert expected_output.strip() in captured.out.strip()
    setup_crawler_cli_mocks["mock_dari_tour_crawler"].crawl.assert_called_once_with(max_items=10)
'''

@pytest.mark.asyncio
async def test_main_cli_delete_and_readd_crawler(monkeypatch, capsys, setup_config_files, setup_crawler_cli_mocks):
    # Setup initial dynamic crawler
    initial_dynamic_config = {"dynamic_test": {"name": "Dynamic Test Crawler", "module": "crawler_cli", "class_name": "MockCrawler"}}
    setup_crawler_cli_mocks["save_crawlers_config"](initial_dynamic_config)

    # Simulate user input: D (delete), 1 (select dynamic_test), R (re-add), 1 (select dynamic_test), 0 (exit)
    inputs = iter(["D", "1", "R", "1", "0"])
    monkeypatch.setattr('builtins.input', lambda _: next(inputs))
    
    # Mock argparse args
    mock_args = MagicMock()
    mock_args.max_items = None

    # Mock importlib.import_module and getattr for dynamic loading
    with patch('importlib.import_module') as mock_import_module:
        with patch('builtins.getattr') as mock_getattr:
            mock_import_module.return_value = sys.modules[__name__] # Point to current module for MockCrawler
            mock_getattr.return_value = MockCrawler

            await setup_crawler_cli_mocks["main_cli"](mock_args)
            captured = capsys.readouterr()

            assert "Successfully moved 'Dynamic Test Crawler' to inactive list." in captured.out
            assert "Successfully re-added 'Dynamic Test Crawler'." in captured.out

            # Verify configs after operations
            active_config = setup_crawler_cli_mocks["load_crawlers_config"]()
            inactive_config = setup_crawler_cli_mocks["load_inactive_crawlers_config"]()

            assert "dynamic_test" in active_config
            assert "dynamic_test" not in inactive_config

@pytest.mark.asyncio
async def test_main_cli_monitor_last_run(monkeypatch, capsys, setup_config_files, setup_crawler_cli_mocks):
    inputs = iter(["M", "", "0"])
    monkeypatch.setattr('builtins.input', lambda _: next(inputs))
    
    # Mock argparse args
    mock_args = MagicMock()
    mock_args.max_items = None

    with patch('crawler_cli.display_csv_summary') as mock_display_csv_summary:
        with patch('crawler_cli.display_log_summary') as mock_display_log_summary:
            with patch('crawler_cli.display_directory_contents') as mock_display_directory_contents:
                await setup_crawler_cli_mocks["main_cli"](mock_args)
                captured = capsys.readouterr()

                assert "--- Monitoring Last Run ---" in captured.out
                mock_display_csv_summary.assert_called()
                mock_display_log_summary.assert_called_once()
                mock_display_directory_contents.assert_called()