#!/usr/bin/env /home/dani/Desktop/Crawl4AI/deepseek-crawler/venv/bin/python3
import asyncio
import sys
import json
import importlib
import os # Import os module for path operations

from dotenv import load_dotenv

# Add the parent directory to the sys.path to allow importing crawlers
# This is crucial for the CLI to locate and import crawler modules dynamically.
sys.path.append('/home/dani/Desktop/Crawl4AI/deepseek-crawler')

from crawlers.dari_tour_crawlers import DariTourCrawler, DariTourDetailedCrawler
from crawlers.hotel_details_crawler import HotelDetailsCrawler
from crawlers.angel_travel_crawlers import AngelTravelCrawler
from crawlers.angel_travel_detailed_crawler import AngelTravelDetailedCrawler
from config import angel_travel_config, dari_tour_config
from models.angel_travel_models import AngelTravelOffer
from models.angel_travel_detailed_models import AngelTravelDetailedOffer
from models.dari_tour_models import DariTourOffer
from models.dari_tour_detailed_models import OfferDetails as DariTourDetailedOffer
from models.hotel_details_model import HotelDetails
from utils.cli_utils import display_csv_summary, display_log_summary, display_directory_contents

# Load environment variables from a .env file.
# This is typically used for sensitive information like API keys or configuration settings.
load_dotenv()

# Define file paths for storing crawler configurations.
# These files allow the CLI to persist dynamic crawler definitions and inactive crawlers across sessions.
CRAWLERS_CONFIG_FILE = '/home/dani/Desktop/Crawl4AI/deepseek-crawler/crawlers_config.json'
INACTIVE_CRAWLERS_CONFIG_FILE = '/home/dani/Desktop/Crawl4AI/deepseek-crawler/inactive_crawlers.json'

def load_crawlers_config():
    """
    Loads dynamically configured crawlers from a JSON file.

    This function checks if the configuration file exists and, if so,
    reads its content to retrieve previously saved crawler configurations.
    This allows for persistent storage of user-defined or modified crawlers.

    Returns:
        dict: A dictionary containing crawler configurations. Returns an empty
              dictionary if the file does not exist or is empty.
    """
    if not os.path.exists(CRAWLERS_CONFIG_FILE):
        return {}
    with open(CRAWLERS_CONFIG_FILE, 'r') as f:
        return json.load(f)

def save_crawlers_config(config):
    """
    Saves dynamically configured crawlers to a JSON file.

    This function writes the current crawler configurations to a specified
    JSON file, ensuring that any changes or additions made during the CLI
    session are persisted for future use. The `indent=4` argument makes
    the JSON file human-readable.

    Args:
        config (dict): The dictionary of crawler configurations to save.
    """
    with open(CRAWLERS_CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=4)

def load_inactive_crawlers_config():
    """
    Loads inactive crawlers from a JSON file.

    This function is responsible for loading crawlers that have been
    deactivated or moved to an 'inactive' state. This allows for easy
    re-activation without needing to re-configure them from scratch.

    Returns:
        dict: A dictionary containing inactive crawler configurations.
              Returns an empty dictionary if the file does not exist or is empty.
    """
    if not os.path.exists(INACTIVE_CRAWLERS_CONFIG_FILE):
        return {}
    with open(INACTIVE_CRAWLERS_CONFIG_FILE, 'r') as f:
        return json.load(f)

def save_inactive_crawlers_config(config):
    """
    Saves inactive crawlers to a JSON file.

    This function persists the list of inactive crawlers to a JSON file.
    This is useful for maintaining a record of available crawlers that are
    not currently active but can be re-enabled later.

    Args:
        config (dict): The dictionary of inactive crawler configurations to save.
    """
    with open(INACTIVE_CRAWLERS_CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=4)

async def run_crawler(crawler_instance, **kwargs):
    """
    Runs the given crawler instance with optional configurations.

    This asynchronous function executes the `crawl` method of a provided
    crawler instance. It includes error handling to catch `TypeError` if
    the `crawl` method does not support the given arguments, and a general
    `Exception` for other unexpected errors during the crawling process.

    Args:
        crawler_instance: An instance of a crawler class (e.g., DariTourCrawler).
        **kwargs: Arbitrary keyword arguments to pass to the crawler's `crawl` method.
                  This allows for flexible configuration of individual crawls.
    """
    print(f"Running {crawler_instance.__class__.__name__} with config: {kwargs}...")
    try:
        await crawler_instance.crawl(**kwargs)
        print(f"{crawler_instance.__class__.__name__} finished successfully.")
    except TypeError as e:
        # Catch TypeError specifically to provide a more informative message
        # when the crawl method's signature doesn't match the provided arguments.
        print(f"Error: The crawler's crawl() method does not accept the provided arguments. {e}")
        print("Please ensure the crawl() method of the selected crawler supports the configuration parameters.")
    except Exception as e:
        # Catch any other exceptions that might occur during the crawling process.
        print(f"Error running {crawler_instance.__class__.__name__}: {e}")

def monitor_last_run():
    """
    Monitors the last crawler run by summarizing output files and logs.

    This function provides an overview of the results from the most recent
    crawler executions. It displays summaries of generated CSV files and
    the main crawler log, as well as the contents of the output directories.
    This helps in quickly assessing the success and output of the crawls.
    """
    print("\n--- Monitoring Last Run ---")
    
    # Define paths to the output CSV files and the main crawler log.
    angel_travel_csv = '/home/dani/Desktop/Crawl4AI/deepseek-crawler/angel_travel_files/complete_offers.csv'
    dari_tour_csv = '/home/dani/Desktop/Crawl4AI/deepseek-crawler/dari_tour_files/complete_offers.csv'
    crawler_log = '/home/dani/Desktop/Crawl4AI/deepseek-crawler/output.log' # Assuming a log file here
    angel_travel_dir = '/home/dani/Desktop/Crawl4AI/deepseek-crawler/angel_travel_files/'
    dari_tour_dir = '/home/dani/Desktop/Crawl4AI/deepseek-crawler/dari_tour_files/'

    # Display summaries for each relevant file and directory.
    display_csv_summary(angel_travel_csv, "Angel Travel Offers")
    display_csv_summary(dari_tour_csv, "Dari Tour Offers")
    display_log_summary(crawler_log, "Crawler")
    display_directory_contents(angel_travel_dir, "Angel Travel Files")
    display_directory_contents(dari_tour_dir, "Dari Tour Files")

    # Pause execution to allow the user to review the displayed information.
    input("Press Enter to continue...")

async def main_cli(args):
    """
    Main function for the command-line interface (CLI).

    This function presents an interactive menu to the user, allowing them to
    select and run various web crawlers, manage dynamic crawler configurations,
    and monitor the results of previous runs. It handles user input and
    orchestrates the execution of selected actions.
    """
    # Pre-defined crawlers that are always available in the menu.
    crawlers_menu = {
        "1": {"name": "Dari Tour Offers Crawler", "instance": DariTourCrawler(session_id="dari_tour_offers", config=dari_tour_config, model_class=DariTourOffer)},
        "2": {"name": "Dari Tour Detailed Offers Crawler", "instance": DariTourDetailedCrawler(session_id="dari_tour_detailed", config=dari_tour_config, model_class=DariTourDetailedOffer)},
        "4": {"name": "Angel Travel Offers Crawler", "instance": AngelTravelCrawler(session_id="angel_travel_offers", config=angel_travel_config, model_class=AngelTravelOffer)},
        "5": {"name": "Angel Travel Detailed Offers Crawler", "instance": AngelTravelDetailedCrawler(session_id="angel_travel_detailed", config=angel_travel_config, model_class=AngelTravelDetailedOffer)},
    }

    # Load dynamically configured crawlers and inactive crawlers from their respective JSON files.
    dynamic_crawlers_config = load_crawlers_config()
    inactive_crawlers_config = load_inactive_crawlers_config()

    def rebuild_crawlers_menu():
        """
        Rebuilds the crawlers menu to include dynamically loaded crawlers.

        This nested function updates the `crawlers_menu` dictionary by adding
        any crawlers defined in `dynamic_crawlers_config`. It uses `importlib`
        to dynamically load the crawler classes based on their module and class names,
        allowing for flexible extension of the CLI's capabilities without code changes.
        """
        nonlocal crawlers_menu, dynamic_crawlers_config
        # Reset to pre-defined crawlers to avoid duplication on successive rebuilds.
        crawlers_menu = {
            "1": {"name": "Dari Tour Offers Crawler", "instance": DariTourCrawler(session_id="dari_tour_offers", config=dari_tour_config, model_class=DariTourOffer)},
            "2": {"name": "Dari Tour Detailed Offers Crawler", "instance": DariTourDetailedCrawler(session_id="dari_tour_detailed", config=dari_tour_config, model_class=DariTourDetailedOffer)},
            "3": {"name": "Hotel Details Crawler", "instance": HotelDetailsCrawler(session_id="hotel_details", config=dari_tour_config, model_class=HotelDetails)},
            "4": {"name": "Angel Travel Offers Crawler", "instance": AngelTravelCrawler(session_id="angel_travel_offers", config=angel_travel_config, model_class=AngelTravelOffer)},
            "5": {"name": "Angel Travel Detailed Offers Crawler", "instance": AngelTravelDetailedCrawler(session_id="angel_travel_detailed", config=angel_travel_config, model_class=AngelTravelDetailedOffer)},
        }
        next_key = len(crawlers_menu) + 1 # Determine the next available key for dynamic crawlers.
        for key, crawler_info in dynamic_crawlers_config.items():
            try:
                # Dynamically import the module and get the crawler class.
                module = importlib.import_module(crawler_info["module"])
                crawler_class = getattr(module, crawler_info["class_name"])
                # Add the dynamically loaded crawler to the menu.
                crawlers_menu[str(next_key)] = {"name": crawler_info["name"], "instance": crawler_class()}
                next_key += 1
            except Exception as e:
                # Log any errors encountered during dynamic crawler loading to aid debugging.
                print(f"Error loading dynamic crawler {crawler_info['name']}: {e}")

    # Initial build of the menu to include any previously configured dynamic crawlers.
    rebuild_crawlers_menu()

    while True:
        print("\n--- Crawler CLI Menu ---")
        # Display all available crawlers, both pre-defined and dynamically loaded.
        for key, value in crawlers_menu.items():
            print(f"{key}. {value['name']}")
        # Provide options for managing inactive crawlers if any exist.
        if inactive_crawlers_config:
            print("R. Re-add Inactive Crawler")
        print("D. Delete Crawler")
        print("M. Monitor Last Run")
        print("0. Exit")

        choice = input("Enter your choice: ").strip().upper()

        if choice == "0":
            # Exit the CLI.
            print("Exiting Crawler CLI. Goodbye!")
            break
        elif choice == "M":
            # Call the function to display monitoring information.
            monitor_last_run()
        elif choice == "D":
            # Handle crawler deletion.
            print("\n--- Delete Crawler ---")
            if not dynamic_crawlers_config:
                print("No dynamic crawlers to delete.")
                continue

            print("Select a crawler to delete:")
            deletable_crawlers = {}
            i = 1
            # List dynamic crawlers for selection.
            for key, crawler_info in dynamic_crawlers_config.items():
                deletable_crawlers[str(i)] = key # Map display number to actual config key for deletion.
                print(f"{i}. {crawler_info['name']}")
                i += 1
            
            delete_choice = input("Enter the number of the crawler to delete (0 to cancel): ").strip()

            if delete_choice == "0":
                print("Deletion cancelled.")
                continue

            if delete_choice in deletable_crawlers:
                config_key_to_delete = deletable_crawlers[delete_choice]
                deleted_crawler_info = dynamic_crawlers_config[config_key_to_delete]
                # Remove the crawler from the active configuration.
                del dynamic_crawlers_config[config_key_to_delete]
                save_crawlers_config(dynamic_crawlers_config)

                # Move the deleted crawler to the inactive list for potential re-addition.
                inactive_crawlers_config[config_key_to_delete] = deleted_crawler_info
                save_inactive_crawlers_config(inactive_crawlers_config)

                # Rebuild the menu to reflect the changes.
                rebuild_crawlers_menu()

                print(f"Successfully moved '{deleted_crawler_info['name']}' to inactive list.")
            else:
                print("Invalid choice. Please try again.")
        elif choice == "R":
            # Handle re-adding inactive crawlers.
            print("\n--- Re-add Inactive Crawler ---")
            if not inactive_crawlers_config:
                print("No inactive crawlers to re-add.")
                continue

            print("Select an inactive crawler to re-add:")
            readdable_crawlers = {}
            i = 1
            # List inactive crawlers for selection.
            for key, crawler_info in inactive_crawlers_config.items():
                readdable_crawlers[str(i)] = key # Map display number to actual config key for re-addition.
                print(f"{i}. {crawler_info['name']}")
                i += 1
            
            readd_choice = input("Enter the number of the crawler to re-add (0 to cancel): ").strip()

            if readd_choice == "0":
                print("Re-addition cancelled.")
                continue

            if readd_choice in readdable_crawlers:
                config_key_to_readd = readdable_crawlers[readd_choice]
                readded_crawler_info = inactive_crawlers_config[config_key_to_readd]
                # Remove the crawler from the inactive list.
                del inactive_crawlers_config[config_key_to_readd]
                save_inactive_crawlers_config(inactive_crawlers_config)

                # Move the crawler back to the active configuration.
                dynamic_crawlers_config[config_key_to_readd] = readded_crawler_info
                save_crawlers_config(dynamic_crawlers_config)

                # Rebuild the menu to reflect the changes.
                rebuild_crawlers_menu()

                print(f"Successfully re-added '{readded_crawler_info['name']}'.")
            else:
                print("Invalid choice. Please try again.")
        elif choice in crawlers_menu:
            # Execute the selected crawler.
            selected_crawler = crawlers_menu[choice]["instance"]
            config_kwargs = {}

            print(f"\nSelected: {crawlers_menu[choice]['name']}")
            
            # Run the chosen crawler with the determined configuration.
            await run_crawler(selected_crawler, **config_kwargs)
        else:
            # Handle invalid menu choices.
            print("Invalid choice. Please try again.")

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a web crawler.")
    parser.add_argument("--max_items", type=int, help="Maximum number of items to process.")
    args = parser.parse_args()

    # Entry point for the CLI script.
    # `asyncio.run()` is used to execute the main asynchronous CLI function.
    asyncio.run(main_cli(args))