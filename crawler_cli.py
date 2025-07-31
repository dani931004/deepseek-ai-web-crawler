#!/usr/bin/env /home/dani/Desktop/Crawl4AI/deepseek-crawler/venv/bin/python3
import asyncio
import sys
import json
import importlib
from dotenv import load_dotenv

# Add the parent directory to the sys.path to allow importing crawlers
sys.path.append('/home/dani/Desktop/Crawl4AI/deepseek-crawler')

from crawlers.dari_tour_crawlers import DariTourCrawler, DariTourDetailedCrawler
from crawlers.hotel_details_crawler import HotelDetailsCrawler
from crawlers.angel_travel_crawlers import AngelTravelCrawler
from crawlers.angel_travel_detailed_crawler import AngelTravelDetailedCrawler
from utils.cli_utils import display_csv_summary, display_log_summary, display_directory_contents

load_dotenv()

CRAWLERS_CONFIG_FILE = '/home/dani/Desktop/Crawl4AI/deepseek-crawler/crawlers_config.json'
INACTIVE_CRAWLERS_CONFIG_FILE = '/home/dani/Desktop/Crawl4AI/deepseek-crawler/inactive_crawlers.json'

def load_crawlers_config():
    """Loads dynamically configured crawlers from a JSON file."""
    if not os.path.exists(CRAWLERS_CONFIG_FILE):
        return {}
    with open(CRAWLERS_CONFIG_FILE, 'r') as f:
        return json.load(f)

def save_crawlers_config(config):
    """Saves dynamically configured crawlers to a JSON file."""
    with open(CRAWLERS_CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=4)

def load_inactive_crawlers_config():
    """Loads inactive crawlers from a JSON file."""
    if not os.path.exists(INACTIVE_CRAWLERS_CONFIG_FILE):
        return {}
    with open(INACTIVE_CRAWLERS_CONFIG_FILE, 'r') as f:
        return json.load(f)

def save_inactive_crawlers_config(config):
    """Saves inactive crawlers to a JSON file."""
    with open(INACTIVE_CRAWLERS_CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=4)

async def run_crawler(crawler_instance, **kwargs):
    """Runs the given crawler instance with optional configurations."""
    print(f"Running {crawler_instance.__class__.__name__} with config: {kwargs}...")
    try:
        await crawler_instance.crawl(**kwargs)
        print(f"{crawler_instance.__class__.__name__} finished successfully.")
    except TypeError as e:
        print(f"Error: The crawler's crawl() method does not accept the provided arguments. {e}")
        print("Please ensure the crawl() method of the selected crawler supports the configuration parameters.")
    except Exception as e:
        print(f"Error running {crawler_instance.__class__.__name__}: {e}")

def monitor_last_run():
    """Monitors the last crawler run by summarizing output files and logs."""
    print("\n--- Monitoring Last Run ---")
    
    angel_travel_csv = '/home/dani/Desktop/Crawl4AI/deepseek-crawler/angel_travel_files/complete_offers.csv'
    dari_tour_csv = '/home/dani/Desktop/Crawl4AI/deepseek-crawler/dari_tour_files/complete_offers.csv'
    crawler_log = '/home/dani/Desktop/Crawl4AI/deepseek-crawler/logs/crawler.log' # Assuming a log file here
    angel_travel_dir = '/home/dani/Desktop/Crawl4AI/deepseek-crawler/angel_travel_files/'
    dari_tour_dir = '/home/dani/Desktop/Crawl4AI/deepseek-crawler/dari_tour_files/'

    display_csv_summary(angel_travel_csv, "Angel Travel Offers")
    display_csv_summary(dari_tour_csv, "Dari Tour Offers")
    display_log_summary(crawler_log, "Crawler")
    display_directory_contents(angel_travel_dir, "Angel Travel Files")
    display_directory_contents(dari_tour_dir, "Dari Tour Files")

    input("Press Enter to continue...") # Pause for user to read



async def main_cli():
    """Main function for the CLI."""
    # Pre-defined crawlers
    crawlers_menu = {
        "1": {"name": "Dari Tour Offers Crawler", "instance": DariTourCrawler()},
        "2": {"name": "Dari Tour Detailed Offers Crawler", "instance": DariTourDetailedCrawler()},
        "3": {"name": "Hotel Details Crawler", "instance": HotelDetailsCrawler()},
        "4": {"name": "Angel Travel Offers Crawler", "instance": AngelTravelCrawler()},
        "5": {"name": "Angel Travel Detailed Offers Crawler", "instance": AngelTravelDetailedCrawler()},
    }

    # Load dynamically configured crawlers
    dynamic_crawlers_config = load_crawlers_config()
    inactive_crawlers_config = load_inactive_crawlers_config()

    def rebuild_crawlers_menu():
        nonlocal crawlers_menu, dynamic_crawlers_config
        crawlers_menu = {
            "1": {"name": "Dari Tour Offers Crawler", "instance": DariTourCrawler()},
            "2": {"name": "Dari Tour Detailed Offers Crawler", "instance": DariTourDetailedCrawler()},
            "3": {"name": "Hotel Details Crawler", "instance": HotelDetailsCrawler()},
            "4": {"name": "Angel Travel Offers Crawler", "instance": AngelTravelCrawler()},
            "5": {"name": "Angel Travel Detailed Offers Crawler", "instance": AngelTravelDetailedCrawler()},
        }
        next_key = len(crawlers_menu) + 1
        for key, crawler_info in dynamic_crawlers_config.items():
            try:
                module = importlib.import_module(crawler_info["module"])
                crawler_class = getattr(module, crawler_info["class_name"])
                crawlers_menu[str(next_key)] = {"name": crawler_info["name"], "instance": crawler_class()}
                next_key += 1
            except Exception as e:
                print(f"Error loading dynamic crawler {crawler_info['name']}: {e}")

    rebuild_crawlers_menu()

    while True:
        print("\n--- Crawler CLI Menu ---")
        for key, value in crawlers_menu.items():
            print(f"{key}. {value['name']}")
        if inactive_crawlers_config:
            print("R. Re-add Inactive Crawler")
        print("D. Delete Crawler")
        print("M. Monitor Last Run")
        print("0. Exit")

        choice = input("Enter your choice: ").strip().upper()

        if choice == "0":
            print("Exiting Crawler CLI. Goodbye!")
            break
        elif choice == "M":
            monitor_last_run()
        elif choice == "D":
            print("\n--- Delete Crawler ---")
            if not dynamic_crawlers_config:
                print("No dynamic crawlers to delete.")
                continue

            print("Select a crawler to delete:")
            deletable_crawlers = {}
            i = 1
            for key, crawler_info in dynamic_crawlers_config.items():
                deletable_crawlers[str(i)] = key # Map display number to actual config key
                print(f"{i}. {crawler_info['name']}")
                i += 1
            
            delete_choice = input("Enter the number of the crawler to delete (0 to cancel): ").strip()

            if delete_choice == "0":
                print("Deletion cancelled.")
                continue

            if delete_choice in deletable_crawlers:
                config_key_to_delete = deletable_crawlers[delete_choice]
                deleted_crawler_info = dynamic_crawlers_config[config_key_to_delete]
                del dynamic_crawlers_config[config_key_to_delete]
                save_crawlers_config(dynamic_crawlers_config)

                # Move to inactive crawlers
                inactive_crawlers_config[config_key_to_delete] = deleted_crawler_info
                save_inactive_crawlers_config(inactive_crawlers_config)

                rebuild_crawlers_menu()

                print(f"Successfully moved '{deleted_crawler_info['name']}' to inactive list.")
            else:
                print("Invalid choice. Please try again.")
        elif choice == "R":
            print("\n--- Re-add Inactive Crawler ---")
            if not inactive_crawlers_config:
                print("No inactive crawlers to re-add.")
                continue

            print("Select an inactive crawler to re-add:")
            readdable_crawlers = {}
            i = 1
            for key, crawler_info in inactive_crawlers_config.items():
                readdable_crawlers[str(i)] = key # Map display number to actual config key
                print(f"{i}. {crawler_info['name']}")
                i += 1
            
            readd_choice = input("Enter the number of the crawler to re-add (0 to cancel): ").strip()

            if readd_choice == "0":
                print("Re-addition cancelled.")
                continue

            if readd_choice in readdable_crawlers:
                config_key_to_readd = readdable_crawlers[readd_choice]
                readded_crawler_info = inactive_crawlers_config[config_key_to_readd]
                del inactive_crawlers_config[config_key_to_readd]
                save_inactive_crawlers_config(inactive_crawlers_config)

                # Move back to active crawlers
                dynamic_crawlers_config[config_key_to_readd] = readded_crawler_info
                save_crawlers_config(dynamic_crawlers_config)

                rebuild_crawlers_menu()

                print(f"Successfully re-added '{readded_crawler_info['name']}'.")
            else:
                print("Invalid choice. Please try again.")
        elif choice in crawlers_menu:
            selected_crawler = crawlers_menu[choice]["instance"]
            config_kwargs = {}

            print(f"\nSelected: {crawlers_menu[choice]['name']}")
            run_option = input("1. Run with default settings\n2. Configure and Run\nEnter option: ").strip()

            if run_option == "2":
                if isinstance(selected_crawler, DariTourCrawler):
                    try:
                        max_items_str = input("Enter max_items (leave empty for no limit): ").strip()
                        if max_items_str:
                            config_kwargs["max_items"] = int(max_items_str)
                    except ValueError:
                        print("Invalid input for max_items. Using default (no limit).")
                else:
                    print("No specific configuration options available for this crawler yet.")
            elif run_option != "1":
                print("Invalid option. Running with default settings.")

            await run_crawler(selected_crawler, **config_kwargs)
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    # Ensure the 'os' module is imported for os.path.exists
    import os 
    asyncio.run(main_cli())