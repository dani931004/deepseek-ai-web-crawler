import csv

from models.offer import Offer


def is_duplicate_offer(offer_name: str, seen_names: set) -> bool:
    return offer_name in seen_names


def is_complete_offer(offer: dict, required_keys: list) -> bool:
    return all(key in offer for key in required_keys)


def save_offers_to_csv(offers: list, filename: str):
    if not offers:
        print("No offers to save.")
        return

    # Use field names from the Offer model
    fieldnames = Offer.model_fields.keys()

    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(offers)
    print(f"Saved {len(offers)} offers to '{filename}'.")
