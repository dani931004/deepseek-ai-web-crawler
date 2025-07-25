import csv
import json
import os
import re

def sanitize_filename(filename: str) -> str:
    """Sanitizes a string to be safe for use as a filename.
    Replaces invalid characters with underscores.
    """
    # Replace any character that is not a letter, number, hyphen, underscore, or dot with an underscore
    sanitized = re.sub(r'[^\w\-.]', '_', filename)
    # Remove leading/trailing underscores or hyphens
    sanitized = sanitized.strip('_-')
    return sanitized

def is_duplicate_offer(offer_name: str, seen_names: set) -> bool:
    return offer_name in seen_names


def is_complete_offer(offer: dict, required_keys: list) -> bool:
    return all(key in offer for key in required_keys)


def save_offers_to_csv(offers: list, filename: str, model: type):
    if not offers:
        print("No offers to save.")
        return

    # Use field names from the DariTourOffer model
    fieldnames = list(model.model_fields.keys())
    
    # Create a copy of each offer without the 'error' field
    cleaned_offers = []
    for offer in offers:
        cleaned_offer = {k: v for k, v in offer.items() if k in fieldnames}
        cleaned_offers.append(cleaned_offer)

    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(cleaned_offers)
    print(f"Saved {len(cleaned_offers)} offers to '{filename}'.")
    return cleaned_offers

def save_to_json(data, filename: str):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
