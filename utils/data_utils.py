import csv
import json
import os
import re
import logging

def slugify(text: str) -> str:
    """
    Converts a given string into a URL-friendly slug.
    Handles Cyrillic characters, replaces non-alphanumeric characters with hyphens,
    and cleans up multiple/leading/trailing hyphens.
    """
    # Convert the input text to lowercase to ensure consistency.
    text = text.lower()
    # Define a mapping for Cyrillic characters to their Latin equivalents.
    cyrillic_to_latin = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ж': 'zh', 'з': 'z',
        'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o', 'п': 'p',
        'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch',
        'ш': 'sh', 'щ': 'sht', 'ъ': 'a', 'ь': 'y', 'ю': 'yu', 'я': 'ya'
    }
    # Replace Cyrillic characters based on the defined mapping.
    for cyr, lat in cyrillic_to_latin.items():
        text = text.replace(cyr, lat)

    # Replace any non-alphanumeric characters (excluding hyphens) with a single hyphen.
    text = re.sub(r'[\s/\\_.,;:\'"()[\]{}|!@#$%^&*+=?<>~`]+|-', '-', text)
    # Remove any leading or trailing hyphens that might have resulted from the replacement.
    text = text.strip('-')
    # Replace multiple consecutive hyphens with a single hyphen to clean up the slug.
    text = re.sub(r'-+', '-', text)
    return text

def sanitize_filename(filename: str) -> str:
    """
    Sanitizes a string to be used as a safe filename.
    Removes invalid characters and limits length.
    """
    # Remove invalid characters
    sanitized = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '', filename)
    # Replace spaces with underscores
    sanitized = sanitized.replace(' ', '_')
    # Limit filename length to 200 characters to avoid OS limitations
    return sanitized[:200]

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
    logging.info(f"Saving {len(cleaned_offers)} offers to '{filename}'.")
    print(f"Saved {len(cleaned_offers)} offers to '{filename}'.")
    return cleaned_offers

def save_to_json(data, filename: str):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    logging.info(f"Saving data to '{filename}'.")
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
