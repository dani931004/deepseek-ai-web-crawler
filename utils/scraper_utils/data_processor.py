"""
Data processing utilities for the web crawler.
"""
import json
from typing import Dict, List, Set, Tuple, Any, Optional

def clean_value(value: Any) -> str:
    """Clean and convert a value to string, handling None and empty values."""
    if value is None:
        return ''
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value).strip()

async def process_extracted_data(
    result: Any, 
    required_keys: List[str], 
    unique_key: str = 'name', 
    seen_values: Optional[Set[str]] = None,
    verbose: bool = True
) -> Tuple[List[Dict[str, str]], bool]:
    """
    Process the extracted data from the crawler result for any structured model.
    
    Args:
        result: The result object from the crawler
        required_keys: List of required keys for each item
        unique_key: The key to use for detecting duplicates (default: 'name')
        seen_values: Set of already seen unique values to avoid duplicates
        verbose: Whether to print debug information
        
    Returns:
        Tuple of (list of processed items, no_results_flag)
    """
    seen_values = seen_values or set()
    processed_items = []
    
    try:
        # Handle case where result is already a list or dict
        if isinstance(result, (list, dict)):
            extracted_data = result
            if verbose:
                print("[DEBUG] Using result directly as extracted data")
        # Handle case where we have an object with extracted_content
        elif hasattr(result, 'extracted_content'):
            if verbose:
                print("\n[DEBUG] Raw extracted content:")
                content_preview = str(result.extracted_content)[:500]
                print(f"{content_preview}..." if len(str(result.extracted_content)) > 500 else content_preview)
                print("\n[DEBUG] Extracted content type:", type(result.extracted_content))
            
            # Parse the extracted data
            try:
                if isinstance(result.extracted_content, str):
                    extracted_data = json.loads(result.extracted_content)
                else:
                    extracted_data = result.extracted_content
                    
                if verbose:
                    print("\n[DEBUG] Parsed data:", 
                          str(extracted_data)[:500] + ('...' if len(str(extracted_data)) > 500 else ''))
                    
            except (json.JSONDecodeError, TypeError) as e:
                if verbose:
                    print(f"\n[ERROR] Failed to parse extracted content: {e}")
                return [], False
        else:
            if verbose:
                print(f"[WARNING] Unexpected result type: {type(result)}")
            return [], False
            
        # Ensure we have a list to process
        if not isinstance(extracted_data, list):
            if isinstance(extracted_data, dict):
                extracted_data = [extracted_data]
            else:
                if verbose:
                    print(f"[WARNING] Expected list or dict, got {type(extracted_data)}")
                return [], False
        
        # Process the extracted items
        processed_items = []
        for item in extracted_data:
            # Skip if item is not a dictionary
            if not isinstance(item, dict):
                continue
                
            # Skip if any required key is missing
            if not all(key in item for key in required_keys):
                continue
                
            # Clean and process all values
            processed_item = {
                k: clean_value(v) 
                for k, v in item.items()
                if v is not None and v != ''  # Skip None and empty values
            }
            
            # Skip if the unique key is missing or empty
            unique_value = processed_item.get(unique_key)
            if not unique_value:
                continue
                
            # Skip duplicates
            if unique_value in seen_values:
                continue
                
            seen_values.add(unique_value)
            processed_items.append(processed_item)
        
        return processed_items, False
        
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON data: {str(e)}")
        return [], False
    except Exception as e:
        print(f"Error processing extracted data: {str(e)}")
        return [], False
