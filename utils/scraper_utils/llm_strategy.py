"""
LLM strategy configuration for the web crawler.
"""
import os
from typing import Type, Any
from crawl4ai import LLMExtractionStrategy

def get_llm_strategy(model: Type[Any]) -> LLMExtractionStrategy:
    """
    Returns the configuration for the language model extraction strategy.
    Implements rate limiting for Groq's 6000 TPM (tokens per minute) limit.

    Args:
        model: The Pydantic model class that defines the schema for extraction
        
    Returns:
        LLMExtractionStrategy: The settings for how to extract data using LLM.
    """
    # Get the model's JSON schema which includes field descriptions
    schema = model.model_json_schema()
    
    # Generate a dynamic instruction based on the model's schema
    field_descriptions = []
    required_fields = schema.get('required', [])
    
    for field_name, field_info in schema.get('properties', {}).items():
        is_required = field_name in required_fields
        description = field_info.get('description', 'No description available')
        field_descriptions.append(f"- {field_name}: {description} {'(required)' if is_required else ''}")
    
    instruction = (
        "Carefully analyze the HTML content and extract structured data according to the following schema:\n\n"
        "Fields to extract (with descriptions):\n" + 
        "\n".join(field_descriptions) + 
        "\n\n"
        "Important guidelines:\n"
        "- Extract ALL available items from the content\n"
        "- If a field is not available, leave it as an empty string\n"
        "- Ensure all required fields are included in each item\n"
        "- For prices, include the currency symbol if visible\n"
        "- For dates, use the exact format found on the page\n"
        "- If no items are found, return an empty array"
    )
    
    # Configure chunking strategy
    chunking_config = {
        'strategy': 'semantic',  # Use semantic chunking
        'chunk_size': 500,       # Smaller chunks to stay under token limits
        'chunk_overlap': 50,     # Small overlap to maintain context
        'max_chars': 2000,       # Hard limit on chunk size
        'split_on_headers': True, # Split on HTML headers
        'respect_sentence_boundary': True
    }
    
    return LLMExtractionStrategy(
        provider="groq/llama3-8b-8192",  # Using a smaller, faster model
        model=model,
        api_key=os.getenv("GROQ_API_KEY"),
        schema=schema,
        extraction_type="schema",
        instruction=instruction,
        input_format="html",
        max_tokens=500,  # Reduced max tokens
        temperature=0.1,  # More deterministic output
        retry_attempts=2,
        retry_delay=5,
        rate_limit={
            'tokens_per_minute': 30000,  # Higher token limit for smaller model
            'requests_per_minute': 10,   # More requests allowed
            'tokens_per_request': 1000   # Smaller chunks per request
        },
        extract_rules={
            'chunking': chunking_config,
            'extraction_type': 'structured',
            'output_format': 'json',
            'required_fields': [
                'title',
                'date',
                'price',
                'transport_type',
                'link'
            ],
            'allow_partial': True,
            'error_handling': 'skip',
            'extract_individual_elements': True  # Extract each offer separately
        },
        content_extraction={
            'extract_main_content': False,  # We're already using CSS selector
            'ignore_boilerplate': True,
            'min_text_length': 50,  # Lower threshold for offer items
            'max_text_length': 1000,  # Smaller max length
            'include_links': True,
            'include_images': False,
            'include_tables': False
        },
        optimization={
            'remove_duplicate_blocks': True,
            'merge_short_blocks': False,  # Keep offers separate
            'max_blocks_per_page': 20,   # More blocks for individual offers
            'sort_blocks': 'position'    # Process blocks in original order
        }
    )
