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
    
    # Configure chunking strategy - very conservative to minimize token usage
    chunking_config = {
        'strategy': 'semantic',
        'chunk_size': 150,    # Very small chunks to stay well under token limits
        'chunk_overlap': 20,  # Small overlap to maintain context
        'max_chars': 500,     # Strict max chars per chunk
        'split_on_headers': False,
        'respect_sentence_boundary': True,
        'min_chunk_size': 50  # Ensure we don't create too many tiny chunks
    }
    
    # Create the LLM extraction strategy with optimized settings
    return LLMExtractionStrategy(
        provider="groq/llama3-8b-8192",
        model=model,
        api_key=os.getenv("GROQ_API_KEY"),
        schema=schema,
        extraction_type="schema",
        instruction=instruction,
        max_tokens=200,         # Very limited tokens per request
        temperature=0.1,        # More deterministic output
        top_p=0.85,             # More focused sampling
        frequency_penalty=0.1,  # Discourage repetition
        presence_penalty=0.1,   # Encourage diversity
        retry_attempts=2,       # Fewer retries to avoid rate limits
        retry_delay=15,         # Longer delay between retries
        request_timeout=120,     # Longer timeout for slower responses
        rate_limit={
            'tokens_per_minute': 1000,  # Very conservative token limit
            'requests_per_minute': 3,   # Very few requests per minute
            'tokens_per_request': 200   # Very small chunks per request
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
            'extract_individual_elements': True
        },
        content_extraction={
            'extract_main_content': False,
            'ignore_boilerplate': True,
            'min_text_length': 50,    # Slightly higher to avoid tiny fragments
            'max_text_length': 500,   # Much smaller max length
            'include_links': False,   # Exclude links to reduce tokens
            'include_images': False,  # No images
            'include_tables': False,  # No tables
            'simplify_html': True,    # Simplify HTML structure
            'remove_empty_nodes': True,  # Remove empty elements
            'normalize_whitespace': True  # Clean up whitespace
        },
        optimization={
            'remove_duplicate_blocks': True,
            'merge_short_blocks': False,
            'max_blocks_per_page': 10,  # Fewer blocks to process
            'sort_blocks': 'position'
        }
    )
