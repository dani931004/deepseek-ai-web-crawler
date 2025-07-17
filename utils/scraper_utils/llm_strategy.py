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
    # Calculate max tokens per request to stay under 6000 TPM
    # Using conservative values to avoid rate limits
    max_tokens_per_request = 2000  # Increased from 500 to allow for larger responses
    requests_per_minute = 3  # Reduced from 10 to stay well under rate limits
    
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
    
    return LLMExtractionStrategy(
        provider="groq/deepseek-r1-distill-llama-70b",
        api_token=os.getenv("GROQ_API_KEY"),
        schema=schema,
        extraction_type="schema",
        instruction=instruction,
        input_format="html",
        verbose=True,
        max_tokens=1000,  # Limit response size
        temperature=0.2,
        top_p=0.95,
        frequency_penalty=0.1,
        presence_penalty=0.1,
        retry_attempts=2,  # Reduced retries to fail faster
        retry_delay=5,     # Shorter delay between retries
        request_timeout=60, # Reduced timeout
        rate_limit={
            'tokens_per_minute': 3000,  # More conservative limit
            'requests_per_minute': requests_per_minute,
            'tokens_per_request': max_tokens_per_request
        },
        extract_rules={
            'strict_mode': False,  # Allow partial matches
            'allow_partial': True,  # Accept partial results
            'max_retries': 1,      # Fewer retries to avoid rate limits
        },
        # Additional parameters to reduce input size
        content_extraction={
            'extract_main_content': True,  # Focus on main content
            'ignore_boilerplate': True,    # Ignore headers/footers
            'min_text_length': 100,        # Skip very short texts
            'max_text_length': 4000,       # Limit input size
            'include_links': False,        # Don't include links in extraction
            'include_images': False        # Don't include images
        }
    )
