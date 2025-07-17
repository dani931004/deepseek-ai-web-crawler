"""
Content processing utilities for the web crawler.
"""
import time
import asyncio
from typing import Any, Callable, List, Optional
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, LLMExtractionStrategy, CacheMode, BrowserConfig

async def process_page_content(
    content: str,
    llm_strategy: LLMExtractionStrategy,
    required_keys: List[str],
    seen_names: set,
    base_url: str,
    crawler: AsyncWebCrawler,
    max_tokens_per_chunk: int = 4000,
    tokens_per_minute: int = 5500,
    verbose: bool = True
) -> List[dict]:
    """
    Process page content in chunks with rate limiting.
    
    Args:
        content: The HTML content to process
        llm_strategy: The LLM extraction strategy
        required_keys: List of required keys in the offer data
        seen_names: Set of already seen offer names
        base_url: The base URL for the content
        browser_config: Configuration for the browser
        max_tokens_per_chunk: Maximum tokens per chunk
        tokens_per_minute: Maximum tokens per minute
        verbose: Whether to print progress information
        
    Returns:
        List of processed offers
    """
    async def process_chunk(chunk: str, crawler: AsyncWebCrawler) -> List[dict]:
        """Process a single chunk of HTML content"""
        try:
            # Create a temporary crawler config for this chunk
            temp_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                extraction_strategy=llm_strategy,
                session_id=f"chunk_{hash(chunk) % 1000:03d}"
            )
            
            # Process the chunk with the provided base URL and browser config
            try:
                result = await crawler.arun(
                    url=base_url,
                    config=temp_config,
                    html_content=chunk
                )
            
                if not result or not hasattr(result, 'success') or not result.success:
                    error_msg = getattr(result, 'error_message', 'Unknown error')
                    if verbose:
                        print(f"[WARNING] Failed to process chunk: {error_msg}")
                    return []
                    
                # Process the extracted data
                from .data_processor import process_extracted_data
                
                # Debug the result structure if needed
                if verbose and hasattr(result, 'extracted_content'):
                    print(f"[DEBUG] Extracted content type: {type(result.extracted_content)}")
                    print(f"[DEBUG] Extracted content preview: {str(result.extracted_content)[:500]}...")
                
                offers, _ = await process_extracted_data(
                    result, 
                    required_keys=required_keys,
                    seen_values=seen_names,
                    verbose=verbose
                )
                return offers if isinstance(offers, list) else []
                
            except Exception as e:
                if verbose:
                    print(f"[ERROR] Error in process_chunk: {str(e)}")
                    import traceback
                    traceback.print_exc()
                return []
                
        except Exception as e:
            if verbose:
                print(f"Error in chunk processing: {str(e)}")
            return []
    
    try:
        # Use the provided crawler instance
        from functools import partial
        process_chunk_with_crawler = partial(process_chunk, crawler=crawler)
        
        # Use the chunk processor to handle the content with a smaller chunk size
        results = await process_text_in_chunks(
            text=content,
            process_func=process_chunk_with_crawler,
            max_tokens_per_chunk=2000,  # Reduced from 4000 to 2000
            tokens_per_minute=tokens_per_minute,
            verbose=verbose
        )
        
        # Flatten the list of lists and filter out None results
        return [offer for sublist in results if sublist is not None for offer in sublist]
    except Exception as e:
        if verbose:
            print(f"[ERROR] Error processing page content: {str(e)}")
        return []

async def process_text_in_chunks(
    text: str,
    process_func: Callable[[str], Any],
    max_tokens_per_chunk: int = 4000,  # Conservative chunk size
    tokens_per_minute: int = 5500,     # Stay under 6000 TPM
    verbose: bool = True
) -> List[Any]:
    """
    Process large text in chunks with rate limiting to respect token limits.
    
    Args:
        text: The input text to process
        process_func: A function that processes a text chunk and returns the result
        max_tokens_per_chunk: Maximum tokens per chunk (default: 4000)
        tokens_per_minute: Maximum tokens per minute (default: 5500)
        verbose: Whether to print progress information
        
    Returns:
        List of processed results from all chunks
    """
    # Simple token estimation (4 chars ~= 1 token)
    def estimate_tokens(t: str) -> int:
        return max(1, len(t) // 4)
    
    # Split text into chunks based on token count
    def split_into_chunks(t: str, max_tokens: int) -> List[str]:
        chunks = []
        words = t.split()
        current_chunk = []
        current_tokens = 0
        
        for word in words:
            word_tokens = estimate_tokens(word)
            if current_tokens + word_tokens > max_tokens and current_chunk:
                chunks.append(' '.join(current_chunk))
                current_chunk = []
                current_tokens = 0
            
            current_chunk.append(word)
            current_tokens += word_tokens
        
        if current_chunk:
            chunks.append(' '.join(current_chunk))
            
        return chunks
    
    chunks = split_into_chunks(text, max_tokens_per_chunk)
    if not chunks:
        return []
    
    if verbose:
        print(f"Processing {len(chunks)} chunks with max {max_tokens_per_chunk} tokens each")
    
    results = []
    tokens_used_in_minute = 0
    minute_start = time.time()
    
    for i, chunk in enumerate(chunks, 1):
        # Check if we need to wait to respect rate limits
        current_time = time.time()
        minute_elapsed = current_time - minute_start
        
        if minute_elapsed < 60:  # Still in the same minute
            if tokens_used_in_minute >= tokens_per_minute:
                # Wait until next minute
                wait_time = 60 - minute_elapsed + 1  # Add 1 second buffer
                if verbose:
                    print(f"Rate limit reached. Waiting {wait_time:.1f} seconds...")
                await asyncio.sleep(wait_time)
                # Reset counters after waiting
                tokens_used_in_minute = 0
                minute_start = time.time()
        else:
            # New minute, reset counters
            tokens_used_in_minute = 0
            minute_start = current_time
        
        # Process the chunk
        if verbose:
            print(f"Processing chunk {i}/{len(chunks)}")
        
        try:
            result = await process_func(chunk)
            results.append(result)
            
            # Update token usage (estimate)
            chunk_tokens = estimate_tokens(chunk)
            tokens_used_in_minute += chunk_tokens
            
            # Add small delay between chunks to avoid overwhelming the API
            await asyncio.sleep(0.5)
            
        except Exception as e:
            if verbose:
                print(f"Error processing chunk {i}: {str(e)}")
            continue
    
    return results
