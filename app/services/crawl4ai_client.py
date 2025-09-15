"""
Crawl4AI client wrapper for structured data extraction from web pages.
Configures WebCrawler + LLMExtractionStrategy with OpenAI-compatible provider.
"""

import asyncio
import logging
import sys
from typing import Any, Dict, Optional, Union

import httpx

logger = logging.getLogger(__name__)

# Try to import crawl4ai, but allow graceful degradation if not available
try:
    import crawl4ai
    from crawl4ai.extractors import LLMExtractionStrategy
    from crawl4ai.crawlers import WebCrawler
    CRAWL4AI_AVAILABLE = True
except ImportError:
    logger.warning("crawl4ai package not available, extraction will fall back to other methods")
    CRAWL4AI_AVAILABLE = False

# Global configuration for Crawl4AI
_CRAWL4AI_CONFIG = {
    "base_url": "https://api.x.ai/v1",
    "api_token": None,
    "model": "grok-2",
    "timeout": 60.0,
    "configured": False
}

# Business data schema for extraction
BUSINESS_SCHEMA = {
    "type": "object",
    "properties": {
        "business_name": {"type": "string"},
        "years_in_business": {"type": ["integer", "string", "null"], "minimum": 0},
        "rating": {"type": ["number", "null"], "minimum": 0, "maximum": 5},
        "review_count": {"type": ["integer", "null"], "minimum": 0},
        "industry": {"type": ["string", "null"]},
        "phone": {"type": ["string", "null"]},
        "website": {"type": ["string", "null"]},
        "email": {"type": ["string", "null"]}
    },
    "required": ["business_name"]
}


def configure_crawl4ai(
    base_url: str,
    api_token: str,
    model: str = "grok-2",
    timeout: float = 60.0
) -> bool:
    """
    Configure the Crawl4AI client with API settings.
    
    Args:
        base_url: Base URL for the OpenAI-compatible API (e.g., https://api.x.ai/v1)
        api_token: API token for authentication
        model: Model to use (default: grok-2)
        timeout: Request timeout in seconds (default: 60.0)
        
    Returns:
        True if configuration was successful, False otherwise
    """
    if not CRAWL4AI_AVAILABLE:
        logger.warning("Cannot configure Crawl4AI: package not available")
        return False
    
    _CRAWL4AI_CONFIG["base_url"] = base_url
    _CRAWL4AI_CONFIG["api_token"] = api_token
    _CRAWL4AI_CONFIG["model"] = model
    _CRAWL4AI_CONFIG["timeout"] = timeout
    _CRAWL4AI_CONFIG["configured"] = True
    
    logger.info(f"Crawl4AI configured with base_url={base_url}, model={model}")
    return True


async def extract_with_crawl4ai(
    url: str,
    proxy_url: Optional[str] = None,
    schema: Optional[Dict[str, Any]] = None,
    referer: Optional[str] = None,
    user_agent: Optional[str] = None,
    timeout: Optional[float] = None
) -> Dict[str, Any]:
    """
    Extract structured data from a URL using Crawl4AI.
    
    Args:
        url: URL to extract data from
        proxy_url: Optional proxy URL to use
        schema: JSON Schema to use for extraction (defaults to BUSINESS_SCHEMA)
        referer: Optional referer header
        user_agent: Optional user agent string
        timeout: Optional timeout override
        
    Returns:
        Dictionary with extracted data, or empty dict if extraction failed
    """
    if not CRAWL4AI_AVAILABLE:
        logger.warning("Crawl4AI extraction skipped: package not available")
        return {}
    
    if not _CRAWL4AI_CONFIG["configured"] or not _CRAWL4AI_CONFIG["api_token"]:
        logger.warning("Crawl4AI not configured or missing API token")
        return {}
    
    # Use default schema if none provided
    if schema is None:
        schema = BUSINESS_SCHEMA
    
    # Use configured timeout if none provided
    req_timeout = timeout if timeout is not None else _CRAWL4AI_CONFIG["timeout"]
    
    try:
        # Configure headers
        headers = {}
        if user_agent:
            headers["User-Agent"] = user_agent
        if referer:
            headers["Referer"] = referer
        
        # Configure proxy
        proxies = None
        if proxy_url:
            proxies = {
                "http://": proxy_url,
                "https://": proxy_url
            }
        
        # Configure OpenAI-compatible provider
        provider_config = {
            "provider": "openai",
            "config": {
                "base_url": _CRAWL4AI_CONFIG["base_url"],
                "api_key": _CRAWL4AI_CONFIG["api_token"],
                "model": _CRAWL4AI_CONFIG["model"],
                "temperature": 0.1,  # Low temperature for deterministic extraction
            }
        }
        
        # Create extraction strategy
        extraction_strategy = LLMExtractionStrategy(
            schema=schema,
            llm_provider=provider_config
        )
        
        # Create web crawler with custom settings
        crawler = WebCrawler(
            extraction_strategy=extraction_strategy,
            headers=headers,
            proxies=proxies,
            timeout=req_timeout
        )
        
        # Execute the crawl and extraction
        result = await crawler.crawl(url)
        
        # Check if extraction was successful
        if result and isinstance(result, dict):
            logger.info(f"Crawl4AI extraction successful for {url}")
            return result
        else:
            logger.warning(f"Crawl4AI extraction returned empty or invalid result for {url}")
            return {}
            
    except Exception as e:
        logger.error(f"Error during Crawl4AI extraction: {str(e)}")
        return {}


async def is_crawl4ai_available() -> bool:
    """
    Check if Crawl4AI is available and properly configured.
    
    Returns:
        True if Crawl4AI is available and configured, False otherwise
    """
    return CRAWL4AI_AVAILABLE and _CRAWL4AI_CONFIG["configured"] and bool(_CRAWL4AI_CONFIG["api_token"])


async def test_crawl4ai_connection() -> bool:
    """
    Test the connection to the configured LLM API.
    
    Returns:
        True if connection is successful, False otherwise
    """
    if not await is_crawl4ai_available():
        return False
    
    try:
        # Simple test request to check if the API is accessible
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {_CRAWL4AI_CONFIG['api_token']}",
        }
        
        url = f"{_CRAWL4AI_CONFIG['base_url']}/models"
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url, headers=headers)
            
            if response.status_code == 200:
                logger.info("Crawl4AI API connection test successful")
                return True
            else:
                logger.warning(f"Crawl4AI API connection test failed: {response.status_code}")
                return False
                
    except Exception as e:
        logger.error(f"Error testing Crawl4AI API connection: {str(e)}")
        return False
