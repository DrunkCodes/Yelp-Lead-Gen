"""
LLM client for structured data extraction from HTML.
Uses OpenAI-compatible API endpoints with Grok or other compatible models.
"""

import asyncio
import json
import logging
import re
from typing import Any, Dict, List, Optional, Union, cast

import httpx

logger = logging.getLogger(__name__)

# Global configuration for the LLM client
_LLM_CONFIG = {
    "base_url": "https://api.x.ai/v1",
    "api_key": None,
    "model": "grok-2",
    "temperature": 0.2,
    "max_tokens": 2000,
    "timeout": 60.0,
}


def configure_llm(
    base_url: str,
    api_key: str,
    model: str = "grok-2",
    temperature: float = 0.2,
    max_tokens: int = 2000,
    timeout: float = 60.0,
) -> None:
    """
    Configure the LLM client with API settings.
    
    Args:
        base_url: Base URL for the OpenAI-compatible API (e.g., https://api.x.ai/v1)
        api_key: API key for authentication
        model: Model to use (default: grok-2)
        temperature: Sampling temperature (default: 0.2)
        max_tokens: Maximum tokens to generate (default: 2000)
        timeout: Request timeout in seconds (default: 60.0)
    """
    _LLM_CONFIG["base_url"] = base_url
    _LLM_CONFIG["api_key"] = api_key
    _LLM_CONFIG["model"] = model
    _LLM_CONFIG["temperature"] = temperature
    _LLM_CONFIG["max_tokens"] = max_tokens
    _LLM_CONFIG["timeout"] = timeout
    
    logger.info(f"LLM client configured with base_url={base_url}, model={model}")


async def call_llm(
    messages: List[Dict[str, str]],
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    timeout: Optional[float] = None,
    retry_count: int = 2,
) -> Optional[str]:
    """
    Call the LLM API with the given messages.
    
    Args:
        messages: List of message objects (role, content)
        temperature: Override default temperature
        max_tokens: Override default max_tokens
        timeout: Override default timeout
        retry_count: Number of retries on failure
        
    Returns:
        The generated text, or None if the request failed
    """
    if not _LLM_CONFIG["api_key"]:
        logger.warning("LLM API key not configured, skipping LLM call")
        return None
    
    # Use configured values if not overridden
    temp = temperature if temperature is not None else _LLM_CONFIG["temperature"]
    tokens = max_tokens if max_tokens is not None else _LLM_CONFIG["max_tokens"]
    req_timeout = timeout if timeout is not None else _LLM_CONFIG["timeout"]
    
    # Prepare the request payload
    payload = {
        "model": _LLM_CONFIG["model"],
        "messages": messages,
        "temperature": temp,
        "max_tokens": tokens,
    }
    
    # Prepare headers with API key
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_LLM_CONFIG['api_key']}",
    }
    
    # Build the full URL
    url = f"{_LLM_CONFIG['base_url']}/chat/completions"
    
    # Make the request with retries
    for attempt in range(retry_count + 1):
        try:
            async with httpx.AsyncClient(timeout=req_timeout) as client:
                response = await client.post(url, json=payload, headers=headers)
                
                # Check for rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", "5"))
                    logger.warning(f"Rate limited by LLM API, retrying after {retry_after}s")
                    if attempt < retry_count:
                        await asyncio.sleep(retry_after)
                        continue
                    return None
                
                # Handle other errors
                if response.status_code != 200:
                    logger.error(f"LLM API error: {response.status_code} - {response.text}")
                    if attempt < retry_count:
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    return None
                
                # Parse the response
                result = response.json()
                
                # Extract the generated text
                if "choices" in result and len(result["choices"]) > 0:
                    if "message" in result["choices"][0]:
                        return result["choices"][0]["message"].get("content", "")
                
                logger.warning(f"Unexpected LLM API response format: {result}")
                return None
                
        except (httpx.RequestError, asyncio.TimeoutError) as e:
            logger.warning(f"LLM API request failed: {e}")
            if attempt < retry_count:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
                continue
            return None
        except Exception as e:
            logger.error(f"Unexpected error calling LLM API: {e}")
            return None
    
    return None


async def extract_structured_from_html(html: str) -> Dict[str, Any]:
    """
    Extract structured business data from HTML using LLM.
    
    Args:
        html: HTML content of the business page
        
    Returns:
        Dictionary with extracted business data fields, or empty dict if extraction failed
    """
    if not html or not _LLM_CONFIG["api_key"]:
        return {}
    
    # Truncate HTML if it's too long (most LLMs have context limits)
    # Keep the beginning and end which often contain the most relevant info
    max_html_length = 50000
    if len(html) > max_html_length:
        half_length = max_html_length // 2
        html = html[:half_length] + "\n...[content truncated]...\n" + html[-half_length:]
    
    # Craft the prompt for extracting business information
    prompt = f"""
You are an expert data extractor. Extract the following business information from the HTML content:

1. business_name: The name of the business
2. years_in_business: Number of years in business as an integer, or null if unknown
3. rating: Star rating (0-5) as a float, or null if unknown
4. review_count: Number of reviews as an integer, or null if unknown
5. industry: Business category or industry, or null if unknown
6. phone: Phone number as a string, or null if unknown
7. website: Website URL as a string, or null if unknown
8. email: Email address as a string, or null if unknown

IMPORTANT INSTRUCTIONS:
- Return ONLY a valid JSON object with exactly these 8 keys
- Use null for missing values, not empty strings
- For years_in_business, calculate from founding date if available
- For rating, ensure it's a float between 0 and 5
- For review_count, ensure it's a non-negative integer
- Format the output as a clean JSON object

HTML CONTENT:
{html}
"""
    
    messages = [
        {"role": "system", "content": "You are a precise data extraction assistant that returns only valid JSON."},
        {"role": "user", "content": prompt}
    ]
    
    # Call the LLM with a low temperature for deterministic results
    response_text = await call_llm(
        messages=messages,
        temperature=0.1,  # Lower temperature for more deterministic extraction
        max_tokens=1000,  # Limit tokens since we only need a small JSON response
    )
    
    if not response_text:
        logger.warning("Failed to get LLM response for HTML extraction")
        return {}
    
    # Try to parse the JSON response with multiple fallback methods
    result = parse_json_with_fallbacks(response_text)
    if not result:
        logger.warning("Failed to parse JSON from LLM response")
        return {}
    
    # Validate the extracted fields
    expected_keys = {
        "business_name", "years_in_business", "rating", "review_count",
        "industry", "phone", "website", "email"
    }
    
    # Ensure all expected keys are present, with None for missing values
    for key in expected_keys:
        if key not in result:
            result[key] = None
    
    # Convert numeric fields to the right types
    try:
        if result.get("rating") is not None and result["rating"] != "null":
            result["rating"] = float(result["rating"])
        else:
            result["rating"] = None
            
        if result.get("review_count") is not None and result["review_count"] != "null":
            result["review_count"] = int(float(result["review_count"]))
        else:
            result["review_count"] = None
            
        if result.get("years_in_business") is not None and result["years_in_business"] != "null":
            result["years_in_business"] = int(float(result["years_in_business"]))
        else:
            result["years_in_business"] = None
    except (ValueError, TypeError):
        logger.warning("Error converting numeric fields in LLM extraction result")
    
    return result


def parse_json_with_fallbacks(text: str) -> Dict[str, Any]:
    """
    Parse JSON from text with multiple fallback methods.
    
    Args:
        text: Text containing JSON
        
    Returns:
        Parsed JSON as a dictionary, or empty dict if parsing failed
    """
    if not text:
        return {}
    
    # Method 1: Direct JSON parsing
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    
    # Method 2: Extract JSON from code blocks
    code_block_pattern = r"```(?:json)?\s*([\s\S]*?)\s*```"
    code_blocks = re.findall(code_block_pattern, text)
    
    for block in code_blocks:
        try:
            return json.loads(block)
        except json.JSONDecodeError:
            continue
    
    # Method 3: Find JSON-like structures with balanced braces
    brace_pattern = r"\{[\s\S]*?\}"
    potential_jsons = re.findall(brace_pattern, text)
    
    for potential_json in potential_jsons:
        try:
            return json.loads(potential_json)
        except json.JSONDecodeError:
            continue
    
    # Method 4: More aggressive brace extraction with stack-based parsing
    try:
        start_idx = text.find('{')
        if start_idx != -1:
            stack = 1
            for i in range(start_idx + 1, len(text)):
                if text[i] == '{':
                    stack += 1
                elif text[i] == '}':
                    stack -= 1
                    if stack == 0:
                        json_str = text[start_idx:i+1]
                        return json.loads(json_str)
    except (json.JSONDecodeError, IndexError):
        pass
    
    # All methods failed
    logger.warning("All JSON parsing methods failed")
    return {}


def clean_json_string(text: str) -> str:
    """
    Clean a string that might contain JSON to improve parsing success.
    
    Args:
        text: Text that might contain JSON
        
    Returns:
        Cleaned text
    """
    # Remove common prefixes that LLMs might add
    prefixes = [
        "```json", "```", "JSON:", "Here's the JSON:", 
        "The extracted information is:", "Result:"
    ]
    
    for prefix in prefixes:
        if text.strip().startswith(prefix):
            text = text[len(prefix):].strip()
    
    # Remove common suffixes
    suffixes = ["```", "```json"]
    for suffix in suffixes:
        if text.strip().endswith(suffix):
            text = text[:text.rfind(suffix)].strip()
    
    return text
