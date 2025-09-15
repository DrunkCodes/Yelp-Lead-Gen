"""
Email extraction utilities for finding contact emails from websites.
Provides functions to extract emails from text and to discover emails from website URLs.
"""

import asyncio
import logging
import random
import re
from typing import Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urljoin, urlparse

import httpx

from app.services.llm_structured import call_llm

logger = logging.getLogger(__name__)

# Comprehensive regex pattern for email addresses
# Supports various formats including quoted local parts and special characters
EMAIL_REGEX = r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'
EMAIL_REGEX_QUOTED = r'"[^"]+?"@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'

# Common non-contact emails to filter out
NON_CONTACT_EMAILS = {
    'noreply', 'no-reply', 'donotreply', 'do-not-reply', 'no_reply',
    'info', 'admin', 'administrator', 'webmaster', 'hostmaster',
    'postmaster', 'abuse', 'spam', 'support', 'help', 'sales',
    'marketing', 'privacy', 'legal', 'billing', 'accounts',
    'example', 'test', 'user', 'username', 'email', 'mail'
}

# User agent strings for randomization
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
]


def extract_emails_from_text(text: str) -> List[str]:
    """
    Extract email addresses from text using regex.
    
    Args:
        text: Text to extract emails from
        
    Returns:
        List of extracted email addresses
    """
    if not text:
        return []
    
    # Find all standard emails
    emails = re.findall(EMAIL_REGEX, text, re.IGNORECASE)
    
    # Also find quoted emails
    quoted_emails = re.findall(EMAIL_REGEX_QUOTED, text, re.IGNORECASE)
    
    # Combine and deduplicate
    all_emails = list(set(emails + quoted_emails))
    
    # Clean up emails (lowercase, strip whitespace)
    cleaned_emails = [email.lower().strip() for email in all_emails]
    
    return cleaned_emails


def is_likely_contact_email(email: str) -> bool:
    """
    Check if an email is likely to be a contact email (not a generic service email).
    
    Args:
        email: Email address to check
        
    Returns:
        True if the email is likely a contact email, False otherwise
    """
    if not email or '@' not in email:
        return False
    
    # Extract local part (before @)
    local_part = email.split('@')[0].lower()
    
    # Check if local part contains common non-contact patterns
    for pattern in NON_CONTACT_EMAILS:
        if pattern == local_part:
            return False
    
    # Prefer emails with names (john.doe@example.com)
    if '.' in local_part and len(local_part) > 5:
        return True
    
    # Avoid very short local parts
    if len(local_part) < 3:
        return False
    
    return True


def prioritize_emails(emails: List[str]) -> List[str]:
    """
    Sort emails by likelihood of being a contact email.
    
    Args:
        emails: List of email addresses
        
    Returns:
        Sorted list of email addresses, with most likely contact emails first
    """
    if not emails:
        return []
    
    # Define priority tiers
    contact_emails = []
    likely_emails = []
    other_emails = []
    
    for email in emails:
        # Skip invalid emails
        if not email or '@' not in email:
            continue
            
        local_part = email.split('@')[0].lower()
        
        # Highest priority: Emails with common contact patterns
        if any(pattern in local_part for pattern in ['contact', 'inquiry', 'hello', 'info']):
            contact_emails.append(email)
        # Medium priority: Emails that look like personal emails (with dots, longer)
        elif is_likely_contact_email(email):
            likely_emails.append(email)
        # Lowest priority: All other valid emails
        else:
            other_emails.append(email)
    
    # Combine the tiers in priority order
    return contact_emails + likely_emails + other_emails


async def extract_email_from_website(
    url: str,
    proxy_url: Optional[str] = None,
    referer: Optional[str] = None,
    timeout: float = 20.0,
    max_size: int = 5 * 1024 * 1024,  # 5MB
    llm_enabled: bool = False
) -> Optional[str]:
    """
    Extract email addresses from a website URL.
    
    Args:
        url: Website URL to extract emails from
        proxy_url: Optional proxy URL to use
        referer: Optional referer header
        timeout: Request timeout in seconds
        max_size: Maximum response size in bytes
        llm_enabled: Whether LLM fallback is enabled
        
    Returns:
        The most likely contact email, or None if no emails found
    """
    if not url:
        return None
    
    # Validate URL
    try:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            logger.warning(f"Invalid URL: {url}")
            return None
    except Exception as e:
        logger.warning(f"Error parsing URL {url}: {e}")
        return None
    
    # Prepare headers with random user agent
    user_agent = random.choice(USER_AGENTS)
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }
    
    # Add referer if provided
    if referer:
        headers["Referer"] = referer
    
    # Configure client with proxy if provided
    client_kwargs = {
        "timeout": timeout,
        "follow_redirects": True,
        "headers": headers
    }
    
    if proxy_url:
        client_kwargs["proxies"] = {
            "http://": proxy_url,
            "https://": proxy_url
        }
    
    emails_found = []
    
    try:
        async with httpx.AsyncClient(**client_kwargs) as client:
            # Fetch the website with size limit
            response = await client.get(url, timeout=timeout)
            
            # Check if response is HTML
            content_type = response.headers.get("content-type", "")
            if "text/html" not in content_type.lower() and "application/xhtml+xml" not in content_type.lower():
                logger.info(f"Skipping non-HTML content: {content_type} for {url}")
                return None
            
            # Extract emails from HTML
            html_content = response.text
            emails_found = extract_emails_from_text(html_content)
            
            # If no emails found, check for contact page links
            if not emails_found:
                # Look for contact page links
                contact_links = re.findall(r'href=[\'"]?([^\'" >]+)[\'"]?[^>]*>.*?contact', html_content, re.IGNORECASE)
                
                # Try up to 2 contact pages
                for i, link in enumerate(contact_links[:2]):
                    try:
                        # Resolve relative URLs
                        contact_url = urljoin(url, link)
                        
                        # Skip if same as original URL
                        if contact_url == url:
                            continue
                            
                        logger.info(f"Checking contact page: {contact_url}")
                        
                        # Fetch the contact page
                        contact_response = await client.get(contact_url, timeout=timeout)
                        contact_emails = extract_emails_from_text(contact_response.text)
                        
                        if contact_emails:
                            emails_found.extend(contact_emails)
                            break
                    except Exception as e:
                        logger.warning(f"Error fetching contact page {link}: {e}")
            
            # If still no emails found and LLM is enabled, try LLM extraction
            if not emails_found and llm_enabled:
                logger.info(f"No emails found via regex, trying LLM extraction for {url}")
                
                # Truncate HTML if it's too long
                max_llm_length = 50000
                if len(html_content) > max_llm_length:
                    half_length = max_llm_length // 2
                    html_content = html_content[:half_length] + "\n...[content truncated]...\n" + html_content[-half_length:]
                
                # Craft prompt for email extraction
                prompt = f"""
Extract ONLY the most likely public contact email address from this website content.
If multiple emails exist, return only the one most likely to be for general contact.
If no email is found, respond with exactly "No email found".

Website content:
{html_content}
"""
                
                messages = [
                    {"role": "system", "content": "You are a precise email extraction assistant. Return only the email address, nothing else."},
                    {"role": "user", "content": prompt}
                ]
                
                # Call LLM with email extraction prompt
                llm_response = await call_llm(
                    messages=messages,
                    temperature=0.1,
                    max_tokens=100
                )
                
                if llm_response and "no email found" not in llm_response.lower():
                    # Extract emails from LLM response
                    llm_emails = extract_emails_from_text(llm_response)
                    if llm_emails:
                        logger.info(f"Found email via LLM: {llm_emails[0]}")
                        emails_found.extend(llm_emails)
    
    except httpx.TimeoutException:
        logger.warning(f"Timeout fetching {url}")
        return None
    except httpx.RequestError as e:
        logger.warning(f"Request error fetching {url}: {e}")
        return None
    except Exception as e:
        logger.warning(f"Unexpected error extracting emails from {url}: {e}")
        return None
    
    # Deduplicate and prioritize emails
    unique_emails = list(set(emails_found))
    prioritized_emails = prioritize_emails(unique_emails)
    
    # Return the first email or None
    if prioritized_emails:
        return prioritized_emails[0]
    
    return None
