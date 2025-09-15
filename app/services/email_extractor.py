"""
Email extraction utilities for finding contact emails from websites.
Provides functions to extract emails from text and to discover emails from website URLs.
"""

import asyncio
import json
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

# Regex for obfuscated emails
OBFUSCATED_EMAIL_PATTERNS = [
    r'[a-zA-Z0-9._%+\-]+\s*[\[\(]at[\]\)]\s*[a-zA-Z0-9.\-]+\s*[\[\(]dot[\]\)]\s*[a-zA-Z]{2,}',  # name [at] domain [dot] com
    r'[a-zA-Z0-9._%+\-]+\s*@\s*[a-zA-Z0-9.\-]+\s*\.\s*[a-zA-Z]{2,}',  # name @ domain . com (with spaces)
    r'[a-zA-Z0-9._%+\-]+\s+at\s+[a-zA-Z0-9.\-]+\s+dot\s+[a-zA-Z]{2,}',  # name at domain dot com
    r'[a-zA-Z0-9._%+\-]+\s*\(at\)\s*[a-zA-Z0-9.\-]+\s*\(dot\)\s*[a-zA-Z]{2,}',  # name(at)domain(dot)com
]

# Regex for mailto links
MAILTO_REGEX = r'mailto:([^"\'\s\?]+)'

# Common non-contact emails to filter out
NON_CONTACT_EMAILS = {
    'noreply', 'no-reply', 'donotreply', 'do-not-reply', 'no_reply',
    'info', 'admin', 'administrator', 'webmaster', 'hostmaster',
    'postmaster', 'abuse', 'spam', 'support', 'help', 'sales',
    'marketing', 'privacy', 'legal', 'billing', 'accounts',
    'example', 'test', 'user', 'username', 'email', 'mail'
}

# Contact page patterns for following
CONTACT_PAGE_PATTERNS = [
    'contact', 'about', 'team', 'support', 'impressum', 'imprint', 
    'kontakt', 'about-us', 'our-team', 'get-in-touch', 'reach-us',
    'connect', 'talk-to-us', 'feedback', 'help', 'customer-service'
]

# User agent strings for randomization
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
]


def deobfuscate_email(text: str) -> str:
    """
    Deobfuscate common email obfuscation patterns.
    
    Args:
        text: Text containing obfuscated email
        
    Returns:
        Deobfuscated email address
    """
    # Replace [at] or (at) with @
    text = re.sub(r'[\[\(]\s*at\s*[\]\)]', '@', text)
    text = re.sub(r'\s+at\s+', '@', text)
    text = re.sub(r'\(at\)', '@', text)
    
    # Replace [dot] or (dot) with .
    text = re.sub(r'[\[\(]\s*dot\s*[\]\)]', '.', text)
    text = re.sub(r'\s+dot\s+', '.', text)
    text = re.sub(r'\(dot\)', '.', text)
    
    # Remove spaces around @ and .
    text = re.sub(r'\s*@\s*', '@', text)
    text = re.sub(r'\s*\.\s*', '.', text)
    
    return text


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
    
    # Find obfuscated emails and deobfuscate them
    obfuscated_emails = []
    for pattern in OBFUSCATED_EMAIL_PATTERNS:
        matches = re.findall(pattern, text, re.IGNORECASE)
        for match in matches:
            deobfuscated = deobfuscate_email(match)
            # Verify it's now a valid email
            if re.match(EMAIL_REGEX, deobfuscated, re.IGNORECASE):
                obfuscated_emails.append(deobfuscated)
    
    # Find mailto links
    mailto_emails = re.findall(MAILTO_REGEX, text, re.IGNORECASE)
    
    # Combine and deduplicate
    all_emails = list(set(emails + quoted_emails + obfuscated_emails + mailto_emails))
    
    # Clean up emails (lowercase, strip whitespace)
    cleaned_emails = [email.lower().strip() for email in all_emails]
    
    return cleaned_emails


def extract_email_from_jsonld(html: str) -> Optional[str]:
    """
    Extract email from JSON-LD structured data in HTML.
    
    Args:
        html: HTML content
        
    Returns:
        Email address if found, None otherwise
    """
    if not html:
        return None
    
    # Find all JSON-LD scripts
    jsonld_scripts = re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.DOTALL)
    
    for script in jsonld_scripts:
        try:
            # Parse JSON
            data = json.loads(script)
            
            # Handle both single objects and arrays
            if isinstance(data, list):
                items = data
            else:
                items = [data]
            
            # Look for email in each item
            for item in items:
                # Direct email property
                if isinstance(item, dict):
                    email = item.get('email')
                    if email and isinstance(email, str) and '@' in email:
                        return email
                    
                    # Check in nested Organization, Person, LocalBusiness, etc.
                    for prop in ['author', 'creator', 'publisher', 'provider', 'employee', 'founder']:
                        entity = item.get(prop)
                        if isinstance(entity, dict) and entity.get('email'):
                            return entity.get('email')
                    
                    # Check in contactPoint
                    contact_point = item.get('contactPoint')
                    if isinstance(contact_point, dict) and contact_point.get('email'):
                        return contact_point.get('email')
                    
        except Exception as e:
            logger.debug(f"Error parsing JSON-LD: {e}")
            continue
    
    return None


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
    visited_urls = set([url])  # Track visited URLs to avoid loops
    
    try:
        async with httpx.AsyncClient(**client_kwargs) as client:
            # Fetch the website with size limit
            response = await client.get(url, timeout=timeout)
            
            # Check if response is HTML
            content_type = response.headers.get("content-type", "")
            if "text/html" not in content_type.lower() and "application/xhtml+xml" not in content_type.lower():
                logger.info(f"Skipping non-HTML content: {content_type} for {url}")
                return None
            
            # Get HTML content
            html_content = response.text
            
            # Extract emails from HTML
            emails_found = extract_emails_from_text(html_content)
            
            # Extract email from JSON-LD
            jsonld_email = extract_email_from_jsonld(html_content)
            if jsonld_email:
                logger.info(f"Found email in JSON-LD: {jsonld_email}")
                emails_found.append(jsonld_email)
            
            # Extract emails from script tags
            script_tags = re.findall(r'<script[^>]*>(.*?)</script>', html_content, re.DOTALL)
            for script in script_tags:
                script_emails = extract_emails_from_text(script)
                if script_emails:
                    logger.info(f"Found {len(script_emails)} emails in script tags")
                    emails_found.extend(script_emails)
            
            # Extract mailto links explicitly
            mailto_links = re.findall(MAILTO_REGEX, html_content)
            if mailto_links:
                logger.info(f"Found {len(mailto_links)} mailto links")
                emails_found.extend(mailto_links)
            
            # If no emails found or we want more, check for contact page links
            if len(emails_found) < 2:  # Continue looking if we found fewer than 2 emails
                # Look for contact page links
                contact_links = []
                
                # Generate regex for contact page patterns
                pattern_str = '|'.join(CONTACT_PAGE_PATTERNS)
                contact_regex = rf'href=[\'"]?([^\'" >]+)[\'"]?[^>]*>.*?({pattern_str})'
                
                # Find all contact-like links
                matches = re.findall(contact_regex, html_content, re.IGNORECASE)
                contact_links = [match[0] for match in matches]
                
                # Also look for links containing contact patterns in the URL
                for pattern in CONTACT_PAGE_PATTERNS:
                    url_pattern_links = re.findall(rf'href=[\'"]?([^\'" >]+{pattern}[^\'" >]*)[\'"]?', html_content, re.IGNORECASE)
                    contact_links.extend(url_pattern_links)
                
                # Deduplicate links
                contact_links = list(set(contact_links))
                
                # Try up to 4 contact pages
                for i, link in enumerate(contact_links[:4]):
                    try:
                        # Resolve relative URLs
                        contact_url = urljoin(url, link)
                        
                        # Skip if already visited or same as original URL
                        if contact_url in visited_urls:
                            continue
                        
                        visited_urls.add(contact_url)
                        logger.info(f"Checking contact page: {contact_url}")
                        
                        # Fetch the contact page
                        contact_response = await client.get(contact_url, timeout=timeout)
                        contact_html = contact_response.text
                        
                        # Extract emails from contact page
                        contact_emails = extract_emails_from_text(contact_html)
                        
                        # Extract email from JSON-LD on contact page
                        contact_jsonld_email = extract_email_from_jsonld(contact_html)
                        if contact_jsonld_email:
                            contact_emails.append(contact_jsonld_email)
                        
                        # Extract emails from script tags on contact page
                        contact_script_tags = re.findall(r'<script[^>]*>(.*?)</script>', contact_html, re.DOTALL)
                        for script in contact_script_tags:
                            script_emails = extract_emails_from_text(script)
                            if script_emails:
                                contact_emails.extend(script_emails)
                        
                        # Extract mailto links from contact page
                        contact_mailto_links = re.findall(MAILTO_REGEX, contact_html)
                        if contact_mailto_links:
                            contact_emails.extend(contact_mailto_links)
                        
                        if contact_emails:
                            emails_found.extend(contact_emails)
                            # If we found good emails, we can stop looking
                            if any(is_likely_contact_email(email) for email in contact_emails):
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
