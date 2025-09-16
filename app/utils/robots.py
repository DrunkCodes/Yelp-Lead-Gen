"""
Robots.txt parsing and checking utilities.
Provides functions to fetch and check if paths are allowed by a website's robots.txt file.
"""

import asyncio
import logging
import re
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import httpx

logger = logging.getLogger(__name__)

# Cache for robots.txt content to avoid repeated fetches
_robots_cache: Dict[str, Tuple[List[str], List[str]]] = {}


async def fetch_robots_txt(domain: str, proxy_url: Optional[str] = None, timeout: float = 10.0) -> str:
    """
    Fetch the robots.txt file for a domain.
    
    Args:
        domain: The domain to fetch robots.txt from (e.g., 'www.yelp.com')
        proxy_url: Optional proxy URL to use for the request
        timeout: Timeout in seconds for the request
        
    Returns:
        The content of the robots.txt file as a string, or empty string if not found
    """
    robots_url = f"https://{domain}/robots.txt"
    
    # Set up httpx client with proxy if provided
    client_kwargs = {
        "timeout": timeout,
        "follow_redirects": True,
        "headers": {
            "User-Agent": "Mozilla/5.0 (compatible; YelpScraper/1.0; +https://www.apify.com)"
        }
    }
    
    if proxy_url:
        client_kwargs["proxies"] = {
            "http://": proxy_url,
            "https://": proxy_url
        }
    
    try:
        async with httpx.AsyncClient(**client_kwargs) as client:
            response = await client.get(robots_url)
            
            if response.status_code == 200:
                return response.text
            elif response.status_code == 404:
                logger.info(f"No robots.txt found at {robots_url}")
                return ""
            else:
                logger.warning(f"Failed to fetch robots.txt from {robots_url}: HTTP {response.status_code}")
                return ""
    except httpx.RequestError as e:
        logger.warning(f"Error fetching robots.txt from {robots_url}: {e}")
        return ""
    except asyncio.TimeoutError:
        logger.warning(f"Timeout fetching robots.txt from {robots_url}")
        return ""
    except Exception as e:
        logger.warning(f"Unexpected error fetching robots.txt from {robots_url}: {e}")
        return ""


def parse_robots_txt(content: str) -> Tuple[List[str], List[str]]:
    """
    Parse robots.txt content and extract Allow and Disallow rules for User-agent '*'.
    
    Args:
        content: The content of the robots.txt file
        
    Returns:
        A tuple of (allow_rules, disallow_rules) as lists of path patterns
    """
    if not content:
        return [], []
    
    allow_rules: List[str] = []
    disallow_rules: List[str] = []
    
    # Split content into lines and normalize
    lines = [line.strip() for line in content.splitlines()]
    
    # Track if we're in a relevant user-agent section
    in_relevant_section = False
    current_agent = None
    
    for line in lines:
        # Skip empty lines and comments
        if not line or line.startswith('#'):
            continue
        
        # Check for User-agent line
        if line.lower().startswith('user-agent:'):
            agent = line.split(':', 1)[1].strip()
            current_agent = agent
            in_relevant_section = (agent == '*')
        
        # If we're in a relevant section, process rules
        elif in_relevant_section:
            # If we encounter a new User-agent section, exit the current one
            if line.lower().startswith('user-agent:'):
                in_relevant_section = False
                continue
            
            # Process Allow rules
            if line.lower().startswith('allow:'):
                path = line.split(':', 1)[1].strip()
                if path:
                    allow_rules.append(path)
            
            # Process Disallow rules
            elif line.lower().startswith('disallow:'):
                path = line.split(':', 1)[1].strip()
                if path:
                    disallow_rules.append(path)
    
    # If we didn't find any rules for User-agent: *, look for rules that apply to all agents
    if not in_relevant_section and not allow_rules and not disallow_rules:
        # Reset and scan again for any global rules
        in_relevant_section = False
        
        for line in lines:
            if not line or line.startswith('#'):
                continue
                
            if line.lower().startswith('user-agent:'):
                agent = line.split(':', 1)[1].strip()
                in_relevant_section = (agent == '*')
            elif in_relevant_section:
                if line.lower().startswith('allow:'):
                    path = line.split(':', 1)[1].strip()
                    if path:
                        allow_rules.append(path)
                elif line.lower().startswith('disallow:'):
                    path = line.split(':', 1)[1].strip()
                    if path:
                        disallow_rules.append(path)
    
    return allow_rules, disallow_rules


def is_path_allowed(path: str, allow_rules: List[str], disallow_rules: List[str]) -> bool:
    """
    Check if a path is allowed based on the robots.txt rules.
    
    Args:
        path: The path to check (e.g., '/search')
        allow_rules: List of Allow rules from robots.txt
        disallow_rules: List of Disallow rules from robots.txt
        
    Returns:
        True if the path is allowed, False if disallowed
    """
    # If there are no rules, everything is allowed
    if not allow_rules and not disallow_rules:
        return True
    
    # An empty Disallow value means allow all
    if len(disallow_rules) == 1 and disallow_rules[0] == '':
        return True
    
    # Check if path matches any Disallow rule
    for rule in disallow_rules:
        if rule == '/':  # Disallow all
            # Check if there's a more specific Allow rule
            for allow_rule in allow_rules:
                if path.startswith(allow_rule):
                    return True
            return False
        
        # Handle wildcard at the end (e.g., /search*)
        if rule.endswith('*') and path.startswith(rule[:-1]):
            # Check for a more specific Allow rule
            for allow_rule in allow_rules:
                if path.startswith(allow_rule) and len(allow_rule) > len(rule) - 1:
                    return True
            return False
        
        # Regular path matching
        if path.startswith(rule):
            # Check for a more specific Allow rule
            for allow_rule in allow_rules:
                if path.startswith(allow_rule) and len(allow_rule) > len(rule):
                    return True
            return False
    
    # If no Disallow rule matched, it's allowed
    return True


async def check_robots_allowed(domain: str, paths: List[str], proxy_url: Optional[str] = None) -> bool:
    """
    Check if all specified paths are allowed by the domain's robots.txt.
    
    Args:
        domain: The domain to check (e.g., 'www.yelp.com')
        paths: List of paths to check (e.g., ['/search', '/biz'])
        proxy_url: Optional proxy URL to use for fetching robots.txt
        
    Returns:
        True if all paths are allowed or no robots.txt exists, False otherwise
    """
    # Check cache first
    if domain in _robots_cache:
        allow_rules, disallow_rules = _robots_cache[domain]
    else:
        # Fetch and parse robots.txt
        content = await fetch_robots_txt(domain, proxy_url)
        allow_rules, disallow_rules = parse_robots_txt(content)
        
        # Cache the results
        _robots_cache[domain] = (allow_rules, disallow_rules)
    
    # If no rules, everything is allowed
    if not allow_rules and not disallow_rules:
        return True
    
    # Check each path
    for path in paths:
        if not is_path_allowed(path, allow_rules, disallow_rules):
            logger.warning(f"Path '{path}' is disallowed by robots.txt for {domain}")
            return False
    
    # All paths are allowed
    return True
