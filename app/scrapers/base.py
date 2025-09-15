"""
Base scraper class for Playwright-based web scraping.
Provides common functionality for browser automation, stealth, rate limiting, and error handling.
"""

import asyncio
import json
import logging
import os
import random
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urljoin, urlparse

import aiolimiter
from apify import Actor
from playwright.async_api import Browser, BrowserContext, Page, Response

from app.utils.retry import retry_async, with_retry

logger = logging.getLogger(__name__)

# User agent strings for randomization
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
]

# Locales for randomization
LOCALES = ['en-US', 'en-GB', 'en-CA', 'en-AU']

# Timezones for randomization
TIMEZONES = [
    'America/New_York', 'America/Los_Angeles', 'America/Chicago', 'Europe/London',
    'Europe/Paris', 'Europe/Berlin', 'Australia/Sydney', 'Asia/Tokyo'
]

# Common viewport sizes
VIEWPORTS = [
    {'width': 1920, 'height': 1080},
    {'width': 1680, 'height': 1050},
    {'width': 1600, 'height': 900},
    {'width': 1440, 'height': 900},
    {'width': 1366, 'height': 768},
]

# Common cookie banner accept button selectors
COOKIE_ACCEPT_SELECTORS = [
    'button[id*="accept"], button[class*="accept"]',
    'button[id*="agree"], button[class*="agree"]',
    'button[id*="cookie"][id*="accept"], button[class*="cookie"][class*="accept"]',
    'button:has-text("Accept"), button:has-text("Accept All")',
    'button:has-text("I Accept"), button:has-text("Allow")',
    'button:has-text("Agree"), button:has-text("Agree to All")',
    'button:has-text("Got it"), button:has-text("OK")',
    'a:has-text("Accept"), a:has-text("Accept All")',
    'a[id*="accept"], a[class*="accept"]',
    'div[id*="accept"]:not(:has(button)), div[class*="accept"]:not(:has(button))',
]

# Soft block detection patterns
SOFT_BLOCK_PATTERNS = [
    'unusual traffic', 'automated requests', 'are you a robot', 'access denied',
    'too many requests', 'rate limit exceeded', 'blocked', 'captcha',
    'security check', 'please wait', 'temporarily unavailable',
    'suspicious activity', 'verify you are a human'
]

# CAPTCHA detection patterns
CAPTCHA_PATTERNS = [
    'captcha', 'recaptcha', 'hcaptcha', 'cloudflare', 'security check',
    'human verification', 'bot protection', 'are you human'
]


class BaseScraper:
    """
    Base scraper class with common functionality for Playwright-based scraping.
    """
    
    def __init__(
        self,
        browser: Browser,
        proxy_configuration: Any = None,
        sessions_store: Any = None,
        snapshots_store: Any = None,
        concurrency: int = 10,
        per_business_isolation: bool = False,
        llm_enabled: bool = False,
        metrics: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the base scraper.
        
        Args:
            browser: Playwright browser instance
            proxy_configuration: Apify proxy configuration
            sessions_store: Apify key-value store for session storage
            snapshots_store: Apify key-value store for debug snapshots
            concurrency: Maximum number of concurrent operations
            per_business_isolation: Whether to use isolated contexts per business
            llm_enabled: Whether LLM features are enabled
            metrics: Dictionary for tracking metrics
        """
        self.browser = browser
        self.proxy_configuration = proxy_configuration
        self.sessions_store = sessions_store
        self.snapshots_store = snapshots_store
        self.concurrency = concurrency
        self.per_business_isolation = per_business_isolation
        self.llm_enabled = llm_enabled
        self.metrics = metrics or {}
        
        # Set up navigation rate limiter (1 request per second)
        self.limiter = aiolimiter.AsyncLimiter(1, 1)
        
        # Track contexts for cleanup
        self.contexts: List[BrowserContext] = []
        
        # Semaphore for limiting concurrent operations
        self.semaphore = asyncio.Semaphore(concurrency)
        
        # Session profiles for persistent cookies
        self.session_profiles: Dict[str, Dict[str, Any]] = {}
        
        # Track the last URL for each domain for referer chaining
        self.last_urls: Dict[str, str] = {}
    
    async def _load_session_profiles(self, domain_key: str = 'yelp', count: int = 5) -> None:
        """
        Load session profiles from the key-value store.
        
        Args:
            domain_key: Domain key for the session profiles
            count: Number of session profiles to load
        """
        if not self.sessions_store:
            return
        
        for i in range(count):
            profile_key = f'{domain_key}/profile_{i}.json'
            try:
                profile_data = await self.sessions_store.get(profile_key)
                if profile_data:
                    self.session_profiles[profile_key] = profile_data
                    logger.info(f"Loaded session profile: {profile_key}")
            except Exception as e:
                logger.warning(f"Failed to load session profile {profile_key}: {e}")
    
    async def _save_session_profile(self, context: BrowserContext, profile_key: str) -> None:
        """
        Save a session profile to the key-value store.
        
        Args:
            context: Browser context to save
            profile_key: Key for the session profile
        """
        if not self.sessions_store:
            return
        
        try:
            storage_state = await context.storage_state()
            await self.sessions_store.set(profile_key, storage_state)
            self.session_profiles[profile_key] = storage_state
            logger.info(f"Saved session profile: {profile_key}")
        except Exception as e:
            logger.warning(f"Failed to save session profile {profile_key}: {e}")
    
    def _get_random_profile_key(self, domain_key: str = 'yelp') -> str:
        """
        Get a random session profile key.
        
        Args:
            domain_key: Domain key for the session profiles
            
        Returns:
            A session profile key
        """
        # Create a list of profile keys for the domain
        profile_keys = [k for k in self.session_profiles.keys() if k.startswith(f'{domain_key}/')]
        
        # If no profiles exist, create a new one
        if not profile_keys:
            return f'{domain_key}/profile_{random.randint(0, 4)}.json'
        
        # Return a random existing profile
        return random.choice(profile_keys)
    
    def _build_headers(self, referer: Optional[str] = None) -> Dict[str, str]:
        """
        Build randomized headers for HTTP requests.
        
        Args:
            referer: Optional referer URL
            
        Returns:
            Dictionary of HTTP headers
        """
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": f"{random.choice(LOCALES)},en;q=0.9",
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
            # Set appropriate Sec-Fetch-Site based on referer
            referer_domain = urlparse(referer).netloc
            current_domain = urlparse(referer).netloc  # Placeholder, will be replaced in navigate()
            if referer_domain == current_domain:
                headers["Sec-Fetch-Site"] = "same-origin"
            else:
                headers["Sec-Fetch-Site"] = "cross-site"
        
        return headers
    
    async def new_context(
        self,
        domain_key: str = 'yelp',
        use_session_profile: bool = True,
        proxy_url: Optional[str] = None
    ) -> BrowserContext:
        """
        Create a new browser context with stealth settings.
        
        Args:
            domain_key: Domain key for session profiles
            use_session_profile: Whether to use a session profile
            proxy_url: Optional proxy URL to use
            
        Returns:
            A new browser context
        """
        # Get proxy URL if not provided
        if not proxy_url and self.proxy_configuration:
            proxy_url = await self.proxy_configuration.new_url()
        
        # Parse proxy URL if provided
        context_proxy = None
        if proxy_url:
            parsed = urlparse(proxy_url)
            context_proxy = {
                "server": f"{parsed.scheme}://{parsed.netloc}",
            }
            if parsed.username and parsed.password:
                context_proxy["username"] = parsed.username
                context_proxy["password"] = parsed.password
        
        # Randomize user agent, locale, timezone, and viewport
        user_agent = random.choice(USER_AGENTS)
        locale = random.choice(LOCALES)
        timezone = random.choice(TIMEZONES)
        viewport = random.choice(VIEWPORTS)
        
        # Prepare context options
        context_options = {
            "user_agent": user_agent,
            "locale": locale,
            "timezone_id": timezone,
            "viewport": viewport,
            "proxy": context_proxy,
            "bypass_csp": True,  # Bypass Content Security Policy
            "ignore_https_errors": True,  # Ignore HTTPS errors
            "extra_http_headers": self._build_headers(),
        }
        
        # Load storage state if using session profile
        storage_state = None
        profile_key = None
        if use_session_profile and self.sessions_store:
            profile_key = self._get_random_profile_key(domain_key)
            if profile_key in self.session_profiles:
                storage_state = self.session_profiles[profile_key]
                context_options["storage_state"] = storage_state
        
        # Create the context
        context = await self.browser.new_context(**context_options)
        self.contexts.append(context)
        
        # Set default timeouts
        context.set_default_navigation_timeout(60000)  # 60 seconds
        context.set_default_timeout(30000)  # 30 seconds
        
        # Apply stealth scripts
        await self._apply_stealth_scripts(context)
        
        # Save the profile key for later
        if profile_key:
            context._profile_key = profile_key  # type: ignore
        
        logger.info(f"Created new context with UA: {user_agent[:30]}...")
        return context
    
    async def _apply_stealth_scripts(self, context: BrowserContext) -> None:
        """
        Apply stealth scripts to a browser context.
        
        Args:
            context: Browser context to apply scripts to
        """
        # Script to hide webdriver
        await context.add_init_script("""
        () => {
            // Hide webdriver
            Object.defineProperty(navigator, 'webdriver', {
                get: () => false,
                configurable: true
            });
            
            // Hide automation
            Object.defineProperty(navigator, 'plugins', {
                get: () => {
                    return [1, 2, 3, 4, 5];
                }
            });
            
            // Hide Chrome
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
            
            // Pass webdriver tests
            if (window.navigator.userAgent.includes('Chrome')) {
                // We are Chrome
                delete Object.getPrototypeOf(navigator).webdriver;
                
                // Patch chrome runtime
                if (window.chrome) {
                    window.chrome.runtime = {};
                }
            }
        }
        """)
    
    async def close_context(self, context: BrowserContext) -> None:
        """
        Close a browser context and save its session state.
        
        Args:
            context: Browser context to close
        """
        try:
            # Save storage state if profile key exists
            if hasattr(context, '_profile_key'):
                await self._save_session_profile(context, context._profile_key)  # type: ignore
            
            # Close the context
            await context.close()
            
            # Remove from tracked contexts
            if context in self.contexts:
                self.contexts.remove(context)
                
        except Exception as e:
            logger.warning(f"Error closing context: {e}")
    
    async def close_all_contexts(self) -> None:
        """Close all tracked browser contexts."""
        for context in list(self.contexts):
            await self.close_context(context)
    
    @with_retry(max_tries=3, base_delay=2.0, jitter=True)
    async def navigate(
        self,
        page: Page,
        url: str,
        referer: Optional[str] = None,
        wait_until: str = 'domcontentloaded',
        timeout: Optional[int] = None
    ) -> Response:
        """
        Navigate to a URL with rate limiting and retries.
        
        Args:
            page: Playwright page
            url: URL to navigate to
            referer: Optional referer URL
            wait_until: Navigation wait condition
            timeout: Navigation timeout in milliseconds
            
        Returns:
            Playwright response object
        """
        # Apply rate limiting
        async with self.limiter:
            # Update headers with referer if provided
            if referer:
                await page.set_extra_http_headers(self._build_headers(referer))
            
            # Store the URL domain for referer chaining
            domain = urlparse(url).netloc
            self.last_urls[domain] = url
            
            # Add a small random delay for human-like behavior
            await asyncio.sleep(random.uniform(0.5, 2.0))
            
            # Navigate to the URL
            logger.info(f"Navigating to: {url}")
            response = await page.goto(url, wait_until=wait_until, timeout=timeout)
            
            if not response:
                raise Exception(f"Navigation to {url} failed: no response")
            
            # Check for soft blocks or CAPTCHAs
            is_blocked = await self.soft_blocked(page)
            has_captcha = await self.has_captcha(page)
            
            if has_captcha:
                if self.metrics:
                    self.metrics["captcha_hits"] = self.metrics.get("captcha_hits", 0) + 1
                raise Exception(f"CAPTCHA detected at {url}")
            
            if is_blocked:
                if self.metrics:
                    self.metrics["soft_blocks"] = self.metrics.get("soft_blocks", 0) + 1
                raise Exception(f"Soft block detected at {url}")
            
            # Try to accept cookies
            await self.accept_cookies(page)
            
            # Add a small delay after navigation
            await asyncio.sleep(random.uniform(1.0, 3.0))
            
            # Perform some random scrolling for human-like behavior
            await self._random_scroll(page)
            
            return response
    
    async def _random_scroll(self, page: Page) -> None:
        """
        Perform random scrolling on a page for human-like behavior.
        
        Args:
            page: Playwright page
        """
        # Get page height
        height = await page.evaluate("document.body.scrollHeight")
        
        # Perform 2-5 scroll actions
        scroll_count = random.randint(2, 5)
        
        for _ in range(scroll_count):
            # Scroll to a random position
            position = random.randint(100, max(height - 800, 100))
            await page.evaluate(f"window.scrollTo(0, {position})")
            
            # Wait a random amount of time
            await asyncio.sleep(random.uniform(0.3, 1.5))
    
    async def soft_blocked(self, page: Page) -> bool:
        """
        Check if a page has soft-blocked the scraper.
        
        Args:
            page: Playwright page
            
        Returns:
            True if the page has soft-blocked the scraper, False otherwise
        """
        try:
            # Get page content and title
            content = await page.content()
            title = await page.title()
            
            # Check for block patterns in content and title
            content_lower = content.lower()
            title_lower = title.lower()
            
            for pattern in SOFT_BLOCK_PATTERNS:
                if pattern in content_lower or pattern in title_lower:
                    logger.warning(f"Soft block detected: '{pattern}' found on page")
                    return True
            
            # Check for specific block elements
            block_selectors = [
                'div[class*="captcha"]',
                'div[class*="block"]',
                'div[class*="security"]',
                'iframe[src*="captcha"]',
                'iframe[src*="challenge"]',
            ]
            
            for selector in block_selectors:
                if await page.locator(selector).count() > 0:
                    logger.warning(f"Soft block detected: '{selector}' element found on page")
                    return True
            
            # Check URL for block indicators
            url = page.url
            if 'captcha' in url or 'challenge' in url or 'blocked' in url or 'security' in url:
                logger.warning(f"Soft block detected in URL: {url}")
                return True
            
            return False
            
        except Exception as e:
            logger.warning(f"Error checking for soft block: {e}")
            return False
    
    async def has_captcha(self, page: Page) -> bool:
        """
        Check if a page has a CAPTCHA.
        
        Args:
            page: Playwright page
            
        Returns:
            True if the page has a CAPTCHA, False otherwise
        """
        try:
            # Get page content and title
            content = await page.content()
            title = await page.title()
            url = page.url
            
            # Check for CAPTCHA patterns in content, title, and URL
            content_lower = content.lower()
            title_lower = title.lower()
            url_lower = url.lower()
            
            for pattern in CAPTCHA_PATTERNS:
                if pattern in content_lower or pattern in title_lower or pattern in url_lower:
                    logger.warning(f"CAPTCHA detected: '{pattern}' found on page")
                    return True
            
            # Check for CAPTCHA-specific elements
            captcha_selectors = [
                'iframe[src*="recaptcha"]',
                'iframe[src*="hcaptcha"]',
                'iframe[src*="captcha"]',
                'div[class*="g-recaptcha"]',
                'div[class*="h-captcha"]',
                'div[class*="captcha"]',
                'div[id*="captcha"]',
            ]
            
            for selector in captcha_selectors:
                if await page.locator(selector).count() > 0:
                    logger.warning(f"CAPTCHA detected: '{selector}' element found on page")
                    return True
            
            return False
            
        except Exception as e:
            logger.warning(f"Error checking for CAPTCHA: {e}")
            return False
    
    async def accept_cookies(self, page: Page) -> bool:
        """
        Try to accept cookies on a page.
        
        Args:
            page: Playwright page
            
        Returns:
            True if cookies were accepted, False otherwise
        """
        try:
            # Try each cookie accept selector
            for selector in COOKIE_ACCEPT_SELECTORS:
                try:
                    # Check if the selector exists
                    count = await page.locator(selector).count()
                    if count > 0:
                        # Click the first matching element
                        await page.locator(selector).first.click(timeout=5000)
                        logger.info(f"Accepted cookies using selector: {selector}")
                        
                        # Wait a moment for the banner to disappear
                        await asyncio.sleep(1)
                        return True
                except Exception:
                    # Continue to the next selector
                    continue
            
            # No cookie banner found or none could be accepted
            return False
            
        except Exception as e:
            logger.warning(f"Error accepting cookies: {e}")
            return False
    
    async def reroll_identity(
        self,
        context: BrowserContext,
        domain_key: str = 'yelp',
        force_new_proxy: bool = False
    ) -> BrowserContext:
        """
        Reroll identity by creating a new context with potentially new proxy.
        
        Args:
            context: Current browser context
            domain_key: Domain key for session profiles
            force_new_proxy: Whether to force a new proxy URL
            
        Returns:
            A new browser context
        """
        logger.info("Rerolling identity")
        
        if self.metrics:
            self.metrics["rerolls"] = self.metrics.get("rerolls", 0) + 1
        
        # Close the current context
        await self.close_context(context)
        
        # Get a new proxy URL if forcing or randomly (50% chance)
        proxy_url = None
        if self.proxy_configuration and (force_new_proxy or random.random() < 0.5):
            proxy_url = await self.proxy_configuration.new_url()
        
        # Create a new context
        new_context = await self.new_context(
            domain_key=domain_key,
            use_session_profile=True,
            proxy_url=proxy_url
        )
        
        return new_context
    
    async def save_snapshot(self, page: Page, name: str) -> Optional[str]:
        """
        Save a snapshot of a page to the key-value store.
        
        Args:
            page: Playwright page
            name: Snapshot name
            
        Returns:
            The key of the saved snapshot, or None if saving failed
        """
        if not self.snapshots_store:
            return None
        
        try:
            # Generate a unique key
            timestamp = int(time.time())
            key = f"snapshot_{name}_{timestamp}.html"
            
            # Get page content
            content = await page.content()
            
            # Save to key-value store
            await self.snapshots_store.set(key, content)
            
            logger.info(f"Saved snapshot: {key}")
            return key
            
        except Exception as e:
            logger.warning(f"Error saving snapshot: {e}")
            return None
    
    async def __aenter__(self) -> 'BaseScraper':
        """Context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit with cleanup."""
        await self.close_all_contexts()
