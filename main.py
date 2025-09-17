#!/usr/bin/env python3
"""
Apify Actor for Yelp scraping using Playwright, Crawl4AI, and Grok LLM.
Extracts business details including name, rating, reviews, contact info, and more.
"""

import asyncio
import os
import sys
import time
from typing import Dict, Any, Optional, List, Tuple
from urllib.parse import urlparse

from apify import Actor
from playwright.async_api import async_playwright
from dotenv import load_dotenv  # NEW

# Import our modules
from app.scrapers.yelp_scraper import YelpScraper
from app.services.crawl4ai_client import configure_crawl4ai
from app.services.llm_structured import configure_llm

# Load environment variables from a local .env file (if present) for
# convenient local development. This is a no-op on Apify platform where
# variables are injected directly into the environment.
load_dotenv()


async def main():
    """Main entry point for the Apify Actor."""
    
    # Initialize the Actor
    async with Actor:
        # Get input and set defaults
        actor_input = await Actor.get_input() or {}
        
        # ------------------------------------------------------------------
        # 1) Build unified list of search tasks from multiple input flavours
        # ------------------------------------------------------------------

        keyword = actor_input.get('keyword')
        location = actor_input.get('location')
        single_search_url = actor_input.get('searchUrl')

        # Arrays
        queries_array = actor_input.get('queries') or []
        keywords_array = actor_input.get('keywords') or []
        locations_array = actor_input.get('locations') or []
        search_urls_array = actor_input.get('searchUrls') or []

        # Normalise to lists
        if isinstance(queries_array, dict):
            queries_array = [queries_array]

        search_tasks: List[Dict[str, Any]] = []

        # a) explicit query objects
        for q in queries_array:
            kw = q.get('keyword')
            loc = q.get('location')
            if kw and loc:
                search_tasks.append({'type': 'query', 'keyword': kw, 'location': loc})

        # b) cross-product keywords × locations arrays
        if keywords_array and locations_array:
            for kw in keywords_array:
                for loc in locations_array:
                    search_tasks.append({'type': 'query', 'keyword': kw, 'location': loc})

        # c) direct search URLs array
        for url in search_urls_array:
            if isinstance(url, str) and url.startswith('http'):
                search_tasks.append({'type': 'url', 'url': url})

        # d) legacy single fields (added last to preserve previous behaviour)
        if single_search_url:
            search_tasks.append({'type': 'url', 'url': single_search_url})
        elif keyword and location:
            search_tasks.append({'type': 'query', 'keyword': keyword, 'location': location})

        # Validate we have at least one task
        if not search_tasks:
            await Actor.fail(
                "No valid search task supplied. Provide 'queries', "
                "'keywords' + 'locations', 'searchUrls', or legacy fields."
            )
            return

        Actor.log.info(f"Prepared {len(search_tasks)} search task(s)")
        for idx, t in enumerate(search_tasks, 1):
            if t['type'] == 'url':
                Actor.log.info(f"  Task {idx}: URL -> {t['url']}")
            else:
                Actor.log.info(f"  Task {idx}: Query -> '{t['keyword']}' in '{t['location']}'")
        
        # Extract other inputs with defaults
        num_businesses = min(actor_input.get('numBusinesses', 50), 500)
        # Clamp concurrency between 3 and 5 (default 5)
        requested_concurrency = int(actor_input.get('concurrency', 5) or 5)
        concurrency = max(3, min(5, requested_concurrency))
        Actor.log.info(f"Concurrency set to {concurrency} (requested {requested_concurrency})")
        natural_navigation = actor_input.get('naturalNavigation', False)
        per_business_isolation = actor_input.get('perBusinessIsolation', False)
        entry_flow_ratios_str = actor_input.get('entryFlowRatios', 'google:0.6,direct:0.3,bing:0.1')
        debug_snapshot = actor_input.get('debugSnapshot', False)
        grok_model = actor_input.get('grokModel', 'grok-2')
        country = actor_input.get('country')

        # New configurable limits
        captcha_timeout_sec = int(actor_input.get('captchaTimeoutSeconds', 300))
        email_max_contact_pages = int(actor_input.get('emailMaxContactPages', 10))
        Actor.log.info(
            f"CAPTCHA timeout seconds: {captcha_timeout_sec}; "
            f"Email max contact pages: {email_max_contact_pages}"
        )
        
        # Parse entry flow ratios
        entry_flow_ratios = {}
        try:
            for part in entry_flow_ratios_str.split(','):
                if ':' in part:
                    k, v = part.split(':')
                    entry_flow_ratios[k.strip()] = float(v.strip())
            
            # Normalize ratios to sum to 1.0
            if entry_flow_ratios:
                total = sum(entry_flow_ratios.values())
                if total > 0:
                    entry_flow_ratios = {k: v/total for k, v in entry_flow_ratios.items()}
        except Exception as e:
            Actor.log.warning(f"Failed to parse entryFlowRatios: {e}. Using defaults.")
            entry_flow_ratios = {'google': 0.6, 'direct': 0.3, 'bing': 0.1}
        
        # ------------------------------------------------------------------
        # 2) Proxy configuration (SDK v2 automatically uses env variables)
        # ------------------------------------------------------------------
        # Simply create a ProxyConfiguration instance without arguments.
        # The Apify platform injects all required credentials via env vars.
        proxy_configuration = await Actor.create_proxy_configuration()
        if not proxy_configuration:
            Actor.log.warning("No proxy configuration available. Proceeding without proxy.")
        else:
            proxy_info = "RESIDENTIAL"
            if country:
                proxy_info += f" ({country})"
            Actor.log.info(f"Using Apify {proxy_info} proxy")
        
        # Check if Grok API key is available
        grok_api_key = os.environ.get('GROK_API_KEY')
        llm_enabled = bool(grok_api_key)
        
        if not llm_enabled:
            Actor.log.info("GROK_API_KEY not provided. LLM features will be disabled.")
        else:
            # Configure Crawl4AI and LLM services with Grok
            configure_crawl4ai(
                base_url="https://api.x.ai/v1",
                api_token=grok_api_key,
                model=grok_model
            )
            configure_llm(
                base_url="https://api.x.ai/v1",
                api_key=grok_api_key,
                model=grok_model
            )
        
        # Check if 2Captcha API key is available
        two_captcha_api_key = os.environ.get('TWO_CAPTCHA_API_KEY')
        captcha_solver_enabled = bool(two_captcha_api_key)
        
        if not captcha_solver_enabled:
            Actor.log.info("TWO_CAPTCHA_API_KEY not provided. Will retry with new identity on CAPTCHA.")
        else:
            Actor.log.info("2Captcha API key provided (not used for Yelp's custom CAPTCHA).")
        
        # Open key-value stores (SDK v2: pass name via keyword argument)
        sessions_store = await Actor.open_key_value_store(name="sessions")
        snapshots_store = (
            await Actor.open_key_value_store(name="snapshots") if debug_snapshot else None
        )
        
        # Log that robots.txt is being ignored per configuration
        Actor.log.info("Robots.txt checking disabled per configuration. Proceeding regardless of robots.txt rules.")
        
        # Initialize metrics
        metrics = {
            "businesses_attempted": 0,
            "businesses_scraped": 0,
            "jsonld_hits": 0,
            "crawl4ai_hits": 0,
            "llm_fallbacks": 0,
            "dom_fallbacks": 0,
            "email_found": 0,
            "email_missing": 0,
            "soft_blocks": 0,
            "captcha_hits": 0,
            "captcha_solved": 0,
            "captcha_failed": 0,
            "rerolls": 0,
            "errors": 0,
            "start_time": time.time()
        }
        
        # Launch Playwright and run the scraper
        async with async_playwright() as playwright:
            # Create browser instance with proxy
            browser_proxy = None
            if proxy_configuration:
                proxy_url = await proxy_configuration.new_url()
                # Inject country code into the proxy URL if requested and not present
                if country and "country=" not in proxy_url:
                    separator = "&" if "?" in proxy_url else "?"
                    proxy_url = f"{proxy_url}{separator}country={country}"
                parsed = urlparse(proxy_url)
                browser_proxy = {
                    "server": f"{parsed.scheme}://{parsed.netloc}",
                }
                if parsed.username and parsed.password:
                    browser_proxy["username"] = parsed.username
                    browser_proxy["password"] = parsed.password
                
                Actor.log.info(f"Configured Playwright with Residential proxy: {parsed.netloc}")
            
            # Launch Chromium with additional stealth arguments to reduce bot detection
            browser = await playwright.chromium.launch(
                headless=True,
                proxy=browser_proxy,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--disable-web-security',
                    '--disable-features=IsolateOrigins,site-per-process',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-accelerated-2d-canvas',
                    '--disable-gpu',
                    '--window-size=1920,1080',
                    '--start-maximized',
                    '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/120.0.0.0 Safari/537.36',
                ],
            )
            
            try:
                # Create YelpScraper instance
                scraper = YelpScraper(
                    browser=browser,
                    proxy_configuration=proxy_configuration,
                    sessions_store=sessions_store,
                    snapshots_store=snapshots_store,
                    concurrency=concurrency,
                    per_business_isolation=per_business_isolation,
                    llm_enabled=llm_enabled,
                    metrics=metrics,
                    solver_api_key=two_captcha_api_key,
                    captcha_timeout=captcha_timeout_sec,
                    email_max_contact_pages=email_max_contact_pages
                )
                
                # Calculate businesses per task (distribute evenly)
                businesses_per_task = num_businesses // len(search_tasks)
                # Add remainder to first task
                remainder = num_businesses % len(search_tasks)
                
                # Process each search task
                for task_idx, task in enumerate(search_tasks):
                    # Skip if we've already reached the target
                    if metrics["businesses_scraped"] >= num_businesses:
                        Actor.log.info(
                            f"Reached target of {num_businesses} businesses, skipping remaining tasks"
                        )
                        break
                    
                    # Calculate remaining budget
                    remaining_budget = num_businesses - metrics["businesses_scraped"]
                    # Calculate task budget (either the per-task allocation or what's left)
                    task_budget = min(
                        businesses_per_task + (remainder if task_idx == 0 else 0),
                        remaining_budget
                    )
                    
                    Actor.log.info(f"Processing task {task_idx+1}/{len(search_tasks)}, budget: {task_budget} businesses")
                    
                    # Run the scraper based on task type
                    if task["type"] == "url":
                        await scraper.scrape(
                            search_url=task["url"],
                            num_businesses=task_budget,
                            natural_navigation=natural_navigation,
                            entry_flow_ratios=entry_flow_ratios
                        )
                    else:  # task["type"] == "query"
                        await scraper.scrape(
                            keyword=task["keyword"],
                            location=task["location"],
                            num_businesses=task_budget,
                            natural_navigation=natural_navigation,
                            entry_flow_ratios=entry_flow_ratios
                        )
                    
                    # Log progress after each task
                    Actor.log.info(
                        f"Task {task_idx+1} complete: {metrics['businesses_scraped']} "
                        f"businesses scraped so far ({remaining_budget} remaining)"
                    )
                
            except Exception as e:
                Actor.log.error(f"Scraper failed: {e}")
                metrics["errors"] += 1
                raise
            finally:
                # Close the browser
                await browser.close()
                
                # Log final metrics
                elapsed = time.time() - metrics["start_time"]
                Actor.log.info(f"Scraping completed in {elapsed:.2f} seconds")
                Actor.log.info(f"Businesses attempted: {metrics['businesses_attempted']}")
                Actor.log.info(f"Businesses successfully scraped: {metrics['businesses_scraped']}")
                Actor.log.info(f"JSON-LD extractions: {metrics['jsonld_hits']}")
                
                if llm_enabled:
                    Actor.log.info(f"Crawl4AI extractions: {metrics['crawl4ai_hits']}")
                    Actor.log.info(f"LLM fallbacks: {metrics['llm_fallbacks']}")
                
                Actor.log.info(f"DOM fallbacks: {metrics['dom_fallbacks']}")
                Actor.log.info(f"Emails found: {metrics['email_found']}")
                Actor.log.info(f"Emails missing: {metrics['email_missing']}")
                Actor.log.info(f"Soft blocks encountered: {metrics['soft_blocks']}")
                Actor.log.info(f"CAPTCHA challenges: {metrics['captcha_hits']}")

                # For Yelp we do not actually solve CAPTCHAs via 2Captcha,
                # therefore omit misleading “solved / failures” logs.
                
                Actor.log.info(f"Identity rerolls: {metrics['rerolls']}")
                Actor.log.info(f"Errors: {metrics['errors']}")
                
                # Check if we met the target number of businesses
                if metrics["businesses_scraped"] < num_businesses:
                    Actor.log.warning(
                        f"Only scraped {metrics['businesses_scraped']} businesses "
                        f"out of requested {num_businesses}"
                    )


if __name__ == "__main__":
    # Run the Actor using the asyncio entry-point pattern (SDK v2+)
    asyncio.run(main())
