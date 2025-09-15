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

# Import our modules
from app.scrapers.yelp_scraper import YelpScraper
from app.utils.robots import check_robots_allowed
from app.services.crawl4ai_client import configure_crawl4ai
from app.services.llm_structured import configure_llm


async def main():
    """Main entry point for the Apify Actor."""
    
    # Initialize the Actor
    async with Actor:
        # Get input and set defaults
        actor_input = await Actor.get_input() or {}
        
        # Extract and validate required inputs
        keyword = actor_input.get('keyword')
        location = actor_input.get('location')
        search_url = actor_input.get('searchUrl')
        
        # Validate inputs
        if not search_url and not (keyword and location):
            await Actor.fail("Either 'searchUrl' or both 'keyword' and 'location' must be provided")
            return
        
        # Extract other inputs with defaults
        num_businesses = min(actor_input.get('numBusinesses', 50), 500)
        # Clamp concurrency between 3 and 5 (default 5)
        requested_concurrency = int(actor_input.get('concurrency', 5) or 5)
        concurrency = max(3, min(5, requested_concurrency))
        await Actor.log.info(f"Concurrency set to {concurrency} (requested {requested_concurrency})")
        natural_navigation = actor_input.get('naturalNavigation', False)
        per_business_isolation = actor_input.get('perBusinessIsolation', False)
        entry_flow_ratios_str = actor_input.get('entryFlowRatios', 'google:0.6,direct:0.3,bing:0.1')
        debug_snapshot = actor_input.get('debugSnapshot', False)
        grok_model = actor_input.get('grokModel', 'grok-2')
        country = actor_input.get('country')
        
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
            await Actor.log.warning(f"Failed to parse entryFlowRatios: {e}. Using defaults.")
            entry_flow_ratios = {'google': 0.6, 'direct': 0.3, 'bing': 0.1}
        
        # Set up proxy configuration
        proxy_config_options = {
            'groups': ['RESIDENTIAL']
        }
        if country:
            proxy_config_options['countryCode'] = country
            
        proxy_configuration = await Actor.create_proxy_configuration(proxy_config_options)
        if not proxy_configuration:
            await Actor.log.warning("No proxy configuration available. Proceeding without proxy.")
        
        # Check if Grok API key is available
        grok_api_key = os.environ.get('GROK_API_KEY')
        llm_enabled = bool(grok_api_key)
        
        if not llm_enabled:
            await Actor.log.info("GROK_API_KEY not provided. LLM features will be disabled.")
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
        
        # Open key-value stores for sessions and debug snapshots
        sessions_store = await Actor.open_key_value_store('sessions')
        snapshots_store = await Actor.open_key_value_store('snapshots') if debug_snapshot else None
        
        # Check robots.txt before proceeding
        if search_url:
            domain = urlparse(search_url).netloc
        else:
            domain = "www.yelp.com"
        
        robots_allowed = await check_robots_allowed(
            domain=domain,
            paths=["/", "/search", "/biz"],
            proxy_url=await proxy_configuration.new_url() if proxy_configuration else None
        )
        
        if not robots_allowed:
            await Actor.log.error(f"Access disallowed by robots.txt for {domain}")
            await Actor.fail("Access disallowed by robots.txt")
            return
        
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
                parsed = urlparse(proxy_url)
                browser_proxy = {
                    "server": f"{parsed.scheme}://{parsed.netloc}",
                }
                if parsed.username and parsed.password:
                    browser_proxy["username"] = parsed.username
                    browser_proxy["password"] = parsed.password
            
            browser = await playwright.chromium.launch(
                headless=True,
                proxy=browser_proxy
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
                    metrics=metrics
                )
                
                # Run the scraper
                await scraper.scrape(
                    keyword=keyword,
                    location=location,
                    search_url=search_url,
                    num_businesses=num_businesses,
                    natural_navigation=natural_navigation,
                    entry_flow_ratios=entry_flow_ratios
                )
                
            except Exception as e:
                await Actor.log.error(f"Scraper failed: {e}")
                metrics["errors"] += 1
                raise
            finally:
                # Close the browser
                await browser.close()
                
                # Log final metrics
                elapsed = time.time() - metrics["start_time"]
                await Actor.log.info(f"Scraping completed in {elapsed:.2f} seconds")
                await Actor.log.info(f"Businesses attempted: {metrics['businesses_attempted']}")
                await Actor.log.info(f"Businesses successfully scraped: {metrics['businesses_scraped']}")
                await Actor.log.info(f"JSON-LD extractions: {metrics['jsonld_hits']}")
                
                if llm_enabled:
                    await Actor.log.info(f"Crawl4AI extractions: {metrics['crawl4ai_hits']}")
                    await Actor.log.info(f"LLM fallbacks: {metrics['llm_fallbacks']}")
                
                await Actor.log.info(f"DOM fallbacks: {metrics['dom_fallbacks']}")
                await Actor.log.info(f"Emails found: {metrics['email_found']}")
                await Actor.log.info(f"Emails missing: {metrics['email_missing']}")
                await Actor.log.info(f"Soft blocks encountered: {metrics['soft_blocks']}")
                await Actor.log.info(f"CAPTCHA challenges: {metrics['captcha_hits']}")
                await Actor.log.info(f"Identity rerolls: {metrics['rerolls']}")
                await Actor.log.info(f"Errors: {metrics['errors']}")
                
                # Check if we met the target number of businesses
                if metrics["businesses_scraped"] < num_businesses:
                    await Actor.log.warning(
                        f"Only scraped {metrics['businesses_scraped']} businesses "
                        f"out of requested {num_businesses}"
                    )


if __name__ == "__main__":
    # Run the Actor
    Actor.main(main)
