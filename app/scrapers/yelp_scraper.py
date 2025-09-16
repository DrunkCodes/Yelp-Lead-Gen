"""
Yelp scraper implementation using Playwright.
Extracts business data from Yelp search results and detail pages.
"""

import asyncio
import json
import logging
import random
import re
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import parse_qs, quote, unquote, urlencode, urljoin, urlparse

from apify import Actor
from playwright.async_api import BrowserContext, Page, Response

from app.models.schemas import YelpBusiness, compute_years_in_business, merge_business_data
from app.scrapers.base import BaseScraper, SoftBlockError, CaptchaError
from app.services.crawl4ai_client import extract_with_crawl4ai, is_crawl4ai_available
from app.services.email_extractor import extract_email_from_website
from app.services.llm_structured import extract_structured_from_html
from app.utils.retry import retry_async, with_retry

logger = logging.getLogger(__name__)


class YelpScraper(BaseScraper):
    """
    Yelp scraper for extracting business data from Yelp.
    """
    
    def __init__(self, *args, **kwargs):
        """Initialize the Yelp scraper."""
        super().__init__(*args, **kwargs)
        
        # Set up search engines for natural navigation
        self.search_engines = {
            'google': 'https://www.google.com/search?q=site%3Ayelp.com+{query}',
            'bing': 'https://www.bing.com/search?q=site%3Ayelp.com+{query}',
        }
        
        # Initialize set to track seen business URLs
        self.seen_business_urls: Set[str] = set()
    
    async def scrape(
        self,
        keyword: Optional[str] = None,
        location: Optional[str] = None,
        search_url: Optional[str] = None,
        num_businesses: int = 50,
        natural_navigation: bool = False,
        entry_flow_ratios: Optional[Dict[str, float]] = None
    ) -> None:
        """
        Main scraping method.
        
        Args:
            keyword: Search keyword (e.g., 'restaurants')
            location: Search location (e.g., 'New York, NY')
            search_url: Direct Yelp search URL (overrides keyword/location)
            num_businesses: Number of businesses to scrape
            natural_navigation: Whether to use natural navigation via search engines
            entry_flow_ratios: Ratios for different entry flows (e.g., {'google': 0.6, 'direct': 0.3, 'bing': 0.1})
        """
        # Load session profiles
        await self._load_session_profiles('yelp', 5)
        
        # Determine the entry flow
        if not entry_flow_ratios:
            entry_flow_ratios = {'google': 0.6, 'direct': 0.3, 'bing': 0.1}
        
        # Build the search URL if not provided
        if not search_url and keyword and location:
            search_url = self.build_search_url(keyword, location)
        
        if not search_url:
            await Actor.log.error("No search URL provided or could not be built")
            return
        
        # Create initial context
        context = await self.new_context('yelp', True)
        
        try:
            # Create a new page
            page = await context.new_page()
            
            # Determine entry flow based on ratios
            entry_flow = 'direct'
            if natural_navigation and random.random() < sum(v for k, v in entry_flow_ratios.items() if k != 'direct'):
                # Choose a search engine based on ratios
                engines = [k for k in entry_flow_ratios.keys() if k != 'direct']
                weights = [entry_flow_ratios[k] for k in engines]
                total_weight = sum(weights)
                if total_weight > 0:
                    normalized_weights = [w / total_weight for w in weights]
                    entry_flow = random.choices(engines, normalized_weights)[0]
            
            # Navigate to Yelp
            yelp_url = search_url
            referer = None
            
            # If using natural navigation, go through a search engine first
            if entry_flow != 'direct' and entry_flow in self.search_engines:
                search_query = f"{keyword} {location}" if keyword and location else "yelp"
                engine_url = self.search_engines[entry_flow].format(query=quote(search_query))
                
                await Actor.log.info(f"Using natural navigation via {entry_flow}")
                
                # retry loop with reroll
                max_rerolls = 3
                for reroll_idx in range(max_rerolls + 1):
                    try:
                        # Navigate to the search engine
                        await self.navigate(page, engine_url)
                        # Look for Yelp links
                        yelp_link_selector = 'a[href*="yelp.com"]'
                        await page.wait_for_selector(yelp_link_selector, timeout=10000)

                        # Get all Yelp links
                        yelp_links = await page.query_selector_all(yelp_link_selector)

                        if yelp_links:
                            # Click the first Yelp link
                            await yelp_links[0].click()

                            # Wait for navigation
                            await page.wait_for_load_state('domcontentloaded')

                            # Update the Yelp URL and referer
                            yelp_url = page.url
                            referer = engine_url

                            await Actor.log.info(
                                f"Natural navigation successful, landed on: {yelp_url}"
                            )

                            # Success – exit reroll loop
                            break
                        else:
                            await Actor.log.warning(
                                "No Yelp links found in search results, falling back to direct navigation"
                            )
                            break
                    except (SoftBlockError, CaptchaError) as e:
                        if reroll_idx < max_rerolls:
                            await Actor.log.warning(
                                f"Soft block/ CAPTCHA ({e}) on engine entry, reroll {reroll_idx+1}/{max_rerolls}")
                            context = await self.reroll_identity(context, 'yelp', True)
                            page = await context.new_page()
                        else:
                            await Actor.log.warning("Natural navigation failed after rerolls, falling back to direct navigation")
                            break
                    except Exception as e:
                        await Actor.log.warning(f"Natural navigation failed: {e}, falling back to direct navigation")
                        break
            
            # If we're not already on a search results page, navigate to the search URL
            if not re.search(r'/search\?', page.url):
                max_rerolls = 3
                for reroll_idx in range(max_rerolls + 1):
                    try:
                        await self.navigate(page, yelp_url, referer)
                        break
                    except (SoftBlockError, CaptchaError) as e:
                        if reroll_idx < max_rerolls:
                            await Actor.log.warning(
                                f"Soft block/ CAPTCHA on Yelp search page ({e}), reroll {reroll_idx+1}/{max_rerolls}")
                            context = await self.reroll_identity(context, 'yelp', True)
                            page = await context.new_page()
                        else:
                            raise
            
            # Collect business links
            business_links = await self.collect_business_links(
                page, num_businesses, search_url
            )
            
            await Actor.log.info(f"Collected {len(business_links)} business links")
            
            # Process business links
            tasks = []
            for i, link in enumerate(business_links[:num_businesses]):
                task = asyncio.create_task(
                    self.process_business(
                        link,
                        referer=search_url,
                        index=i,
                        total=min(len(business_links), num_businesses)
                    )
                )
                tasks.append(task)
                
                # Start tasks in small batches to avoid overwhelming the system
                if len(tasks) >= self.concurrency:
                    # Wait for some tasks to complete before adding more
                    done, tasks = await asyncio.wait(
                        tasks, return_when=asyncio.FIRST_COMPLETED
                    )
            
            # Wait for all remaining tasks to complete
            if tasks:
                await asyncio.gather(*tasks)
                
        except Exception as e:
            await Actor.log.error(f"Error during scraping: {e}")
            if self.metrics:
                self.metrics["errors"] = self.metrics.get("errors", 0) + 1
        finally:
            # Close the context
            await self.close_context(context)
    
    def build_search_url(self, keyword: str, location: str) -> str:
        """
        Build a Yelp search URL from keyword and location.
        
        Args:
            keyword: Search keyword (e.g., 'restaurants')
            location: Search location (e.g., 'New York, NY')
            
        Returns:
            Yelp search URL
        """
        # Clean and encode the keyword and location
        keyword_clean = quote(keyword.strip())
        location_clean = quote(location.strip())
        
        # Build the search URL
        search_url = f"https://www.yelp.com/search?find_desc={keyword_clean}&find_loc={location_clean}"
        
        return search_url
    
    async def collect_business_links(
        self,
        page: Page,
        num_links: int,
        referer: Optional[str] = None
    ) -> List[str]:
        """
        Collect business links from search results pages.
        
        Args:
            page: Playwright page
            num_links: Number of links to collect
            referer: Referer URL for pagination
            
        Returns:
            List of business detail page URLs
        """
        business_links: List[str] = []
        page_num = 1
        max_pages = 25  # Limit to 25 pages to avoid excessive scraping
        
        while len(business_links) < num_links and page_num <= max_pages:
            await Actor.log.info(f"Collecting links from page {page_num}")
            
            # Wait for business links to load
            try:
                await page.wait_for_selector('a[href^="/biz/"]', timeout=10000)
            except Exception as e:
                await Actor.log.warning(f"No business links found on page {page_num}: {e}")
                break
            
            # Extract business links
            links = await page.query_selector_all('a[href^="/biz/"]')
            
            # Process links
            for link in links:
                try:
                    href = await link.get_attribute('href')
                    if not href:
                        continue
                    
                    # Normalize the URL
                    full_url = urljoin('https://www.yelp.com', href)
                    
                    # Skip links with query parameters (often not business detail pages)
                    if '?' in full_url:
                        continue
                    
                    # Skip links to reviews, photos, etc.
                    if any(segment in full_url for segment in ['/review/', '/reviews/', '/photos/', '/menu/', '/questions/']):
                        continue
                    
                    # Skip already seen URLs
                    if full_url in self.seen_business_urls:
                        continue
                    
                    # Add to the list and mark as seen
                    business_links.append(full_url)
                    self.seen_business_urls.add(full_url)
                    
                    # Break if we have enough links
                    if len(business_links) >= num_links:
                        break
                        
                except Exception as e:
                    await Actor.log.warning(f"Error processing link: {e}")
            
            # Check if we have enough links
            if len(business_links) >= num_links:
                break
            
            # Try to go to the next page
            next_page = await self._go_to_next_page(page, referer)
            if not next_page:
                # Try infinite scroll as a fallback
                scrolled = await self._try_infinite_scroll(page)
                if not scrolled:
                    await Actor.log.info("No more pages available")
                    break
            
            page_num += 1
        
        return business_links
    
    async def _go_to_next_page(self, page: Page, referer: Optional[str] = None) -> bool:
        """
        Navigate to the next page of search results.
        
        Args:
            page: Playwright page
            referer: Referer URL
            
        Returns:
            True if navigation was successful, False otherwise
        """
        # List of possible next page selectors
        next_selectors = [
            'a.next-link',
            'a[aria-label="Next"]',
            'a.next',
            'a[href*="start="] span:has-text("Next")',
            '.pagination a:has-text("Next")',
            'a.next-page',
            '.pagination-links .next',
            '.pagination .next a',
        ]
        
        for selector in next_selectors:
            try:
                # Check if the selector exists
                next_link = await page.query_selector(selector)
                if next_link:
                    # Click the next link
                    await next_link.click()
                    
                    # Wait for navigation
                    await page.wait_for_load_state('domcontentloaded')
                    
                    # Wait for business links to appear
                    try:
                        await page.wait_for_selector('a[href^="/biz/"]', timeout=10000)
                        return True
                    except Exception:
                        continue
            except Exception:
                continue
        
        return False
    
    async def _try_infinite_scroll(self, page: Page) -> bool:
        """
        Try to load more results by scrolling down (for infinite scroll pages).
        
        Args:
            page: Playwright page
            
        Returns:
            True if more results were loaded, False otherwise
        """
        # Get initial number of business links
        initial_count = await page.locator('a[href^="/biz/"]').count()
        
        # Scroll to the bottom 3 times
        for _ in range(3):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(2)  # Wait for content to load
        
        # Check if more links were loaded
        new_count = await page.locator('a[href^="/biz/"]').count()
        
        return new_count > initial_count
    
    @with_retry(max_tries=3, base_delay=2.0, jitter=True)
    async def process_business(
        self,
        url: str,
        referer: Optional[str] = None,
        index: int = 0,
        total: int = 0
    ) -> None:
        """
        Process a business detail page.
        
        Args:
            url: Business detail page URL
            referer: Referer URL
            index: Index of the business in the list
            total: Total number of businesses
        """
        if self.metrics:
            self.metrics["businesses_attempted"] = self.metrics.get("businesses_attempted", 0) + 1
        
        # Use a semaphore to limit concurrent processing
        async with self.semaphore:
            # Create a new context if per-business isolation is enabled
            if self.per_business_isolation:
                context = await self.new_context('yelp', True)
            else:
                # Reuse the existing context
                context = self.contexts[0] if self.contexts else await self.new_context('yelp', True)
            
            try:
                # Create a new page
                page = await context.new_page()
                
                # Navigate to the business page with reroll logic
                max_rerolls = 3
                for reroll_idx in range(max_rerolls + 1):
                    try:
                        await self.navigate(page, url, referer)
                        break
                    except (SoftBlockError, CaptchaError) as e:
                        if reroll_idx < max_rerolls:
                            await Actor.log.warning(
                                f"Soft block/ CAPTCHA on business page ({e}), reroll {reroll_idx+1}/{max_rerolls}")
                            context = await self.reroll_identity(context, 'yelp', True)
                            page = await context.new_page()
                        else:
                            raise
                
                # Save snapshot if debug is enabled
                if self.snapshots_store:
                    await self.save_snapshot(page, f"business_{index}")
                
                # Extract business data using multiple methods
                await Actor.log.info(f"Extracting data for business {index+1}/{total}: {url}")
                
                # Layer A: Extract data from JSON-LD
                jsonld_data = await self._extract_jsonld(page)
                
                # Track JSON-LD hits
                if jsonld_data and self.metrics:
                    self.metrics["jsonld_hits"] = self.metrics.get("jsonld_hits", 0) + 1
                
                # Layer B: Extract data using Crawl4AI if LLM is enabled
                crawl4ai_data = {}
                if self.llm_enabled and await is_crawl4ai_available():
                    try:
                        # Get proxy URL for Crawl4AI
                        proxy_url = None
                        if self.proxy_configuration:
                            proxy_url = await self.proxy_configuration.new_url()
                        
                        # Extract data using Crawl4AI
                        crawl4ai_data = await extract_with_crawl4ai(
                            url=url,
                            proxy_url=proxy_url,
                            referer=referer,
                            user_agent=await page.evaluate("navigator.userAgent")
                        )
                        
                        if crawl4ai_data and self.metrics:
                            self.metrics["crawl4ai_hits"] = self.metrics.get("crawl4ai_hits", 0) + 1
                    except Exception as e:
                        await Actor.log.warning(f"Crawl4AI extraction failed: {e}")
                
                # Layer C: Extract data using LLM fallback if LLM is enabled
                llm_data = {}
                if self.llm_enabled and (not jsonld_data or not all(jsonld_data.values())):
                    try:
                        # Get HTML content
                        html_content = await page.content()
                        
                        # Extract data using LLM
                        llm_data = await extract_structured_from_html(html_content)
                        
                        if llm_data and self.metrics:
                            self.metrics["llm_fallbacks"] = self.metrics.get("llm_fallbacks", 0) + 1
                    except Exception as e:
                        await Actor.log.warning(f"LLM extraction failed: {e}")
                
                # Layer D: Extract data using DOM selectors as fallback
                dom_data = await self._extract_dom_data(page)
                
                if dom_data and self.metrics:
                    self.metrics["dom_fallbacks"] = self.metrics.get("dom_fallbacks", 0) + 1
                
                # Merge data from all sources, with priority: JSON-LD > Crawl4AI > LLM > DOM
                business_data = jsonld_data or {}
                
                # Merge in order of decreasing priority
                if crawl4ai_data:
                    business_data = merge_business_data(business_data, crawl4ai_data)
                
                if llm_data:
                    business_data = merge_business_data(business_data, llm_data)
                
                if dom_data:
                    business_data = merge_business_data(business_data, dom_data)
                
                # Skip if no business name
                if not business_data.get('business_name'):
                    await Actor.log.warning(f"No business name found for {url}, skipping")
                    return
                
                # Dereference website URL if it's a Yelp redirect
                if business_data.get('website'):
                    website = business_data['website']
                    if '/biz_redir?' in website:
                        try:
                            # Dereference the redirect
                            real_url = await self._dereference_yelp_redirect(page, website)
                            if real_url:
                                business_data['website'] = real_url
                        except Exception as e:
                            await Actor.log.warning(f"Failed to dereference website: {e}")
                
                # Extract email from website if available
                email = None
                if business_data.get('website'):
                    try:
                        # Get proxy URL for email extraction
                        proxy_url = None
                        if self.proxy_configuration:
                            proxy_url = await self.proxy_configuration.new_url()
                        
                        # Extract email from website
                        email = await extract_email_from_website(
                            url=business_data['website'],
                            proxy_url=proxy_url,
                            referer=url,
                            llm_enabled=self.llm_enabled,
                            max_contact_pages=self.email_max_contact_pages
                        )
                        
                        if email:
                            business_data['email'] = email
                            if self.metrics:
                                self.metrics["email_found"] = self.metrics.get("email_found", 0) + 1
                        elif self.metrics:
                            self.metrics["email_missing"] = self.metrics.get("email_missing", 0) + 1
                    except Exception as e:
                        await Actor.log.warning(f"Email extraction failed: {e}")
                        if self.metrics:
                            self.metrics["email_missing"] = self.metrics.get("email_missing", 0) + 1
                
                # Create YelpBusiness object for validation
                try:
                    business = YelpBusiness(**business_data)
                    
                    # Push data to dataset
                    await Actor.push_data(business.dict_for_dataset())
                    
                    if self.metrics:
                        self.metrics["businesses_scraped"] = self.metrics.get("businesses_scraped", 0) + 1
                    
                    await Actor.log.info(f"Successfully scraped business: {business.business_name}")
                except Exception as e:
                    await Actor.log.error(f"Failed to create business object: {e}")
                    if self.metrics:
                        self.metrics["errors"] = self.metrics.get("errors", 0) + 1
                
            except Exception as e:
                await Actor.log.error(f"Error processing business {url}: {e}")
                if self.metrics:
                    self.metrics["errors"] = self.metrics.get("errors", 0) + 1
                raise
            finally:
                # Close the page
                await page.close()
                
                # Close the context if per-business isolation is enabled
                if self.per_business_isolation:
                    await self.close_context(context)
    
    async def _extract_jsonld(self, page: Page) -> Dict[str, Any]:
        """
        Extract business data from JSON-LD on the page.
        
        Args:
            page: Playwright page
            
        Returns:
            Dictionary with extracted business data
        """
        try:
            # Extract JSON-LD data
            jsonld_data = await page.evaluate("""
            () => {
                const jsonLdScripts = document.querySelectorAll('script[type="application/ld+json"]');
                const results = [];
                
                for (const script of jsonLdScripts) {
                    try {
                        const data = JSON.parse(script.textContent);
                        results.push(data);
                    } catch (e) {
                        // Skip invalid JSON
                    }
                }
                
                return results;
            }
            """)
            
            if not jsonld_data:
                return {}
            
            # Find the business JSON-LD
            business_data = {}
            for data in jsonld_data:
                # Check if it's a business
                if isinstance(data, dict) and data.get('@type') in ['LocalBusiness', 'Restaurant', 'Store', 'Organization']:
                    business_data = data
                    break
                # Handle array of items
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get('@type') in ['LocalBusiness', 'Restaurant', 'Store', 'Organization']:
                            business_data = item
                            break
            
            if not business_data:
                return {}
            
            # Map JSON-LD fields to our schema
            result = {}
            
            # Business name
            if business_data.get('name'):
                result['business_name'] = business_data['name']
            
            # Rating
            if business_data.get('aggregateRating', {}).get('ratingValue'):
                try:
                    result['rating'] = float(business_data['aggregateRating']['ratingValue'])
                except (ValueError, TypeError):
                    pass
            
            # Review count
            if business_data.get('aggregateRating', {}).get('reviewCount'):
                try:
                    result['review_count'] = int(business_data['aggregateRating']['reviewCount'])
                except (ValueError, TypeError):
                    pass
            
            # Industry/category
            if business_data.get('category'):
                result['industry'] = business_data['category']
            elif business_data.get('servesCuisine'):
                result['industry'] = business_data['servesCuisine']
            
            # Phone
            if business_data.get('telephone'):
                result['phone'] = business_data['telephone']
            
            # Website
            if business_data.get('url'):
                result['website'] = business_data['url']
            
            # Years in business (compute from founding date)
            founding_date = None
            if business_data.get('foundingDate'):
                founding_date = business_data['foundingDate']
            elif business_data.get('dateCreated'):
                founding_date = business_data['dateCreated']
            
            if founding_date:
                years = compute_years_in_business(founding_date)
                if years is not None:
                    result['years_in_business'] = years
            
            return result
            
        except Exception as e:
            await Actor.log.warning(f"Error extracting JSON-LD: {e}")
            return {}
    
    async def _extract_dom_data(self, page: Page) -> Dict[str, Any]:
        """
        Extract business data from DOM elements.
        
        Args:
            page: Playwright page
            
        Returns:
            Dictionary with extracted business data
        """
        result = {}
        
        try:
            # Business name
            name_element = await page.query_selector('h1')
            if name_element:
                name = await name_element.text_content()
                if name:
                    result['business_name'] = name.strip()
            
            # Rating
            rating_selectors = [
                '[role="img"][aria-label*="star rating"]',
                'meta[itemprop="ratingValue"]',
                '.rating-info .star-rating',
                '.rating-large',
                '.rating'
            ]
            
            for selector in rating_selectors:
                try:
                    rating_element = await page.query_selector(selector)
                    if rating_element:
                        if selector == 'meta[itemprop="ratingValue"]':
                            rating_str = await rating_element.get_attribute('content')
                        else:
                            rating_str = await rating_element.get_attribute('aria-label')
                            if rating_str and 'star rating' in rating_str.lower():
                                rating_str = re.search(r'([\d.]+)\s*star', rating_str.lower())
                                if rating_str:
                                    rating_str = rating_str.group(1)
                        
                        if rating_str:
                            try:
                                result['rating'] = float(rating_str)
                                break
                            except (ValueError, TypeError):
                                pass
                except Exception:
                    continue
            
            # Review count
            review_count_selectors = [
                '[href*="reviews"] .review-count',
                '[href*="reviews"]',
                '.review-count',
                '.rating-qualifier'
            ]
            
            for selector in review_count_selectors:
                try:
                    review_element = await page.query_selector(selector)
                    if review_element:
                        review_text = await review_element.text_content()
                        if review_text:
                            review_match = re.search(r'(\d+)\s*review', review_text.lower())
                            if review_match:
                                try:
                                    result['review_count'] = int(review_match.group(1))
                                    break
                                except (ValueError, TypeError):
                                    pass
                except Exception:
                    continue
            
            # Industry/category
            category_selectors = [
                '.category-str-list a',
                '.category-links a',
                '[href*="c_"]:not([href*="search"])',
                '.category'
            ]
            
            categories = []
            for selector in category_selectors:
                try:
                    category_elements = await page.query_selector_all(selector)
                    for element in category_elements:
                        category = await element.text_content()
                        if category:
                            categories.append(category.strip())
                except Exception:
                    continue
            
            if categories:
                result['industry'] = ', '.join(categories)
            
            # Phone
            phone_selectors = [
                'a[href^="tel:"]',
                '[href*="phone"]',
                '.phone',
                '.phone-number'
            ]
            
            for selector in phone_selectors:
                try:
                    phone_element = await page.query_selector(selector)
                    if phone_element:
                        if selector == 'a[href^="tel:"]':
                            phone = await phone_element.get_attribute('href')
                            if phone:
                                phone = phone.replace('tel:', '')
                        else:
                            phone = await phone_element.text_content()
                        
                        if phone:
                            result['phone'] = phone.strip()
                            break
                except Exception:
                    continue
            
            # Website
            website_selectors = [
                'a:has-text("Website")',
                'a:has-text("Official Website")',
                'a[href*="biz_redir"]',
                '.website-link'
            ]
            
            for selector in website_selectors:
                try:
                    website_element = await page.query_selector(selector)
                    if website_element:
                        website = await website_element.get_attribute('href')
                        if website and not website.startswith('mailto:') and not website.startswith('tel:'):
                            result['website'] = website
                            break
                except Exception:
                    continue
            
            # Years in business
            years_selectors = [
                '.years-in-business',
                ':has-text("Established in")',
                ':has-text("Est.")',
                ':has-text("Established")',
                ':has-text("Founded")',
                ':has-text("In business since")',
                ':has-text("years in business")'
            ]
            
            for selector in years_selectors:
                try:
                    years_element = await page.query_selector(selector)
                    if years_element:
                        years_text = await years_element.text_content()
                        if years_text:
                            # Try to extract year established
                            year_match = re.search(r'(?:established|est\.?|founded)(?:\s+in)?\s+(\d{4})', years_text.lower())
                            if year_match:
                                founding_year = int(year_match.group(1))
                                years = compute_years_in_business(founding_year)
                                if years is not None:
                                    result['years_in_business'] = years
                                    break
                            
                            # Try to extract years in business directly
                            years_match = re.search(r'(\d+)\s*(?:years?|yrs?)(?:\s+in\s+business)?', years_text.lower())
                            if years_match:
                                try:
                                    result['years_in_business'] = int(years_match.group(1))
                                    break
                                except (ValueError, TypeError):
                                    pass
                except Exception:
                    continue
            
            return result
            
        except Exception as e:
            await Actor.log.warning(f"Error extracting DOM data: {e}")
            return {}
    
    async def _dereference_yelp_redirect(self, page: Page, redirect_url: str) -> Optional[str]:
        """
        Dereference a Yelp redirect URL to get the actual website URL.
        
        Args:
            page: Playwright page
            redirect_url: Yelp redirect URL
            
        Returns:
            The actual website URL, or None if dereferencing failed
        """
        # If it's not a Yelp redirect, return as is
        if not redirect_url or '/biz_redir?' not in redirect_url:
            return redirect_url
        
        try:
            # Try to extract the URL from the redirect parameter
            parsed = urlparse(redirect_url)
            query_params = parse_qs(parsed.query)
            
            if 'url' in query_params:
                return unquote(query_params['url'][0])
            
            # If extraction failed, follow the redirect
            redirect_page = await page.context.new_page()
            try:
                response = await redirect_page.goto(redirect_url, wait_until='domcontentloaded')
                if response and response.status in [301, 302, 303, 307, 308]:
                    return response.headers.get('location')
                return redirect_page.url
            finally:
                await redirect_page.close()
                
        except Exception as e:
            await Actor.log.warning(f"Error dereferencing redirect: {e}")
            return None
