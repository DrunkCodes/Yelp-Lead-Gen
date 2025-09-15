"""
CAPTCHA solver integration using 2Captcha service.
Supports reCAPTCHA v2, hCaptcha, and Cloudflare Turnstile.
"""

import asyncio
import json
import logging
import re
import time
from typing import Dict, Optional, Tuple, Any, Union, List

import httpx
from playwright.async_api import Page

logger = logging.getLogger(__name__)

# 2Captcha API endpoints
CAPTCHA_API_URL = "https://2captcha.com/in.php"
CAPTCHA_RESULT_URL = "https://2captcha.com/res.php"

# Constants
POLLING_INTERVAL = 5  # seconds
MAX_POLLING_TIME = 180  # seconds (3 minutes)


async def detect_captcha(page: Page) -> Dict[str, Any]:
    """
    Detect the type of CAPTCHA present on the page and extract its sitekey.
    
    Args:
        page: Playwright page object
        
    Returns:
        Dictionary with 'type' (recaptcha, hcaptcha, turnstile, or none) and 'sitekey' if found
    """
    result = {
        'type': 'none',
        'sitekey': None,
        'action': None,
        'data_s': None,
        'url': page.url,
    }
    
    try:
        # Check for reCAPTCHA
        recaptcha_sitekey = await _extract_recaptcha_sitekey(page)
        if recaptcha_sitekey:
            result['type'] = 'recaptcha'
            result['sitekey'] = recaptcha_sitekey
            logger.info(f"Detected reCAPTCHA with sitekey: {recaptcha_sitekey}")
            return result
        
        # Check for hCaptcha
        hcaptcha_sitekey = await _extract_hcaptcha_sitekey(page)
        if hcaptcha_sitekey:
            result['type'] = 'hcaptcha'
            result['sitekey'] = hcaptcha_sitekey
            logger.info(f"Detected hCaptcha with sitekey: {hcaptcha_sitekey}")
            return result
        
        # Check for Cloudflare Turnstile
        turnstile_data = await _extract_turnstile_data(page)
        if turnstile_data.get('sitekey'):
            result['type'] = 'turnstile'
            result['sitekey'] = turnstile_data.get('sitekey')
            result['action'] = turnstile_data.get('action')
            result['data_s'] = turnstile_data.get('data_s')
            logger.info(f"Detected Cloudflare Turnstile with sitekey: {turnstile_data.get('sitekey')}")
            return result
        
        # No CAPTCHA detected
        return result
        
    except Exception as e:
        logger.warning(f"Error detecting CAPTCHA: {e}")
        return result


async def _extract_recaptcha_sitekey(page: Page) -> Optional[str]:
    """
    Extract reCAPTCHA sitekey from the page.
    
    Args:
        page: Playwright page object
        
    Returns:
        reCAPTCHA sitekey if found, None otherwise
    """
    # Method 1: Check for g-recaptcha div with data-sitekey
    try:
        recaptcha_div = await page.query_selector('div.g-recaptcha[data-sitekey], div[data-sitekey*="recaptcha"]')
        if recaptcha_div:
            sitekey = await recaptcha_div.get_attribute('data-sitekey')
            if sitekey:
                return sitekey
    except Exception:
        pass
    
    # Method 2: Check for reCAPTCHA iframe
    try:
        recaptcha_iframe = await page.query_selector('iframe[src*="recaptcha/api2/anchor"], iframe[src*="recaptcha/api2/bframe"]')
        if recaptcha_iframe:
            iframe_src = await recaptcha_iframe.get_attribute('src')
            if iframe_src:
                match = re.search(r'[?&]k=([^&]+)', iframe_src)
                if match:
                    return match.group(1)
    except Exception:
        pass
    
    # Method 3: Look for reCAPTCHA in page content
    try:
        content = await page.content()
        match = re.search(r'(?:data-sitekey|grecaptcha.execute|grecaptcha.render).*?[\'"]([0-9A-Za-z_-]{40,})[\'""]', content)
        if match:
            return match.group(1)
    except Exception:
        pass
    
    return None


async def _extract_hcaptcha_sitekey(page: Page) -> Optional[str]:
    """
    Extract hCaptcha sitekey from the page.
    
    Args:
        page: Playwright page object
        
    Returns:
        hCaptcha sitekey if found, None otherwise
    """
    # Method 1: Check for h-captcha div with data-sitekey
    try:
        hcaptcha_div = await page.query_selector('div.h-captcha[data-sitekey], div[data-sitekey*="hcaptcha"]')
        if hcaptcha_div:
            sitekey = await hcaptcha_div.get_attribute('data-sitekey')
            if sitekey:
                return sitekey
    except Exception:
        pass
    
    # Method 2: Check for hCaptcha iframe
    try:
        hcaptcha_iframe = await page.query_selector('iframe[src*="hcaptcha.com/captcha"]')
        if hcaptcha_iframe:
            iframe_src = await hcaptcha_iframe.get_attribute('src')
            if iframe_src:
                match = re.search(r'[?&]sitekey=([^&]+)', iframe_src)
                if match:
                    return match.group(1)
    except Exception:
        pass
    
    # Method 3: Look for hCaptcha in page content
    try:
        content = await page.content()
        match = re.search(r'data-sitekey=[\'"]([0-9a-f-]{36,})[\'"]', content)
        if match:
            return match.group(1)
    except Exception:
        pass
    
    return None


async def _extract_turnstile_data(page: Page) -> Dict[str, Any]:
    """
    Extract Cloudflare Turnstile data from the page.
    
    Args:
        page: Playwright page object
        
    Returns:
        Dictionary with turnstile data if found, empty dict otherwise
    """
    result = {
        'sitekey': None,
        'action': None,
        'data_s': None,
    }
    
    # Method 1: Check for turnstile div with data-sitekey
    try:
        turnstile_div = await page.query_selector('div.cf-turnstile[data-sitekey], div[data-sitekey*="turnstile"]')
        if turnstile_div:
            result['sitekey'] = await turnstile_div.get_attribute('data-sitekey')
            result['action'] = await turnstile_div.get_attribute('data-action')
            result['data_s'] = await turnstile_div.get_attribute('data-s')
            if result['sitekey']:
                return result
    except Exception:
        pass
    
    # Method 2: Check for turnstile iframe
    try:
        turnstile_iframe = await page.query_selector('iframe[src*="challenges.cloudflare.com"]')
        if turnstile_iframe:
            iframe_src = await turnstile_iframe.get_attribute('src')
            if iframe_src:
                sitekey_match = re.search(r'[?&]k=([^&]+)', iframe_src)
                if sitekey_match:
                    result['sitekey'] = sitekey_match.group(1)
                
                action_match = re.search(r'[?&]action=([^&]+)', iframe_src)
                if action_match:
                    result['action'] = action_match.group(1)
                
                data_s_match = re.search(r'[?&]s=([^&]+)', iframe_src)
                if data_s_match:
                    result['data_s'] = data_s_match.group(1)
                
                if result['sitekey']:
                    return result
    except Exception:
        pass
    
    # Method 3: Look for turnstile in page content
    try:
        content = await page.content()
        sitekey_match = re.search(r'turnstile.*?sitekey.*?[\'"]([0-9A-Za-z_-]{40,})[\'"]', content)
        if sitekey_match:
            result['sitekey'] = sitekey_match.group(1)
            
            # Try to find action and data-s
            action_match = re.search(r'turnstile.*?action.*?[\'"]([^\'"]+)[\'"]', content)
            if action_match:
                result['action'] = action_match.group(1)
            
            data_s_match = re.search(r'turnstile.*?data-s.*?[\'"]([^\'"]+)[\'"]', content)
            if data_s_match:
                result['data_s'] = data_s_match.group(1)
            
            if result['sitekey']:
                return result
    except Exception:
        pass
    
    return result


async def solve_with_2captcha(
    page: Page,
    api_key: str,
    captcha_type: str = None,
    sitekey: str = None,
    action: str = None,
    data_s: str = None,
    timeout: int = MAX_POLLING_TIME
) -> bool:
    """
    Solve CAPTCHA using 2Captcha service.
    
    Args:
        page: Playwright page object
        api_key: 2Captcha API key
        captcha_type: Type of CAPTCHA ('recaptcha', 'hcaptcha', 'turnstile', or None for auto-detect)
        sitekey: CAPTCHA sitekey (if already known)
        action: Action parameter for Turnstile (if applicable)
        data_s: Data-s parameter for Turnstile (if applicable)
        timeout: Maximum time to wait for solution in seconds
        
    Returns:
        True if CAPTCHA was solved successfully, False otherwise
    """
    if not api_key:
        logger.warning("No 2Captcha API key provided, cannot solve CAPTCHA")
        return False
    
    # Auto-detect CAPTCHA if type not specified
    if not captcha_type or not sitekey:
        captcha_data = await detect_captcha(page)
        captcha_type = captcha_data['type']
        sitekey = captcha_data['sitekey']
        action = captcha_data.get('action')
        data_s = captcha_data.get('data_s')
    
    if captcha_type == 'none' or not sitekey:
        logger.info("No CAPTCHA detected or sitekey not found")
        return False
    
    try:
        # Submit CAPTCHA to 2Captcha
        logger.info(f"Submitting {captcha_type} to 2Captcha with sitekey: {sitekey}")
        
        captcha_id = await _submit_captcha(
            api_key=api_key,
            captcha_type=captcha_type,
            sitekey=sitekey,
            page_url=page.url,
            action=action,
            data_s=data_s
        )
        
        if not captcha_id:
            logger.warning("Failed to submit CAPTCHA to 2Captcha")
            return False
        
        # Poll for the solution
        logger.info(f"Waiting for 2Captcha solution (ID: {captcha_id})")
        solution = await _poll_for_solution(api_key, captcha_id, timeout)
        
        if not solution:
            logger.warning("Failed to get CAPTCHA solution from 2Captcha")
            return False
        
        logger.info("Got CAPTCHA solution, injecting into page")
        
        # Inject the solution into the page
        success = await _inject_captcha_solution(page, captcha_type, solution)
        
        if success:
            # Verify CAPTCHA is gone
            await asyncio.sleep(3)  # Wait for page to process the solution
            captcha_data = await detect_captcha(page)
            if captcha_data['type'] == 'none':
                logger.info("CAPTCHA solved successfully")
                return True
            else:
                logger.warning("CAPTCHA still present after solution")
                return False
        else:
            logger.warning("Failed to inject CAPTCHA solution")
            return False
            
    except Exception as e:
        logger.error(f"Error solving CAPTCHA: {e}")
        return False


async def _submit_captcha(
    api_key: str,
    captcha_type: str,
    sitekey: str,
    page_url: str,
    action: Optional[str] = None,
    data_s: Optional[str] = None
) -> Optional[str]:
    """
    Submit CAPTCHA to 2Captcha service.
    
    Args:
        api_key: 2Captcha API key
        captcha_type: Type of CAPTCHA ('recaptcha', 'hcaptcha', 'turnstile')
        sitekey: CAPTCHA sitekey
        page_url: URL of the page with CAPTCHA
        action: Action parameter for Turnstile (if applicable)
        data_s: Data-s parameter for Turnstile (if applicable)
        
    Returns:
        CAPTCHA ID if submission was successful, None otherwise
    """
    params = {
        'key': api_key,
        'json': 1,
        'pageurl': page_url,
    }
    
    if captcha_type == 'recaptcha':
        params['method'] = 'userrecaptcha'
        params['googlekey'] = sitekey
        params['invisible'] = 1  # Handle both visible and invisible reCAPTCHA
    elif captcha_type == 'hcaptcha':
        params['method'] = 'hcaptcha'
        params['sitekey'] = sitekey
    elif captcha_type == 'turnstile':
        params['method'] = 'turnstile'
        params['sitekey'] = sitekey
        if action:
            params['action'] = action
        if data_s:
            params['data-s'] = data_s
    else:
        logger.warning(f"Unsupported CAPTCHA type: {captcha_type}")
        return None
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(CAPTCHA_API_URL, params=params)
            
            if response.status_code != 200:
                logger.warning(f"2Captcha API error: {response.status_code} - {response.text}")
                return None
            
            result = response.json()
            
            if result.get('status') == 1:
                return result.get('request')
            else:
                logger.warning(f"2Captcha submission error: {result.get('request')}")
                return None
                
    except Exception as e:
        logger.error(f"Error submitting CAPTCHA to 2Captcha: {e}")
        return None


async def _poll_for_solution(
    api_key: str,
    captcha_id: str,
    timeout: int = MAX_POLLING_TIME
) -> Optional[str]:
    """
    Poll 2Captcha service for CAPTCHA solution.
    
    Args:
        api_key: 2Captcha API key
        captcha_id: CAPTCHA ID from submission
        timeout: Maximum time to wait for solution in seconds
        
    Returns:
        CAPTCHA solution if successful, None otherwise
    """
    params = {
        'key': api_key,
        'action': 'get',
        'id': captcha_id,
        'json': 1
    }
    
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(CAPTCHA_RESULT_URL, params=params)
                
                if response.status_code != 200:
                    logger.warning(f"2Captcha API error: {response.status_code} - {response.text}")
                    await asyncio.sleep(POLLING_INTERVAL)
                    continue
                
                result = response.json()
                
                if result.get('status') == 1:
                    return result.get('request')
                elif result.get('request') == 'CAPCHA_NOT_READY':
                    logger.info("CAPTCHA solution not ready yet, waiting...")
                    await asyncio.sleep(POLLING_INTERVAL)
                    continue
                else:
                    logger.warning(f"2Captcha error: {result.get('request')}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error polling 2Captcha: {e}")
            await asyncio.sleep(POLLING_INTERVAL)
    
    logger.warning(f"Timeout waiting for CAPTCHA solution after {timeout} seconds")
    return None


async def _inject_captcha_solution(
    page: Page,
    captcha_type: str,
    solution: str
) -> bool:
    """
    Inject CAPTCHA solution into the page.
    
    Args:
        page: Playwright page object
        captcha_type: Type of CAPTCHA ('recaptcha', 'hcaptcha', 'turnstile')
        solution: CAPTCHA solution from 2Captcha
        
    Returns:
        True if solution was injected successfully, False otherwise
    """
    try:
        if captcha_type == 'recaptcha':
            # Method 1: Set response in textarea (visible reCAPTCHA)
            try:
                await page.evaluate(f"""
                    document.querySelector('textarea#g-recaptcha-response').innerHTML = '{solution}';
                """)
            except Exception:
                pass
            
            # Method 2: Set response in hidden input (invisible reCAPTCHA)
            try:
                await page.evaluate(f"""
                    document.querySelector('#g-recaptcha-response').innerHTML = '{solution}';
                """)
            except Exception:
                pass
            
            # Method 3: Use grecaptcha.execute callback
            try:
                success = await page.evaluate(f"""
                    if (typeof grecaptcha !== 'undefined' && grecaptcha.enterprise) {{
                        grecaptcha.enterprise.execute = function() {{
                            return {{
                                then: function(callback) {{
                                    callback('{solution}');
                                    return {{
                                        then: function() {{}}
                                    }};
                                }}
                            }};
                        }};
                        return true;
                    }} else if (typeof grecaptcha !== 'undefined') {{
                        grecaptcha.execute = function() {{
                            return {{
                                then: function(callback) {{
                                    callback('{solution}');
                                    return {{
                                        then: function() {{}}
                                    }};
                                }}
                            }};
                        }};
                        return true;
                    }}
                    return false;
                """)
                if success:
                    logger.info("Injected solution into grecaptcha.execute")
            except Exception:
                pass
            
            # Try to submit the form or click submit button
            try:
                submit_button = await page.query_selector('button[type="submit"], input[type="submit"], button:has-text("Submit"), button:has-text("Continue")')
                if submit_button:
                    await submit_button.click()
                    logger.info("Clicked submit button after reCAPTCHA solution")
                    return True
            except Exception:
                pass
            
            # If no submit button found, try to submit the form
            try:
                form_submitted = await page.evaluate("""
                    const form = document.querySelector('form');
                    if (form) {
                        form.submit();
                        return true;
                    }
                    return false;
                """)
                if form_submitted:
                    logger.info("Submitted form after reCAPTCHA solution")
                    return True
            except Exception:
                pass
            
        elif captcha_type == 'hcaptcha':
            # Method 1: Set response in textarea
            try:
                await page.evaluate(f"""
                    document.querySelector('textarea[name="h-captcha-response"]').innerHTML = '{solution}';
                """)
            except Exception:
                pass
            
            # Method 2: Set response in hidden input
            try:
                await page.evaluate(f"""
                    document.querySelector('[name="h-captcha-response"]').value = '{solution}';
                """)
            except Exception:
                pass
            
            # Try to submit the form or click submit button
            try:
                submit_button = await page.query_selector('button[type="submit"], input[type="submit"], button:has-text("Submit"), button:has-text("Verify"), button:has-text("Continue")')
                if submit_button:
                    await submit_button.click()
                    logger.info("Clicked submit button after hCaptcha solution")
                    return True
            except Exception:
                pass
            
            # If no submit button found, try to submit the form
            try:
                form_submitted = await page.evaluate("""
                    const form = document.querySelector('form');
                    if (form) {
                        form.submit();
                        return true;
                    }
                    return false;
                """)
                if form_submitted:
                    logger.info("Submitted form after hCaptcha solution")
                    return True
            except Exception:
                pass
            
        elif captcha_type == 'turnstile':
            # Method 1: Set response in textarea
            try:
                await page.evaluate(f"""
                    document.querySelector('[name="cf-turnstile-response"]').value = '{solution}';
                """)
            except Exception:
                pass
            
            # Method 2: Set global callback
            try:
                await page.evaluate(f"""
                    if (typeof turnstileCallback === 'function') {{
                        turnstileCallback('{solution}');
                        return true;
                    }}
                    return false;
                """)
            except Exception:
                pass
            
            # Try to submit the form or click submit button
            try:
                submit_button = await page.query_selector('button[type="submit"], input[type="submit"], button:has-text("Submit"), button:has-text("Continue")')
                if submit_button:
                    await submit_button.click()
                    logger.info("Clicked submit button after Turnstile solution")
                    return True
            except Exception:
                pass
            
            # If no submit button found, try to submit the form
            try:
                form_submitted = await page.evaluate("""
                    const form = document.querySelector('form');
                    if (form) {
                        form.submit();
                        return true;
                    }
                    return false;
                """)
                if form_submitted:
                    logger.info("Submitted form after Turnstile solution")
                    return True
            except Exception:
                pass
        
        # If we got here, we injected the solution but couldn't find a way to submit
        # Let's assume it worked and the page will handle it
        logger.info(f"Injected {captcha_type} solution, but couldn't find submit mechanism")
        return True
        
    except Exception as e:
        logger.error(f"Error injecting CAPTCHA solution: {e}")
        return False


async def solve_captcha(page: Page, api_key: str, timeout: int = MAX_POLLING_TIME) -> bool:
    """
    Main function to detect and solve CAPTCHA on a page.
    
    Args:
        page: Playwright page object
        api_key: 2Captcha API key
        timeout: Maximum time to wait for solution in seconds
        
    Returns:
        True if CAPTCHA was solved successfully, False otherwise
    """
    if not api_key:
        logger.warning("No 2Captcha API key provided, cannot solve CAPTCHA")
        return False
    
    # Detect CAPTCHA
    captcha_data = await detect_captcha(page)
    
    if captcha_data['type'] == 'none':
        logger.info("No CAPTCHA detected on the page")
        return True
    
    # Solve CAPTCHA
    return await solve_with_2captcha(
        page=page,
        api_key=api_key,
        captcha_type=captcha_data['type'],
        sitekey=captcha_data['sitekey'],
        action=captcha_data.get('action'),
        data_s=captcha_data.get('data_s'),
        timeout=timeout
    )
