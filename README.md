# Apify Actor – Yelp Scraper (Playwright + Crawl4AI + Grok)

Production-grade Apify Python actor that collects enriched lead data from Yelp business pages.  
The actor automates the full pipeline: search navigation → pagination → detail extraction → email discovery, combining multiple extraction layers (JSON-LD, DOM, Crawl4AI schema, Grok LLM fallback).

Each dataset item contains exactly eight keys:

| key | type | notes |
| --- | --- | --- |
| business_name | string | required |
| years_in_business | int \| null | computed from founding date or text |
| rating | float \| null | 0–5 |
| review_count | int \| null | number of reviews |
| industry | string \| null | first category list |
| phone | string \| null | cleaned `tel:` |
| website | string \| null | dereferenced from `/biz_redir?url=…` |
| email | string \| null | scraped/LLM-derived from website |

---

## Actor input

| field | type | default | description |
| ----- | ---- | ------- | ----------- |
| `keyword` | string | – | Search keyword (e.g. “cafes”). Required if `searchUrl` omitted. |
| `location` | string | – | Location (e.g. “Seattle, WA”). Required if `searchUrl` omitted. |
| `searchUrl` | string | – | Direct Yelp search URL. Overrides `keyword`+`location`. |
| `queries` | array\<object\> | – | List of objects `{ "keyword": "...", "location": "..." }`. Each entry is scraped as an independent task. |
| `keywords` | array\<string\> | – | List of keywords to combine with every value in `locations` (cross-product mode). |
| `locations` | array\<string\> | – | List of locations to combine with every value in `keywords` (cross-product mode). |
| `searchUrls` | array\<string\> | – | Direct Yelp search URLs processed as-is (skips keyword/location building). |
| `numBusinesses` | integer | **50** | Max 500. Stops when reached or pages exhausted. |
| `concurrency` | integer | **5** | Actor clamps the value between **3 – 5**; governs parallel detail fetches (HTTP & LLM guarded internally). |
| `perBusinessIsolation` | boolean | **false** | Use fresh Playwright context per business (slower, stealthier). |
| `entryFlowRatios` | string | `"google:0.6,direct:0.3,bing:0.1"` | Weights for entry modes, normalized automatically. |
| `debugSnapshot` | boolean | **false** | Save HTML snapshots to KV store for troubleshooting. |
| `grokModel` | string | `"grok-2"` | Model name passed to xAI endpoint. |
| `country` | string | – | ISO2 code to pin Apify Residential proxy exit country. |
| `captchaTimeoutSeconds` | integer | **300** | Max time (sec) to wait for 2Captcha solution before giving up. |
| `emailMaxContactPages` | integer | **10** | How many internal “contact/about” pages to follow when hunting for emails. |

### Derived defaults
If `GROK_API_KEY` is **not** set the actor disables Crawl4AI & LLM fallbacks and still works with JSON-LD + DOM only (email may stay `null`).

Note: Regardless of the input, the actor enforces concurrency within 3 – 5 to balance stealth, memory usage and proxy bandwidth.

---

## Environment variables

| variable | required | purpose |
| -------- | -------- | ------- |
| `GROK_API_KEY` | optional | Enables Grok (xAI) for Crawl4AI & HTML fallback. |
| `APIFY_PROXY_PASSWORD` | provided by platform | Authenticates default Apify Residential proxy. |
| `TWO_CAPTCHA_API_KEY` | optional | Enables automatic solving of reCAPTCHA, hCaptcha and Turnstile via 2Captcha. |

*(Any CAPTCHA-solver keys may be added but are not required.)*

---

## Running locally

Prerequisites  
```bash
# one-time
npm -g install apify-cli          # CLI
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python -m playwright install --with-deps chromium
export GROK_API_KEY=sk-...        # optional for LLM
```

Run:

```bash
apify run . -p \
  -i '{
        "keyword": "cafes",
        "location": "Seattle, WA",
        "numBusinesses": 3
      }'
```

During the run you’ll see structured logs: navigation URLs, block rerolls, extraction hits, counters, etc.

You can also pass multiple queries at once, e.g.:

```bash
apify run . -p -i '{
  "queries": [
    { "keyword": "plumbers", "location": "Austin, TX" },
    { "keyword": "plumbers", "location": "Dallas, TX" }
  ],
  "numBusinesses": 10
}'
```

### Expected dataset output (example)

```json
{
  "business_name": "Moore Coffee Shop",
  "years_in_business": 12,
  "rating": 4.5,
  "review_count": 1573,
  "industry": "Coffee & Tea",
  "phone": "(206) 555-1234",
  "website": "https://www.moorecoffeeshop.com",
  "email": "hello@moorecoffeeshop.com"
}
```

---

## Proxy, LLM and Crawl4AI notes

* **Proxy** – Actor auto-creates an Apify Residential proxy (`groups:["RESIDENTIAL"]`).  
  Pass `country` in input to lock exit IP region, or override globally in *Develop* tab.

* **Crawl4AI** – Configured at runtime if `GROK_API_KEY` is present.  
  Uses `LLMExtractionStrategy` with provider type **openai**  
  (`base_url=https://api.x.ai/v1`, `model=input.grokModel`, `api_key=$GROK_API_KEY`).

* **Grok fallback** – For pages where JSON-LD & Crawl4AI miss fields, raw HTML is sent to Grok with a strict “return only JSON” prompt and parsed defensively.

---

## Limitations & tips

• Very high concurrency may exhaust proxy bandwidth or trigger blocks—keep it in the 3–5 range.  
• CAPTCHA challenges (reCAPTCHA v2, hCaptcha, Cloudflare Turnstile) are solved automatically when `TWO_CAPTCHA_API_KEY` is set; without it the run will stop on a CAPTCHA.  
• Email discovery depends on public availability; corporate sites that hide emails behind forms will return `null` (the actor now crawls contact pages, JSON-LD and script tags to maximise coverage).  
• The actor intentionally ignores robots.txt, following the user’s instruction—ensure you have the right to scrape the target site.
• Navigation now retries up to **5** times with exponential back-off to reduce transient failures.

Happy scraping! ✨
