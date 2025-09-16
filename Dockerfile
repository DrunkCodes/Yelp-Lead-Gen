# Dockerfile for Yelp Scraper Apify Actor
# Uses Playwright with Chromium for web scraping

FROM apify/actor-python:3.12

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright with Chromium browser
RUN python -m playwright install --with-deps chromium

# Copy the actor code
COPY . ./

# Run the actor
CMD ["python", "main.py"]
