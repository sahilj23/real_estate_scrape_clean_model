"""
Scraper for MagicBricks — Noida residential-for-sale search results.
Workflow:
  - Crawl paginated search results
  - Parse card-level info (title, url, price, area, bhk, locality)
  - Sanitize numeric fields
  - Save checkpoints and final CSV

Usage:
  python scraping.py
"""

import time, random, csv, os, json, math
from datetime import datetime, timezone
## from fake_useragent import UserAgent
## from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
## from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, WebDriverException
## from selenium.webdriver.chrome.service import Service
## from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import re
import json
import undetected_chromedriver as uc


START_URL = ("https://www.magicbricks.com/property-for-sale/residential-real-estate?bedroom=1,3,4,5&proptype=Multistorey-Apartment,Builder-Floor-Apartment,Penthouse,Studio-Apartment,Residential-House,Villa&cityName=Noida")
OUTPUT_CSV = "scraped_listings_cards.csv"
CHECKPOINT_EVERY = 100
MAX_PAGES = None   # set to int for testing, or None for all pages
HEADLESS = True

## -- Helpers -- ##
## ua = UserAgent()

def init_driver(headless=True):
    # This library handles stealth arguments more effectively than manual configuration.
    options = uc.ChromeOptions()

    if headless:
        # Note: uc.Chrome requires setting headless mode in options.
        #options.add_argument("--headless=new")
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')

    # Using a unique user profile path. This prevents a site from using local storage/fingerprint
    # data from a previous session to flag the new run.
    user_data_dir = os.path.join(os.getcwd(), 'chrome_user_data_stealth')
    options.add_argument(f'--user-data-dir={user_data_dir}')

    # Initialize the undetectable driver
    driver = uc.Chrome(options=options)

    # Standard settings (kept)
    driver.delete_all_cookies()
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(20)

    return driver


def parse_card(card_soup):
    try:
        title = card_soup.select_one(".mb-srp__card--title").get_text(strip = True)
    except:
        title = None

    listing_url = None
    # Find the JSON-LD script tag
    json_script = card_soup.find('script', type='application/ld+json')

    if json_script and json_script.string:
        try:
            # Parse the JSON content
            data = json.loads(json_script.string)

            # The content is often a list, or the main data is the first item.
            # Look for the 'url' key, which is present in image.
            if isinstance(data, list) and len(data) > 0 and 'url' in data[0]:
                listing_url = data[0].get('url')
            elif isinstance(data, dict) and 'url' in data:
                listing_url = data.get('url')

        except json.JSONDecodeError:
            print("[!] Could not parse JSON-LD script.")

    # --- Fallback (optional, safer to rely on JSON) ---
    if not listing_url:
        # Fallback to the title tag like previously, just in case the JSON is missing
        title_h2 = card_soup.find('h2', class_='mb-srp__card__title')
        listing_url_tag = title_h2.find('a') if title_h2 else None

        if listing_url_tag and listing_url_tag.get('href'):
            relative_url = listing_url_tag.get('href')
            if relative_url.startswith('/'):
                listing_url = "https://www.magicbricks.com" + relative_url
            else:
                listing_url = relative_url


    try:
        price_raw = card_soup.select_one(".mb-srp__card__price--amount").get_text(strip = True)
    except:
        price_raw = None

    locality = None
    if title and ' in ' in title:
        locality = title.split(' in ', 1)[-1].strip()

    bhk = None
    if title:
        bhk_match = re.search(r"(\d+)\s*BHK", title, re.IGNORECASE)
        if bhk_match:
            bhk = int(bhk_match.group(1))

    # --- CONSOLIDATED SUMMARY EXTRACTION (FINAL ATTEMPT: Global Search) ---
    area_raw = None
    status = None
    floor = None
    transaction = None
    furnishing = None
    facing = None

    try:
        # Select the main container that holds ALL summary items on the card
        summary_container = card_soup.select_one("div.mb-srp__card__summary")

        if summary_container:
            # Selecting ALL labels and ALL values within that container
            label_tags = summary_container.select("div.mb-srp__card__summary--label")

            for label_tag in label_tags:
                label = label_tag.get_text(strip=True).lower()

                # The value is the immediate next sibling element
                value_tag = label_tag.find_next_sibling('div', class_='mb-srp__card__summary--value')

                if value_tag:
                    value = value_tag.get_text(strip=True)

                    if 'area' in label:
                        area_raw = value
                    elif 'status' in label:
                        status = value
                    elif 'floor' in label:
                        floor = value
                    elif 'transaction' in label:
                        transaction = value
                    elif 'furnishing' in label:
                        furnishing = value
                    elif 'facing' in label:
                        facing = value

    except Exception as e:
        # print(f"Summary extraction failed: {e}") # Debugging aid
        pass

    # Extracting property id from the complete url
    property_id = None
    if listing_url:
        # The ID is after '&id=' or a similar unique string in the full URL
        # Example URL: ...&id=4d423830393632373133
        id_match = re.search(r"[&?]id=([a-fA-F0-9]+)", listing_url)
        if id_match:
            property_id = id_match.group(1)
        # Handle the new "pdpid" format often found in the JSON-LD URL slug (e.g., greatvalue-sharanam-sector-107-noida-pdpid-4d4235330383383331)
        else:
            id_match_pdpid = re.search(r"pdpid-([a-fA-F0-9]+)", listing_url)
            if id_match_pdpid:
                property_id = id_match_pdpid.group(1)

    # --- final dict ---
    return {
        "title": title,
        "listing_url": listing_url,
        "area_raw": area_raw,
        "status": status,
        "floor" : floor,
        "transaction": transaction,
        "furnishing": furnishing,
        "facing": facing,
        "bhk": bhk,
        "locality": locality,
        "property_id": property_id,
        "scraped_timestamp": datetime.now(timezone.utc).isoformat(),
        "price_raw": price_raw
    }


# ======== SCRAPER LOOP ======== #
def crawl_listings(max_pages=120, headless=HEADLESS, max_records=3200):
    # --- Checkpoint/Resume Logic ---
    if os.path.exists("checkpoint.csv"):
        print(f"[*] Resuming from checkpoint: checkpoint.csv")
        # Load existing data
        df_checkpoint = pd.read_csv("checkpoint.csv")
        rows = df_checkpoint.to_dict('records')

        # Calculate the starting page (30 listings per page, plus 1 to start on the next page)
        page = (len(rows) // 30) + 1

        # Set tqdm to reflect already scraped records
        pbar = tqdm(total=max_records, initial=len(rows), desc="Scraping")

        # Initialize driver only now, as it wasn't running
        driver = init_driver(headless)

    else:
        # Initial setup if no checkpoint exists
        driver = init_driver(headless)
        rows = []
        page = 1
        pbar = tqdm(total=max_records, desc="Scraping")
        # -----------------------------

    try:
        while True:
            url = START_URL + f"&page={page}"
            print(f"[>] Loading page {page}")
            driver.get(url)

            # --- EXPLICIT WAIT BLOCK ---
            try:
                # Giving it up to 25 seconds for the content to load
                wait = WebDriverWait(driver, 25)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.mb-srp__card")))
            except TimeoutException:
                # Check the page title or URL to see if it was blocked/redirected
                current_url = driver.current_url
                print(f"[!] Timeout on page {page}. Current URL: {current_url}. Assuming content block.")

                # If we hit this, we are likely blocked. Breaking the loop.
                break
                # --- END EXPLICIT WAIT BLOCK ---

            # Adding a human-like scroll
            driver.execute_script("window.scrollBy(0, 150);")

            # Adding a tiny, extra wait after the scroll (0.5 to 1.5 seconds)
            time.sleep(random.uniform(0.5, 1.5))

            # --- Parse and Check (Executed only ONCE) ---
            soup = BeautifulSoup(driver.page_source, "html.parser")
            cards = soup.select("div.mb-srp__card")

            if not cards:
                # This check catches any remaining edge cases where cards are not found
                print(f"[!] Selector check: No cards found on page {page}. Exiting.")
                break

            for c in cards:
                data = parse_card(c)
                rows.append(data)
                # Checking if the maximum record count has been reached
                if len(rows) >= max_records:
                    pbar.update(len(cards))  # Update pbar for the last page
                    print(f"[*] Reached max_records limit ({max_records}).")
                    break  # Break out of the inner card loop

                if len(rows) % CHECKPOINT_EVERY == 0:
                    pd.DataFrame(rows).to_csv("checkpoint.csv", index=False)
                    print(f"[✓] Checkpoint saved ({len(rows)} records).")
                # Check if we broke out of the inner loop
                if len(rows) >= max_records:
                    break  # Break out of the outer while True loop

            pbar.update(len(cards))
            print(f"Page {page} done: {len(cards)} listings.")
            page += 1

            if max_pages and page > max_pages:
                print("[*] Reached max_pages limit.")
                break

            time.sleep(random.uniform(5.0, 10.0))

    finally:
        driver.quit()
        pbar.close()

    pd.DataFrame(rows).to_csv(OUTPUT_CSV, index=False)
    print(f"[✓] Scraping finished. Total records: {len(rows)}")
    return rows


if __name__ == "__main__":

    crawl_listings(max_records=3200, headless=False)
