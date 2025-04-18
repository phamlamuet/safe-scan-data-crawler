import requests
import json
import gzip
import time
from tqdm import tqdm
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crawler.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# API configuration
BASE_URL = "https://datasets.safe-scan.ai/api/entry"
PAGE_SIZE = 100  # Maximum allowed items per page
OUTPUT_FILE = "skin_disease_data.jsonl.gz"
TOTAL_ITEMS = 47663  # Total items from initial API response


def fetch_page(page_num):
    """Fetch a single page of data from the API."""
    url = f"{BASE_URL}?page={page_num}&page_size={PAGE_SIZE}"
    max_retries = 3
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()  # Raise exception for HTTP errors
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logging.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logging.error(f"Failed to fetch page {page_num} after {max_retries} attempts")
                raise


def crawl_data():
    """Crawl all pages and save data to a gzip file."""
    total_pages = (TOTAL_ITEMS + PAGE_SIZE - 1) // PAGE_SIZE  # Ceiling division

    with gzip.open(OUTPUT_FILE, 'wt', encoding='utf-8') as f:
        # Use tqdm for progress bar
        for page_num in tqdm(range(1, total_pages + 1), desc="Crawling pages"):
            try:
                data = fetch_page(page_num)

                # Write each item as a separate JSON line
                for item in data['items']:
                    f.write(json.dumps(item) + '\n')

                # Optional: add a small delay to avoid overwhelming the API
                time.sleep(0.5)

            except Exception as e:
                logging.error(f"Error processing page {page_num}: {e}")
                # Continue with next page instead of stopping completely
                continue

            # Log progress periodically
            if page_num % 10 == 0:
                logging.info(f"Processed {page_num}/{total_pages} pages")


def main():
    start_time = time.time()
    logging.info("Starting data crawling process")

    try:
        crawl_data()
        elapsed_time = time.time() - start_time
        logging.info(f"Crawling completed successfully in {elapsed_time:.2f} seconds")
    except Exception as e:
        logging.error(f"Crawling process failed: {e}")

    logging.info(f"Output saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()