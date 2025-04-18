import requests
import json
import gzip
import csv
import os
import time
from tqdm import tqdm
import logging
import sys
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("image_downloader.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration
INPUT_FILE = "skin_disease_data.jsonl.gz"
OUTPUT_DIR = "Images"
PROGRESS_FILE = "download_progress.txt"
LABEL_CSV = "labels.csv"
BASE_URL = "https://datasets.safe-scan.ai"
MAX_WORKERS = 5  # Number of parallel downloads
CANCEROUS_TYPES = ["melanoma", "basal_cell_carcinoma"]


def ensure_dir(directory):
    """Create directory if it doesn't exist."""
    if not os.path.exists(directory):
        os.makedirs(directory)


def get_processed_ids():
    """Read the IDs of already processed entries."""
    if not os.path.exists(PROGRESS_FILE):
        return set()

    with open(PROGRESS_FILE, 'r') as f:
        return set(line.strip() for line in f)


def mark_as_processed(entry_id):
    """Mark an entry as processed by appending its ID to the progress file."""
    with open(PROGRESS_FILE, 'a') as f:
        f.write(f"{entry_id}\n")


def download_image(item):
    """Download an image for a specific data item."""
    image_url = BASE_URL + item["image_path"]
    entry_id = item["id"]
    disease_type = item["disease_type"]

    # Create a safe filename
    filename = os.path.basename(item["image_path"])
    output_path = os.path.join(OUTPUT_DIR, filename)

    # Skip if already downloaded
    if os.path.exists(output_path):
        return entry_id, output_path, True

    # Download the image
    max_retries = 3
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            response = requests.get(image_url, stream=True, timeout=30, verify=False)
            response.raise_for_status()

            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            return entry_id, output_path, True

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logging.error(f"Failed to download image for ID {entry_id}: {e}")
                return entry_id, None, False


def process_and_download():
    """Process the JSONL file, download images, and create labels CSV."""
    ensure_dir(OUTPUT_DIR)
    processed_ids = get_processed_ids()

    # Check if labels CSV exists and get header row
    csv_exists = os.path.exists(LABEL_CSV)

    # Open the CSV file for writing labels
    csv_mode = 'a' if csv_exists else 'w'
    with open(LABEL_CSV, csv_mode, newline='') as csvfile:
        csv_writer = csv.writer(csvfile)

        # Write header if creating a new file
        if not csv_exists:
            csv_writer.writerow(['image_path', 'label'])

        # Open and process the JSONL file
        with gzip.open(INPUT_FILE, 'rt', encoding='utf-8') as f:
            # Count total lines for progress display
            total_lines = sum(1 for _ in f)

        # Reopen the file for actual processing
        with gzip.open(INPUT_FILE, 'rt', encoding='utf-8') as f:
            # Create a progress bar
            pbar = tqdm(total=total_lines, desc="Processing entries")

            # Create a ThreadPoolExecutor for parallel downloads
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = []
                batch_size = 100  # Process in batches to avoid memory issues
                batch = []

                for line in f:
                    item = json.loads(line)
                    entry_id = item["id"]

                    # Skip already processed entries
                    if entry_id in processed_ids:
                        pbar.update(1)
                        continue

                    batch.append(item)

                    # Process a batch
                    if len(batch) >= batch_size:
                        # Submit download tasks
                        for item in batch:
                            futures.append(executor.submit(download_image, item))

                        # Process completed downloads
                        for future in as_completed(futures):
                            entry_id, image_path, success = future.result()

                            if success and image_path:
                                # Get the item again to determine label
                                for batch_item in batch:
                                    if batch_item["id"] == entry_id:
                                        disease_type = batch_item["disease_type"]
                                        is_cancerous = disease_type in CANCEROUS_TYPES
                                        label = "True" if is_cancerous else "False"

                                        # Write to CSV
                                        rel_path = os.path.relpath(image_path, start=os.getcwd())
                                        csv_writer.writerow([rel_path, label])

                                        # Mark as processed
                                        mark_as_processed(entry_id)
                                        break

                            pbar.update(1)

                        # Clear batch and futures
                        batch = []
                        futures = []

                # Process remaining items
                for item in batch:
                    futures.append(executor.submit(download_image, item))

                for future in as_completed(futures):
                    entry_id, image_path, success = future.result()

                    if success and image_path:
                        # Get the item again to determine label
                        for batch_item in batch:
                            if batch_item["id"] == entry_id:
                                disease_type = batch_item["disease_type"]
                                is_cancerous = disease_type in CANCEROUS_TYPES
                                label = "True" if is_cancerous else "False"

                                # Write to CSV
                                rel_path = os.path.relpath(image_path, start=os.getcwd())
                                csv_writer.writerow([rel_path, label])

                                # Mark as processed
                                mark_as_processed(entry_id)
                                break

                    pbar.update(1)

            pbar.close()


def main():
    start_time = time.time()
    logging.info("Starting image download process")

    try:
        process_and_download()
        elapsed_time = time.time() - start_time
        logging.info(f"Download process completed in {elapsed_time:.2f} seconds")
    except Exception as e:
        logging.error(f"Download process failed: {e}")

    logging.info(f"Labels saved to {LABEL_CSV}")


if __name__ == "__main__":
    main()