import os
import csv
import logging
from collections import Counter
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("discrepancy_analysis.log"),
        logging.StreamHandler()
    ]
)

# Configuration
LABEL_CSV = "labels.csv"
IMAGES_DIR = "Images"


def analyze_discrepancy():
    """Analyze the discrepancy between labels.csv entries and actual image files."""

    # Step 1: Count actual files in the Images directory
    logging.info(f"Counting actual files in {IMAGES_DIR}...")
    actual_files = []

    for root, dirs, files in os.walk(IMAGES_DIR):
        for file in files:
            if not file.startswith('.'):  # Skip hidden files
                full_path = os.path.join(root, file)
                actual_files.append(full_path)

    actual_file_count = len(actual_files)
    logging.info(f"Found {actual_file_count} actual files in {IMAGES_DIR}")

    # Step 2: Read paths from labels.csv
    logging.info("Reading paths from labels.csv...")
    csv_paths = []

    with open(LABEL_CSV, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header

        for row in reader:
            csv_paths.append(row[0])

    csv_path_count = len(csv_paths)
    unique_csv_path_count = len(set(csv_paths))

    logging.info(f"Total paths in labels.csv: {csv_path_count}")
    logging.info(f"Unique paths in labels.csv: {unique_csv_path_count}")

    # Step 3: Check for duplicate paths in labels.csv
    if csv_path_count != unique_csv_path_count:
        path_counts = Counter(csv_paths)
        duplicates = {path: count for path, count in path_counts.items() if count > 1}
        logging.info(f"Found {len(duplicates)} duplicated paths in labels.csv")
        logging.info(f"Top 10 duplicated paths: {list(duplicates.items())[:10]}")

    # Step 4: Check if actual files match what's expected in labels.csv
    csv_path_set = set(csv_paths)
    actual_file_set = set(actual_files)

    in_csv_not_in_dir = csv_path_set - actual_file_set
    in_dir_not_in_csv = actual_file_set - csv_path_set

    logging.info(f"Paths in labels.csv but not in directory: {len(in_csv_not_in_dir)}")
    if in_csv_not_in_dir:
        logging.info(f"Examples: {list(in_csv_not_in_dir)[:5]}")

    logging.info(f"Files in directory but not in labels.csv: {len(in_dir_not_in_csv)}")
    if in_dir_not_in_csv:
        logging.info(f"Examples: {list(in_dir_not_in_csv)[:5]}")

    # Step 5: Check path normalization issues
    if in_csv_not_in_dir:
        logging.info("Checking for path normalization issues...")
        normalized_actual = {os.path.normpath(p) for p in actual_files}
        normalized_csv = {os.path.normpath(p) for p in csv_paths}

        normalized_diff = normalized_csv - normalized_actual
        logging.info(f"After path normalization, missing files: {len(normalized_diff)}")


def main():
    logging.info("Starting discrepancy analysis")
    analyze_discrepancy()
    logging.info("Analysis complete")


if __name__ == "__main__":
    main()