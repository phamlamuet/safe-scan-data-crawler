import gzip
import json
import csv
import os

file_path = "skin_disease_data.jsonl.gz"
output_file = "skin_disease_labels.csv"

# Extract image paths and labels
with gzip.open(file_path, 'rt', encoding='utf-8') as f:
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)

        # Write header
        writer.writerow(['image_path', 'label'])

        for line in f:
            json_obj = json.loads(line.strip())

            # Get the image path and just extract the filename
            full_image_path = json_obj.get('image_path')
            if full_image_path:
                # Extract just the filename (e.g., ISIC_1030777.jpg)
                image_filename = os.path.basename(full_image_path)
                # Format it as Images/filename.jpg
                formatted_path = f"Images/{image_filename}"

            # Get the disease type as label
            disease_type = json_obj.get('disease_type')

            # Write to CSV if both values exist
            if formatted_path and disease_type:
                writer.writerow([formatted_path, disease_type])

print(f"Label file created: {output_file}")