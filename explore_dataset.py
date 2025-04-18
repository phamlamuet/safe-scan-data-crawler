import gzip
import json
from collections import Counter

file_path = "skin_disease_data.jsonl.gz"
disease_types = Counter()

with gzip.open(file_path, 'rt', encoding='utf-8') as f:
    for line in f:
        json_obj = json.loads(line.strip())
        disease_type = json_obj.get('disease_type')
        if disease_type:  # Ensure disease_type exists
            disease_types[disease_type] += 1

# Print unique disease types and their counts
for disease_type, count in disease_types.items():
    print(f"{disease_type}: {count}")

# Print total unique disease types
print(f"Total unique disease types: {len(disease_types)}")