import csv
import json
import os

def extract_pii_contexts_to_csv(output_folder, csv_output_file):
    with open(csv_output_file, mode='w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['context', 'pii_candidate', 'type'])

        for jsonl_file in os.listdir(output_folder):
            if jsonl_file.endswith('.jsonl'):
                jsonl_path = os.path.join(output_folder, jsonl_file)
                with open(jsonl_path, mode='r', encoding='utf-8') as file:
                    for line in file:
                        document = json.loads(line)
                        context = document.get('text', '')
                        pii_metadata = document.get('metadata', {}).get('pii_metadata', {})
                        if isinstance(pii_metadata, dict):
                            pii_candidate = pii_metadata.get('pii_candidate', '').replace('\n', '')
                            candidate_type = pii_metadata.get('type', '').replace('\n', '')
                            csv_writer.writerow([context, pii_candidate, candidate_type])

    print(f"CSV file '{csv_output_file}' created.")
