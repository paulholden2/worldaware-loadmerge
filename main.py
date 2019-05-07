import os
import csv

output_dir = '\\\\stria-prod1\\CID01570 - WorldAware\\JID01215 - CaaS\\Output\\Client'

id_column = 'Origin Path'

merge_columns = [
    'Products.Product Name'
]

for dir in os.listdir(output_dir):
    delivery_dir = os.path.join(output_dir, dir)
    load_file_path = os.path.join(delivery_dir, 'Ingestion Load File.csv')
    out_load_file_path = os.path.join(delivery_dir, 'Processed Ingestion Load File.csv')
    if not os.path.isfile(load_file_path):
        print('No load file found for: %s' % dir)
        continue

    last_id = None
    last_row = None
    row_data = []
    # Populate row_data with data
    with open(load_file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            if last_id == row[id_column]:
                for col in merge_columns:
                    new_val = row[col]
                    if new_val is not None and new_val != '':
                        last_row[col] += '|' + row[col]
            else:
                last_row = row.copy()
                row_data.append(last_row)
                last_id = row[id_column]

        with open(out_load_file_path, 'wb') as csv_file:
            csv_writer = csv.DictWriter(f=csv_file, fieldnames=csv_reader.fieldnames)
            for row in row_data:
                csv_writer.writerow(row)