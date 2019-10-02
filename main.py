import os
import re
import csv
import codecs

output_dirs = [
    '\\\\stria-prod1\\CID01570 - WorldAware\\JID01215 - CaaS\\Output\\Client',
    '\\\\stria-prod1\\CID01570 - WorldAware\\JID01215 - CaaS\\Output\\Partner',
    '\\\\stria-prod1\\CID01570 - WorldAware\\JID01215 - CaaS\\Output\\Contractor',
    '\\\\stria-prod1\\CID01570 - WorldAware\\JID01215 - CaaS\\Output\\NDA',
    '\\\\stria-prod1\\CID01570 - WorldAware\\JID01215 - CaaS\\Output\\Vendor'
]

id_column = 'Document Path'

merge_columns = [
    'Products Product Name'
]

class UnicodeCsvDictReader:
    def __init__(self, utf8_data):
        self.csv_reader = csv.DictReader(utf8_data)

    def __iter__(self):
        for row in self.csv_reader:
            yield { str(key): str(value) for key, value in row.items() }

    @property
    def fieldnames(self):
        return self.csv_reader.fieldnames

def months_in(unit):
    if unit == 'years':
        return 12
    elif unit == 'months':
        return 1
    elif unit == 'weeks':
        return 7 / 30
    elif unit == 'days':
        return 1 / 30
    else:
        return 0

def convert_period(phrase):
    m = re.search('^(\d+) (years|months|days|weeks)$', phrase)
    if m is None:
        return m
    n = int(m.group(1))
    unit = m.group(2)
    months = int(round(months_in(unit) * n))
    return str(months)

def scrub_load_row(row):
    periods = [
        'Legal Terms Term Non-Renewal Notice',
        'Legal Terms Term for Convenience Notice Period (days)'
    ]

    for p in periods:
        if p in row:
            m = re.search('(\d+) (Days|days)', row[p])
            if m is not None:
                row[p] = m.group(1)

    for k, v in row.items():
        if v is not None:
            row[k] = v.replace('\n', ' ').replace('\r', ' ')

    amounts = [
        'Total Contract Amount',
        'Contract Amount Year 1',
        'Money Contract Amount Year 2'
        'Money Contract Amount Year 3'
        'Money Contract Amount Year 4'
        'Money Contract Amount Year 5'
    ]

    for field in amounts:
        if field in row:
            try:
                amount = float(row[field])
                row[field] = '$%.2f' % amount
            except ValueError:
                row[field] = ''

    field = 'Legal Terms Term for Convenience'
    if field in row:
        if row[field] == '':
            row[field] = 'No'

    field = 'General Document Type'
    if field in row:
        if row[field] == 'Referral':
            row[field] = 'Referral Agreement'

    field = 'Direct Client And Indirect Partner Bordereau Reporting'
    if field in row:
        if row[field] == '':
            row[field] = 'No'

    if row['General WA Contract Entity'] == 'WorldAware, Inc':
        row['General WA Contract Entity'] = 'WorldAware, Inc.'

    if row['General Agreement Format'] == 'Vendor':
        c = 'Vendor Cancellation Notice Period'
        if c in row:
            row[c] = row[c].replace(' Days', '').replace(' days', '')
        for amt in amounts:
            if amt in row:
                row[amt] = row[amt].replace('$', '')

    return row

def process_output_dir(output_dir):
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
        print(delivery_dir)
        # Populate row_data with data
        with codecs.open(load_file_path, 'r', encoding='utf-8-sig') as csv_file:
            csv_reader = UnicodeCsvDictReader(csv_file)
            for row in csv_reader:
                if last_id == row[id_column]:
                    for col in merge_columns:
                        # If the file contains the column to merge...
                        if col in row:
                            new_val = row[col]
                            if new_val is not None and new_val != '' and new_val != 'NOT NEEDED' and new_val != '!!NOPRODUCT!!':
                                last_row[col] += '|' + row[col]
                else:
                    last_row = row.copy()
                    row_data.append(last_row)
                    last_id = row[id_column]

            with open(out_load_file_path, 'w', newline='') as csv_file:
                fieldnames = list(csv_reader.fieldnames)
                for i, v in enumerate(fieldnames):
                    if v == 'Money Total Contract Amount Year 1 (incl multi yr, fee)':
                        fieldnames[i] = 'Contract Amount Year 1'
                csv_writer = csv.DictWriter(f=csv_file, fieldnames=fieldnames)
                csv_writer.writeheader()
                for row in row_data:
                    if 'Money Total Contract Amount Year 1 (incl multi yr, fee)' in row:
                        row['Contract Amount Year 1'] = row.pop('Money Total Contract Amount Year 1 (incl multi yr, fee)')
                    csv_writer.writerow(scrub_load_row(row))

for dir in output_dirs:
    process_output_dir(dir)
