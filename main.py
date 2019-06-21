import os
import re
import csv
import codecs

output_dirs = [
    '\\\\stria-prod1\\CID01570 - WorldAware\\JID01215 - CaaS\\Output\\Client',
    '\\\\stria-prod1\\CID01570 - WorldAware\\JID01215 - CaaS\\Output\\Partner',
    '\\\\stria-prod1\\CID01570 - WorldAware\\JID01215 - CaaS\\Output\\Contractor',
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
            yield { unicode(key, 'utf-8'): unicode(value, 'utf-8') for key, value in row.iteritems() }

    @property
    def fieldnames(self):
        return self.csv_reader.fieldnames

def days_in(unit):
    if unit == 'years':
        return 365
    elif unit == 'months':
        return 30
    elif unit == 'weeks':
        return 7
    elif unit == 'days':
        return 1
    else:
        return 0

def convert_period(phrase):
    m = re.search('^(\d+) (years|months|days|weeks)$', phrase)
    if m is None:
        return m
    n = int(m.group(1))
    unit = m.group(2)
    days = days_in(unit) * n
    return str(days)

def scrub_load_row(row):
    periods = [
        'Direct Client And Indirect Partner MSA Initial Term Length',
        'Direct Client And Indirect Partner PA Initial Term Length',
        'Legal Terms Term Non-Renewal Notice',
        'Legal Terms PA Term Non-Renewal Notice',
        'Legal Terms Term for Convenience Notice Period (days)'
    ]

    for p in periods:
        if p in row:
            row[p] = convert_period(row[p])

    for k, v in row.iteritems():
        if v is not None:
            row[k] = v.replace('\n', ' ').replace('\r', ' ')

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

            with open(out_load_file_path, 'wb') as csv_file:
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
