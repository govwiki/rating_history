import csv
import os
import time
import psutil

import requests


AUTH_KEY = 'eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJwcm9kLXVzZXItY2xpZW50Om11bmktZmluYW5jZSIsImlzcyI6ImFnZW50Om11bmktZmluYW5jZTo6NDc2MDAxMDItYTU3ZS00ZmE0LWJiM2YtNDA3ZmIyMWIwNzRhIiwiaWF0IjoxNDg4MzM3OTQxLCJyb2xlIjpbInVzZXJfYXBpX3JlYWQiLCJ1c2VyX2FwaV93cml0ZSJdLCJnZW5lcmFsLXB1cnBvc2UiOnRydWV9.Pm3m4O4eB0p9Jrb7UyYyaZ9naFFmVp2qxkfpUkPJnvwNU_1ECrEssrDyKCSPcbr5kPr1EItuiAjOjfKvb_nFkQ'
USERNAME = 'muni-finance'


HEADER = (
    'rating_agency_name',
    'file_creating_date',
    'sec_category',
    'issuer_name',
    'legal_entity_identifier',
    'object_type_rated',
    'instrument_name',
    'CUSIP_number',
    'coupon_date',
    'maturity_date',
    'par_value',
    'issued_paid',
    'rating',
    'rating_action_date',
    'rating_action_class',
    'rating_type',
    'rating_sub_type',
    'rating_type_term',
    'other_announcement_type',
    'watch_status',
    'rating_outlook',
    'issuer_identifier',
    'issuer_identifier_schema',
    'instrument_identifier',
    'instrument_identifier_schema',
    'central_index_key',
    'obligor_identifier',
    'obligor_identifier_schema',
    'obligor_identifier_other',
    'obligor_sec_category',
    'obligor_industry_group',
    'obligor_name',
)


if __name__ == '__main__':
    with open('../tmp/ratings_for_upload.csv', 'w') as out_file:
        writer = csv.writer(out_file)
        writer.writerow(HEADER)  #!!!
        print(f"Begin first loop")
        for file_name in os.listdir('../var/csv_path/'):
            with open(os.path.join('../var/csv_path/', file_name), 'r') as in_file:
                print(f"{file_name=}")
                reader = csv.reader(in_file)
                #reader.next()
                for row in reader:
                    writer.writerow(row) #!!!
            out_file.flush()
    print(f"Begin second loop")

    with open('../tmp/ratings_for_upload.csv', 'rb') as uploaded_file:
        headers = {'Authorization': f"Bearer {AUTH_KEY}"}
        print(f"{headers=}")
        ut = psutil.virtual_memory()
        print(f"{ut} usage memory")
        files = {'file': uploaded_file}
        print(f"{files=}")
        ut = psutil.virtual_memory()
        print(f"{ut} usage memory")
        print(uploaded_file.readlines())

        r = requests.post(
            f"https://api.data.world/v0/datasets/{USERNAME}/api-sandbox/files",
            files=files, headers=headers,
        )
    print("Upload success!")

