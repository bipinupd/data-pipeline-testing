from faker import Faker
from string import Template
import datetime
import json
import argparse
import random
from datetime import date

def generate_trnascations(total_number_of_records_per_file, number_of_files):
    fake = Faker()
    for file_index in range (0, number_of_files):
        print("In file index {file_index} with {records} records".format(file_index=file_index, records=total_number_of_records_per_file))
        file_to_write=open("file_{i}.csv".format(i=file_index), "w")
        for record in range (0, total_number_of_records_per_file):
            rate = fake.pyfloat(min_value=2, max_value=100)
            count = random.randint(1, 90)
            data=f"{fake.date_time_between(start_date=date.today(),end_date=date.today())},{fake.uuid4()},store-{random.randint(0, 20)},product-{random.randint(1, 5)},{count},{count * rate}"
            file_to_write.write(data)
            file_to_write.write("\n")
        file_to_write.close()
    
def main():
    generate_trnascations(90000,2)

if __name__ == "__main__":
    main()