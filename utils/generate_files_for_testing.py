from faker import Faker
import random
from datetime import date, datetime
import time

def generate_transcations_for_store(total_number_of_records_per_file, number_of_files):
    fake = Faker()
    for file_index in range(0, number_of_files):
        print("In file index {file_index} with {records} records".format(
            file_index=file_index, records=total_number_of_records_per_file))
        file_to_write = open("file_{i}.csv".format(i=file_index), "w")
        for record in range(0, total_number_of_records_per_file):
            rate = fake.pyfloat(min_value=2, max_value=100)
            count = random.randint(1, 90)
            data = f"{fake.date_time_between(start_date=date.today(),end_date=date.today())},{fake.uuid4()},store-{random.randint(0, 20)},product-{random.randint(1, 5)},{count},{count * rate}"
            file_to_write.write(data)
            file_to_write.write("\n")
        file_to_write.close()

def generate_rides(total_number_of_records_per_file, number_of_files):
    fake = Faker()
    import uuid
    import random
    status = ["pickup", "dropoff", "enroute", "finished"]
    for file_index in range(0, number_of_files):
        print("In file index {file_index} with {records} records".format(
            file_index=file_index, records=total_number_of_records_per_file))
        file_to_write = open("file_{i}.json".format(i=file_index), "w")
        for record in range(0, total_number_of_records_per_file):
            meter_increment = fake.pyfloat(min_value=0, max_value=1)
            longitude = fake.pyfloat(min_value=-74.747, max_value=-73.969)
            latitude = fake.pyfloat(min_value=40.699, max_value=40.720)
            count = random.randint(1, 90)
            now = datetime.now()
            dt = now.strftime("%Y-%m-%dT%H:%M:%S")
            data = f"{{\"ride_id\": \"{str(uuid.uuid4())}\",\"point_idx\": {random.randrange(100, 1000, 3)}, \"meter_increment\": {meter_increment}, \"passenger_count\": {count},\"ride_status\": \"{status[random.randrange(0,4)]}\",\"meter_reading\": {random.randrange(1, 200)},\"timestamp\": \"{dt}\",\"latitude\": {latitude},\"longitude\": {longitude} }}"
            file_to_write.write(data)
            file_to_write.write("\n")
        file_to_write.close()
        
def main():
    # generate_transcations_for_store(90000, 2)
    generate_rides(10000,1)

if __name__ == "__main__":
    main()
