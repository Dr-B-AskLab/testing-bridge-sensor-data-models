import pandas as pd
import datetime
from pymongo import MongoClient
import numpy as np

# Establish MongoDB database connection
client = MongoClient('mongodb://localhost:27017/')
db = client['mydatabase']

# Create the MongoDB data collections
m_sens_data_collection = db.create_collection('sensordata')
m_sens_hold_collection = db.create_collection('sensorhold')

# Define a function to process a single CSV file
def m_sens_process_csv(file_path, sensor_number):
    print("Processing: " + file_path)
    # Read CSV into a Pandas DataFrame
    df = pd.read_csv(file_path, skiprows=1)
    df = df.rename(columns={'128': 'value'})

    # Calculate timestamp for first row based on final file name
    file_name = file_path.split('/')[-1]
    timestamp = datetime.datetime.strptime(file_name[-18:-4], '%Y%m%d%H%M%S')
    df['timestamp'] = timestamp

    # Calculate timestamps for remaining rows
    time_delta = datetime.timedelta(seconds=1/128)
    for i in range(1, len(df)):
        df.iloc[i, df.columns.get_loc('timestamp')] = timestamp + (i*time_delta)

    # Add sensor number column
    df['sensor_number'] = sensor_number

    # Convert each row of DataFrame to a JSON object and insert into collection
    m_sens_hold_collection.insert_many(df.to_dict('records'))

# Define a function to process multiple CSV files
def m_sens_process_multiple_csv(file_paths):
    for i, file_path in enumerate(file_paths):
        # Process current CSV file
        m_sens_process_csv(file_path, i+1)

def m_sens_insert(items):
    m_sens_data_collection.insert_many(items)

# Process each CSV file
file_paths = [f'DISPLACEMENT_WIRELESS#{j+1}_20171111061047.csv' for j in range(6)]

m_sens_process_multiple_csv(file_paths)
m_sens_insert(m_sens_hold_collection.find())

intervals = [(datetime.datetime(2017, 11, 11, 6, 45, 54), datetime.datetime(2017, 11, 11, 6, 46, 9)), (datetime.datetime(2017, 11, 11, 7, 8, 35), datetime.datetime(2017, 11, 11, 7, 8, 50)), (datetime.datetime(2017, 11, 11, 6, 52, 18), datetime.datetime(2017, 11, 11, 6, 52, 33)), (datetime.datetime(2017, 11, 11, 7, 4, 40), datetime.datetime(2017, 11, 11, 7, 4, 55)), (datetime.datetime(2017, 11, 11, 6, 31, 59), datetime.datetime(2017, 11, 11, 6, 32, 14)), (datetime.datetime(2017, 11, 11, 6, 31, 31), datetime.datetime(2017, 11, 11, 6, 31, 46)), (datetime.datetime(2017, 11, 11, 6, 11, 9), datetime.datetime(2017, 11, 11, 6, 11, 24)), (datetime.datetime(2017, 11, 11, 6, 47, 22), datetime.datetime(2017, 11, 11, 6, 47, 37)), (datetime.datetime(2017, 11, 11, 6, 15, 28), datetime.datetime(2017, 11, 11, 6, 15, 43)), (datetime.datetime(2017, 11, 11, 6, 46, 4), datetime.datetime(2017, 11, 11, 6, 46, 19)), (datetime.datetime(2017, 11, 11, 6, 53, 9), datetime.datetime(2017, 11, 11, 6, 53, 24)), (datetime.datetime(2017, 11, 11, 7, 0, 5), datetime.datetime(2017, 11, 11, 7, 0, 20)), (datetime.datetime(2017, 11, 11, 6, 53, 58), datetime.datetime(2017, 11, 11, 6, 54, 13)), (datetime.datetime(2017, 11, 11, 6, 43, 12), datetime.datetime(2017, 11, 11, 6, 43, 27)), (datetime.datetime(2017, 11, 11, 7, 7, 12), datetime.datetime(2017, 11, 11, 7, 7, 27)), (datetime.datetime(2017, 11, 11, 6, 20, 3), datetime.datetime(2017, 11, 11, 6, 20, 18)), (datetime.datetime(2017, 11, 11, 6, 44, 28), datetime.datetime(2017, 11, 11, 6, 44, 43)), (datetime.datetime(2017, 11, 11, 6, 53), datetime.datetime(2017, 11, 11, 6, 53, 15)), (datetime.datetime(2017, 11, 11, 7, 5, 18), datetime.datetime(2017, 11, 11, 7, 5, 33)), (datetime.datetime(2017, 11, 11, 6, 28, 25), datetime.datetime(2017, 11, 11, 6, 28, 40)), (datetime.datetime(2017, 11, 11, 7, 3, 41), datetime.datetime(2017, 11, 11, 7, 3, 56)), (datetime.datetime(2017, 11, 11, 6, 42, 52), datetime.datetime(2017, 11, 11, 6, 43, 7)), (datetime.datetime(2017, 11, 11, 6, 45, 4), datetime.datetime(2017, 11, 11, 6, 45, 19)), (datetime.datetime(2017, 11, 11, 6, 33, 36), datetime.datetime(2017, 11, 11, 6, 33, 51)), (datetime.datetime(2017, 11, 11, 7, 3, 29), datetime.datetime(2017, 11, 11, 7, 3, 44)), (datetime.datetime(2017, 11, 11, 6, 15, 55), datetime.datetime(2017, 11, 11, 6, 16, 10)), (datetime.datetime(2017, 11, 11, 6, 52, 18), datetime.datetime(2017, 11, 11, 6, 52, 33)), (datetime.datetime(2017, 11, 11, 6, 19, 44), datetime.datetime(2017, 11, 11, 6, 19, 59)), (datetime.datetime(2017, 11, 11, 6, 30, 52), datetime.datetime(2017, 11, 11, 6, 31, 7)), (datetime.datetime(2017, 11, 11, 6, 20, 13), datetime.datetime(2017, 11, 11, 6, 20, 28)), (datetime.datetime(2017, 11, 11, 6, 15, 13), datetime.datetime(2017, 11, 11, 6, 15, 28)), (datetime.datetime(2017, 11, 11, 7, 5, 13), datetime.datetime(2017, 11, 11, 7, 5, 28)), (datetime.datetime(2017, 11, 11, 6, 23, 53), datetime.datetime(2017, 11, 11, 6, 24, 8)), (datetime.datetime(2017, 11, 11, 6, 16, 28), datetime.datetime(2017, 11, 11, 6, 16, 43)), (datetime.datetime(2017, 11, 11, 6, 25, 43), datetime.datetime(2017, 11, 11, 6, 25, 58)), (datetime.datetime(2017, 11, 11, 6, 47, 51), datetime.datetime(2017, 11, 11, 6, 48, 6)), (datetime.datetime(2017, 11, 11, 6, 20, 8), datetime.datetime(2017, 11, 11, 6, 20, 23)), (datetime.datetime(2017, 11, 11, 7, 7, 22), datetime.datetime(2017, 11, 11, 7, 7, 37)), (datetime.datetime(2017, 11, 11, 6, 32, 52), datetime.datetime(2017, 11, 11, 6, 33, 7)), (datetime.datetime(2017, 11, 11, 6, 50), datetime.datetime(2017, 11, 11, 6, 50, 15)), (datetime.datetime(2017, 11, 11, 6, 40, 31), datetime.datetime(2017, 11, 11, 6, 40, 46)), (datetime.datetime(2017, 11, 11, 6, 20, 23), datetime.datetime(2017, 11, 11, 6, 20, 38)), (datetime.datetime(2017, 11, 11, 6, 49, 20), datetime.datetime(2017, 11, 11, 6, 49, 35)), (datetime.datetime(2017, 11, 11, 7, 6, 41), datetime.datetime(2017, 11, 11, 7, 6, 56)), (datetime.datetime(2017, 11, 11, 7, 2, 43), datetime.datetime(2017, 11, 11, 7, 2, 58)), (datetime.datetime(2017, 11, 11, 6, 46, 11), datetime.datetime(2017, 11, 11, 6, 46, 26)), (datetime.datetime(2017, 11, 11, 6, 37, 56), datetime.datetime(2017, 11, 11, 6, 38, 11)), (datetime.datetime(2017, 11, 11, 6, 12, 23), datetime.datetime(2017, 11, 11, 6, 12, 38)), (datetime.datetime(2017, 11, 11, 6, 26, 26), datetime.datetime(2017, 11, 11, 6, 26, 41)), (datetime.datetime(2017, 11, 11, 6, 30), datetime.datetime(2017, 11, 11, 6, 30, 15)), (datetime.datetime(2017, 11, 11, 6, 36, 51), datetime.datetime(2017, 11, 11, 6, 37, 6)), (datetime.datetime(2017, 11, 11, 6, 50, 28), datetime.datetime(2017, 11, 11, 6, 50, 43)), (datetime.datetime(2017, 11, 11, 6, 46, 2), datetime.datetime(2017, 11, 11, 6, 46, 17)), (datetime.datetime(2017, 11, 11, 6, 25, 10), datetime.datetime(2017, 11, 11, 6, 25, 25)), (datetime.datetime(2017, 11, 11, 6, 14, 34), datetime.datetime(2017, 11, 11, 6, 14, 49)), (datetime.datetime(2017, 11, 11, 6, 32, 10), datetime.datetime(2017, 11, 11, 6, 32, 25)), (datetime.datetime(2017, 11, 11, 6, 32, 21), datetime.datetime(2017, 11, 11, 6, 32, 36)), (datetime.datetime(2017, 11, 11, 6, 58, 17), datetime.datetime(2017, 11, 11, 6, 58, 32)), (datetime.datetime(2017, 11, 11, 6, 55, 15), datetime.datetime(2017, 11, 11, 6, 55, 30)), (datetime.datetime(2017, 11, 11, 6, 33, 6), datetime.datetime(2017, 11, 11, 6, 33, 21)), (datetime.datetime(2017, 11, 11, 6, 25, 50), datetime.datetime(2017, 11, 11, 6, 26, 5)), (datetime.datetime(2017, 11, 11, 6, 59, 9), datetime.datetime(2017, 11, 11, 6, 59, 24)), (datetime.datetime(2017, 11, 11, 7, 8, 31), datetime.datetime(2017, 11, 11, 7, 8, 46)), (datetime.datetime(2017, 11, 11, 6, 50, 43), datetime.datetime(2017, 11, 11, 6, 50, 58)), (datetime.datetime(2017, 11, 11, 6, 22, 2), datetime.datetime(2017, 11, 11, 6, 22, 17)), (datetime.datetime(2017, 11, 11, 6, 16, 26), datetime.datetime(2017, 11, 11, 6, 16, 41)), (datetime.datetime(2017, 11, 11, 6, 31, 6), datetime.datetime(2017, 11, 11, 6, 31, 21)), (datetime.datetime(2017, 11, 11, 7, 1, 37), datetime.datetime(2017, 11, 11, 7, 1, 52)), (datetime.datetime(2017, 11, 11, 6, 37, 58), datetime.datetime(2017, 11, 11, 6, 38, 13)), (datetime.datetime(2017, 11, 11, 6, 48, 30), datetime.datetime(2017, 11, 11, 6, 48, 45)), (datetime.datetime(2017, 11, 11, 6, 41, 41), datetime.datetime(2017, 11, 11, 6, 41, 56)), (datetime.datetime(2017, 11, 11, 6, 22, 58), datetime.datetime(2017, 11, 11, 6, 23, 13)), (datetime.datetime(2017, 11, 11, 6, 45, 10), datetime.datetime(2017, 11, 11, 6, 45, 25)), (datetime.datetime(2017, 11, 11, 6, 31, 32), datetime.datetime(2017, 11, 11, 6, 31, 47)), (datetime.datetime(2017, 11, 11, 6, 32, 11), datetime.datetime(2017, 11, 11, 6, 32, 26)), (datetime.datetime(2017, 11, 11, 7, 1, 38), datetime.datetime(2017, 11, 11, 7, 1, 53)), (datetime.datetime(2017, 11, 11, 7, 1, 1), datetime.datetime(2017, 11, 11, 7, 1, 16)), (datetime.datetime(2017, 11, 11, 6, 52, 47), datetime.datetime(2017, 11, 11, 6, 53, 2)), (datetime.datetime(2017, 11, 11, 6, 33, 14), datetime.datetime(2017, 11, 11, 6, 33, 29)), (datetime.datetime(2017, 11, 11, 7, 2, 26), datetime.datetime(2017, 11, 11, 7, 2, 41)), (datetime.datetime(2017, 11, 11, 6, 23, 27), datetime.datetime(2017, 11, 11, 6, 23, 42)), (datetime.datetime(2017, 11, 11, 7, 4, 50), datetime.datetime(2017, 11, 11, 7, 5, 5)), (datetime.datetime(2017, 11, 11, 6, 17, 14), datetime.datetime(2017, 11, 11, 6, 17, 29)), (datetime.datetime(2017, 11, 11, 6, 23, 30), datetime.datetime(2017, 11, 11, 6, 23, 45)), (datetime.datetime(2017, 11, 11, 6, 27, 52), datetime.datetime(2017, 11, 11, 6, 28, 7)), (datetime.datetime(2017, 11, 11, 6, 37, 17), datetime.datetime(2017, 11, 11, 6, 37, 32)), (datetime.datetime(2017, 11, 11, 6, 25, 20), datetime.datetime(2017, 11, 11, 6, 25, 35)), (datetime.datetime(2017, 11, 11, 6, 31, 44), datetime.datetime(2017, 11, 11, 6, 31, 59)), (datetime.datetime(2017, 11, 11, 6, 51), datetime.datetime(2017, 11, 11, 6, 51, 15)), (datetime.datetime(2017, 11, 11, 6, 14, 27), datetime.datetime(2017, 11, 11, 6, 14, 42)), (datetime.datetime(2017, 11, 11, 6, 45, 11), datetime.datetime(2017, 11, 11, 6, 45, 26)), (datetime.datetime(2017, 11, 11, 7, 4, 9), datetime.datetime(2017, 11, 11, 7, 4, 24)), (datetime.datetime(2017, 11, 11, 6, 44), datetime.datetime(2017, 11, 11, 6, 44, 15)), (datetime.datetime(2017, 11, 11, 6, 21, 44), datetime.datetime(2017, 11, 11, 6, 21, 59)), (datetime.datetime(2017, 11, 11, 6, 35, 34), datetime.datetime(2017, 11, 11, 6, 35, 49)), (datetime.datetime(2017, 11, 11, 6, 12, 11), datetime.datetime(2017, 11, 11, 6, 12, 26)), (datetime.datetime(2017, 11, 11, 6, 51, 55), datetime.datetime(2017, 11, 11, 6, 52, 10)), (datetime.datetime(2017, 11, 11, 6, 27, 16), datetime.datetime(2017, 11, 11, 6, 27, 31)), (datetime.datetime(2017, 11, 11, 6, 54, 33), datetime.datetime(2017, 11, 11, 6, 54, 48)), (datetime.datetime(2017, 11, 11, 6, 45, 1), datetime.datetime(2017, 11, 11, 6, 45, 16))]

def m_sens_search(sensor_number, interval_start, interval_end):
    data = []
    for record in m_sens_data_collection.find({'sensor_number': sensor_number, 'timestamp': {'$gte': interval_start, '$lt': interval_end}}).sort('timestamp'):
        data.append(record['value'])
    return data

m_sens_trials = [[] for _ in range(6)]

interval_count = 1
for interval in intervals:
    print("interval count: " + str(interval_count))
    interval_count += 1

    m_sens_values = [[] for _ in range(6)]

    interval_start, interval_end = interval
    for sensor_number in range(1, 7):

        start_time = datetime.datetime.now()
        m_sens_values[sensor_number-1].extend(m_sens_search(sensor_number, interval_start, interval_end))
        end_time = datetime.datetime.now()
        m_sens_elapsed_time = (end_time - start_time).total_seconds() * 1000
        m_sens_trials[sensor_number-1].append(m_sens_elapsed_time)

for i in range(6):
    print("Sensor " + str(i+1))
    print("Time to Retrieve (milliseconds): " + str(np.average(m_sens_trials[i])))