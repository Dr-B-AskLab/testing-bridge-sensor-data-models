import pandas as pd
import datetime
from elasticsearch import Elasticsearch
import numpy as np

# Establish Elasticsearch connection
es = Elasticsearch(['localhost:9200'])

# Define Elasticsearch index mappings
ES_TIME_DATA_INDEX_NAME = 'timedataindex'
def es_time_create_index():
    es.indices.create(index=ES_TIME_DATA_INDEX_NAME)
es_time_create_index()

global id
id = 1

# Define a function to process a single CSV
def es_time_process_csv(file_path, sensor_number):
    print("Processing: " + file_path)
    # Read CSV into a Pandas DataFrame
    df = pd.read_csv(file_path, skiprows=1)
    df = df.rename(columns={'128': f'sensor_{sensor_number}'})
    return df

# Define a function to process multiple CSV files
def es_time_process_multiple_csv(file_paths):
    id = 1
    num_file_paths = len(file_paths)

    df = es_time_process_csv(file_paths[0], 1)

    # Calculate timestamp for first row based on final file name
    file_name = file_paths[0].split('/')[-1]
    timestamp = datetime.datetime.strptime(file_name[-18:-4], '%Y%m%d%H%M%S')
    df['timestamp'] = timestamp

    # Calculate timestamps for remaining rows
    time_delta = datetime.timedelta(seconds=1/128)
    for i in range(1, len(df)):
        df.iloc[i, df.columns.get_loc('timestamp')] = timestamp + (i*time_delta)

    file_paths.pop(0)
    for i, file_path in enumerate(file_paths):
        # Process current CSV file
        temp_df = es_time_process_csv(file_path, i+2)
        df = df.join(temp_df.set_index(df.index))

    # Define a dictionary that maps column names to key names in the `doc` dictionary
    sensor_column_mapping = {f'sensor_{i+1}': f'sensor_{i+1}' for i in range(num_file_paths)}

    actions = []
    print(df)
    for index, row in df.iterrows():
        action = {"index": {"_index": ES_TIME_DATA_INDEX_NAME, "_id": id}}
        doc = {
            "timestamp": row['timestamp'],
        }
        # Add sensor data to `doc` dictionary based on the column name and key name mapping
        for column_name, key_name in sensor_column_mapping.items():
            if column_name in row:
                doc[key_name] = row[column_name]
        
        actions.append(action)
        actions.append(doc)
        if(id%100==0):
            es_time_insert(actions)
            actions = []
        id += 1
    if(len(actions) != 0):
        es_time_insert(actions)

def es_time_insert(actions):
    # Insert data into Elasticsearch index
    es.bulk(index=ES_TIME_DATA_INDEX_NAME, operations=actions)

# Process each CSV file
file_paths = [f'DISPLACEMENT_WIRELESS#{j+1}_20171111061047.csv' for j in range(6)]

es_time_process_multiple_csv(file_paths)

intervals = [(datetime.datetime(2017, 11, 11, 6, 45, 54), datetime.datetime(2017, 11, 11, 6, 46, 9)), (datetime.datetime(2017, 11, 11, 7, 8, 35), datetime.datetime(2017, 11, 11, 7, 8, 50)), (datetime.datetime(2017, 11, 11, 6, 52, 18), datetime.datetime(2017, 11, 11, 6, 52, 33)), (datetime.datetime(2017, 11, 11, 7, 4, 40), datetime.datetime(2017, 11, 11, 7, 4, 55)), (datetime.datetime(2017, 11, 11, 6, 31, 59), datetime.datetime(2017, 11, 11, 6, 32, 14)), (datetime.datetime(2017, 11, 11, 6, 31, 31), datetime.datetime(2017, 11, 11, 6, 31, 46)), (datetime.datetime(2017, 11, 11, 6, 11, 9), datetime.datetime(2017, 11, 11, 6, 11, 24)), (datetime.datetime(2017, 11, 11, 6, 47, 22), datetime.datetime(2017, 11, 11, 6, 47, 37)), (datetime.datetime(2017, 11, 11, 6, 15, 28), datetime.datetime(2017, 11, 11, 6, 15, 43)), (datetime.datetime(2017, 11, 11, 6, 46, 4), datetime.datetime(2017, 11, 11, 6, 46, 19)), (datetime.datetime(2017, 11, 11, 6, 53, 9), datetime.datetime(2017, 11, 11, 6, 53, 24)), (datetime.datetime(2017, 11, 11, 7, 0, 5), datetime.datetime(2017, 11, 11, 7, 0, 20)), (datetime.datetime(2017, 11, 11, 6, 53, 58), datetime.datetime(2017, 11, 11, 6, 54, 13)), (datetime.datetime(2017, 11, 11, 6, 43, 12), datetime.datetime(2017, 11, 11, 6, 43, 27)), (datetime.datetime(2017, 11, 11, 7, 7, 12), datetime.datetime(2017, 11, 11, 7, 7, 27)), (datetime.datetime(2017, 11, 11, 6, 20, 3), datetime.datetime(2017, 11, 11, 6, 20, 18)), (datetime.datetime(2017, 11, 11, 6, 44, 28), datetime.datetime(2017, 11, 11, 6, 44, 43)), (datetime.datetime(2017, 11, 11, 6, 53), datetime.datetime(2017, 11, 11, 6, 53, 15)), (datetime.datetime(2017, 11, 11, 7, 5, 18), datetime.datetime(2017, 11, 11, 7, 5, 33)), (datetime.datetime(2017, 11, 11, 6, 28, 25), datetime.datetime(2017, 11, 11, 6, 28, 40)), (datetime.datetime(2017, 11, 11, 7, 3, 41), datetime.datetime(2017, 11, 11, 7, 3, 56)), (datetime.datetime(2017, 11, 11, 6, 42, 52), datetime.datetime(2017, 11, 11, 6, 43, 7)), (datetime.datetime(2017, 11, 11, 6, 45, 4), datetime.datetime(2017, 11, 11, 6, 45, 19)), (datetime.datetime(2017, 11, 11, 6, 33, 36), datetime.datetime(2017, 11, 11, 6, 33, 51)), (datetime.datetime(2017, 11, 11, 7, 3, 29), datetime.datetime(2017, 11, 11, 7, 3, 44)), (datetime.datetime(2017, 11, 11, 6, 15, 55), datetime.datetime(2017, 11, 11, 6, 16, 10)), (datetime.datetime(2017, 11, 11, 6, 52, 18), datetime.datetime(2017, 11, 11, 6, 52, 33)), (datetime.datetime(2017, 11, 11, 6, 19, 44), datetime.datetime(2017, 11, 11, 6, 19, 59)), (datetime.datetime(2017, 11, 11, 6, 30, 52), datetime.datetime(2017, 11, 11, 6, 31, 7)), (datetime.datetime(2017, 11, 11, 6, 20, 13), datetime.datetime(2017, 11, 11, 6, 20, 28)), (datetime.datetime(2017, 11, 11, 6, 15, 13), datetime.datetime(2017, 11, 11, 6, 15, 28)), (datetime.datetime(2017, 11, 11, 7, 5, 13), datetime.datetime(2017, 11, 11, 7, 5, 28)), (datetime.datetime(2017, 11, 11, 6, 23, 53), datetime.datetime(2017, 11, 11, 6, 24, 8)), (datetime.datetime(2017, 11, 11, 6, 16, 28), datetime.datetime(2017, 11, 11, 6, 16, 43)), (datetime.datetime(2017, 11, 11, 6, 25, 43), datetime.datetime(2017, 11, 11, 6, 25, 58)), (datetime.datetime(2017, 11, 11, 6, 47, 51), datetime.datetime(2017, 11, 11, 6, 48, 6)), (datetime.datetime(2017, 11, 11, 6, 20, 8), datetime.datetime(2017, 11, 11, 6, 20, 23)), (datetime.datetime(2017, 11, 11, 7, 7, 22), datetime.datetime(2017, 11, 11, 7, 7, 37)), (datetime.datetime(2017, 11, 11, 6, 32, 52), datetime.datetime(2017, 11, 11, 6, 33, 7)), (datetime.datetime(2017, 11, 11, 6, 50), datetime.datetime(2017, 11, 11, 6, 50, 15)), (datetime.datetime(2017, 11, 11, 6, 40, 31), datetime.datetime(2017, 11, 11, 6, 40, 46)), (datetime.datetime(2017, 11, 11, 6, 20, 23), datetime.datetime(2017, 11, 11, 6, 20, 38)), (datetime.datetime(2017, 11, 11, 6, 49, 20), datetime.datetime(2017, 11, 11, 6, 49, 35)), (datetime.datetime(2017, 11, 11, 7, 6, 41), datetime.datetime(2017, 11, 11, 7, 6, 56)), (datetime.datetime(2017, 11, 11, 7, 2, 43), datetime.datetime(2017, 11, 11, 7, 2, 58)), (datetime.datetime(2017, 11, 11, 6, 46, 11), datetime.datetime(2017, 11, 11, 6, 46, 26)), (datetime.datetime(2017, 11, 11, 6, 37, 56), datetime.datetime(2017, 11, 11, 6, 38, 11)), (datetime.datetime(2017, 11, 11, 6, 12, 23), datetime.datetime(2017, 11, 11, 6, 12, 38)), (datetime.datetime(2017, 11, 11, 6, 26, 26), datetime.datetime(2017, 11, 11, 6, 26, 41)), (datetime.datetime(2017, 11, 11, 6, 30), datetime.datetime(2017, 11, 11, 6, 30, 15)), (datetime.datetime(2017, 11, 11, 6, 36, 51), datetime.datetime(2017, 11, 11, 6, 37, 6)), (datetime.datetime(2017, 11, 11, 6, 50, 28), datetime.datetime(2017, 11, 11, 6, 50, 43)), (datetime.datetime(2017, 11, 11, 6, 46, 2), datetime.datetime(2017, 11, 11, 6, 46, 17)), (datetime.datetime(2017, 11, 11, 6, 25, 10), datetime.datetime(2017, 11, 11, 6, 25, 25)), (datetime.datetime(2017, 11, 11, 6, 14, 34), datetime.datetime(2017, 11, 11, 6, 14, 49)), (datetime.datetime(2017, 11, 11, 6, 32, 10), datetime.datetime(2017, 11, 11, 6, 32, 25)), (datetime.datetime(2017, 11, 11, 6, 32, 21), datetime.datetime(2017, 11, 11, 6, 32, 36)), (datetime.datetime(2017, 11, 11, 6, 58, 17), datetime.datetime(2017, 11, 11, 6, 58, 32)), (datetime.datetime(2017, 11, 11, 6, 55, 15), datetime.datetime(2017, 11, 11, 6, 55, 30)), (datetime.datetime(2017, 11, 11, 6, 33, 6), datetime.datetime(2017, 11, 11, 6, 33, 21)), (datetime.datetime(2017, 11, 11, 6, 25, 50), datetime.datetime(2017, 11, 11, 6, 26, 5)), (datetime.datetime(2017, 11, 11, 6, 59, 9), datetime.datetime(2017, 11, 11, 6, 59, 24)), (datetime.datetime(2017, 11, 11, 7, 8, 31), datetime.datetime(2017, 11, 11, 7, 8, 46)), (datetime.datetime(2017, 11, 11, 6, 50, 43), datetime.datetime(2017, 11, 11, 6, 50, 58)), (datetime.datetime(2017, 11, 11, 6, 22, 2), datetime.datetime(2017, 11, 11, 6, 22, 17)), (datetime.datetime(2017, 11, 11, 6, 16, 26), datetime.datetime(2017, 11, 11, 6, 16, 41)), (datetime.datetime(2017, 11, 11, 6, 31, 6), datetime.datetime(2017, 11, 11, 6, 31, 21)), (datetime.datetime(2017, 11, 11, 7, 1, 37), datetime.datetime(2017, 11, 11, 7, 1, 52)), (datetime.datetime(2017, 11, 11, 6, 37, 58), datetime.datetime(2017, 11, 11, 6, 38, 13)), (datetime.datetime(2017, 11, 11, 6, 48, 30), datetime.datetime(2017, 11, 11, 6, 48, 45)), (datetime.datetime(2017, 11, 11, 6, 41, 41), datetime.datetime(2017, 11, 11, 6, 41, 56)), (datetime.datetime(2017, 11, 11, 6, 22, 58), datetime.datetime(2017, 11, 11, 6, 23, 13)), (datetime.datetime(2017, 11, 11, 6, 45, 10), datetime.datetime(2017, 11, 11, 6, 45, 25)), (datetime.datetime(2017, 11, 11, 6, 31, 32), datetime.datetime(2017, 11, 11, 6, 31, 47)), (datetime.datetime(2017, 11, 11, 6, 32, 11), datetime.datetime(2017, 11, 11, 6, 32, 26)), (datetime.datetime(2017, 11, 11, 7, 1, 38), datetime.datetime(2017, 11, 11, 7, 1, 53)), (datetime.datetime(2017, 11, 11, 7, 1, 1), datetime.datetime(2017, 11, 11, 7, 1, 16)), (datetime.datetime(2017, 11, 11, 6, 52, 47), datetime.datetime(2017, 11, 11, 6, 53, 2)), (datetime.datetime(2017, 11, 11, 6, 33, 14), datetime.datetime(2017, 11, 11, 6, 33, 29)), (datetime.datetime(2017, 11, 11, 7, 2, 26), datetime.datetime(2017, 11, 11, 7, 2, 41)), (datetime.datetime(2017, 11, 11, 6, 23, 27), datetime.datetime(2017, 11, 11, 6, 23, 42)), (datetime.datetime(2017, 11, 11, 7, 4, 50), datetime.datetime(2017, 11, 11, 7, 5, 5)), (datetime.datetime(2017, 11, 11, 6, 17, 14), datetime.datetime(2017, 11, 11, 6, 17, 29)), (datetime.datetime(2017, 11, 11, 6, 23, 30), datetime.datetime(2017, 11, 11, 6, 23, 45)), (datetime.datetime(2017, 11, 11, 6, 27, 52), datetime.datetime(2017, 11, 11, 6, 28, 7)), (datetime.datetime(2017, 11, 11, 6, 37, 17), datetime.datetime(2017, 11, 11, 6, 37, 32)), (datetime.datetime(2017, 11, 11, 6, 25, 20), datetime.datetime(2017, 11, 11, 6, 25, 35)), (datetime.datetime(2017, 11, 11, 6, 31, 44), datetime.datetime(2017, 11, 11, 6, 31, 59)), (datetime.datetime(2017, 11, 11, 6, 51), datetime.datetime(2017, 11, 11, 6, 51, 15)), (datetime.datetime(2017, 11, 11, 6, 14, 27), datetime.datetime(2017, 11, 11, 6, 14, 42)), (datetime.datetime(2017, 11, 11, 6, 45, 11), datetime.datetime(2017, 11, 11, 6, 45, 26)), (datetime.datetime(2017, 11, 11, 7, 4, 9), datetime.datetime(2017, 11, 11, 7, 4, 24)), (datetime.datetime(2017, 11, 11, 6, 44), datetime.datetime(2017, 11, 11, 6, 44, 15)), (datetime.datetime(2017, 11, 11, 6, 21, 44), datetime.datetime(2017, 11, 11, 6, 21, 59)), (datetime.datetime(2017, 11, 11, 6, 35, 34), datetime.datetime(2017, 11, 11, 6, 35, 49)), (datetime.datetime(2017, 11, 11, 6, 12, 11), datetime.datetime(2017, 11, 11, 6, 12, 26)), (datetime.datetime(2017, 11, 11, 6, 51, 55), datetime.datetime(2017, 11, 11, 6, 52, 10)), (datetime.datetime(2017, 11, 11, 6, 27, 16), datetime.datetime(2017, 11, 11, 6, 27, 31)), (datetime.datetime(2017, 11, 11, 6, 54, 33), datetime.datetime(2017, 11, 11, 6, 54, 48)), (datetime.datetime(2017, 11, 11, 6, 45, 1), datetime.datetime(2017, 11, 11, 6, 45, 16))]

# Define function to get records for a given sensor and time interval
def es_time_search(sensor_number, interval_start, interval_end):
    query = {
        "query": {
            "bool": {
                "must": [
                    {"range": {"timestamp": {"gte": interval_start, "lt": interval_end}}}
                ]
            }
        },
        "sort": [{"timestamp": {"order": "asc"}}]
    }
    result = es.search(index=ES_TIME_DATA_INDEX_NAME, body=query, size=10000)
    print(result)
    values = []
    for hit in result['hits']['hits']:
        for i in range(1, sensor_number):
            if f'sensor_{i}' in hit['_source']:
                values.append(hit['_source'][f'sensor_{i}'])
    return values

es_time_trials = [[] for _ in range(6)]

interval_count = 1
for interval in intervals:
    print("interval count: " + str(interval_count))
    interval_count += 1

    es_time_values = [[] for _ in range(6)]

    interval_start, interval_end = interval
    for sensor_number in range(1, 7):

        start_time = datetime.datetime.now()
        records = es_time_search(sensor_number, interval_start, interval_end)
        es_time_values[sensor_number-1].extend(records)
        end_time = datetime.datetime.now()
        es_time_elapsed_time = (end_time - start_time).total_seconds() * 1000
        es_time_trials[sensor_number-1].append(es_time_elapsed_time)

print(es_time_values[0])

for i in range(6):
    print("Sensor " + str(i+1))
    print("Time to Retrieve (milliseconds): " + str(np.average(es_time_trials[i])))