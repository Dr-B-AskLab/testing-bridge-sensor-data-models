import pandas as pd
import datetime
from elasticsearch import Elasticsearch

# Establish Elasticsearch connection
es = Elasticsearch(['localhost:9200'])

# Define Elasticsearch index mappings
DATA_INDEX_NAME = 'dataindex'
INDEX_MAPPING = {
    'mappings': {
        'properties': {
            'timestamp': {'type': 'date'},
            'value': {'type': 'float'},
            'sensor_number': {'type': 'integer'}
        }
    }
}
def create_index():
    es.indices.create(index=DATA_INDEX_NAME, body=INDEX_MAPPING)
create_index()

global id
id = 1

# Define a function to process a single CSV
def process_csv(file_path, sensor_number, actions):
    global id

    # Read CSV into a Pandas DataFrame
    df = pd.read_csv(file_path, skiprows=1)
    df = df.rename(columns={'128': 'value'})
    df = df.iloc[0:128]

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
    
    for index, row in df.iterrows():
        action = {"index": {"_index": DATA_INDEX_NAME, "_id": id}}
        doc = {
            "value": row['value'],
            "timestamp": row['timestamp'],
            "sensor_number": row['sensor_number']
        }
        actions.append(action)
        actions.append(doc)
        id += 1

    return actions

# Define a function to process multiple CSV files
def process_multiple_csv(file_paths):
    actions = []
    for i, file_path in enumerate(file_paths):
        # Process current CSV file
        actions = process_csv(file_path, i+1, actions)
    return actions

# Define a function to calculate the average of multiple trials
def calculate_average(actions, num_trials):
    avg = 0

    for i in range(num_trials):

        # Insert data into Elasticsearch index
        start_time = datetime.datetime.now()

        es.bulk(index=DATA_INDEX_NAME, operations=actions)

        end_time = datetime.datetime.now()

        # Calculate elapsed time in milliseconds
        elapsed_time = (end_time - start_time).total_seconds() * 1000

        # Print elapsed time for current trial
        print(f'Trial {i+1}: {elapsed_time:.2f} ms')

        # Add elapsed time to running average
        avg += elapsed_time

        # Delete data from Elasticsearch index
        es.indices.delete(index=DATA_INDEX_NAME)
        create_index()

    # Calculate average elapsed time across all trials
    avg /= num_trials

    return avg

# Define a function to output average elapsed time to a text file
def output_to_file(avg, file_path):
    with open(file_path, 'w') as f:
        f.write(f'Average elapsed time: {avg:.2f} ms')

# Process each CSV file and calculate average elapsed time for each set of files
for i in range(6):
    file_paths = [f'DISPLACEMENT_WIRELESS#{j+1}_20171111061047.csv' for j in range(i+1)]

    print(f'Processing files: {file_paths}')

    actions = process_multiple_csv(file_paths)

    avg = calculate_average(actions, 100)

    output_to_file(avg, f'average_elapsed_time_{i+1}.txt')

    print(f'Average elapsed time: {avg:.2f} ms')