import pandas as pd
import datetime
from pymongo import MongoClient

# Establish database connection
client = MongoClient('mongodb://localhost:27017/')
db = client['mydatabase']

# Create the data collections
try:
    data_collection = db.create_collection('data')
except:
    data_collection = db['data']
try:
    hold_collection = db.create_collection('hold')
except:
    hold_collection = db['hold']

# Define a function to process a single CSV file
def process_csv(file_path, sensor_number):
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

    # Convert each row of DataFrame to a JSON object and insert into collection
    hold_collection.insert_many(df.to_dict('records'))

# Define a function to process multiple CSV files
def process_multiple_csv(file_paths):
    for i, file_path in enumerate(file_paths):
        # Process current CSV file
        process_csv(file_path, i+1)

# Define a function to calculate the average of multiple trials
def calculate_average(df, num_trials):
    avg = 0

    for i in range(num_trials):
        # Insert data into MongoDB collection
        start_time = datetime.datetime.now()

        data_collection.insert_many(df)

        end_time = datetime.datetime.now()

        # Calculate elapsed time in milliseconds
        elapsed_time = (end_time - start_time).total_seconds() * 1000

        # Print elapsed time for current trial
        print(f'Trial {i+1}: {elapsed_time:.2f} ms')

        # Add elapsed time to running average
        avg += elapsed_time

        data_collection.delete_many({})

        df = hold_collection.find()
    
    hold_collection.drop()

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

    process_multiple_csv(file_paths)

    avg = calculate_average(hold_collection.find(), 100)

    output_to_file(avg, f'average_elapsed_time_{i+1}.txt')

    print(f'Average elapsed time: {avg:.2f} ms')