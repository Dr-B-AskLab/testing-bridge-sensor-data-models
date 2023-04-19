import pandas as pd
import datetime
import mysql.connector

# Establish database connection
mydb = mysql.connector.connect(
  host="db",
  user="myuser",
  password="mypassword",
  database="mydatabase"
)
cursor = mydb.cursor()

# Drop the data table if exists
drop_table_query = """
DROP TABLE IF EXISTS data
"""
cursor.execute(drop_table_query)

# Create the data table
create_table_query = """
CREATE TABLE IF NOT EXISTS data (
    timestamp DATETIME NOT NULL,
    value FLOAT NOT NULL,
    sensor_number INT NOT NULL
)
"""
cursor.execute(create_table_query)

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

    return df

# Define a function to process multiple CSV files
def process_multiple_csv(file_paths):
    df = pd.DataFrame(columns=['timestamp', 'value', 'sensor_number'])

    for i, file_path in enumerate(file_paths):
        # Extract data from current CSV file
        new_df = process_csv(file_path, i+1)

        # Concatenate new data with existing data
        df = pd.concat([df, new_df])

    return df

# Define a function to calculate the average of multiple trials
def calculate_average(df, num_trials):
    avg = 0

    for i in range(num_trials):

        # Insert data into MySQL database
        sql = "INSERT INTO data (timestamp, value, sensor_number) VALUES (%s, %s, %s)"
        values = [(row['timestamp'], row['value'], row['sensor_number']) for index, row in df.iterrows()]

        start_time = datetime.datetime.now()

        cursor.executemany(sql, values)
        mydb.commit()

        end_time = datetime.datetime.now()

        # Calculate elapsed time in milliseconds
        elapsed_time = (end_time - start_time).total_seconds() * 1000

        # Print elapsed time for current trial
        print(f'Trial {i+1}: {elapsed_time:.2f} ms')

        # Add elapsed time to running average
        avg += elapsed_time

        cursor.execute("DELETE FROM data")

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

    df = process_multiple_csv(file_paths)

    avg = calculate_average(df, 100)

    output_to_file(avg, f'average_elapsed_time_{i+1}.txt')

    print(f'Average elapsed time: {avg:.2f} ms')