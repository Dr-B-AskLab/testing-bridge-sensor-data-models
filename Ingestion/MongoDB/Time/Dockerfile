# Use an official Python runtime as a parent image
FROM python:3.10-buster

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Copy the process_csv.py script into the container root directory
COPY process_csv.py /

# Make sure the script is executable
RUN chmod +x /process_csv.py

# Run app.py when the container launches
CMD ["python", "process_csv.py"]