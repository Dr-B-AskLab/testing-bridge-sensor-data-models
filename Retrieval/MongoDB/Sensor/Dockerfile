# Base image
FROM python:3.9-slim-buster

# Set working directory
WORKDIR /app

# Copy Python dependencies file to the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python script to the container
COPY process_csv.py .

# Run the Python script
CMD ["python", "process_csv.py"]