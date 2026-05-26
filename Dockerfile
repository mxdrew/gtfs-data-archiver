FROM python:3.12-slim

WORKDIR /app

# Install the zstd utility (pyarrow handles compression internally, 
# but the OS tool is handy for manual container inspections)
RUN apt-get update && \
    apt-get install -y zstd && \
    rm -rf /var/lib/apt/lists/*

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the ingestion script
COPY gtfs_archiver.py .

# Ensure the data directory exists
RUN mkdir -p /app/data

# -u disables output buffering so Docker logs stream in real time
CMD ["python", "-u", "gtfs_archiver.py"]