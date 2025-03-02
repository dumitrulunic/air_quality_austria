FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    libpq-dev \
    gdal-bin \
    python3-gdal \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/data/raw/air_quality /app/data/processed/air_quality

RUN mkdir -p /app/data/raw/air_quality /app/data/processed/air_quality /app/logs


CMD ["sh", "-c", "echo 'Running download_data.py...' && python /app/scripts/download_data.py && echo 'Running process_air_quality.py...' && python /app/scripts/process_air_quality.py && echo 'Running process_shapefiles.py...' && python /app/scripts/process_shapefiles.py && echo 'Uploading data to PostgreSQL...' && python /app/scripts/upload_to_db.py"]