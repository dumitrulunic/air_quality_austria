FROM python:3.10-slim

WORKDIR /app


RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    libpq-dev \
    gdal-bin \
    python3-gdal \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH"

COPY requirements.txt .

COPY . /app/scripts

RUN /app/.venv/bin/python -m pip install --upgrade pip
RUN /app/.venv/bin/pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/data/raw/air_quality /app/data/processed/air_quality

RUN mkdir -p /app/data/raw/air_quality /app/data/processed/air_quality /app/logs


CMD ["sh", "-c", "echo 'Running download_data.py...' && python /app/scripts/download_data.py && echo 'Running process_air_quality.py...' && python /app/scripts/process_air_quality.py && echo 'Running process_shapefiles.py...' && python /app/scripts/process_shapefiles.py && echo 'Uploading data to PostgreSQL...' && python /app/scripts/upload_to_db.py && echo 'Data was processed successfuly, exiting...' && exit 0"]