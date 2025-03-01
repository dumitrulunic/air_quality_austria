FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/data/raw/air_quality /app/data/processed/air_quality

RUN mkdir -p /app/data/raw/air_quality /app/data/processed/air_quality /app/logs


CMD ["sh", "-c", "echo '🔄 Running download_data.py...' && python /app/scripts/download_data.py 2>&1 | tee /app/logs/download.log && echo '🔄 Running process_air_quality.py...' && python /app/scripts/process_air_quality.py 2>&1 | tee /app/logs/air_quality.log && echo '🔄 Running process_shapefiles.py...' && python /app/scripts/process_shapefiles.py 2>&1 | tee /app/logs/shapefiles.log && exit $?"]