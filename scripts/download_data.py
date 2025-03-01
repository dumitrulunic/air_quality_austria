import requests
import os
import pandas as pd
import zipfile

DATA_RAW_DIR = './data/raw'
SHAPEFILES_DIR = os.path.join(DATA_RAW_DIR, 'shapefiles')
AIR_QUALITY_DIR = os.path.join(DATA_RAW_DIR, 'air_quality')

SHAPEFILES_URL = "https://download.geofabrik.de/europe/austria-latest-free.shp.zip"
URL_LIST = "./data/raw/ParquetFilesUrls.csv"

def download_shapefiles():
    os.makedirs(DATA_RAW_DIR, exist_ok=True)
    os.makedirs(DATA_RAW_DIR + '/shapefiles', exist_ok=True)
    
    if any(f.endswith(".shp") for f in os.listdir(SHAPEFILES_DIR)):
        print("Shapefiles already exist. Skipping download.")
        return

    if any(f.endswith(".parquet") for f in os.listdir("./data/processed/shapefiles")):
        print("Shapefiles already processed. Skipping download.")
        return
    
    print("Downloading shapefiles...")
    r = requests.get(SHAPEFILES_URL)
    with open(DATA_RAW_DIR + '/shapefiles/austria-latest-free.shp.zip', 'wb') as f:
        f.write(r.content)
    print("Shapefiles downloaded.")
    
    print("Extracting shapefiles...")
    with zipfile.ZipFile(DATA_RAW_DIR + '/shapefiles/austria-latest-free.shp.zip', 'r') as zip_ref:
        zip_ref.extractall(DATA_RAW_DIR + '/shapefiles')
    print("Shapefiles extracted.")
    
    print("Shapefiles downloaded and extracted. Deliting zip file...")
    os.remove(DATA_RAW_DIR + '/shapefiles/austria-latest-free.shp.zip')
    print("Zip file deleted.")
    
def download_air_quality_data():
    os.makedirs(AIR_QUALITY_DIR, exist_ok=True)

    if not os.path.exists(URL_LIST):
        print(f"URL list file {URL_LIST} not found. Skipping air quality data download.")
        return

    print("Downloading air quality data...")
    df = pd.read_csv(URL_LIST, header=0)
    
    for index, row in df.iterrows():
        parquet_url = row['ParquetFileUrl']
        filename = os.path.basename(parquet_url)
        file_path = os.path.join(AIR_QUALITY_DIR, filename)
        
        if os.path.exists(file_path):
            print(f"File {filename} already exists. Skipping download.")
            continue

        print(f"Downloading {filename}...")
        response = requests.get(parquet_url, stream=True)
        
        if response.status_code == 200:
            with open(file_path, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded {filename}")
        else:
            print(f"Failed to download {filename}. Status code: {response.status_code}")
            
        print("Air quality data downloaded successfully.")

if __name__ == "__main__":
    download_shapefiles()
    download_air_quality_data()
    