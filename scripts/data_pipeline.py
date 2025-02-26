import os
import requests
import zipfile
import tarfile
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

# Define dataset URL and paths
DATASET_URL = "https://download.geofabrik.de/europe/austria-latest-free.shp.zip"
DATA_DIR = os.path.abspath("./data")
DATA_ZIP = os.path.join(DATA_DIR, "austria-latest-free.shp.zip")
EXTRACT_DIR = os.path.join(DATA_DIR, "austria_shapefiles")
IPYNB_FILE = os.path.abspath("./notebooks/process_data.ipynb")

os.makedirs(DATA_DIR, exist_ok=True)

# Function to download the dataset for austria shapefiles
def download_file(url, dest):
    if os.path.exists(dest):
        print(f"File already exists: {dest}")
        return
    
    print(f"Downloading dataset from {url} ...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    with open(dest, "wb") as file:
        for chunk in response.iter_content(chunk_size=1024):
            file.write(chunk)
    
    print(f"Download complete: {dest}")

# Function to extract ZIP
def extract_archive(file_path, extract_to):
    if os.path.exists(extract_to) and os.listdir(extract_to):
        print(f"Data already extracted in {extract_to}")
        return
    
    print(f"Extracting {file_path} ...")
    os.makedirs(extract_to, exist_ok=True)

    if file_path.endswith(".zip"):
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)
    elif file_path.endswith((".tar.gz", ".tgz")):
        with tarfile.open(file_path, "r:gz") as tar_ref:
            tar_ref.extractall(extract_to)
    
    print("Extraction complete!")

    print("Extracted files:", os.listdir(extract_to))

# Function to execute a Jupyter Notebook
def run_notebook(notebook_path):
    print(f"Running {notebook_path} ...")
    
    if not os.path.exists(notebook_path):
        print(f"Notebook not found: {notebook_path}")
        return

    with open(notebook_path) as f:
        nb = nbformat.read(f, as_version=4)
    
    ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
    ep.preprocess(nb, {"metadata": {"path": os.path.dirname(notebook_path)}})
    
    print("Notebook execution complete!")

# Run the pipeline
if __name__ == "__main__":
    print(f"Running from: {os.getcwd()}")
    print(f"DATA_ZIP path: {DATA_ZIP}")
    print(f"EXTRACT_DIR path: {EXTRACT_DIR}")
    
    # check if zip file exists
    if not os.path.exists(DATA_ZIP):
        download_file(DATASET_URL, DATA_ZIP)
    
    if os.path.exists(DATA_ZIP):
        extract_archive(DATA_ZIP, EXTRACT_DIR)
    else:
        print(f"ZIP file missing after download: {DATA_ZIP}")

    run_notebook(IPYNB_FILE)
    print("Data pipeline completed!")
