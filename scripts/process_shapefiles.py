import geopandas as gpd
import os

# Define directories
SHAPEFILE_DIR = "data/raw/shapefiles"
OUTPUT_DIR = "data/processed/shapefiles"

def process_shapefiles():
    print(f"Creating directories: {SHAPEFILE_DIR} and {OUTPUT_DIR}")
    os.makedirs(SHAPEFILE_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("Checking if shapefiles exist...")
    if not os.listdir(SHAPEFILE_DIR):
        print(f"No shapefiles found in {SHAPEFILE_DIR}!")
        return
    
    print("Checking if shapefiles have already been processed...")
    if os.listdir(OUTPUT_DIR):
        print(f"Shapefiles have already been processed. Skipping...")
        return

    shapefiles = [
        "gis_osm_roads_free_1.shp",
        "gis_osm_landuse_a_free_1.shp",
        "gis_osm_water_a_free_1.shp",
        "gis_osm_buildings_a_free_1.shp"
    ]

    for shp in shapefiles:
        shp_path = os.path.join(SHAPEFILE_DIR, shp)
        if not os.path.exists(shp_path):
            print(f"Skipping missing file: {shp}")
            continue

        print(f"Processing {shp}...")
        gdf = gpd.read_file(shp_path)

        if "geometry" in gdf:
            gdf = gdf[["osm_id", "fclass", "geometry"]]

        output_path = os.path.join(OUTPUT_DIR, shp.replace(".shp", ".parquet"))
        gdf.to_parquet(output_path, index=False)
        print(f"Saved: {output_path}")

    print(f"Contents of {OUTPUT_DIR}: {os.listdir(OUTPUT_DIR)}")
    print("Shapefile processing and cleanup complete!")

if __name__ == "__main__":
    process_shapefiles()
