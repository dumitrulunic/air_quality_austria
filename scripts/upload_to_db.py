import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
from geoalchemy2 import Geometry


DB_URL = "postgresql://postgres:password@db:5432/air_quality"
engine = create_engine(DB_URL)

def load_shapefile_to_db(shapefile_path, table_name, chunksize=4000):
    
    if table_has_data(table_name):
        print(f"Skipping {table_name}: Data already exists in the database.")
        return
    
    print(f"Uploading {table_name} to database in chunks...")

    try:
        gdf = gpd.read_parquet(shapefile_path)

        if "geometry" not in gdf.columns:
            raise ValueError(f"No geometry column found in {table_name}")
        
        # Convert MultiPolygon to Polygon
        gdf["geometry"] = gdf["geometry"].apply(lambda geom: geom if geom.geom_type == "Polygon" else geom.convex_hull)
        

        # with engine.connect() as conn:
        #     conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE;"))
        #     conn.commit()


        for i in range(0, len(gdf), chunksize):
            if i == 0:
                gdf.iloc[i:i+chunksize].to_postgis(table_name, engine, if_exists="replace", index=False)
            else:
                gdf.iloc[i:i+chunksize].to_postgis(table_name, engine, if_exists="append", index=False)

            print(f"Uploaded chunk {i} - {i+chunksize}")

        print(f"Finished uploading {table_name}")

    except Exception as e:
        print(f"Error uploading {table_name}: {e}")

def load_air_quality_to_db(data_path, table_name):
    
    if table_has_data(table_name):
        print(f"Skipping {table_name}: Data already exists in the database.")
        return
    
    print(f"Uploading {table_name} to database...")

    df = pd.read_parquet(data_path)
    
    # with engine.connect() as conn:
    #     conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE;"))
    #     conn.commit()

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])

    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"Uploaded: {table_name}")
    
def table_has_data(table_name):
    """Check if a table exists and has data."""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='{table_name}');"))
        table_exists = result.scalar()
        
        if not table_exists:
            return False
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name};"))
        row_count = result.scalar()
        
        return row_count > 0

# Paths to processed shapefiles
shapefiles = {
    "roads": "data/processed/shapefiles/gis_osm_roads_free_1.parquet",
    "landuse": "data/processed/shapefiles/gis_osm_landuse_a_free_1.parquet",
    "water": "data/processed/shapefiles/gis_osm_water_a_free_1.parquet",
    "buildings": "data/processed/shapefiles/gis_osm_buildings_a_free_1.parquet"
}

# Paths to air quality data
air_quality_data = {
    "air_quality": "data/processed/air_quality/cleaned_air_quality.parquet",
    "metadata": "data/processed/air_quality/cleaned_metadata.parquet"
}

for table, path in shapefiles.items():
    load_shapefile_to_db(path, table)

for table, path in air_quality_data.items():
    load_air_quality_to_db(path, table)

print("All data uploaded successfully!")
