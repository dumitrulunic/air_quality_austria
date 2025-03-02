from fastapi import FastAPI
from core.config import settings
from api import air_quality, metadata, roads, landuse, water, buildings
from core.db import engine, Base

Base.metadata.create_all(bind=engine)

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.PROJECT_VERSION
)

# Include routers
app.include_router(air_quality.router, prefix="/api/air_quality", tags=["Air Quality"])
app.include_router(metadata.router, prefix="/api/metadata", tags=["Metadata"])
app.include_router(roads.router, prefix="/api/roads", tags=["Roads"])
app.include_router(landuse.router, prefix="/api/landuse", tags=["Land Use"])
app.include_router(water.router, prefix="/api/water", tags=["Water"])
app.include_router(buildings.router, prefix="/api/buildings", tags=["Buildings"])

# Root route
@app.get("/")
def read_root():
    return {"message": "Air Quality API"}
