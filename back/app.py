import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from api.air_quality_routes import air_quality_bp
from api.shapefile_routes import shapefile_bp
from utils.config import Config

# Initialize Database
db = SQLAlchemy()

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    
    # Initialize extensions
    db.init_app(app)
    CORS(app)  # Allow cross-origin requests
    
    # Register Blueprints
    app.register_blueprint(air_quality_bp, url_prefix='/api/air_quality')
    app.register_blueprint(shapefile_bp, url_prefix='/api/shapefiles')
    
    # Default route
    @app.route('/')
    def home():
        return {"message": "Welcome to the Air Quality API"}
    
    return app

if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)
