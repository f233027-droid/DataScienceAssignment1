
import os
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
AGGREGATED_DATA_DIR = PROCESSED_DATA_DIR / "aggregated"
AUDIT_LOG_DIR = PROCESSED_DATA_DIR / "audit"

# Create directories if they don't exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
AGGREGATED_DATA_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_LOG_DIR.mkdir(parents=True, exist_ok=True)

# Scraper Settings
TLC_BASE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
YEARS_TO_SCRAPE = [2025]
TAXI_TYPES = ["yellow", "green"]

# Spark Settings
SPARK_APP_NAME = "NYC_Congestion_Audit"

# Congestion Zone Settings
# Manhattan South of 60th St approximation or Shapefile handling
SHAPEFILE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"

# Thresholds
SPEED_THRESHOLD_MPH = 65
MIN_TRIP_TIME_MINUTES = 1
MIN_TRIP_FARE = 20
