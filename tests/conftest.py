import pytest
import duckdb
import pandas as pd
from datetime import datetime, timedelta
import os
import pyspark
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    spark = (SparkSession.builder
            .appName("ShipTelemetryTest")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .getOrCreate())
    
    yield spark
    
    spark.stop()

@pytest.fixture(scope="session")
def test_db():
    """Create a test DuckDB database"""
    # Create a temporary database
    conn = duckdb.connect(':memory:')
    
    # Create test tables
    conn.execute("""
        CREATE TABLE ship_telemetry (
            ship_id VARCHAR,
            timestamp TIMESTAMP,
            lat DOUBLE,
            lon DOUBLE,
            fuel_level DOUBLE,
            distance_to_destination DOUBLE,
            weather_condition VARCHAR,
            speed_knots DOUBLE,
            wind_speed DOUBLE,
            heading DOUBLE,
            engine_temp DOUBLE
        )
    """)
    
    conn.execute("""
        CREATE TABLE metadata_processing (
            last_processed_date TIMESTAMP,
            status VARCHAR,
            records_processed INTEGER,
            processing_time DOUBLE
        )
    """)
    
    yield conn
    
    conn.close()

@pytest.fixture
def sample_telemetry_data():
    """Generate sample telemetry data"""
    now = datetime.now()
    data = {
        'ship_id': ['SHIP001', 'SHIP001', 'SHIP002', 'SHIP002'],
        'timestamp': [
            now - timedelta(minutes=10),
            now - timedelta(minutes=9),
            now - timedelta(minutes=8),
            now - timedelta(minutes=7)
        ],
        'lat': [40.7128, 40.7130, 41.8781, 41.8783],
        'lon': [-74.0060, -74.0058, -87.6298, -87.6296],
        'fuel_level': [75.5, 75.0, 82.3, 82.0],
        'distance_to_destination': [150.0, 149.0, 200.0, 199.0],
        'weather_condition': ['CLEAR', 'CLEAR', 'CLOUDY', 'CLOUDY'],
        'speed_knots': [15.5, 15.8, 14.8, 15.0],
        'wind_speed': [10.0, 10.2, 12.0, 12.2],
        'heading': [45.0, 45.5, 90.0, 90.5],
        'engine_temp': [85.0, 85.5, 84.0, 84.5]
    }
    return pd.DataFrame(data)

@pytest.fixture
def test_delta_path(tmp_path):
    """Create a temporary path for Delta tables"""
    return str(tmp_path / "delta") 