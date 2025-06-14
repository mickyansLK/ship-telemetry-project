import pytest
import streamlit as st
import pandas as pd
import duckdb
import os
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from monitoring_dashboard import (
    get_db_connection,
    get_data_quality_metrics,
    get_performance_metrics,
    detect_anomalies,
    get_ship_routes
)

@pytest.fixture
def sample_data():
    """Create sample data for testing"""
    conn = duckdb.connect(':memory:')
    now = datetime.now()
    # Create sample telemetry data
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
    # Insert sample data with timestamps within the last 24 hours
    sample_data = [
        ('SHIP001', (now - timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S'), 40.7128, -74.0060, 75.5, 150.0, 'CLEAR', 15.5, 10.0, 45.0, 85.0),
        ('SHIP001', (now - timedelta(minutes=9)).strftime('%Y-%m-%d %H:%M:%S'), 40.7130, -74.0058, 75.0, 149.0, 'CLEAR', 15.8, 10.2, 45.5, 85.5),
        ('SHIP002', (now - timedelta(minutes=8)).strftime('%Y-%m-%d %H:%M:%S'), 41.8781, -87.6298, 82.3, 200.0, 'CLOUDY', 14.8, 12.0, 90.0, 84.0),
        ('SHIP002', (now - timedelta(minutes=7)).strftime('%Y-%m-%d %H:%M:%S'), 41.8783, -87.6296, 82.0, 199.0, 'CLOUDY', 15.0, 12.2, 90.5, 84.5)
    ]
    conn.executemany("""
        INSERT INTO ship_telemetry VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, sample_data)
    return conn

def test_db_connection():
    """Test database connection"""
    conn = get_db_connection()
    assert conn is not None
    conn.close()

def test_data_quality_metrics(sample_data):
    """Test data quality metrics calculation"""
    metrics = get_data_quality_metrics(sample_data)
    
    # Check if all required metrics are present
    assert 'completeness' in metrics
    assert 'validity' in metrics
    assert 'timeliness' in metrics
    
    # Check completeness metrics
    completeness = metrics['completeness']
    assert completeness['total_records'].iloc[0] == 4
    assert completeness['valid_ship_ids'].iloc[0] == 4
    
    # Check validity metrics
    validity = metrics['validity']
    assert validity['valid_coordinates'].iloc[0] == 4
    assert validity['valid_fuel_levels'].iloc[0] == 4

def test_performance_metrics(sample_data):
    """Test performance metrics calculation"""
    metrics = get_performance_metrics(sample_data)
    
    # Check if metrics are calculated correctly
    assert len(metrics) == 2  # Two ships
    assert 'ship_id' in metrics.columns
    assert 'avg_speed' in metrics.columns
    assert 'avg_fuel_level' in metrics.columns
    assert 'avg_engine_temp' in metrics.columns

def test_anomaly_detection(sample_data):
    """Test anomaly detection"""
    # Add more normal data points to establish baseline statistics
    now = datetime.now()
    normal_data = [
        ('SHIP001', (now - timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S'), 40.7120, -74.0050, 75.2, 148.0, 'CLEAR', 15.2, 9.8, 44.8, 84.8),
        ('SHIP001', (now - timedelta(minutes=14)).strftime('%Y-%m-%d %H:%M:%S'), 40.7125, -74.0055, 75.1, 147.5, 'CLEAR', 15.3, 9.9, 44.9, 84.9),
        ('SHIP002', (now - timedelta(minutes=13)).strftime('%Y-%m-%d %H:%M:%S'), 41.8785, -87.6300, 82.1, 198.5, 'CLOUDY', 14.9, 12.1, 90.2, 84.2),
        ('SHIP002', (now - timedelta(minutes=12)).strftime('%Y-%m-%d %H:%M:%S'), 41.8787, -87.6302, 82.2, 198.0, 'CLOUDY', 15.1, 12.3, 90.3, 84.3),
    ]
    sample_data.executemany("""
        INSERT INTO ship_telemetry VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, normal_data)
    
    # Add an anomalous record with extreme values
    anomalous_data = [
        ('SHIP003', (now - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S'), 40.7128, -74.0060, 150.0, 150.0, 'CLEAR', 45.0, 10.0, 45.0, 150.0)
    ]
    sample_data.executemany("""
        INSERT INTO ship_telemetry VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, anomalous_data)
    
    anomalies = detect_anomalies(sample_data)
    
    # Check if anomalies are detected
    assert len(anomalies) > 0
    assert 'anomaly_type' in anomalies.columns
    assert 'SHIP003' in anomalies['ship_id'].values

def test_ship_routes(sample_data):
    """Test ship routes data retrieval"""
    routes = get_ship_routes(sample_data)
    
    # Check if routes are retrieved correctly
    assert len(routes) == 4  # Total number of records
    assert 'ship_id' in routes.columns
    assert 'lat' in routes.columns
    assert 'lon' in routes.columns
    assert 'timestamp' in routes.columns

def test_dashboard_components():
    """Test dashboard components rendering"""
    # Mock Streamlit components
    st.title = lambda *args, **kwargs: None
    st.header = lambda *args, **kwargs: None
    st.metric = lambda *args, **kwargs: None
    st.plotly_chart = lambda *args, **kwargs: None
    st.dataframe = lambda *args, **kwargs: None
    st.sidebar = type('obj', (object,), {
        'header': lambda *args, **kwargs: None,
        'selectbox': lambda *args, **kwargs: args[1][0] if len(args) > 1 and isinstance(args[1], list) else None,
        'multiselect': lambda *args, **kwargs: [],
        'slider': lambda *args, **kwargs: args[1] if len(args) > 1 else 0,
        'checkbox': lambda *args, **kwargs: False
    })()
    
    # Mock the database connection to use in-memory database with test data
    import monitoring_dashboard
    original_get_db_connection = monitoring_dashboard.get_db_connection
    
    def mock_get_db_connection():
        conn = duckdb.connect(':memory:')
        now = datetime.now()
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
        # Insert test data
        test_data = [
            ('SHIP001', (now - timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S'), 40.7128, -74.0060, 75.5, 150.0, 'CLEAR', 15.5, 10.0, 45.0, 85.0),
            ('SHIP002', (now - timedelta(minutes=8)).strftime('%Y-%m-%d %H:%M:%S'), 41.8781, -87.6298, 82.3, 200.0, 'CLOUDY', 14.8, 12.0, 90.0, 84.0)
        ]
        conn.executemany("""
            INSERT INTO ship_telemetry VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, test_data)
        return conn
    
    # Apply the mock
    monitoring_dashboard.get_db_connection = mock_get_db_connection
    
    # Test if dashboard runs without errors
    try:
        from monitoring_dashboard import main
        main()
        assert True
    except Exception as e:
        pytest.fail(f"Dashboard failed to run: {str(e)}")
    finally:
        # Restore original function
        monitoring_dashboard.get_db_connection = original_get_db_connection

def test_data_validation():
    """Test data validation in the dashboard"""
    # Create test data with invalid values
    conn = duckdb.connect(':memory:')
    now = datetime.now()
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
    
    # Insert invalid data within the last 24 hours
    invalid_data = [
        ('SHIP001', (now - timedelta(minutes=3)).strftime('%Y-%m-%d %H:%M:%S'), 200.0, 300.0, 150.0, 150.0, 'CLEAR', 60.0, 10.0, 45.0, 200.0),
        ('SHIP002', (now - timedelta(minutes=2)).strftime('%Y-%m-%d %H:%M:%S'), 41.8781, -87.6298, 82.3, 200.0, 'CLOUDY', 14.8, 12.0, 90.0, 84.0)
    ]
    
    conn.executemany("""
        INSERT INTO ship_telemetry VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, invalid_data)
    
    # Test metrics with invalid data
    metrics = get_data_quality_metrics(conn)
    validity = metrics['validity']
    
    # Check if invalid data is properly flagged
    assert validity['valid_coordinates'].iloc[0] == 1  # Only one valid coordinate
    assert validity['valid_fuel_levels'].iloc[0] == 1  # Only one valid fuel level
    assert validity['valid_speeds'].iloc[0] == 1  # Only one valid speed
    assert validity['valid_temps'].iloc[0] == 2  # Both temperatures are valid (84.0 and 200.0 are both between 0-200)

def test_error_handling():
    """Test error handling in the dashboard"""
    # Test with empty database
    conn = duckdb.connect(':memory:')
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
    
    # Test metrics with empty data
    metrics = get_data_quality_metrics(conn)
    assert metrics['completeness']['total_records'].iloc[0] == 0
    
    # Test performance metrics with empty data
    metrics = get_performance_metrics(conn)
    assert len(metrics) == 0
    
    # Test anomaly detection with empty data
    anomalies = detect_anomalies(conn)
    assert len(anomalies) == 0

if __name__ == '__main__':
    pytest.main([__file__]) 