#!/usr/bin/env python3
"""
Sample Data Generator for Ship Telemetry Dashboard
Creates realistic ship telemetry data in DuckDB for testing the dashboard.
"""

import duckdb
import pandas as pd
from datetime import datetime, timedelta
import random
import math
import os

def create_sample_data():
    """Generate sample ship telemetry data"""
    
    # Connect to DuckDB database
    db_path = "ship_telemetry.duckdb"
    conn = duckdb.connect(db_path)
    
    # Create the ship_telemetry table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ship_telemetry (
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
    
    # Clear existing data
    conn.execute("DELETE FROM ship_telemetry")
    
    # Generate sample data for the last 24 hours
    now = datetime.now()
    ships = ['SHIP001', 'SHIP002', 'SHIP003', 'SHIP004', 'SHIP005']
    weather_conditions = ['CLEAR', 'CLOUDY', 'RAINY', 'STORMY', 'FOGGY']
    
    # Starting positions for each ship (different ports)
    ship_positions = {
        'SHIP001': {'lat': 40.7128, 'lon': -74.0060, 'heading': 45},   # New York
        'SHIP002': {'lat': 51.5074, 'lon': -0.1278, 'heading': 90},    # London
        'SHIP003': {'lat': 35.6762, 'lon': 139.6503, 'heading': 180},  # Tokyo
        'SHIP004': {'lat': -33.8688, 'lon': 151.2093, 'heading': 270}, # Sydney
        'SHIP005': {'lat': 37.7749, 'lon': -122.4194, 'heading': 135}  # San Francisco
    }
    
    sample_data = []
    
    for ship_id in ships:
        # Get starting position
        start_pos = ship_positions[ship_id]
        current_lat = start_pos['lat']
        current_lon = start_pos['lon']
        current_heading = start_pos['heading']
        
        # Initial ship parameters
        fuel_level = random.uniform(70, 95)
        distance_to_dest = random.uniform(100, 500)
        base_speed = random.uniform(12, 18)
        base_engine_temp = random.uniform(80, 90)
        
        # Generate data points every 5 minutes for the last 24 hours
        for i in range(288):  # 24 hours * 60 minutes / 5 minutes
            timestamp = now - timedelta(minutes=i*5)
            
            # Simulate ship movement
            speed_variation = random.uniform(0.8, 1.2)
            current_speed = base_speed * speed_variation
            
            # Update position based on speed and heading
            # Rough conversion: 1 knot ≈ 0.000278 degrees per minute
            distance_per_minute = current_speed * 0.000278
            lat_change = distance_per_minute * math.cos(math.radians(current_heading)) * 5
            lon_change = distance_per_minute * math.sin(math.radians(current_heading)) * 5
            
            current_lat += lat_change
            current_lon += lon_change
            
            # Add some randomness to heading
            current_heading += random.uniform(-5, 5)
            current_heading = current_heading % 360
            
            # Simulate fuel consumption
            fuel_consumption = current_speed * 0.02  # Rough fuel consumption rate
            fuel_level = max(10, fuel_level - fuel_consumption * 5/60)  # 5 minutes worth
            
            # Update distance to destination (generally decreasing)
            distance_to_dest = max(0, distance_to_dest - current_speed * 5/60)
            
            # Simulate weather effects
            weather = random.choice(weather_conditions)
            if weather == 'STORMY':
                wind_speed = random.uniform(25, 40)
                current_speed *= 0.8  # Slower in storms
                base_engine_temp += 5  # Higher temp in storms
            elif weather == 'CLEAR':
                wind_speed = random.uniform(5, 15)
            else:
                wind_speed = random.uniform(10, 25)
            
            # Engine temperature with some variation
            engine_temp = base_engine_temp + random.uniform(-5, 10)
            
            # Add some anomalies occasionally (5% chance)
            if random.random() < 0.05:
                if random.random() < 0.5:
                    # Speed anomaly
                    current_speed = random.uniform(30, 45)
                else:
                    # Temperature anomaly
                    engine_temp = random.uniform(120, 150)
            
            sample_data.append((
                ship_id,
                timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                round(current_lat, 6),
                round(current_lon, 6),
                round(fuel_level, 1),
                round(distance_to_dest, 1),
                weather,
                round(current_speed, 1),
                round(wind_speed, 1),
                round(current_heading, 1),
                round(engine_temp, 1)
            ))
    
    # Insert data into database
    conn.executemany("""
        INSERT INTO ship_telemetry VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, sample_data)
    
    # Verify data insertion
    result = conn.execute("SELECT COUNT(*) as total_records FROM ship_telemetry").fetchone()
    print(f"✅ Successfully created {result[0]} sample telemetry records")
    
    # Show sample of data
    sample = conn.execute("""
        SELECT ship_id, timestamp, lat, lon, fuel_level, speed_knots, engine_temp 
        FROM ship_telemetry 
        ORDER BY timestamp DESC 
        LIMIT 5
    """).fetchdf()
    
    print("\n📊 Sample data preview:")
    print(sample.to_string(index=False))
    
    # Show data quality summary
    quality_summary = conn.execute("""
        SELECT 
            COUNT(DISTINCT ship_id) as unique_ships,
            MIN(timestamp) as earliest_record,
            MAX(timestamp) as latest_record,
            AVG(fuel_level) as avg_fuel_level,
            AVG(speed_knots) as avg_speed,
            AVG(engine_temp) as avg_engine_temp
        FROM ship_telemetry
    """).fetchdf()
    
    print("\n📈 Data quality summary:")
    print(quality_summary.to_string(index=False))
    
    conn.close()
    print(f"\n💾 Database saved to: {os.path.abspath(db_path)}")
    print("🚀 You can now run the dashboard with: streamlit run monitoring_dashboard.py")

if __name__ == "__main__":
    create_sample_data() 