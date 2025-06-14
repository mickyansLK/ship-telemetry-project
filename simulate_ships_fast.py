import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer
import duckdb
import pandas as pd

KAFKA_BROKER = 'localhost:9092'
TOPIC = "ship-telemetry"

# Initialize Kafka producer
producer = Producer({
    'bootstrap.servers': KAFKA_BROKER
})

# Connect to DuckDB for local storage
conn = duckdb.connect(database="ship_dashboard.duckdb", read_only=False)

# Create a table to store ship telemetry if it doesn't exist
conn.execute("""
    CREATE TABLE IF NOT EXISTS ship_telemetry (
        ship_id VARCHAR,
        timestamp TIMESTAMP,
        lat DOUBLE,
        lon DOUBLE,
        fuel_level DOUBLE,
        distance_to_destination DOUBLE,
        weather_condition VARCHAR,
        speed DOUBLE,
        wind_speed DOUBLE
    )
""")

# Ship starting positions
ships = {
    "bellisimo": {"lat": 37.9838, "lon": 23.7275, "fuel": 100.0, "distance": 2400},
    "santa-maria": {"lat": 38.9637, "lon": 35.2433, "fuel": 100.0, "distance": 3000},
    "titanic": {"lat": 41.9028, "lon": 12.4964, "fuel": 100.0, "distance": 4000}
}

weather_conditions = ['clear', 'rain', 'storm', 'fog']

def simulate_movement(ship):
    ship["lat"] += random.uniform(-0.01, 0.01)
    ship["lon"] += random.uniform(-0.01, 0.01)
    ship["fuel"] -= random.uniform(0.1, 0.5)
    ship["distance"] -= random.uniform(1, 5)
    return ship

# 🚀 FAST MODE: Send 30 ship events instantly
for _ in range(30):
    batch_records = []
    for ship_id, ship in ships.items():
        simulate_movement(ship)
        
        data = {
            "ship_id": ship_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "lat": round(ship["lat"], 5),
            "lon": round(ship["lon"], 5),
            "fuel_level": round(max(ship["fuel"], 0.0), 2),
            "distance_to_destination": round(max(ship["distance"], 0.0), 2),
            "weather_condition": random.choice(weather_conditions),
            "speed": round(random.uniform(10.0, 20.0), 2),
            "wind_speed": round(random.uniform(0.0, 30.0), 2),
        }
        
        # Send to Kafka
        producer.produce(TOPIC, value=json.dumps(data).encode('utf-8'))
        batch_records.append(data)
        print(f"Sent data: {data}")
    
    # Save this batch into DuckDB
    batch_df = pd.DataFrame(batch_records)
    conn.register("batch_df", batch_df)
    conn.execute("INSERT INTO ship_telemetry SELECT * FROM batch_df")

producer.flush()
conn.close()
print("\n✅ Finished sending and storing 30 ship telemetry events FAST!")
