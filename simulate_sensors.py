import json
import time
import random
import math
from datetime import datetime
from kafka import KafkaProducer

# Kafka setup
KAFKA_BROKER = 'localhost:9092'
TOPIC = "ship-telemetry"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Destination: Southampton
DESTINATION = {"lat": 50.9097, "lon": -1.4044}

# Ships starting from various locations
ships = {
    "bellisimo": {
        "lat": 37.9838, "lon": 23.7275, "fuel": 1000.0, "distance": 2400.0, "heading": 330.0, "speed": 15.0, "engine_temp": 75.0
    },
    "santa-maria": {
        "lat": 38.9637, "lon": 35.2433, "fuel": 1000.0, "distance": 3000.0, "heading": 320.0, "speed": 14.0, "engine_temp": 74.0
    },
    "titanic": {
        "lat": 41.9028, "lon": 12.4964, "fuel": 1000.0, "distance": 4000.0, "heading": 315.0, "speed": 16.0, "engine_temp": 76.0
    }
}

weather_conditions = ['clear', 'rain', 'storm', 'fog', 'windy']
weather_impact = {
    'clear': 1.0,
    'rain': 1.1,
    'storm': 1.3,
    'fog': 1.05,
    'windy': 1.2
}

def move_ship(lat, lon, speed_knots, heading_deg):
    heading_rad = math.radians(heading_deg)
    # Rough conversion of knots to degree change
    deg_per_sec = speed_knots * 0.00015
    new_lat = lat + deg_per_sec * math.cos(heading_rad)
    new_lon = lon + deg_per_sec * math.sin(heading_rad)
    return round(new_lat, 5), round(new_lon, 5)

def simulate_movement(ship):
    # Randomly assign weather
    weather = random.choice(weather_conditions)
    impact = weather_impact[weather]

    # Adjust speed and heading slightly
    ship["heading"] = (ship["heading"] + random.uniform(-2, 2)) % 360
    ship["speed"] = max(8.0, min(ship["speed"] + random.uniform(-0.5, 0.5), 20.0))

    # Move position
    ship["lat"], ship["lon"] = move_ship(ship["lat"], ship["lon"], ship["speed"], ship["heading"])

    # Update fuel consumption based on weather impact
    fuel_burn = ship["speed"] * 0.2 * impact
    ship["fuel"] = max(0.0, ship["fuel"] - fuel_burn)

    # Reduce distance based on speed
    ship["distance"] = max(0.0, ship["distance"] - ship["speed"] * 0.1)

    # Engine temperature variation
    ship["engine_temp"] += random.uniform(-0.2, 0.3)

    # Return current weather for record
    return weather

# Main telemetry loop
try:
    while True:
        for ship_id, ship in ships.items():
            weather = simulate_movement(ship)

            data = {
                "ship_id": ship_id,
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "lat": ship["lat"],
                "lon": ship["lon"],
                "fuel_level": round(ship["fuel"], 2),
                "distance_to_destination": round(ship["distance"], 2),
                "weather_condition": weather,
                "speed_knots": round(ship["speed"], 2),
                "wind_speed": round(random.uniform(5.0, 30.0), 2),
                "heading": round(ship["heading"], 2),
                "engine_temp": round(ship["engine_temp"], 1)
            }

            producer.send(TOPIC, value=data)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Sent: {data}")

        producer.flush()
        time.sleep(5)

except KeyboardInterrupt:
    print("Simulation stopped.")
finally:
    producer.close()