import json
import time
import random 
from datetime import datetime 
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
TOPIC = "ship-telemetry"

# Initialize kafka producer
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# configurations for simulation
ships = {
    "bellisimo": {
        "lat": 37.9838, "lon": 23.7275, "fuel": 100.0, "distance": 2400 #Athens
    },
    "santa-maria": {
        "lat": 38.9637, "lon": 35.2433, "fuel": 100.0, "distance": 3000 #Cappadocia
    },
    "titanic": {
        "lat": 41.9028, "lon": 12.4964, "fuel": 100.0, "distance": 4000 #Rome
    }
} 

DESTINATION = {"lat": 50.9097, "lon": -1.4044} #Southampton

weather_conditions = ['clear', 'rain', 'storm', 'fog']

def simulate_movement(ship):
    # ver basic: move Northwest by a small amount
    ship["lat"] += random.uniform(-0.01, 0.01) # northward
    ship["lon"] += random.uniform(-0.01, 0.01) # westward
    ship["fuel"] -= random.uniform(0.1, 0.5) # consume fuel
    ship["distance"] -= random.uniform(1, 5) # reduce distance to destination
    ship["weather"] = random.choice(weather_conditions) # random weather condition

while True:
    for ship_id, ship in ships.items():
        simulate_movement(ship)
        
        data = {
            "ship_id": ship_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "lat": round(ship["lat"], 5),
            "lon": round(ship["lon"], 5),
            "fuel_level":round(max(ship["fuel"], 0.0), 2),
            "distance_to_destination": round(max(ship["distance"], 0.0), 2),
            "weather_condition": ship["weather"],
            "speed": round(random.uniform(10.0, 20.0), 2), # random speed
            "wind_speed": round(random.uniform(0.0, 30.0), 2), # random wind speed
        }
        
        # send data to kafka
        producer.send(TOPIC, value=data)
        print(f"Sent data: {data}")
        
    producer.flush()
    time.sleep(5)  # wait for 5 seconds before sending the next batch of data