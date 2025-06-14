import json
import time
import random
import math
import os
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer
from typing import Dict, Any
from dataclasses import dataclass
from contextlib import contextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC = os.getenv('KAFKA_TOPIC', "ship-telemetry")
SIMULATION_INTERVAL = int(os.getenv('SIMULATION_INTERVAL', '5'))
MAX_RETRIES = int(os.getenv('KAFKA_MAX_RETRIES', '3'))

@dataclass
class ShipState:
    lat: float
    lon: float
    fuel: float
    distance: float
    heading: float
    speed: float
    engine_temp: float
    last_update: datetime

# Destination: Southampton
DESTINATION = {"lat": 50.9097, "lon": -1.4044}

# Ships starting from various locations with improved initial states
ships: Dict[str, ShipState] = {
    "bellisimo": ShipState(
        lat=37.9838, lon=23.7275, fuel=1000.0,
        distance=2400.0, heading=330.0, speed=15.0,
        engine_temp=75.0, last_update=datetime.now(timezone.utc)
    ),
    "santa-maria": ShipState(
        lat=38.9637, lon=35.2433, fuel=1000.0,
        distance=3000.0, heading=320.0, speed=14.0,
        engine_temp=74.0, last_update=datetime.now(timezone.utc)
    ),
    "titanic": ShipState(
        lat=41.9028, lon=12.4964, fuel=1000.0,
        distance=4000.0, heading=315.0, speed=16.0,
        engine_temp=76._
