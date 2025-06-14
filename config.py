import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Airflow Configuration
AIRFLOW_CONFIG = {
    'dags_folder': 'dags',
    'load_examples': False,
    'api': {
        'auth_backend': 'airflow.api.auth.backend.basic_auth',
        'jwt_secret': os.getenv('AIRFLOW_JWT_SECRET', 'your-secret-key-here'),
        'port': 8080,
        'workers': 4,
        'host': '0.0.0.0',
        'access_logfile': '-',
        'ssl_cert': '',
        'ssl_key': ''
    }
}

# Stream Processing Configuration
STREAM_CONFIG = {
    'kafka': {
        'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        'topic': os.getenv('KAFKA_TOPIC', 'ship-telemetry')
    },
    'storage': {
        'base_path': os.getenv('EXPORT_PATH', 'warehouse/default'),
        'checkpoint_base': os.getenv('CHECKPOINT_PATH', 'checkpoints')
    },
    'retry': {
        'max_retries': int(os.getenv('MAX_RETRIES', '3')),
        'retry_delay': int(os.getenv('RETRY_DELAY', '5'))
    }
}

# Dashboard Configuration
DASHBOARD_CONFIG = {
    'parquet_path': os.getenv('PARQUET_PATH', 'warehouse/default/ship_telemetry'),
    'refresh_interval': int(os.getenv('DASHBOARD_REFRESH_INTERVAL', '10')),
    'cache_ttl': int(os.getenv('CACHE_TTL', '30')),
    'baseline_consumption': {
        'bellisimo': 5.0,
        'santa-maria': 4.0,
        'titanic': 6.0
    }
}

# Logging Configuration
LOGGING_CONFIG = {
    'level': logging.INFO,
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
} 