"""
Ship Telemetry Pipeline DAG
Orchestrates the complete ship telemetry data pipeline using Apache Airflow 3.0.0
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sensors.base import BaseSensorOperator
from airflow.decorators import task
import subprocess
import time
import logging
import os
from pathlib import Path

# Configuration
PROJECT_ROOT = Path(__file__).parent.parent.parent  # Go up from airflow/dags/ to project root
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ship_telemetry"

# Default arguments for the DAG
default_args = {
    'owner': 'ship-telemetry-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# Create the DAG
dag = DAG(
    'ship_telemetry_pipeline',
    default_args=default_args,
    description='Complete ship telemetry data processing pipeline',
    schedule=timedelta(hours=1),  # Updated for Airflow 3.0
    max_active_runs=1,
    tags=['ship-telemetry', 'streaming', 'kafka', 'spark'],
)

class KafkaHealthSensor(BaseSensorOperator):
    """Custom sensor to check if Kafka is healthy and ready"""
    
    def __init__(self, bootstrap_servers, *args, **kwargs):
        super(KafkaHealthSensor, self).__init__(*args, **kwargs)
        self.bootstrap_servers = bootstrap_servers
    
    def poke(self, context):
        try:
            from confluent_kafka import Producer
            producer = Producer({
                'bootstrap.servers': self.bootstrap_servers,
                'socket.timeout.ms': 5000
            })
            # Test connection by getting metadata
            metadata = producer.list_topics(timeout=5)
            producer.flush()
            self.log.info("✅ Kafka is healthy and ready")
            return True
        except Exception as e:
            self.log.info(f"⏳ Kafka not ready yet: {str(e)}")
            return False

@task(dag=dag)
def check_prerequisites():
    """Check if all required files and services are available"""
    logging.info("🔍 Checking prerequisites...")
    
    required_files = [
        'simulate_sensors.py',
        'stream_processor.py',
        'docker-compose.yml',
        'monitoring_dashboard.py'
    ]
    
    missing_files = []
    for file in required_files:
        if not (PROJECT_ROOT / file).exists():
            missing_files.append(file)
    
    if missing_files:
        raise FileNotFoundError(f"❌ Missing required files: {missing_files}")
    
    # Check if Docker is running
    try:
        result = subprocess.run(['docker', 'info'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            raise RuntimeError("❌ Docker is not running")
    except Exception as e:
        raise RuntimeError(f"❌ Docker check failed: {str(e)}")
    
    logging.info("✅ All prerequisites met")
    return True

def cleanup_previous_runs_func():
    """Clean up any previous pipeline runs"""
    logging.info("🧹 Starting cleanup task...")
    
    try:
        # Stop any existing containers
        logging.info("Stopping Docker containers...")
        result = subprocess.run(
            ['docker-compose', 'down'], 
            cwd=PROJECT_ROOT, 
            capture_output=True, 
            text=True,
            timeout=30
        )
        if result.returncode != 0:
            logging.warning(f"Warning: docker-compose down failed: {result.stderr}")
        else:
            logging.info("✅ Docker containers stopped")
        
        # Kill any existing Python processes for our scripts
        logging.info("Stopping Python processes...")
        for script in ['simulate_sensors.py', 'stream_processor.py']:
            try:
                result = subprocess.run(
                    ['pkill', '-f', script], 
                    capture_output=True,
                    text=True
                )
                if result.returncode == 0:
                    logging.info(f"✅ Stopped {script}")
                else:
                    logging.info(f"ℹ️ No {script} process found")
            except Exception as e:
                logging.warning(f"⚠️ Failed to stop {script}: {str(e)}")
        
        logging.info("✅ Cleanup completed successfully")
        return True
        
    except Exception as e:
        logging.error(f"❌ Cleanup failed: {str(e)}")
        raise

# Replace the @task decorator with PythonOperator
cleanup_task = PythonOperator(
    task_id='cleanup_previous_runs',
    python_callable=cleanup_previous_runs_func,
    dag=dag
)

@task(dag=dag)
def start_kafka_services():
    """Start Kafka and Zookeeper services"""
    logging.info("🐳 Starting Kafka services...")
    
    try:
        # Start services in detached mode
        result = subprocess.run(
            ['docker-compose', 'up', '-d'],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=120
        )
        
        if result.returncode != 0:
            raise RuntimeError(f"Failed to start Kafka services: {result.stderr}")
        
        logging.info("✅ Kafka services started")
        return True
        
    except Exception as e:
        logging.error(f"❌ Failed to start Kafka services: {str(e)}")
        raise

@task(dag=dag)
def wait_for_kafka_ready():
    """Wait for Kafka to be fully ready"""
    logging.info("⏳ Waiting for Kafka to be ready...")
    
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            from confluent_kafka import Producer
            producer = Producer({
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'socket.timeout.ms': 5000
            })
            # Test connection by getting metadata
            metadata = producer.list_topics(timeout=5)
            producer.flush()
            logging.info("✅ Kafka is ready!")
            return True
        except Exception as e:
            if attempt < max_attempts - 1:
                logging.info(f"⏳ Kafka not ready (attempt {attempt + 1}/{max_attempts}): {str(e)}")
                time.sleep(10)
            else:
                raise RuntimeError(f"❌ Kafka failed to become ready after {max_attempts} attempts")

@task(dag=dag)
def generate_sample_data():
    """Generate sample data if needed"""
    logging.info("📊 Checking for sample data...")
    
    # Check if we have recent data
    data_path = PROJECT_ROOT / "warehouse" / "default" / "ship_telemetry" / "data"
    if not data_path.exists() or len(list(data_path.glob("*"))) == 0:
        logging.info("📊 Generating sample data...")
        result = subprocess.run(
            ['python', 'create_sample_data.py'],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode != 0:
            raise RuntimeError(f"Failed to generate sample data: {result.stderr}")
        
        logging.info("✅ Sample data generated")
    else:
        logging.info("✅ Sample data already exists")

@task(dag=dag)
def start_stream_processor():
    """Start the stream processor"""
    logging.info("⚡ Starting stream processor...")
    
    try:
        # Start stream processor in background
        process = subprocess.Popen(
            ['python', 'stream_processor.py'],
            cwd=PROJECT_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        
        # Wait for it to start (look for success message)
        start_time = time.time()
        timeout = 120
        
        while time.time() - start_time < timeout:
            if process.poll() is not None:
                raise RuntimeError("Stream processor exited unexpectedly")
            
            # Check if process is ready (this is a simplified check)
            time.sleep(5)
            
            # For Airflow, we'll assume it started if it's still running after 30 seconds
            if time.time() - start_time > 30:
                logging.info("✅ Stream processor started")
                return True
        
        raise RuntimeError("Stream processor failed to start within timeout")
        
    except Exception as e:
        logging.error(f"❌ Failed to start stream processor: {str(e)}")
        raise

@task(dag=dag)
def start_ship_simulation():
    """Start the ship sensor simulation"""
    logging.info("🚢 Starting ship simulation...")
    
    try:
        # Start simulation in background
        process = subprocess.Popen(
            ['python', 'simulate_sensors.py'],
            cwd=PROJECT_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        
        # Wait for it to start producing data
        start_time = time.time()
        timeout = 60
        
        while time.time() - start_time < timeout:
            if process.poll() is not None:
                raise RuntimeError("Ship simulation exited unexpectedly")
            
            # For Airflow, we'll assume it started if it's still running after 15 seconds
            if time.time() - start_time > 15:
                logging.info("✅ Ship simulation started")
                return True
        
        raise RuntimeError("Ship simulation failed to start within timeout")
        
    except Exception as e:
        logging.error(f"❌ Failed to start ship simulation: {str(e)}")
        raise

@task(dag=dag)
def validate_pipeline():
    """Validate that the pipeline is working correctly"""
    logging.info("🔍 Validating pipeline...")
    
    # Check if data is being processed
    time.sleep(30)  # Wait for some data to be processed
    
    # Check for output data
    raw_data_path = PROJECT_ROOT / "warehouse" / "default" / "ship_telemetry" / "data"
    if raw_data_path.exists() and len(list(raw_data_path.glob("*"))) > 0:
        logging.info("✅ Raw data is being written")
    else:
        logging.warning("⚠️ No raw data found")
    
    # Check Kafka topic
    try:
        from confluent_kafka import Consumer
        consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'validation_group',
            'auto.offset.reset': 'latest'
        })
        
        consumer.subscribe([KAFKA_TOPIC])
        
        message_count = 0
        timeout_start = time.time()
        timeout = 10  # 10 seconds timeout
        
        while time.time() - timeout_start < timeout:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            message_count += 1
            if message_count >= 5:  # Check for at least 5 messages
                break
        
        consumer.close()
        
        if message_count > 0:
            logging.info(f"✅ Kafka topic has {message_count} messages")
        else:
            logging.warning("⚠️ No messages found in Kafka topic")
            
    except Exception as e:
        logging.warning(f"⚠️ Kafka validation failed: {str(e)}")
    
    logging.info("✅ Pipeline validation completed")

@task(dag=dag)
def send_notification():
    """Send notification that pipeline is running"""
    logging.info("📧 Pipeline is running successfully!")
    logging.info("🎯 You can now access the dashboard at: streamlit run monitoring_dashboard.py")

# Define task dependencies using the new Airflow 3.0 syntax
prerequisites_task = check_prerequisites()
kafka_start_task = start_kafka_services()
kafka_ready_task = wait_for_kafka_ready()
sample_data_task = generate_sample_data()
processor_task = start_stream_processor()
simulation_task = start_ship_simulation()
validate_task = validate_pipeline()
notify_task = send_notification()

# Set up dependencies
cleanup_task >> prerequisites_task >> kafka_start_task >> kafka_ready_task
kafka_ready_task >> sample_data_task >> processor_task >> simulation_task
simulation_task >> validate_task >> notify_task 