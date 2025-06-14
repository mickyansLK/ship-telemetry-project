from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import timedelta
import subprocess
import os
import time
import logging
from config import AIRFLOW_CONFIG, LOGGING_CONFIG

# Setup logging
logging.basicConfig(**LOGGING_CONFIG)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG using context manager
with DAG(
    'ship_telemetry_pipeline',
    default_args=default_args,
    description='Orchestrates ship telemetry stream and dashboard',
    schedule=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['ship', 'telemetry'],
) as dag:

    @task
    def start_stream_processor():
        """Launch stream processor with process safety."""
        try:
            subprocess.run(['pkill', '-f', 'stream_processor.py'], stderr=subprocess.DEVNULL)
            time.sleep(2)

            process = subprocess.Popen(
                ['bash', '-c', 'source ~/venv/bin/activate && python stream_processor.py'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid
            )
            time.sleep(5)
            if process.poll() is not None:
                _, stderr = process.communicate()
                raise RuntimeError(f"Stream processor failed: {stderr.decode()}")
            logger.info("✅ Stream processor started.")
        except Exception as e:
            logger.error(f"❌ Failed to start stream processor: {e}")
            raise

    @task
    def start_dashboard():
        """Launch Streamlit dashboard with process safety."""
        try:
            subprocess.run(['pkill', '-f', 'streamlit run dashboard.py'], stderr=subprocess.DEVNULL)
            time.sleep(2)

            process = subprocess.Popen(
                ['bash', '-c', 'source ~/venv/bin/activate && streamlit run dashboard.py'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid
            )
            time.sleep(5)
            if process.poll() is not None:
                _, stderr = process.communicate()
                raise RuntimeError(f"Dashboard failed: {stderr.decode()}")
            logger.info("✅ Dashboard started.")
        except Exception as e:
            logger.error(f"❌ Failed to start dashboard: {e}")
            raise

    @task
    def cleanup_processes():
        """Kill residual background processes."""
        try:
            subprocess.run(['pkill', '-f', 'stream_processor.py'], stderr=subprocess.DEVNULL)
            subprocess.run(['pkill', '-f', 'streamlit run dashboard.py'], stderr=subprocess.DEVNULL)
            logger.info("🧹 Cleanup completed.")
        except Exception as e:
            logger.error(f"❌ Cleanup failed: {e}")
            raise

    # Define task dependencies using the new syntax
    start_stream_processor() >> start_dashboard() >> cleanup_processes()
