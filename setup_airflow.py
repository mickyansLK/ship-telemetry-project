#!/usr/bin/env python3
"""
Airflow 3.0.0 Setup Script for Ship Telemetry Project
Initializes Airflow database and configuration
"""

import os
import subprocess
import sys
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_airflow():
    """Setup Airflow 3.0.0 for the ship telemetry project"""
    
    project_root = Path(__file__).parent
    
    # Set environment variables for Airflow 3.0.0
    os.environ['AIRFLOW_HOME'] = str(project_root / 'airflow')
    os.environ['AIRFLOW__CORE__DAGS_FOLDER'] = str(project_root / 'dags')
    os.environ['AIRFLOW__CORE__PLUGINS_FOLDER'] = str(project_root / 'plugins')
    os.environ['AIRFLOW__LOGGING__BASE_LOG_FOLDER'] = str(project_root / 'logs')
    os.environ['AIRFLOW__CORE__EXECUTOR'] = 'LocalExecutor'
    os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'] = f'sqlite:///{project_root}/airflow/airflow.db'
    os.environ['AIRFLOW__CORE__LOAD_EXAMPLES'] = 'False'
    os.environ['AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS'] = 'False'
    os.environ['AIRFLOW__WEBSERVER__WEB_SERVER_PORT'] = '8080'
    os.environ['AIRFLOW__WEBSERVER__SECRET_KEY'] = 'ship_telemetry_secret_key_2024'
    os.environ['AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION'] = 'False'
    
    logger.info("🔧 Setting up Airflow 3.0.0 for Ship Telemetry Project...")
    
    try:
        # Initialize Airflow database
        logger.info("📊 Initializing Airflow database...")
        result = subprocess.run(['airflow', 'db', 'migrate'], 
                              capture_output=True, text=True, timeout=120)
        
        if result.returncode != 0:
            logger.error(f"❌ Failed to initialize database: {result.stderr}")
            return False
        
        logger.info("✅ Database initialized successfully")
        
        # Create admin user
        logger.info("👤 Creating admin user...")
        create_user_cmd = [
            'airflow', 'users', 'create',
            '--username', 'admin',
            '--firstname', 'Ship',
            '--lastname', 'Admin',
            '--role', 'Admin',
            '--email', 'admin@shiptelemetry.com',
            '--password', 'admin123'
        ]
        
        result = subprocess.run(create_user_cmd, 
                              capture_output=True, text=True, timeout=60)
        
        if result.returncode != 0:
            if "already exists" in result.stderr:
                logger.info("✅ Admin user already exists")
            else:
                logger.error(f"❌ Failed to create admin user: {result.stderr}")
                return False
        else:
            logger.info("✅ Admin user created successfully")
        
        # Copy configuration file
        config_source = project_root / 'airflow.cfg'
        config_dest = project_root / 'airflow' / 'airflow.cfg'
        
        if config_source.exists():
            logger.info("📋 Copying configuration file...")
            import shutil
            shutil.copy2(config_source, config_dest)
            logger.info("✅ Configuration file copied")
        
        logger.info("🎉 Airflow 3.0.0 setup completed successfully!")
        logger.info("📋 Next steps:")
        logger.info("   1. Start the scheduler: airflow scheduler")
        logger.info("   2. Start the webserver: airflow webserver")
        logger.info("   3. Access UI at: http://localhost:8080")
        logger.info("   4. Login with: admin / admin123")
        
        return True
        
    except subprocess.TimeoutExpired:
        logger.error("❌ Setup timed out")
        return False
    except Exception as e:
        logger.error(f"❌ Setup failed: {str(e)}")
        return False

def check_airflow_installation():
    """Check if Airflow 3.0.0 is properly installed"""
    try:
        result = subprocess.run(['airflow', 'version'], 
                              capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            version_info = result.stdout.strip()
            logger.info(f"✅ Airflow version: {version_info}")
            
            # Check if it's version 3.0.0
            if "3.0.0" in version_info:
                logger.info("✅ Airflow 3.0.0 detected")
                return True
            else:
                logger.warning(f"⚠️ Expected Airflow 3.0.0, found: {version_info}")
                return True  # Still proceed if it's a compatible version
        else:
            logger.error("❌ Airflow not found or not working")
            return False
    except Exception as e:
        logger.error(f"❌ Failed to check Airflow: {str(e)}")
        return False

def main():
    """Main setup function"""
    logger.info("🚀 Starting Airflow 3.0.0 setup for Ship Telemetry Project")
    
    # Check if Airflow is installed
    if not check_airflow_installation():
        logger.error("❌ Please install Airflow 3.0.0 first: pip install -r requirements.txt")
        sys.exit(1)
    
    # Setup Airflow
    if setup_airflow():
        logger.info("✅ Setup completed successfully!")
        sys.exit(0)
    else:
        logger.error("❌ Setup failed!")
        sys.exit(1)

if __name__ == "__main__":
    main() 