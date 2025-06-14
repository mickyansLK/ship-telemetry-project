#!/bin/bash

# Ship Telemetry Airflow 3.0.0 Startup Script
# Starts both Airflow scheduler and webserver

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Set Airflow 3.0.0 environment variables
export AIRFLOW_HOME="$PROJECT_ROOT/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_ROOT/dags"
export AIRFLOW__CORE__PLUGINS_FOLDER="$PROJECT_ROOT/plugins"
export AIRFLOW__LOGGING__BASE_LOG_FOLDER="$PROJECT_ROOT/logs"
export AIRFLOW__CORE__EXECUTOR="LocalExecutor"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$PROJECT_ROOT/airflow/airflow.db"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS="False"
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT="8080"
export AIRFLOW__WEBSERVER__SECRET_KEY="ship_telemetry_secret_key_2024"
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION="False"

echo -e "${BLUE}🚀 Starting Ship Telemetry Airflow 3.0.0 Services${NC}"
echo -e "${YELLOW}📁 Project Root: $PROJECT_ROOT${NC}"
echo -e "${YELLOW}🏠 Airflow Home: $AIRFLOW_HOME${NC}"

# Check if Airflow is installed
if ! command -v airflow &> /dev/null; then
    echo -e "${RED}❌ Airflow is not installed. Please run: pip install -r requirements.txt${NC}"
    exit 1
fi

# Check Airflow version
AIRFLOW_VERSION=$(airflow version 2>/dev/null | head -n 1 || echo "unknown")
echo -e "${BLUE}📋 Airflow Version: $AIRFLOW_VERSION${NC}"

# Check if database is initialized
if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
    echo -e "${YELLOW}⚠️ Airflow database not found. Running setup...${NC}"
    python setup_airflow.py
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Setup failed. Please check the logs.${NC}"
        exit 1
    fi
fi

# Function to start scheduler
start_scheduler() {
    echo -e "${GREEN}📅 Starting Airflow Scheduler...${NC}"
    airflow scheduler &
    SCHEDULER_PID=$!
    echo -e "${GREEN}✅ Scheduler started with PID: $SCHEDULER_PID${NC}"
}

# Function to start webserver
start_webserver() {
    echo -e "${GREEN}🌐 Starting Airflow Webserver...${NC}"
    airflow webserver --port 8080 &
    WEBSERVER_PID=$!
    echo -e "${GREEN}✅ Webserver started with PID: $WEBSERVER_PID${NC}"
    echo -e "${BLUE}🔗 Access the UI at: http://localhost:8080${NC}"
    echo -e "${BLUE}👤 Login: admin / admin123${NC}"
}

# Function to cleanup on exit
cleanup() {
    echo -e "\n${YELLOW}🛑 Shutting down Airflow services...${NC}"
    
    if [ ! -z "$SCHEDULER_PID" ]; then
        echo -e "${YELLOW}📅 Stopping scheduler (PID: $SCHEDULER_PID)...${NC}"
        kill $SCHEDULER_PID 2>/dev/null || true
    fi
    
    if [ ! -z "$WEBSERVER_PID" ]; then
        echo -e "${YELLOW}🌐 Stopping webserver (PID: $WEBSERVER_PID)...${NC}"
        kill $WEBSERVER_PID 2>/dev/null || true
    fi
    
    # Kill any remaining airflow processes
    pkill -f "airflow scheduler" 2>/dev/null || true
    pkill -f "airflow webserver" 2>/dev/null || true
    
    echo -e "${GREEN}✅ Airflow services stopped${NC}"
    exit 0
}

# Set up signal handlers
trap cleanup SIGINT SIGTERM

# Start services
start_scheduler
sleep 5  # Give scheduler time to start
start_webserver

echo -e "\n${GREEN}🎉 Airflow 3.0.0 services are starting up!${NC}"
echo -e "${BLUE}📋 Next steps:${NC}"
echo -e "   1. Wait for services to fully start (30-60 seconds)"
echo -e "   2. Open http://localhost:8080 in your browser"
echo -e "   3. Login with: admin / admin123"
echo -e "   4. Find the 'ship_telemetry_pipeline' DAG"
echo -e "   5. The DAG should be automatically enabled (not paused)"
echo -e "   6. Click 'Trigger DAG' to run it manually"
echo -e "\n${YELLOW}💡 Press Ctrl+C to stop all services${NC}"

# Wait for processes to finish
wait 