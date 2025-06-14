# Ship Telemetry Pipeline Orchestrator

The orchestrator (`orchestrator.py`) is a comprehensive pipeline manager that coordinates the execution of all ship telemetry components in the correct order.

## What it does

The orchestrator manages the complete data pipeline:

1. **🐳 Kafka Services** - Starts Zookeeper and Kafka using Docker Compose
2. **⚡ Stream Processor** - Starts the Spark streaming application to process telemetry data
3. **🚢 Ship Simulation** - Starts the sensor simulation to generate telemetry data
4. **👀 Monitoring** - Continuously monitors all processes and restarts them if they fail

## Prerequisites

- Docker and Docker Compose installed and running
- Python virtual environment activated
- All dependencies installed (`pip install -r requirements.txt`)

## Usage

### Start the complete pipeline:
```bash
python orchestrator.py
```

### Or run it directly (if executable):
```bash
./orchestrator.py
```

## Features

- **🔍 Prerequisites Check** - Validates all required files and Docker availability
- **📋 Ordered Startup** - Starts services in the correct dependency order
- **⏳ Readiness Detection** - Waits for each service to be ready before starting the next
- **🔄 Auto-restart** - Automatically restarts failed processes
- **🛑 Graceful Shutdown** - Handles Ctrl+C and properly stops all services
- **📝 Comprehensive Logging** - Logs to both console and `orchestrator.log` file
- **🎯 Process Monitoring** - Tracks the health of all running processes

## Process Flow

1. **Validation Phase**
   - Checks for required files (`simulate_sensors.py`, `stream_processor.py`, `docker-compose.yml`)
   - Verifies Docker is running

2. **Startup Phase**
   - Starts Kafka services (waits for Kafka server to be ready)
   - Starts stream processor (waits for "Stream processing started successfully")
   - Starts ship simulation (waits for "Sent telemetry data")

3. **Monitoring Phase**
   - Checks process health every 30 seconds
   - Automatically restarts failed processes
   - Logs pipeline status

4. **Shutdown Phase**
   - Gracefully stops all processes on Ctrl+C or termination signal
   - Stops Docker containers
   - Cleans up resources

## Logs

- **Console Output**: Real-time status with emojis for easy reading
- **Log File**: Detailed logs saved to `orchestrator.log`
- **Process Output**: All subprocess output is captured and logged

## After Starting

Once the orchestrator reports "🎉 Pipeline started successfully!", you can:

1. **View the Dashboard**:
   ```bash
   streamlit run monitoring_dashboard.py
   ```

2. **Check Data Processing**:
   - Raw data: `warehouse/default/ship_telemetry/data/`
   - Processed data: `delta/aggregated_ship_metrics/`

## Stopping

- **Graceful Stop**: Press `Ctrl+C` once and wait for graceful shutdown
- **Force Stop**: If processes don't stop, the orchestrator will force-kill them after 10 seconds

## Troubleshooting

### Common Issues:

1. **Docker not running**: Start Docker service
2. **Port conflicts**: Stop other Kafka/Zookeeper instances
3. **Permission errors**: Ensure orchestrator.py is executable (`chmod +x orchestrator.py`)
4. **Missing dependencies**: Run `pip install -r requirements.txt`

### Log Analysis:

Check `orchestrator.log` for detailed error messages and process output.

### Manual Process Management:

If you need to run components individually:
```bash
# Start Kafka services
docker-compose up

# Start stream processor (in another terminal)
python stream_processor.py

# Start ship simulation (in another terminal)
python simulate_sensors.py
```

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Orchestrator  │───▶│  Kafka Services  │───▶│ Stream Processor│
│                 │    │  (Docker)        │    │   (PySpark)     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       ▲                       │
         │                       │                       ▼
         ▼                       │              ┌─────────────────┐
┌─────────────────┐              │              │   Data Storage  │
│ Ship Simulation │──────────────┘              │   (Parquet)     │
│   (Sensors)     │                             └─────────────────┘
└─────────────────┘
```

The orchestrator ensures all components start in the right order and stay healthy throughout the pipeline execution. 