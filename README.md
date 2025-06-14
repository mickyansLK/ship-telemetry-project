# Ship Telemetry Monitoring System

A real-time telemetry monitoring system for maritime vessels, built with Apache Airflow 3.0.0 and modern data engineering practices.

## Overview

This system provides real-time monitoring and analysis of ship telemetry data, including:
- Ship location and movement tracking
- Fuel consumption monitoring
- Weather condition tracking
- Engine performance metrics
- Real-time alerts and notifications

## Architecture

The system consists of three main components:

1. **Stream Processor**: Processes real-time telemetry data from ships
2. **Monitoring Dashboard**: Visualizes ship data and metrics
3. **Airflow Orchestration**: Manages the entire data pipeline

## Tech Stack

- **Orchestration**: Apache Airflow 3.0.0
- **Stream Processing**: PySpark
- **Data Storage**: DuckDB
- **Visualization**: Streamlit
- **Containerization**: Docker

## Prerequisites

- Python 3.12+
- Docker and Docker Compose
- Apache Airflow 3.0.0

## Installation

1. **Clone the Repository**
   ```bash
   git clone https://github.com/yourusername/ship-telemetry-project.git
   cd ship-telemetry-project
   ```

2. **Set Up Python Environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Configure Environment**
   Create a `.env` file in the project root:
   ```env
   AIRFLOW_HOME=$(pwd)
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   KAFKA_TOPIC=ship_telemetry
   EXPORT_PATH=warehouse/default
   CHECKPOINT_PATH=checkpoints
   DASHBOARD_REFRESH_INTERVAL=10
   CACHE_TTL=30
   ```

4. **Initialize Airflow**
   ```bash
   export AIRFLOW_HOME=$(pwd)
   airflow standalone
   ```

## Usage

### Starting the System

1. **Start Airflow**
   ```bash
   export AIRFLOW_HOME=$(pwd)
   airflow standalone
   ```

2. **Access the Airflow Web Interface**
   - Open http://localhost:8080
   - Log in with the credentials displayed in the terminal
   - Enable the `ship_telemetry_pipeline` DAG
   - Trigger the DAG manually to start the pipeline

### Monitoring

- **Airflow Dashboard**: http://localhost:8080
- **Streamlit Dashboard**: http://localhost:8501

## Project Structure

```
ship-telemetry-project/
├── dags/
│   └── ship_telemetry_dag.py    # Airflow DAG definition
├── stream_processor.py          # Real-time data processing
├── dashboard.py                 # Streamlit dashboard
├── simulate_sensors.py          # Data simulation
├── config.py                    # Configuration management
├── requirements.txt             # Python dependencies
└── airflow.cfg                  # Airflow configuration
```

## Configuration

### Airflow Configuration

The system uses Airflow 3.0.0 with the following key configurations:

```ini
[core]
dags_folder = dags
load_examples = False

[api]
auth_backend = airflow.api.auth.backend.basic_auth
jwt_secret = your-secure-secret-here
port = 8080
workers = 4
host = 0.0.0.0

[webserver]
secret_key = your-secret-key-here
```

### Environment Variables

Key environment variables for system configuration:

- `AIRFLOW_HOME`: Airflow home directory
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka connection string
- `KAFKA_TOPIC`: Kafka topic for telemetry data
- `EXPORT_PATH`: Path for storing processed data
- `CHECKPOINT_PATH`: Path for Spark checkpoints
- `DASHBOARD_REFRESH_INTERVAL`: Dashboard refresh rate in seconds
- `CACHE_TTL`: Cache time-to-live in seconds

## Troubleshooting

### Common Issues

1. **Airflow API Server Not Starting**
   - Ensure `jwt_secret` is set in `airflow.cfg`
   - Check if port 8080 is available
   - Verify environment variables are set correctly

2. **Stream Processing Issues**
   - Check Kafka connection settings
   - Verify checkpoint directory permissions
   - Monitor Spark logs for errors

3. **Dashboard Issues**
   - Ensure Streamlit is running
   - Check data file permissions
   - Verify cache settings

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For support, please open an issue in the GitHub repository.
