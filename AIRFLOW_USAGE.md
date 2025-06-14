# Apache Airflow 3.0.0 Orchestration for Ship Telemetry Pipeline

This guide explains how to use Apache Airflow 3.0.0 to orchestrate the ship telemetry data pipeline with advanced scheduling, monitoring, and dependency management.

## 🎯 Overview

Apache Airflow 3.0.0 provides a production-ready orchestration solution with:
- **Modern Web UI** with enhanced monitoring and management capabilities
- **Task Decorators** for simplified task definition
- **Improved Scheduling** with better performance
- **Enhanced Dependency Management** with task dependencies and sensors
- **Better Error Handling** and retry logic
- **Advanced Logging** and alerting capabilities
- **Improved Scalability** with optimized executors

## 📋 Prerequisites

- Python 3.8+ (Airflow 3.0.0 requirement)
- Python virtual environment activated
- Docker and Docker Compose installed
- All dependencies installed: `pip install -r requirements.txt`

## 🚀 Quick Start

### 1. Setup Airflow 3.0.0
```bash
# Run the setup script
python setup_airflow.py
```

This will:
- Initialize the Airflow database with `airflow db migrate`
- Create an admin user (admin/admin123)
- Set up the configuration for Airflow 3.0.0

### 2. Start Airflow Services

**Option A - Use the startup script (Recommended):**
```bash
./start_airflow.sh
```

**Option B - Manual startup:**

**Terminal 1 - Start the Scheduler:**
```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow scheduler
```

**Terminal 2 - Start the Web Server:**
```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow webserver --port 8080
```

### 3. Access the Web UI
- Open: http://localhost:8080
- Login: `admin` / `admin123`

### 4. Run the DAG
1. In the Airflow UI, find the `ship_telemetry_pipeline` DAG
2. The DAG should be automatically enabled (not paused)
3. Click "Trigger DAG" to run it manually

## 📊 DAG Structure (Airflow 3.0.0 Features)

The `ship_telemetry_pipeline` DAG uses modern Airflow 3.0.0 features:

```
cleanup_previous_runs → check_prerequisites → start_kafka_services → wait_for_kafka_ready
                                                                           ↓
                                                                    generate_sample_data
                                                                           ↓
                                                                  start_stream_processor
                                                                           ↓
                                                                   start_ship_simulation
                                                                           ↓
                                                                    validate_pipeline
                                                                           ↓
                                                                    send_notification
```

### New Airflow 3.0.0 Features Used:

1. **Task Decorators** - All tasks use `@task` decorator for cleaner code
2. **Simplified Dependencies** - Direct task chaining with `>>`
3. **Updated Schedule Parameter** - Uses `schedule` instead of `schedule_interval`
4. **Auto-enabled DAGs** - DAGs are not paused by default

### Task Details:

1. **cleanup_previous_runs** - Stops existing containers and processes
2. **check_prerequisites** - Validates required files and Docker
3. **start_kafka_services** - Starts Kafka and Zookeeper containers
4. **wait_for_kafka_ready** - Waits for Kafka to be fully operational
5. **generate_sample_data** - Creates sample data if needed
6. **start_stream_processor** - Starts the Spark streaming application
7. **start_ship_simulation** - Starts the sensor data simulation
8. **validate_pipeline** - Checks that data is flowing correctly
9. **send_notification** - Logs success and provides next steps

## ⚙️ Configuration (Airflow 3.0.0)

### DAG Configuration
The DAG is configured with Airflow 3.0.0 syntax:
- **Schedule**: `schedule=timedelta(hours=1)` (new syntax)
- **Retries**: 2 retries with 5-minute delay
- **Concurrency**: Only 1 instance at a time (`max_active_runs=1`)
- **Catchup**: Disabled (won't run historical instances)
- **Auto-enabled**: DAGs start enabled by default

### Environment Variables (Updated for 3.0.0)
Key environment variables:
```bash
export AIRFLOW_HOME=$(pwd)/airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:///$(pwd)/airflow/airflow.db
export AIRFLOW__LOGGING__BASE_LOG_FOLDER=$(pwd)/logs
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
```

## 🔧 Customization (Airflow 3.0.0)

### Changing the Schedule
Edit `dags/airflow_dag.py`:
```python
# Run every 30 minutes
schedule=timedelta(minutes=30)

# Run daily at 2 AM
schedule='0 2 * * *'

# Run only when triggered manually
schedule=None
```

### Using Task Decorators
Airflow 3.0.0 makes task creation simpler:
```python
@task
def my_custom_task():
    """Custom task using the @task decorator"""
    # Your task logic here
    return result

# Use in DAG
custom_task = my_custom_task()
```

### Adding Email Notifications
Update the DAG's `default_args`:
```python
default_args = {
    'owner': 'ship-telemetry-team',
    'email': ['admin@yourcompany.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    # ... other args
}
```

## 📈 Monitoring and Troubleshooting (Airflow 3.0.0)

### Enhanced Web UI Features
- **Improved Graph View** - Better visualization of task dependencies
- **Enhanced Grid View** - New default view with better performance
- **Real-time Updates** - Live status updates without page refresh
- **Better Task Logs** - Improved log viewing and searching
- **Performance Metrics** - Enhanced monitoring capabilities

### Useful Commands (Updated for 3.0.0)

```bash
# List all DAGs
airflow dags list

# Test a specific task
airflow tasks test ship_telemetry_pipeline cleanup_previous_runs 2024-01-01

# Check DAG structure
airflow dags show ship_telemetry_pipeline

# Trigger a DAG run
airflow dags trigger ship_telemetry_pipeline

# Check task status
airflow tasks state ship_telemetry_pipeline start_kafka_services 2024-01-01

# View logs
airflow tasks log ship_telemetry_pipeline start_kafka_services 2024-01-01 1

# Database migration (instead of db init)
airflow db migrate
```

## 🔄 Production Considerations (Airflow 3.0.0)

### Executor Options
- **LocalExecutor** (current): Good for single-machine setups
- **CeleryExecutor**: For distributed processing with Redis/RabbitMQ
- **KubernetesExecutor**: For cloud-native deployments
- **Sequential Executor**: For development/testing only

### New in Airflow 3.0.0
- **Improved Performance**: Better scheduler performance
- **Enhanced Security**: Updated security features
- **Better Resource Management**: Optimized resource usage
- **Simplified Configuration**: Streamlined configuration options

## 🆚 Comparison: Airflow 3.0.0 vs 2.x

| Feature | Airflow 2.x | Airflow 3.0.0 |
|---------|-------------|---------------|
| **Task Definition** | PythonOperator | @task decorator |
| **Schedule Parameter** | schedule_interval | schedule |
| **Database Init** | db init | db migrate |
| **Default DAG State** | Paused | Active |
| **Web UI** | Standard | Enhanced |
| **Performance** | Good | Improved |
| **Python Support** | 3.7+ | 3.8+ |

## 🎛️ Advanced Features (Airflow 3.0.0)

### Task Groups
Organize related tasks:
```python
from airflow.utils.task_group import TaskGroup

with TaskGroup("kafka_setup") as kafka_group:
    start_kafka = start_kafka_services()
    wait_kafka = wait_for_kafka_ready()
    start_kafka >> wait_kafka
```

### Dynamic Task Mapping
Create tasks dynamically:
```python
@task
def process_ship_data(ship_id: str):
    # Process data for specific ship
    pass

# Create tasks for multiple ships
ship_ids = ["ship_001", "ship_002", "ship_003"]
process_ship_data.expand(ship_id=ship_ids)
```

### XCom for Data Passing
Share data between tasks:
```python
@task
def extract_data():
    return {"key": "value"}

@task
def process_data(data):
    print(f"Processing: {data}")

# Chain tasks with data passing
data = extract_data()
process_data(data)
```

## 📚 Resources

- [Apache Airflow 3.0.0 Documentation](https://airflow.apache.org/docs/apache-airflow/3.0.0/)
- [Migration Guide to 3.0.0](https://airflow.apache.org/docs/apache-airflow/3.0.0/migration-guide.html)
- [Task Decorators Guide](https://airflow.apache.org/docs/apache-airflow/3.0.0/tutorial/taskflow.html)

## 🆚 Comparison: Airflow vs Simple Orchestrator

| Feature | Simple Orchestrator | Apache Airflow 3.0.0 |
|---------|-------------------|----------------------|
| **Setup Complexity** | Simple | Moderate |
| **Web UI** | None | Advanced |
| **Task Definition** | Functions | @task decorators |
| **Scheduling** | Manual | Cron + Manual |
| **Monitoring** | Basic logging | Advanced UI + Metrics |
| **Retry Logic** | Basic | Configurable |
| **Dependency Management** | Sequential | Complex DAGs |
| **Scalability** | Single machine | Distributed |
| **Production Ready** | Basic | Enterprise-grade |
| **Learning Curve** | Low | Moderate |
| **Python Version** | 3.7+ | 3.8+ |

Choose **Simple Orchestrator** for:
- Quick prototyping
- Simple workflows
- Minimal setup requirements

Choose **Apache Airflow 3.0.0** for:
- Production deployments
- Complex workflows
- Team collaboration
- Advanced monitoring needs
- Scheduled operations
- Modern Python features 