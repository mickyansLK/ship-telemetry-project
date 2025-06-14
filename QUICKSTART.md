# 🚀 Quick Start Guide

This guide will help you get the Ship Telemetry Monitoring System up and running in minutes.

## Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Git

## One-Command Setup

We've created a setup script that automates the entire process. Run:

```bash
curl -sSL https://raw.githubusercontent.com/yourusername/ship-telemetry-project/main/scripts/setup.sh | bash
```

Or follow these manual steps:

## Manual Setup

### 1. Clone and Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/ship-telemetry-project.git
cd ship-telemetry-project

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment

Create a `.env` file:

```bash
cat > .env << EOL
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=ship_telemetry
DUCKDB_PATH=ship_telemetry.db
EXPORT_PATH=./export
CHECKPOINT_PATH=./checkpoints
EOL
```

### 3. Start Services

```bash
# Create required directories
mkdir -p export/raw_ship_snapshot checkpoints

# Start Kafka and Zookeeper
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 30
```

### 4. Generate Test Data

```bash
# Generate initial telemetry data
python simulate_sensors.py
```

### 5. Start the Dashboard

```bash
# Important: Always use streamlit run
streamlit run monitoring_dashboard.py
```

## Common Issues and Solutions

### 1. Streamlit Warnings

If you see warnings like:
```
Thread 'MainThread': missing ScriptRunContext!
```

**Solution**: Always use `streamlit run` instead of `python`:
```bash
# ❌ Don't do this
python monitoring_dashboard.py

# ✅ Do this instead
streamlit run monitoring_dashboard.py
```

### 2. Missing Data Files

If you see:
```
Failed to load telemetry data: IO Error: No files found that match the pattern
```

**Solution**: Run these commands:
```bash
# Create directories if they don't exist
mkdir -p export/raw_ship_snapshot

# Generate test data
python simulate_sensors.py

# Verify data exists
ls -l export/raw_ship_snapshot/
```

### 3. Kafka Connection Issues

If Kafka isn't connecting:

```bash
# Check if Kafka is running
docker-compose ps

# Check Kafka logs
docker-compose logs kafka

# Restart if needed
docker-compose restart kafka
```

### 4. Database Issues

If you see database errors:

```bash
# Check if database exists
ls -l ship_telemetry.db

# Initialize if missing
python scripts/init_db.py
```

## Quick Commands Reference

```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# View logs
docker-compose logs -f

# Generate new test data
python simulate_sensors.py

# Start dashboard
streamlit run monitoring_dashboard.py

# Run tests
pytest tests/
```

## Development Workflow

1. **Start Development Environment**
   ```bash
   # Start services
   docker-compose up -d
   
   # Generate test data
   python simulate_sensors.py
   ```

2. **Run Tests**
   ```bash
   # Run all tests
   pytest tests/
   
   # Run specific test file
   pytest tests/test_stream_processor.py
   ```

3. **Start Dashboard**
   ```bash
   streamlit run monitoring_dashboard.py
   ```

4. **Stop Environment**
   ```bash
   docker-compose down
   ```

## Need Help?

- Check the [Troubleshooting Guide](TROUBLESHOOTING.md)
- Open an [Issue](https://github.com/yourusername/ship-telemetry-project/issues)
- Join our [Discord Community](https://discord.gg/your-server)

## Next Steps

1. Review the [Architecture Documentation](docs/ARCHITECTURE.md)
2. Explore the [API Documentation](docs/API.md)
3. Check out the [Contributing Guide](CONTRIBUTING.md) 