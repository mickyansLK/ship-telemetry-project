# Ship Telemetry Monitoring System Deployment Guide

This guide provides instructions for deploying and configuring the Ship Telemetry Monitoring System.

## Prerequisites

- Python 3.8 or higher
- Docker and Docker Compose
- Git
- Access to a Kafka cluster
- Sufficient disk space for data storage

## System Architecture

The monitoring system consists of the following components:

1. **Data Collection**
   - Kafka for real-time data ingestion
   - Stream processor for data validation and transformation
   - Batch aggregator for historical data analysis

2. **Data Storage**
   - DuckDB for real-time analytics
   - Delta Lake for historical data storage

3. **Monitoring Dashboard**
   - Streamlit-based web interface
   - Real-time metrics and visualizations
   - Anomaly detection and alerts

## Installation Steps

1. **Clone the Repository**
   ```bash
   git clone <repository-url>
   cd ship-telemetry-project
   ```

2. **Set Up Python Environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Configure Environment Variables**
   Create a `.env` file in the project root:
   ```env
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   KAFKA_TOPIC=ship_telemetry
   DUCKDB_PATH=ship_telemetry.db
   EXPORT_PATH=/path/to/export
   CHECKPOINT_PATH=/path/to/checkpoints
   MAX_RETRIES=3
   RETRY_DELAY=5
   ```

4. **Start Kafka and Zookeeper**
   ```bash
   docker-compose up -d
   ```

5. **Initialize Database**
   ```bash
   python scripts/init_db.py
   ```

## Component Configuration

### Stream Processor

The stream processor is configured through environment variables and can be started with:
```bash
python stream_processor.py
```

Key configurations:
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses
- `KAFKA_TOPIC`: Topic for telemetry data
- `CHECKPOINT_PATH`: Path for Spark checkpoints
- `MAX_RETRIES`: Maximum number of retry attempts
- `RETRY_DELAY`: Delay between retries in seconds

### Batch Aggregator

The batch aggregator processes historical data and can be scheduled using cron:
```bash
0 */1 * * * python batch_aggregator.py
```

Key configurations:
- `DUCKDB_PATH`: Path to DuckDB database
- `EXPORT_PATH`: Path for Delta table exports
- `RETENTION_DAYS`: Number of days to retain data

### Monitoring Dashboard

The dashboard can be started with:
```bash
streamlit run monitoring_dashboard.py
```

Configuration options:
- Port: Default is 8501, can be changed with `--server.port`
- Theme: Configured in `.streamlit/config.toml`
- Auto-refresh: Set in the dashboard UI

## Monitoring and Maintenance

### Health Checks

1. **Kafka Health**
   ```bash
   kafka-topics.sh --bootstrap-server localhost:9092 --list
   ```

2. **Database Health**
   ```bash
   python scripts/check_db_health.py
   ```

3. **Stream Processor Health**
   - Check Spark UI at `http://localhost:4040`
   - Monitor checkpoint directory

### Backup and Recovery

1. **Database Backup**
   ```bash
   python scripts/backup_db.py
   ```

2. **Configuration Backup**
   ```bash
   tar -czf config_backup.tar.gz .env docker-compose.yml
   ```

### Scaling Considerations

1. **Kafka Scaling**
   - Increase partitions for the telemetry topic
   - Add more Kafka brokers

2. **Processing Scaling**
   - Adjust Spark executor memory and cores
   - Configure batch sizes in the aggregator

3. **Storage Scaling**
   - Monitor DuckDB size
   - Implement data retention policies
   - Use Delta Lake for historical data

## Troubleshooting

### Common Issues

1. **Kafka Connection Issues**
   - Check broker addresses
   - Verify network connectivity
   - Check Kafka logs

2. **Database Performance**
   - Monitor query execution times
   - Check disk space
   - Optimize indexes

3. **Dashboard Issues**
   - Clear browser cache
   - Check Streamlit logs
   - Verify data freshness

### Logging

Logs are available in:
- Stream processor: `logs/stream_processor.log`
- Batch aggregator: `logs/batch_aggregator.log`
- Dashboard: `logs/dashboard.log`

## Security Considerations

1. **Network Security**
   - Use SSL/TLS for Kafka
   - Implement firewall rules
   - Use VPN for remote access

2. **Data Security**
   - Encrypt sensitive data
   - Implement access controls
   - Regular security audits

3. **Authentication**
   - Use API keys for services
   - Implement user authentication
   - Regular key rotation

## Performance Optimization

1. **Query Optimization**
   - Use appropriate indexes
   - Optimize SQL queries
   - Implement caching

2. **Resource Management**
   - Monitor memory usage
   - Optimize batch sizes
   - Configure garbage collection

3. **Network Optimization**
   - Use compression
   - Implement connection pooling
   - Optimize batch sizes

## Support and Maintenance

For support and maintenance:
1. Monitor system metrics
2. Regular updates and patches
3. Performance tuning
4. Security updates
5. Backup verification

## Contact

For issues and support:
- Email: support@example.com
- Issue Tracker: GitHub Issues
- Documentation: Project Wiki 