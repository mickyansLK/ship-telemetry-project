import pytest
from datetime import datetime, timedelta
import pandas as pd
from batch_aggregator import (
    DataQualityCheck,
    run_data_quality_checks,
    run_aggregation,
    cleanup_old_data,
    write_to_delta
)

def test_data_quality_check_pass(test_db, sample_telemetry_data):
    """Test data quality check passing"""
    # Insert sample data
    test_db.execute("INSERT INTO ship_telemetry SELECT * FROM sample_telemetry_data")
    
    # Run quality checks
    results = run_data_quality_checks(test_db)
    
    # All checks should pass with valid data
    assert all(results.values())

def test_data_quality_check_fail(test_db):
    """Test data quality check failing"""
    # Insert invalid data
    test_db.execute("""
        INSERT INTO ship_telemetry VALUES
        ('SHIP001', CURRENT_TIMESTAMP, 200.0, 300.0, 150.0, 100.0, 'CLEAR', 50.0, 10.0, 45.0, 200.0)
    """)
    
    # Run quality checks
    results = run_data_quality_checks(test_db)
    
    # Some checks should fail with invalid data
    assert not all(results.values())

def test_run_aggregation(test_db, sample_telemetry_data):
    """Test aggregation query"""
    # Insert sample data
    test_db.execute("INSERT INTO ship_telemetry SELECT * FROM sample_telemetry_data")
    
    # Run aggregation
    result_df = run_aggregation()
    
    # Check results
    assert len(result_df) > 0
    assert 'avg_fuel_level' in result_df.columns
    assert 'avg_speed_knots' in result_df.columns
    assert 'total_records' in result_df.columns

def test_incremental_aggregation(test_db, sample_telemetry_data):
    """Test incremental aggregation"""
    # Insert initial data
    test_db.execute("INSERT INTO ship_telemetry SELECT * FROM sample_telemetry_data")
    
    # Run first aggregation
    first_result = run_aggregation()
    first_count = len(first_result)
    
    # Insert more data
    new_data = sample_telemetry_data.copy()
    new_data['timestamp'] = datetime.now() + timedelta(days=1)
    test_db.execute("INSERT INTO ship_telemetry SELECT * FROM new_data")
    
    # Run incremental aggregation
    incremental_result = run_aggregation(datetime.now().date())
    
    # Should only process new data
    assert len(incremental_result) > first_count

def test_cleanup_old_data(test_db, sample_telemetry_data):
    """Test data cleanup"""
    # Insert old data
    old_data = sample_telemetry_data.copy()
    old_data['timestamp'] = datetime.now() - timedelta(days=100)
    test_db.execute("INSERT INTO ship_telemetry SELECT * FROM old_data")
    
    # Insert recent data
    recent_data = sample_telemetry_data.copy()
    recent_data['timestamp'] = datetime.now()
    test_db.execute("INSERT INTO ship_telemetry SELECT * FROM recent_data")
    
    # Run cleanup
    cleanup_old_data(test_db)
    
    # Check results
    result = test_db.execute("SELECT COUNT(*) FROM ship_telemetry").fetchone()[0]
    assert result == len(recent_data)  # Only recent data should remain

def test_write_to_delta(spark, test_delta_path, sample_telemetry_data):
    """Test Delta table write"""
    # Convert sample data to Spark DataFrame
    spark_df = spark.createDataFrame(sample_telemetry_data)
    
    # Write to Delta
    write_to_delta(spark_df, test_delta_path)
    
    # Verify write
    delta_df = spark.read.format("delta").load(test_delta_path)
    assert delta_df.count() == len(sample_telemetry_data) 