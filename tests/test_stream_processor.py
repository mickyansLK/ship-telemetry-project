import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from stream_processor import validate_data, write_with_retry
import pandas as pd
from datetime import datetime
import os
import tempfile

@pytest.fixture
def sample_stream_data(spark):
    """Create sample streaming data"""
    schema = StructType([
        StructField("ship_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("lat", FloatType(), True),
        StructField("lon", FloatType(), True),
        StructField("fuel_level", FloatType(), True),
        StructField("distance_to_destination", FloatType(), True),
        StructField("weather_condition", StringType(), True),
        StructField("speed_knots", FloatType(), True),
        StructField("wind_speed", FloatType(), True),
        StructField("heading", FloatType(), True),
        StructField("engine_temp", FloatType(), True)
    ])
    
    data = [
        ("SHIP001", "2024-02-14 10:00:00", 40.7128, -74.0060, 75.5, 150.0, "CLEAR", 15.5, 10.0, 45.0, 85.0),
        ("SHIP002", "2024-02-14 10:00:00", 41.8781, -87.6298, 82.3, 200.0, "CLOUDY", 14.8, 12.0, 90.0, 84.0)
    ]
    
    return spark.createDataFrame(data, schema)

def test_validate_data_valid(spark, sample_stream_data):
    """Test data validation with valid data"""
    validated_df = validate_data(sample_stream_data)
    assert validated_df.count() == sample_stream_data.count()

def test_validate_data_invalid(spark):
    """Test data validation with invalid data"""
    invalid_data = [
        ("SHIP001", "2024-02-14 10:00:00", 200.0, 300.0, 150.0, 100.0, "CLEAR", 50.0, 10.0, 45.0, 200.0),
        ("SHIP002", "2024-02-14 10:00:00", 41.8781, -87.6298, 82.3, 200.0, "CLOUDY", 14.8, 12.0, 90.0, 84.0)
    ]
    
    schema = StructType([
        StructField("ship_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("lat", FloatType(), True),
        StructField("lon", FloatType(), True),
        StructField("fuel_level", FloatType(), True),
        StructField("distance_to_destination", FloatType(), True),
        StructField("weather_condition", StringType(), True),
        StructField("speed_knots", FloatType(), True),
        StructField("wind_speed", FloatType(), True),
        StructField("heading", FloatType(), True),
        StructField("engine_temp", FloatType(), True)
    ])
    
    invalid_df = spark.createDataFrame(invalid_data, schema)
    validated_df = validate_data(invalid_df)
    assert validated_df.count() == 1  # Only the valid row should remain

def test_validate_data_empty(spark):
    """Test data validation with empty DataFrame"""
    empty_df = spark.createDataFrame([], sample_stream_data.schema)
    validated_df = validate_data(empty_df)
    assert validated_df.count() == 0

def test_validate_data_null_values(spark):
    """Test data validation with null values"""
    null_data = [
        (None, "2024-02-14 10:00:00", 40.7128, -74.0060, 75.5, 150.0, "CLEAR", 15.5, 10.0, 45.0, 85.0),
        ("SHIP002", None, 41.8781, -87.6298, 82.3, 200.0, "CLOUDY", 14.8, 12.0, 90.0, 84.0),
        ("SHIP003", "2024-02-14 10:00:00", None, None, 75.5, 150.0, "CLEAR", 15.5, 10.0, 45.0, 85.0)
    ]
    
    null_df = spark.createDataFrame(null_data, sample_stream_data.schema)
    validated_df = validate_data(null_df)
    assert validated_df.count() == 0  # All rows should be filtered out due to null values

def test_write_with_retry(spark, sample_stream_data, tmp_path):
    """Test write with retry functionality"""
    output_path = str(tmp_path / "test_output")
    
    # Test successful write
    write_with_retry(sample_stream_data, output_path)
    result_df = spark.read.parquet(output_path)
    assert result_df.count() == sample_stream_data.count()

def test_write_with_retry_failure(spark, sample_stream_data, tmp_path, monkeypatch):
    """Test write with retry on failure"""
    output_path = str(tmp_path / "test_output")
    
    # Mock write to fail twice then succeed
    fail_count = 0
    def mock_write(*args, **kwargs):
        nonlocal fail_count
        if fail_count < 2:
            fail_count += 1
            raise Exception("Mock write failure")
        return sample_stream_data.write.mode("overwrite").parquet(output_path)
    
    monkeypatch.setattr(sample_stream_data, "write", mock_write)
    
    # Should succeed after retries
    write_with_retry(sample_stream_data, output_path)
    result_df = spark.read.parquet(output_path)
    assert result_df.count() == sample_stream_data.count()

def test_write_with_retry_max_retries_exceeded(spark, sample_stream_data, tmp_path, monkeypatch):
    """Test write with retry when max retries are exceeded"""
    output_path = str(tmp_path / "test_output")
    
    def mock_write(*args, **kwargs):
        raise Exception("Persistent write failure")
    
    monkeypatch.setattr(sample_stream_data, "write", mock_write)
    
    # Should raise exception after max retries
    with pytest.raises(Exception) as exc_info:
        write_with_retry(sample_stream_data, output_path)
    assert "Persistent write failure" in str(exc_info.value)

def test_write_with_retry_invalid_path(spark, sample_stream_data):
    """Test write with retry with invalid path"""
    invalid_path = "/invalid/path/that/does/not/exist"
    
    with pytest.raises(Exception) as exc_info:
        write_with_retry(sample_stream_data, invalid_path)
    assert "No such file or directory" in str(exc_info.value)

def test_stream_processing_integration(spark, sample_stream_data, tmp_path):
    """Test end-to-end stream processing"""
    # Create a streaming DataFrame
    stream_df = spark.readStream.format("memory") \
        .option("query", "stream_data") \
        .load()
    
    # Write the sample data to the in-memory stream
    sample_stream_data.write.format("memory") \
        .option("query", "stream_data") \
        .mode("append") \
        .save()
    
    # Process the stream
    output_path = str(tmp_path / "stream_output")
    query = stream_df.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", str(tmp_path / "checkpoint")) \
        .start()
    
    # Wait for processing
    query.awaitTermination(timeout=10)
    
    # Verify results
    result_df = spark.read.parquet(output_path)
    assert result_df.count() == sample_stream_data.count()

def test_stream_processing_empty_stream(spark, tmp_path):
    """Test stream processing with empty stream"""
    # Create an empty streaming DataFrame
    empty_df = spark.createDataFrame([], sample_stream_data.schema)
    stream_df = spark.readStream.format("memory") \
        .option("query", "empty_stream") \
        .load()
    
    # Write empty data to the stream
    empty_df.write.format("memory") \
        .option("query", "empty_stream") \
        .mode("append") \
        .save()
    
    # Process the stream
    output_path = str(tmp_path / "stream_output")
    query = stream_df.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", str(tmp_path / "checkpoint")) \
        .start()
    
    # Wait for processing
    query.awaitTermination(timeout=10)
    
    # Verify no data was written
    assert not os.path.exists(output_path)

def test_stream_processing_large_data(spark, tmp_path):
    """Test stream processing with large dataset"""
    # Create a large dataset
    large_data = []
    for i in range(1000):
        large_data.append((
            f"SHIP{i:03d}",
            "2024-02-14 10:00:00",
            40.7128 + (i * 0.01),
            -74.0060 + (i * 0.01),
            75.5,
            150.0,
            "CLEAR",
            15.5,
            10.0,
            45.0,
            85.0
        ))
    
    large_df = spark.createDataFrame(large_data, sample_stream_data.schema)
    
    # Create streaming DataFrame
    stream_df = spark.readStream.format("memory") \
        .option("query", "large_stream") \
        .load()
    
    # Write large data to stream
    large_df.write.format("memory") \
        .option("query", "large_stream") \
        .mode("append") \
        .save()
    
    # Process the stream
    output_path = str(tmp_path / "stream_output")
    query = stream_df.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", str(tmp_path / "checkpoint")) \
        .start()
    
    # Wait for processing
    query.awaitTermination(timeout=10)
    
    # Verify results
    result_df = spark.read.parquet(output_path)
    assert result_df.count() == len(large_data) 