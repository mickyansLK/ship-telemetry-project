from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, current_timestamp, window,
    expr, last, avg, count
)
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import logging
import time
from config import STREAM_CONFIG, LOGGING_CONFIG

# Configure logging
logging.basicConfig(**LOGGING_CONFIG)
logger = logging.getLogger(__name__)

# Initialize Spark
spark = SparkSession.builder \
    .appName("ShipTelemetryStreamEnhanced") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

# Define schema
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

def validate_data(df):
    """Filter out invalid telemetry values"""
    return df.filter(
        (col("ship_id").isNotNull()) &
        (col("timestamp").isNotNull()) &
        (col("lat").between(-90, 90)) &
        (col("lon").between(-180, 180)) &
        (col("fuel_level").between(0, 100)) &
        (col("speed_knots").between(0, 30)) &
        (col("engine_temp").between(0, 150))
    )

def write_with_retry(df_or_writer, path, mode="append", retry_count=0):
    try:
        if hasattr(df_or_writer, 'write'):
            df_or_writer.write.mode(mode).parquet(path)
        else:
            df_or_writer.mode(mode).parquet(path)
        logger.info(f"✅ Successfully wrote data to {path}")
    except Exception as e:
        if retry_count < STREAM_CONFIG['retry']['max_retries']:
            logger.warning(f"⚠️ Write failed, retrying ({retry_count + 1}/{STREAM_CONFIG['retry']['max_retries']}): {str(e)}")
            time.sleep(STREAM_CONFIG['retry']['retry_delay'])
            write_with_retry(df_or_writer, path, mode, retry_count + 1)
        else:
            logger.error(f"❌ Write failed after {STREAM_CONFIG['retry']['max_retries']} retries: {str(e)}")
            raise

def process_stream():
    try:
        # Ingest from Kafka
        kafka_df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", STREAM_CONFIG['kafka']['bootstrap_servers']) \
            .option("subscribe", STREAM_CONFIG['kafka']['topic']) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

        # Parse and validate messages
        parsed_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_str") \
            .filter("json_str IS NOT NULL AND json_str != ''") \
            .select(from_json(col("json_str"), schema).alias("data")) \
            .filter("data IS NOT NULL") \
            .select("data.*") \
            .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withWatermark("timestamp", "1 minute")

        validated_df = validate_data(parsed_df)

        # Raw sink
        def write_raw_batch(batch_df, batch_id):
            try:
                if batch_df.count() > 0:
                    batch_df = batch_df \
                        .withColumn("year", expr("year(timestamp)")) \
                        .withColumn("month", expr("month(timestamp)")) \
                        .withColumn("day", expr("day(timestamp)"))

                    batch_df.write \
                        .mode("append") \
                        .partitionBy("year", "month", "day", "ship_id") \
                        .parquet(f"{STREAM_CONFIG['storage']['base_path']}/ship_telemetry")
                    logger.info(f"✅ Raw batch {batch_id} written to ship_telemetry")
            except Exception as e:
                logger.error(f"❌ Error writing raw batch {batch_id}: {e}")
                raise

        raw_query = validated_df.writeStream \
            .outputMode("append") \
            .option("checkpointLocation", f"{STREAM_CONFIG['storage']['checkpoint_base']}/raw") \
            .foreachBatch(write_raw_batch) \
            .start()

        # Aggregated metrics sink
        windowed_df = validated_df \
            .groupBy(
                window(col("timestamp"), "5 minutes", "1 minute"),
                col("ship_id")
            ).agg(
                last("lat").alias("lat"),
                last("lon").alias("lon"),
                avg("fuel_level").alias("avg_fuel_level"),
                avg("speed_knots").alias("avg_speed"),
                avg("engine_temp").alias("avg_engine_temp"),
                last("weather_condition").alias("weather_condition"),
                count("*").alias("record_count")
            ).select(
                col("ship_id"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "lat", "lon",
                "avg_fuel_level", "avg_speed", "avg_engine_temp",
                "weather_condition", "record_count"
            )

        def write_snapshot_batch(batch_df, batch_id):
            try:
                if batch_df.count() > 0:
                    batch_df.write \
                        .mode("append") \
                        .parquet(f"{STREAM_CONFIG['storage']['base_path']}/ship_metrics")
                    logger.info(f"✅ Aggregated batch {batch_id} written to ship_metrics")
            except Exception as e:
                logger.error(f"❌ Error writing snapshot batch {batch_id}: {e}")
                raise

        snapshot_query = windowed_df.writeStream \
            .outputMode("append") \
            .option("checkpointLocation", f"{STREAM_CONFIG['storage']['checkpoint_base']}/snapshot") \
            .foreachBatch(write_snapshot_batch) \
            .start()

        logger.info("✅ Stream processing started successfully")
        logger.info(f"📊 Raw data path: {STREAM_CONFIG['storage']['base_path']}/ship_telemetry")
        logger.info(f"📊 Aggregated data path: {STREAM_CONFIG['storage']['base_path']}/ship_metrics")

        raw_query.awaitTermination()
        snapshot_query.awaitTermination()

    except Exception as e:
        logger.error(f"❌ Stream processing error: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    process_stream()
