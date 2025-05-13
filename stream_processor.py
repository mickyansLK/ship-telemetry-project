from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# 1. Init Spark
spark = SparkSession.builder \
    .appName("ShipTelemetryStreamEnhanced") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

# 2. Define schema (includes all enhanced fields)
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

# 3. Kafka source
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ship-telemetry") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# 4. Parse JSON
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# 5. File output base
BASE_PATH = "/workspaces/ship-telemetry-project/export"
CHECKPOINT_BASE = "/workspaces/ship-telemetry-project/checkpoints"

# 6. Raw write (append mode)
raw_query = parsed_df.writeStream \
    .format("parquet") \
    .option("path", f"{BASE_PATH}/raw_ship_snapshot") \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/raw") \
    .outputMode("append") \
    .start()

# 7. Live snapshot every 30s (overwrite)
def write_snapshot(batch_df, batch_id):
    print(f"📦 Writing snapshot batch {batch_id} - {batch_df.count()} records")
    batch_df.write.mode("overwrite").parquet(f"{BASE_PATH}/ship_metrics_snapshot")

snapshot_query = parsed_df.writeStream \
    .foreachBatch(write_snapshot) \
    .outputMode("append") \
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/snapshot") \
    .trigger(processingTime="30 seconds") \
    .start()

# 8. Await termination
spark.streams.awaitAnyTermination()


