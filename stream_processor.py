from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# 1. Initialize Spark session
spark = SparkSession.builder \
    .appName("ShipTelemetryStreamFull") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

# 2. Define schema
schema = StructType([
    StructField("ship_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("lat", FloatType(), True),
    StructField("lon", FloatType(), True),
    StructField("fuel_level", FloatType(), True),
    StructField("distance_to_destination", FloatType(), True),
    StructField("weather_condition", StringType(), True),
    StructField("speed", FloatType(), True),
    StructField("wind_speed", FloatType(), True)
])

# 3. Read Kafka stream
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ship-telemetry") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# 4. Parse Kafka JSON values
events_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# 5. Write raw stream to append-only directory
raw_query = events_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/home/mickyans/ship-telemetry-project/checkpoint_raw") \
    .option("path", "/home/mickyans/ship-telemetry-project/export/raw_ship_snapshot") \
    .outputMode("append") \
    .start()

# 6. Write live snapshot every 30s (overwrite mode)
def write_snapshot(batch_df, batch_id):
    print(f"📦 Writing snapshot batch {batch_id}")
    snapshot_path = "/home/mickyans/ship-telemetry-project/export/ship_metrics_snapshot"
    batch_df.write.mode("overwrite").parquet(snapshot_path)
    print("✅ Snapshot updated")

snapshot_query = events_df.writeStream \
    .foreachBatch(write_snapshot) \
    .outputMode("append") \
    .option("checkpointLocation", "/home/mickyans/ship-telemetry-project/checkpoint_snapshot") \
    .trigger(processingTime="30 seconds") \
    .start()

# 7. Wait for both queries
spark.streams.awaitAnyTermination()

