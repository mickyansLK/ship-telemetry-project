from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import duckdb

# 1. Initialize Spark session
spark = SparkSession.builder \
    .appName("ShipStreamProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

# 2. Define the schema of incoming JSON data
schema = StructType([
    StructField("ship_id", StringType(), True),
    StructField("timestamp", StringType(), True),  # We will parse timestamp later
    StructField("lat", FloatType(), True),
    StructField("lon", FloatType(), True),
    StructField("fuel_level", FloatType(), True),
    StructField("distance_to_destination", FloatType(), True),
    StructField("weather_condition", StringType(), True),
    StructField("speed", FloatType(), True),
    StructField("wind_speed", FloatType(), True)
])

# 3. Read from Kafka
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ship-telemetry") \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse the Kafka JSON values
raw_events = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

events_df = raw_events.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# 5. Add parsing for timestamp
from pyspark.sql.functions import to_timestamp

events_df = events_df.withColumn("timestamp", to_timestamp(col("timestamp")))

# 6. Define the process_batch function
def process_batch(batch_df, batch_id):
    print(f"\n=====> Processing micro-batch {batch_id} <=====")
    
    if batch_df.isEmpty():
        print(f"Batch {batch_id} is empty, skipping.")
        return

    try:
        pdf = batch_df.toPandas()
        print(f"Batch {batch_id} received {len(pdf)} rows.")
        
        # Connect to DuckDB
        conn = duckdb.connect(database="ship_telemetry.duckdb", read_only=False)

        # Create table if it doesn't exist
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ship_telemetry (
                ship_id VARCHAR,
                timestamp TIMESTAMP,
                lat DOUBLE,
                lon DOUBLE,
                fuel_level DOUBLE,
                distance_to_destination DOUBLE,
                weather_condition VARCHAR,
                speed DOUBLE,
                wind_speed DOUBLE
            )
        """)
        
        # Insert batch into DuckDB
        conn.register("batch_df", pdf)
        conn.execute("INSERT INTO ship_telemetry SELECT * FROM batch_df")
        conn.close()
        print(f"Batch {batch_id} written successfully to DuckDB!")

    except Exception as e:
        print(f"Error processing batch {batch_id}: {str(e)}")

# 7. Set up streaming query
query = events_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "./checkpoint_ship_telemetry") \
    .start()

# 8. Await termination
query.awaitTermination()
