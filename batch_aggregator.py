# batch_aggregator.py

import duckdb
import pandas as pd
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# 1. Run DuckDB aggregation and fetch results as pandas DataFrame
conn = duckdb.connect(database="ship_telemetry.duckdb", read_only=False)

query = """
    SELECT 
      CAST(timestamp AS DATE) AS day,
      ship_id,
      ROUND(AVG(fuel_level), 2) AS avg_fuel_level,
      ROUND(AVG(speed), 2) AS avg_speed_knots,
      ROUND(AVG(distance_to_destination), 2) AS avg_distance_left_km,
      COUNT(*) AS total_records
    FROM ship_telemetry
    GROUP BY day, ship_id
    ORDER BY day, ship_id;
"""

print("🔍 Running DuckDB aggregation query...")
result_df = conn.execute(query).fetchdf()
conn.close()

print(f"✅ Retrieved {len(result_df)} rows from DuckDB")

# 2. Start Spark session to convert and write to Delta
builder = SparkSession.builder \
    .appName("WriteAggregatesToDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# 3. Convert Pandas to Spark
spark_df = spark.createDataFrame(result_df)

# 4. Write Delta file
delta_path = "file:///home/mickyans/ship-telemetry-project/delta/aggregated_ship_metrics"

spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(delta_path)

print("✅ Delta file written to:", delta_path)
