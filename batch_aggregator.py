import duckdb
import pandas as pd

# 1. Connect to DuckDB
conn = duckdb.connect(database="ship_telemetry.duckdb", read_only=False)

# 2. Aggregation query adapted for ships
query = """
    SELECT 
      timestamp AS day,
      ship_id,
      ROUND(AVG(fuel_level), 2) AS avg_fuel_level,
      ROUND(AVG(speed), 2) AS avg_speed_knots,
      ROUND(AVG(distance_to_destination), 2) AS avg_distance_left_km,
      COUNT(*) AS total_records
    FROM ship_telemetry
    GROUP BY day, ship_id
    ORDER BY day, ship_id;
"""

# 3. Execute and fetch as pandas DataFrame
result_df = conn.execute(query).fetchdf()

# 4. Close DuckDB connection
conn.close()

# 5. Save aggregated result to CSV
result_df.to_csv("aggregated_ship_metrics.csv", index=False)

# 6. Print first few rows for quick check
print("Aggregated ship metrics saved to aggregated_ship_metrics.csv")
print(result_df.head())
