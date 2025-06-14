# batch_aggregator.py

import duckdb
import pandas as pd
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os
from contextlib import contextmanager
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "ship_telemetry.duckdb")
DELTA_PATH = os.getenv("DELTA_PATH", "file:///home/mickyans/ship-telemetry-project/delta/aggregated_ship_metrics")
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "90"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10000"))

class DataQualityCheck:
    def __init__(self, name: str, query: str, threshold: float):
        self.name = name
        self.query = query
        self.threshold = threshold

    def run(self, conn: duckdb.DuckDBPyConnection) -> bool:
        result = conn.execute(self.query).fetchone()[0]
        return result >= self.threshold

@contextmanager
def get_duckdb_connection():
    """Context manager for DuckDB connection"""
    conn = None
    try:
        conn = duckdb.connect(database=DUCKDB_PATH, read_only=False)
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

def get_last_processed_date(conn: duckdb.DuckDBPyConnection) -> Optional[datetime]:
    """Get the last processed date from the metadata table"""
    try:
        result = conn.execute("""
            SELECT MAX(processing_date) 
            FROM metadata_processing 
            WHERE status = 'success'
        """).fetchone()[0]
        return result
    except Exception:
        return None

def update_processing_metadata(conn: duckdb.DuckdbPyConnection, date: datetime, status: str):
    """Update processing metadata"""
    conn.execute("""
        INSERT INTO metadata_processing (processing_date, status, processed_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
    """, [date, status])

def run_data_quality_checks(conn: duckdb.DuckDBPyConnection) -> Dict[str, bool]:
    """Run data quality checks"""
    checks = [
        DataQualityCheck(
            "completeness",
            "SELECT COUNT(*) * 1.0 / (SELECT COUNT(*) FROM ship_telemetry) FROM ship_telemetry WHERE ship_id IS NOT NULL",
            0.95
        ),
        DataQualityCheck(
            "valid_coordinates",
            "SELECT COUNT(*) * 1.0 / (SELECT COUNT(*) FROM ship_telemetry) FROM ship_telemetry WHERE lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180",
            0.99
        ),
        DataQualityCheck(
            "valid_fuel",
            "SELECT COUNT(*) * 1.0 / (SELECT COUNT(*) FROM ship_telemetry) FROM ship_telemetry WHERE fuel_level BETWEEN 0 AND 100",
            0.99
        )
    ]
    
    results = {}
    for check in checks:
        results[check.name] = check.run(conn)
        logger.info(f"Data quality check '{check.name}': {'PASS' if results[check.name] else 'FAIL'}")
    
    return results

def run_aggregation(start_date: Optional[datetime] = None) -> pd.DataFrame:
    """Run the aggregation query and return results"""
    query = """
        WITH filtered_data AS (
            SELECT *
            FROM ship_telemetry
            WHERE 1=1
            {date_filter}
        )
        SELECT 
            CAST(timestamp AS DATE) AS day,
            ship_id,
            ROUND(AVG(fuel_level), 2) AS avg_fuel_level,
            ROUND(AVG(speed_knots), 2) AS avg_speed_knots,
            ROUND(AVG(distance_to_destination), 2) AS avg_distance_left_km,
            COUNT(*) AS total_records,
            CURRENT_TIMESTAMP AS processing_timestamp
        FROM filtered_data
        GROUP BY day, ship_id
        ORDER BY day, ship_id;
    """
    
    date_filter = f"AND timestamp >= '{start_date}'" if start_date else ""
    query = query.format(date_filter=date_filter)
    
    try:
        with get_duckdb_connection() as conn:
            logger.info("🔍 Running DuckDB aggregation query...")
            result_df = conn.execute(query).fetchdf()
            logger.info(f"✅ Retrieved {len(result_df)} rows from DuckDB")
            return result_df
    except Exception as e:
        logger.error(f"Error during aggregation: {str(e)}")
        raise

def cleanup_old_data(conn: duckdb.DuckDBPyConnection):
    """Clean up data older than retention period"""
    cutoff_date = datetime.now() - timedelta(days=RETENTION_DAYS)
    try:
        conn.execute("""
            DELETE FROM ship_telemetry 
            WHERE timestamp < ?
        """, [cutoff_date])
        logger.info(f"🧹 Cleaned up data older than {cutoff_date}")
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")
        raise

def write_to_delta(spark_df, mode: str = "merge"):
    """Write aggregated data to Delta table with optimizations"""
    try:
        # Configure Spark session with Delta optimizations
        builder = SparkSession.builder \
            .appName("WriteAggregatesToDelta") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "10") \
            .config("spark.delta.optimizeWrite.enabled", "true") \
            .config("spark.delta.autoCompact.enabled", "true")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        # Write to Delta with optimizations
        spark_df.write \
            .format("delta") \
            .mode(mode) \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true") \
            .partitionBy("day") \
            .save(DELTA_PATH)

        logger.info(f"✅ Delta file written to: {DELTA_PATH}")
    except Exception as e:
        logger.error(f"Error writing to Delta: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()

def main():
    start_time = time.time()
    try:
        with get_duckdb_connection() as conn:
            # Initialize metadata table if not exists
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata_processing (
                    processing_date DATE,
                    status VARCHAR(10),
                    processed_at TIMESTAMP
                )
            """)
            
            # Get last processed date
            last_date = get_last_processed_date(conn)
            
            # Run data quality checks
            quality_results = run_data_quality_checks(conn)
            if not all(quality_results.values()):
                logger.warning("⚠️ Some data quality checks failed")
            
            # Run aggregation
            result_df = run_aggregation(last_date)
            
            if len(result_df) > 0:
                # Convert to Spark DataFrame
                builder = SparkSession.builder \
                    .appName("ConvertToSpark") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                
                spark = configure_spark_with_delta_pip(builder).getOrCreate()
                spark_df = spark.createDataFrame(result_df)
                
                # Write to Delta
                write_to_delta(spark_df)
                
                # Update metadata
                update_processing_metadata(conn, datetime.now().date(), "success")
                
                # Cleanup old data
                cleanup_old_data(conn)
            else:
                logger.info("No new data to process")
        
        processing_time = time.time() - start_time
        logger.info(f"✅ Batch processing completed in {processing_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Batch processing failed: {str(e)}")
        if 'conn' in locals():
            update_processing_metadata(conn, datetime.now().date(), "failed")
        raise

if __name__ == "__main__":
    main()
