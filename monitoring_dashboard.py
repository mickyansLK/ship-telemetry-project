import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import numpy as np

# Load environment variables
load_dotenv()

# Configure page
st.set_page_config(
    page_title="Ship Telemetry Monitoring",
    page_icon="🚢",
    layout="wide"
)

# Initialize DuckDB connection
def get_db_connection():
    return duckdb.connect(database=os.getenv("DUCKDB_PATH", "ship_telemetry.db"))

# Data quality metrics
def get_data_quality_metrics(conn):
    metrics = {
        "completeness": conn.execute("""
            SELECT
                COUNT(*) as total_records,
                COUNT(CASE WHEN ship_id IS NOT NULL THEN 1 END) as valid_ship_ids,
                COUNT(CASE WHEN timestamp IS NOT NULL THEN 1 END) as valid_timestamps,
                COUNT(CASE WHEN lat IS NOT NULL AND lon IS NOT NULL THEN 1 END) as valid_coordinates,
                COUNT(CASE WHEN fuel_level IS NOT NULL THEN 1 END) as valid_fuel_levels
            FROM ship_telemetry
        """).fetchdf(),

        "validity": conn.execute("""
            SELECT
                COUNT(*) as total_records,
                COUNT(CASE WHEN lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180 THEN 1 END) as valid_coordinates,
                COUNT(CASE WHEN fuel_level BETWEEN 0 AND 100 THEN 1 END) as valid_fuel_levels,
                COUNT(CASE WHEN speed_knots BETWEEN 0 AND 50 THEN 1 END) as valid_speeds,
                COUNT(CASE WHEN engine_temp BETWEEN 0 AND 200 THEN 1 END) as valid_temps
            FROM ship_telemetry
        """).fetchdf(),

        "timeliness": conn.execute("""
            SELECT
                MAX(timestamp) as latest_record,
                MIN(timestamp) as earliest_record,
                COUNT(*) as records_last_hour,
                AVG(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - timestamp))) as avg_latency_seconds
            FROM ship_telemetry
            WHERE timestamp >= (CURRENT_TIMESTAMP - INTERVAL '1 hour')
        """).fetchdf()
    }
    return metrics

# Performance metrics
def get_performance_metrics(conn):
    return conn.execute("""
        SELECT
            ship_id,
            AVG(speed_knots) as avg_speed,
            AVG(fuel_level) as avg_fuel_level,
            AVG(engine_temp) as avg_engine_temp,
            COUNT(*) as record_count,
            MAX(speed_knots) as max_speed,
            MIN(fuel_level) as min_fuel_level,
            MAX(engine_temp) as max_engine_temp
        FROM ship_telemetry
        WHERE timestamp >= (CURRENT_TIMESTAMP - INTERVAL '24 hours')
        GROUP BY ship_id
    """).fetchdf()

# Anomaly detection
def detect_anomalies(conn):
    return conn.execute("""
        WITH stats AS (
            SELECT
                AVG(speed_knots) as avg_speed,
                STDDEV(speed_knots) as std_speed,
                AVG(engine_temp) as avg_temp,
                STDDEV(engine_temp) as std_temp,
                AVG(fuel_level) as avg_fuel,
                STDDEV(fuel_level) as std_fuel
            FROM ship_telemetry
            WHERE timestamp >= (CURRENT_TIMESTAMP - INTERVAL '24 hours')
        )
        SELECT
            ship_id,
            timestamp,
            speed_knots,
            engine_temp,
            fuel_level,
            CASE
                WHEN ABS(speed_knots - stats.avg_speed) > 2 * stats.std_speed THEN 'Speed Anomaly'
                WHEN ABS(engine_temp - stats.avg_temp) > 2 * stats.std_temp THEN 'Temperature Anomaly'
                WHEN ABS(fuel_level - stats.avg_fuel) > 2 * stats.std_fuel THEN 'Fuel Level Anomaly'
                ELSE 'Normal'
            END as anomaly_type
        FROM ship_telemetry, stats
        WHERE timestamp >= (CURRENT_TIMESTAMP - INTERVAL '24 hours')
        AND (
            ABS(speed_knots - stats.avg_speed) > 2 * stats.std_speed OR
            ABS(engine_temp - stats.avg_temp) > 2 * stats.std_temp OR
            ABS(fuel_level - stats.avg_fuel) > 2 * stats.std_fuel
        )
        ORDER BY timestamp DESC
    """).fetchdf()

# Ship route visualization
def get_ship_routes(conn):
    return conn.execute("""
        SELECT
            ship_id,
            lat,
            lon,
            timestamp,
            speed_knots,
            heading
        FROM ship_telemetry
        WHERE timestamp >= (CURRENT_TIMESTAMP - INTERVAL '24 hours')
        ORDER BY ship_id, timestamp
    """).fetchdf()

# Main dashboard
def main():
    st.title("🚢 Ship Telemetry Monitoring Dashboard")
    
    # Sidebar filters
    st.sidebar.header("Filters")
    time_range = st.sidebar.selectbox(
        "Time Range",
        ["Last Hour", "Last 24 Hours", "Last 7 Days", "Last 30 Days"]
    )
    
    ship_filter = st.sidebar.multiselect(
        "Select Ships",
        ["All"] + [f"SHIP{i:03d}" for i in range(1, 11)]
    )
    
    # Get data
    conn = get_db_connection()
    quality_metrics = get_data_quality_metrics(conn)
    performance_metrics = get_performance_metrics(conn)
    anomalies = detect_anomalies(conn)
    ship_routes = get_ship_routes(conn)
    
    # Data Quality Section
    st.header("Data Quality Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Data Completeness",
            f"{quality_metrics['completeness']['valid_ship_ids'].iloc[0] / quality_metrics['completeness']['total_records'].iloc[0] * 100:.1f}%"
        )
    
    with col2:
        st.metric(
            "Data Validity",
            f"{quality_metrics['validity']['valid_coordinates'].iloc[0] / quality_metrics['validity']['total_records'].iloc[0] * 100:.1f}%"
        )
    
    with col3:
        st.metric(
            "Records Last Hour",
            f"{quality_metrics['timeliness']['records_last_hour'].iloc[0]}"
        )
    
    with col4:
        st.metric(
            "Avg. Latency",
            f"{quality_metrics['timeliness']['avg_latency_seconds'].iloc[0]:.1f}s"
        )
    
    # Performance Metrics
    st.header("Performance Metrics")
    
    # Ship Performance Chart
    fig = px.bar(
        performance_metrics,
        x="ship_id",
        y=["avg_speed", "avg_fuel_level", "avg_engine_temp"],
        title="Ship Performance Metrics",
        barmode="group"
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Performance Trends
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.line(
            performance_metrics,
            x="ship_id",
            y=["max_speed", "avg_speed"],
            title="Speed Metrics by Ship"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.line(
            performance_metrics,
            x="ship_id",
            y=["max_engine_temp", "avg_engine_temp"],
            title="Engine Temperature Metrics by Ship"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Data Quality Trends
    st.header("Data Quality Trends")
    
    # Create time series data for quality metrics
    quality_trends = conn.execute("""
        SELECT 
            date_trunc('hour', timestamp) as hour,
            COUNT(*) as total_records,
            COUNT(CASE WHEN lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180 THEN 1 END) as valid_coordinates,
            COUNT(CASE WHEN fuel_level BETWEEN 0 AND 100 THEN 1 END) as valid_fuel_levels,
            AVG(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - timestamp))) as avg_latency
        FROM ship_telemetry
        WHERE timestamp >= (CURRENT_TIMESTAMP - INTERVAL '24 hours')
        GROUP BY hour
        ORDER BY hour
    """).fetchdf()
    
    fig = px.line(
        quality_trends,
        x="hour",
        y=["valid_coordinates", "valid_fuel_levels"],
        title="Data Quality Trends Over Time"
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Ship Routes
    st.header("Ship Routes")
    
    if not ship_routes.empty:
        fig = px.scatter_geo(
            ship_routes,
            lat="lat",
            lon="lon",
            color="ship_id",
            hover_name="ship_id",
            hover_data=["speed_knots", "heading", "timestamp"],
            title="Ship Routes (Last 24 Hours)"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Anomalies Section
    st.header("Anomalies")
    
    if not anomalies.empty:
        st.warning(f"⚠️ {len(anomalies)} Anomalies Detected")
        
        # Anomaly distribution
        fig = px.pie(
            anomalies,
            names="anomaly_type",
            title="Anomaly Distribution"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Anomaly details
        st.dataframe(anomalies)
    else:
        st.success("✅ No anomalies detected in the last 24 hours")
    
    # System Health
    st.header("System Health")
    
    # Calculate system metrics
    system_metrics = {
        "Total Records": quality_metrics['completeness']['total_records'].iloc[0],
        "Active Ships": len(performance_metrics['ship_id'].unique()),
        "Anomaly Rate": len(anomalies) / quality_metrics['completeness']['total_records'].iloc[0] * 100 if quality_metrics['completeness']['total_records'].iloc[0] > 0 else 0,
        "Data Freshness": quality_metrics['timeliness']['avg_latency_seconds'].iloc[0]
    }
    
    # Display system health metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Records", f"{system_metrics['Total Records']:,}")
    
    with col2:
        st.metric("Active Ships", system_metrics['Active Ships'])
    
    with col3:
        st.metric("Anomaly Rate", f"{system_metrics['Anomaly Rate']:.1f}%")
    
    with col4:
        st.metric("Data Freshness", f"{system_metrics['Data Freshness']:.1f}s")

if __name__ == "__main__":
    main() 