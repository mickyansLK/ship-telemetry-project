import streamlit as st
import duckdb
import pandas as pd
import pydeck as pdk
from streamlit_autorefresh import st_autorefresh
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from config import DASHBOARD_CONFIG, LOGGING_CONFIG

# Configure logging
logging.basicConfig(**LOGGING_CONFIG)
logger = logging.getLogger(__name__)

# === Streamlit Config ===
st.set_page_config(
    page_title="🚢 Ship Telemetry Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state for caching
if 'last_cache_update' not in st.session_state:
    st.session_state.last_cache_update = datetime.min
if 'cached_data' not in st.session_state:
    st.session_state.cached_data = None

# === Helper Functions ===
@st.cache_data(ttl=DASHBOARD_CONFIG['cache_ttl'])
def load_telemetry_data() -> Optional[pd.DataFrame]:
    """Load and process telemetry data with caching."""
    try:
        con = duckdb.connect()
        query = f"""
            SELECT 
                *,
                DATE_TRUNC('hour', timestamp) as hour_bucket
            FROM parquet_scan('{DASHBOARD_CONFIG['parquet_path']}/**/*.parquet')
            WHERE timestamp::TIMESTAMP >= CURRENT_TIMESTAMP::TIMESTAMP - INTERVAL '24 hours'
        """
        df = con.execute(query).fetchdf()
        if df.empty:
            logger.warning("No telemetry data available")
            return None
        
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    except Exception as e:
        logger.error(f"Failed to load telemetry data: {str(e)}")
        return None

def calculate_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """Calculate derived metrics from telemetry data."""
    try:
        df["baseline_fuel_loss"] = df["ship_id"].map(
            lambda sid: DASHBOARD_CONFIG['baseline_consumption'].get(sid, 4.0)
        ) * (1 - (df["distance_to_destination"] / 4000))
        
        df["fuel_consumption_deviation"] = (
            df["baseline_fuel_loss"] - (100 - df["fuel_level"])
        ).round(2)
        
        df["CII_rating"] = df["fuel_consumption_deviation"].apply(
            lambda dev: "A" if dev < -5 else "B" if dev < 0 else "C" if dev < 5 else "D" if dev < 10 else "E"
        )
        
        return {
            "latest_data": df.sort_values("timestamp", ascending=False).head(10),
            "hourly_avg": df.groupby(["ship_id", "hour_bucket"]).agg({
                "fuel_level": "mean",
                "speed_knots": "mean",
                "distance_to_destination": "mean"
            }).reset_index(),
            "cii_summary": df.groupby("ship_id")["CII_rating"].agg(
                lambda x: x.mode()[0]
            ).reset_index(),
            "weather_impact": df.groupby("weather_condition")["fuel_level"].mean(),
            "critical_ships": df[df["fuel_level"] < 30]
        }
    except Exception as e:
        logger.error(f"Error calculating metrics: {str(e)}")
        return {}

def create_map_layers(df: pd.DataFrame) -> list:
    """Create map visualization layers."""
    try:
        arcs = []
        for _, row in df.iterrows():
            arcs.append({
                "from_lon": row["lon"], "from_lat": row["lat"],
                "to_lon": -1.4044, "to_lat": 50.9097,
                "ship_id": row["ship_id"], "weather": row["weather_condition"]
            })

        return [
            pdk.Layer(
                "ArcLayer",
                data=arcs,
                get_source_position=["from_lon", "from_lat"],
                get_target_position=["to_lon", "to_lat"],
                get_source_color=[0, 0, 255, 160],
                get_target_color=[255, 0, 0, 160],
                get_width=3,
                pickable=True
            ),
            pdk.Layer(
                "ScatterplotLayer",
                data=df[["ship_id", "lat", "lon", "weather_condition"]],
                get_position=["lon", "lat"],
                get_color="[200, 30, 0, 160]",
                get_radius=30000,
                pickable=True
            )
        ]
    except Exception as e:
        logger.error(f"Error creating map layers: {str(e)}")
        return []

# === Main Dashboard ===
st.title("🚢 Live Ship Telemetry Dashboard")
st.caption(f"Updated every {DASHBOARD_CONFIG['refresh_interval']} seconds")

# Auto-refresh
st_autorefresh(interval=DASHBOARD_CONFIG['refresh_interval'] * 1000, key="refresh_dashboard")

# === Ship Filter ===
st.sidebar.header("🛳️ Filter by Ship")
ship_filter = st.sidebar.selectbox(
    "Select ship",
    options=["All Ships", "bellisimo", "santa-maria", "titanic"]
)

# === Load and Process Data ===
df = load_telemetry_data()
if df is not None:
    if ship_filter != "All Ships":
        df = df[df["ship_id"] == ship_filter]
    
    metrics = calculate_metrics(df)
    
    # === Ship Telemetry Table ===
    st.subheader("📊 Telemetry Overview with Fuel Analysis")
    if not metrics["latest_data"].empty:
        def fuel_color(val):
            if val < 30: return "background-color: red; color: white"
            elif val < 50: return "background-color: orange"
            return "background-color: lightgreen"
        
        st.dataframe(
            metrics["latest_data"].style.applymap(fuel_color, subset=["fuel_level"]),
            use_container_width=True
        )
    
    # === Live Map ===
    st.subheader("🌍 Live Ship Map")
    if not df.empty:
        layers = create_map_layers(df)
        st.pydeck_chart(pdk.Deck(
            initial_view_state=pdk.ViewState(
                latitude=45,
                longitude=5,
                zoom=3.5,
                pitch=30
            ),
            layers=layers,
            tooltip={"html": "<b>{ship_id}</b><br/>Weather: {weather}"}
        ))
    
    # === Metrics Dashboard ===
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("⛽ Average Fuel by Ship")
        if not metrics["hourly_avg"].empty:
            st.bar_chart(metrics["hourly_avg"].pivot(
                index="hour_bucket",
                columns="ship_id",
                values="fuel_level"
            ))
    
    with col2:
        st.subheader("📍 Distance to Destination")
        if not metrics["hourly_avg"].empty:
            st.line_chart(metrics["hourly_avg"].pivot(
                index="hour_bucket",
                columns="ship_id",
                values="distance_to_destination"
            ))
    
    # === Alerts ===
    st.subheader("🚨 Fuel Alerts")
    if not metrics["critical_ships"].empty:
        st.error("⚠️ Low fuel detected!")
        st.dataframe(metrics["critical_ships"])
    else:
        st.success("✅ All ships have safe fuel levels.")
    
    # === CII Rating Summary ===
    st.subheader("🏅 CII Ratings")
    if not metrics["cii_summary"].empty:
        st.dataframe(metrics["cii_summary"])
    
    # === Weather Impact ===
    st.subheader("⛈️ Weather Impact on Fuel")
    if not metrics["weather_impact"].empty:
        st.bar_chart(metrics["weather_impact"])
else:
    st.warning("No telemetry data available. Please check the data source.")
