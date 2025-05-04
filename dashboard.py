import streamlit as st
import duckdb
import pandas as pd
import pydeck as pdk
from streamlit_autorefresh import st_autorefresh

# Set page config
st.set_page_config(page_title="🚢 Ship Telemetry Dashboard", layout="wide")

# 🚀 Auto-refresh every 10 seconds
st_autorefresh(interval=10 * 1000, limit=None, key="shipdashboardrefresh")

# Title
st.title("🚢 Live Ship Telemetry Monitoring")

# Sidebar for ship selection
st.sidebar.title("🛳️ Select Ship")
selected_ship = st.sidebar.selectbox(
    "Choose a ship to monitor:",
    ("All Ships", "bellisimo", "santa-maria", "titanic")
)

import pyarrow.parquet as pq
import os

# 📦 Path where Spark writes the Parquet snapshot
# Read raw telemetry from Spark output (Parquet)
raw_data_path = "/home/mickyans/ship-telemetry-project/export/raw_ship_snapshot"

try:
    files = [os.path.join(raw_data_path, f) for f in os.listdir(raw_data_path) if f.endswith(".parquet")]
    if not files:
        st.warning("⚠️ No raw telemetry data found yet.")
        df = pd.DataFrame()
    else:
        table = pq.read_table(files)
        df = table.to_pandas()
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        st.success("✅ Raw telemetry data loaded")
except Exception as e:
    st.error(f"❌ Failed to load raw data: {e}")
    df = pd.DataFrame()


# Apply ship filter
if selected_ship != "All Ships":
    df = df[df['ship_id'] == selected_ship]

# 🚢 Add baseline fuel consumption assumptions
baseline_consumption_rate = {
    "bellisimo": 5.0,    # 5% baseline
    "santa-maria": 4.0,
    "titanic": 6.0
}

# Calculate baseline expected fuel loss
df["baseline_fuel_loss"] = df["ship_id"].map(baseline_consumption_rate) * (1 - (df["distance_to_destination"] / 4000))
df["fuel_consumption_deviation"] = (df["baseline_fuel_loss"] - (100 - df["fuel_level"])).round(2)

# 🚢 Simulate CII Classification
def calculate_cii(deviation):
    if deviation < -5:
        return "A"  # Very efficient
    elif -5 <= deviation < 0:
        return "B"  # Slightly efficient
    elif 0 <= deviation < 5:
        return "C"  # Normal
    elif 5 <= deviation < 10:
        return "D"  # Slightly inefficient
    else:
        return "E"  # Very inefficient

df["CII_rating"] = df["fuel_consumption_deviation"].apply(calculate_cii)

# 🛠️ Color coding fuel levels
def fuel_level_color(val):
    if val < 30:
        return 'background-color: red; color: white'
    elif val < 50:
        return 'background-color: orange;'
    else:
        return 'background-color: lightgreen;'

# 🚢 Latest telemetry table
st.subheader("🚢 Latest Ship Telemetry (Fuel + CII Analysis)")
if not df.empty:
    styled_df = df.style.applymap(fuel_level_color, subset=['fuel_level'])
    st.dataframe(styled_df, use_container_width=True)
else:
    st.warning("No ship telemetry data available yet.")

# 🌍 Live Ship Map
st.subheader("🌍 Live Ship Locations & Destinations")
if not df.empty:
    map_df = df[['ship_id', 'lat', 'lon', 'weather_condition']]
    destination = {"lat": 50.9097, "lon": -1.4044}  # Southampton

    arcs_data = []
    for _, row in map_df.iterrows():
        arcs_data.append({
            "ship_id": row["ship_id"],
            "from_lat": row["lat"],
            "from_lon": row["lon"],
            "to_lat": destination["lat"],
            "to_lon": destination["lon"],
            "weather_condition": row["weather_condition"]
        })

    arcs_df = pd.DataFrame(arcs_data)

    arc_layer = pdk.Layer(
        "ArcLayer",
        data=arcs_df,
        get_source_position=["from_lon", "from_lat"],
        get_target_position=["to_lon", "to_lat"],
        get_width=3,
        get_tilt=15,
        get_source_color=[0, 0, 255, 160],
        get_target_color=[255, 0, 0, 160],
        pickable=True,
        auto_highlight=True,
    )

    point_layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_df,
        get_position=["lon", "lat"],
        get_color="[200, 30, 0, 160]",
        get_radius=50000,
        pickable=True,
    )

    tooltip = {
        "html": "<b>Ship:</b> {ship_id} <br/><b>Weather:</b> {weather_condition}",
        "style": {"backgroundColor": "steelblue", "color": "white"}
    }

    view_state = pdk.ViewState(
        latitude=45,
        longitude=5,
        zoom=3.5,
        pitch=30
    )

    st.pydeck_chart(pdk.Deck(
        map_style="mapbox://styles/mapbox/light-v9",
        initial_view_state=view_state,
        layers=[arc_layer, point_layer],
        tooltip=tooltip
    ))
else:
    st.warning("🚢 No ship telemetry to show on map yet.")

# 📈 Average Fuel per Ship
st.subheader("📈 Average Fuel Level per Ship")
if not df.empty:
    fuel_chart = df.groupby('ship_id')['fuel_level'].mean().reset_index()
    st.bar_chart(fuel_chart.set_index('ship_id'))
else:
    st.warning("No fuel level data available.")

# 🛣️ Distance to Destination Chart
st.subheader("🛣️ Average Distance to Destination per Ship")
if not df.empty:
    distance_chart = df.groupby('ship_id')['distance_to_destination'].mean().reset_index()
    st.bar_chart(distance_chart.set_index('ship_id'))
else:
    st.warning("No distance data available.")

# 🚨 Critical Fuel Alerts
st.subheader("🚨 Critical Fuel Alerts")
critical_fuel_df = df[df['fuel_level'] < 30]
if not critical_fuel_df.empty:
    st.error("⚠️ Some ships have dangerously low fuel levels!")
    st.dataframe(critical_fuel_df)
else:
    st.success("✅ All ships have safe fuel levels.")

# 🏅 CII Classification Summary
st.subheader("🏅 CII Classification of Ships")
if not df.empty:
    cii_summary = df.groupby('ship_id')['CII_rating'].agg(lambda x: x.mode()[0]).reset_index()
    st.dataframe(cii_summary)
else:
    st.warning("No data to classify ships.")

# ⛈️ Weather Impact on Fuel Consumption
st.subheader("⛈️ Weather Impact on Fuel Consumption")
if not df.empty:
    weather_fuel_df = df.groupby('weather_condition')['fuel_level'].mean().reset_index()
    st.bar_chart(weather_fuel_df.set_index('weather_condition'))
    st.info("💡 Lower average fuel levels during storms may indicate increased consumption due to rough seas.")
else:
    st.warning("No data yet to analyze weather impact.")

# 🎥 Playback Mode
st.subheader("🎥 Ship Movement Playback")
if not df.empty:
    time_window = st.slider("Select minutes to look back", 5, 60, 15)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    latest_time = df["timestamp"].max()
    filtered_df = df[df["timestamp"] >= (latest_time - pd.Timedelta(minutes=time_window))]

    if not filtered_df.empty:
        playback_map = pdk.Layer(
            "ScatterplotLayer",
            data=filtered_df,
            get_position=["lon", "lat"],
            get_color="[0, 100, 200, 160]",
            get_radius=30000,
            pickable=True,
        )

        playback_view = pdk.ViewState(
            latitude=45,
            longitude=5,
            zoom=3.5,
            pitch=30
        )

        st.pydeck_chart(pdk.Deck(
            map_style="mapbox://styles/mapbox/light-v9",
            initial_view_state=playback_view,
            layers=[playback_map]
        ))
        st.success(f"Showing ship movements in the past {time_window} minutes")
    else:
        st.warning("No telemetry records found in selected time window.")
else:
    st.warning("No ship telemetry available for playback.")
