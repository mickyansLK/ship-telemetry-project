
import streamlit as st
import duckdb
import pandas as pd
import pydeck as pdk
from streamlit_autorefresh import st_autorefresh
import os

# === Streamlit Config ===
st.set_page_config(page_title="🚢 Ship Telemetry Dashboard", layout="wide")
st_autorefresh(interval=10 * 1000, key="refresh_dashboard")

st.title("🚢 Live Ship Telemetry Dashboard")
st.caption("Updated every 10 seconds")

# === Ship Filter ===
st.sidebar.header("🛳️ Filter by Ship")
ship_filter = st.sidebar.selectbox("Select ship", options=["All Ships", "bellisimo", "santa-maria", "titanic"])

# === Read Latest Parquet Data via DuckDB ===
parquet_path = "/workspaces/ship-telemetry-project/export/raw_ship_snapshot"

try:
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM parquet_scan('{parquet_path}/*.parquet')").fetchdf()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    if df.empty:
        st.warning("No ship telemetry available yet.")
except Exception as e:
    st.error(f"Failed to load data: {e}")
    df = pd.DataFrame()

# === Filter by Ship ===
if not df.empty and ship_filter != "All Ships":
    df = df[df["ship_id"] == ship_filter]

# === Derived Metrics ===
baseline_consumption_rate = {"bellisimo": 5.0, "santa-maria": 4.0, "titanic": 6.0}
df["baseline_fuel_loss"] = df["ship_id"].map(lambda sid: baseline_consumption_rate.get(sid, 4.0)) * (1 - (df["distance_to_destination"] / 4000))
df["fuel_consumption_deviation"] = (df["baseline_fuel_loss"] - (100 - df["fuel_level"])).round(2)

def calculate_cii(dev):
    if dev < -5: return "A"
    elif -5 <= dev < 0: return "B"
    elif 0 <= dev < 5: return "C"
    elif 5 <= dev < 10: return "D"
    else: return "E"

df["CII_rating"] = df["fuel_consumption_deviation"].apply(calculate_cii)

# === Ship Telemetry Table ===
st.subheader("📊 Telemetry Overview with Fuel Analysis")
if not df.empty:
    def fuel_color(val):
        if val < 30: return "background-color: red; color: white"
        elif val < 50: return "background-color: orange"
        return "background-color: lightgreen"
    
    st.dataframe(df.style.applymap(fuel_color, subset=["fuel_level"]), use_container_width=True)
else:
    st.info("No data to display.")

# === Live Map ===
st.subheader("🌍 Live Ship Map")
if not df.empty:
    arcs = []
    for _, row in df.iterrows():
        arcs.append({
            "from_lon": row["lon"], "from_lat": row["lat"],
            "to_lon": -1.4044, "to_lat": 50.9097,
            "ship_id": row["ship_id"], "weather": row["weather_condition"]
        })

    map_df = df[["ship_id", "lat", "lon", "weather_condition"]]
    arc_layer = pdk.Layer(
        "ArcLayer", data=arcs,
        get_source_position=["from_lon", "from_lat"],
        get_target_position=["to_lon", "to_lat"],
        get_source_color=[0, 0, 255, 160],
        get_target_color=[255, 0, 0, 160],
        get_width=3, pickable=True
    )
    point_layer = pdk.Layer(
        "ScatterplotLayer", data=map_df,
        get_position=["lon", "lat"],
        get_color="[200, 30, 0, 160]",
        get_radius=30000, pickable=True
    )
    st.pydeck_chart(pdk.Deck(
        initial_view_state=pdk.ViewState(latitude=45, longitude=5, zoom=3.5, pitch=30),
        layers=[arc_layer, point_layer],
        tooltip={"html": "<b>{ship_id}</b><br/>Weather: {weather}"}
    ))
else:
    st.info("Waiting for ship positions...")

# === Fuel Level Chart ===
st.subheader("⛽ Average Fuel by Ship")
if not df.empty:
    st.bar_chart(df.groupby("ship_id")["fuel_level"].mean())
else:
    st.info("No fuel data available.")

# === Distance Chart ===
st.subheader("📍 Avg Distance to Destination")
if not df.empty:
    st.bar_chart(df.groupby("ship_id")["distance_to_destination"].mean())
else:
    st.info("No distance data.")

# === Alerts ===
st.subheader("🚨 Fuel Alerts")
critical = df[df["fuel_level"] < 30]
if not critical.empty:
    st.error("⚠️ Low fuel detected!")
    st.dataframe(critical)
else:
    st.success("✅ All ships have safe fuel levels.")

# === CII Rating Summary ===
st.subheader("🏅 CII Ratings")
if not df.empty:
    cii_df = df.groupby("ship_id")["CII_rating"].agg(lambda x: x.mode()[0]).reset_index()
    st.dataframe(cii_df)
else:
    st.info("No CII data.")

# === Weather Impact ===
st.subheader("⛈️ Weather Impact on Fuel")
if not df.empty:
    st.bar_chart(df.groupby("weather_condition")["fuel_level"].mean())
else:
    st.info("Not enough data yet.")
