# 🚢 Ship Telemetry Project
🚢 Embark on a Journey with Our Real-Time Ship Telemetry Monitoring System

## 🚀 Live App

[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://ship-telemetry-project-xyokwb3tg3fbnkirhjj4qr.streamlit.app/)



This project not only simulates and processes but also visualises 'real-time telemetry data' from ships in transit from various locations (e.g., Athens, Rome, Cappadocia) to Southampton, providing immediate and relevant insights.

It demonstrates **streaming IoT analytics**, using:
- **Kafka** for real-time telemetry ingestion
- **Duckdb** for efficient analytics storage
- **PySpark** for streaming ETL
- **Streamlit** for live dashboard visualisation
- **Python** for ship movement simulation

---

## 📦 Tech Stack

| Technology | Purpose |
|------------|---------|
| Kafka | Ingest real-time ship telemetry events |
| Duckdb | Lightweight database for storing ship telemetry |
| PySpark Structured Streaming | Process Kafka streams into DuckDB |
| Streamlit | Real-time dashboard visualization |
| Python | Ship movement, fuel simulation |

---

## 🚀 Project Structure

```bash
├── simulate_ships_fast.py       # Ship telemetry event simulator
├── stream_processor.py          # Spark Structured Streaming ETL
├── dashboard.py                  # Streamlit dashboard app
├── batch_aggregator.py           # Batch processing for summaries
├── requirements.txt              # Python dependencies
├── README.md                     # Project documentation
└── ship_telemetry.duckdb         # Duckdb database (auto-created)


## ✅ Dashboard Features

- 🔄 Auto-refresh every 10s
- 🌍 Live map of ship locations and weather
- 🧮 Fuel deviation + CII efficiency grading
- 🛟 Critical alerts for low fuel
- ⏪ Time playback slider

---


![Ship Telemetry Architecture](<Ship Telemetry.png>)