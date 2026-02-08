# 2025 NYC Congestion Pricing Audit

Complete Big Data Pipeline for analyzing the impact of Manhattan's Congestion Relief Zone Toll.

## 📋 System Requirements

- **Python**: 3.8 or higher
- **RAM**: Minimum 2GB (pipeline enforces 1GB DuckDB limit for safety)
- **Disk Space**: 15GB (for downloaded parquet files)
- **Internet**: Required for data download and weather API

## 🚀 Quick Start Guide

### Step 1: Install Dependencies

```bash
pip install -r requirements.txt
```

This will install:
- `duckdb` - Big data engine (memory-safe)
- `pandas` - Data manipulation
- `requests` - HTTP requests for data download
- `streamlit` - Interactive dashboard
- `plotly` - Professional visualizations
- `numpy` - Numerical operations
- `statsmodels` - Statistical analysis

### Step 2: Run the Pipeline

```bash
python pipeline.py
```

**What this does:**
1. Downloads Yellow & Green taxi data for all 2025 months from NYC TLC
2. Automatically imputes December 2025 if missing (weighted 30% Dec 2023 + 70% Dec 2024)
3. Filters "Ghost Trips" (Impossible Physics, Teleporters, Stationary Rides)
4. Calculates congestion zone metrics (compliance, leakage, volume decline)
5. Performs geospatial and weather analysis
6. Exports all aggregated results to `exports/` directory

**⏱️ Expected Runtime:** 15-45 minutes (depending on internet speed)

**💾 Memory Safety:** Uses DuckDB streaming with 1GB hard limit - will NOT crash your computer

### Step 3: Launch Dashboard

```bash
streamlit run app.py
```

Your browser will open automatically at `http://localhost:8501`

## 📊 Dashboard Tabs

1. **🗺️ Border Effect** - Interactive map showing % change in drop-offs near 60th St
2. **⚡ Traffic Flow** - Heatmaps comparing Q1 2024 vs Q1 2025 velocity
3. **💸 Economics** - Dual-axis chart showing Tip "Crowding Out" + Forensic Audit
4. **🌧️ Weather** - Rain Elasticity scatter plot with correlation analysis

## 📁 Project Structure

```
hashim/
├── ingestion.py        # Automated data download & schema unification
├── audit.py           # Ghost trip filters & compliance audits
├── analytics.py       # Geospatial, velocity, tip, and weather analysis
├── pipeline.py        # Main orchestrator (run this first)
├── app.py            # Streamlit dashboard (run after pipeline)
├── requirements.txt   # Python dependencies
└── exports/          # Generated CSV files (created by pipeline)
    ├── leakage_report.csv
    ├── q1_decline.csv
    ├── suspicious_vendors.csv
    ├── border_effect.csv
    ├── velocity_24.csv
    ├── velocity_25.csv
    ├── tip_crowding.csv
    ├── rain_data.csv
    └── rain_stats.txt
```

## 🎯 Key Features

### ✅ Memory Safety
- DuckDB streams data from disk (never loads full dataset into RAM)
- Hard memory limit: 1GB
- Aggregation-First Rule: All groupby/agg in SQL before pandas conversion

### ✅ Automatic Imputation
- Detects missing December 2025 data
- **Timestamp-Shifted Weighted Sampling**: Shifts 2023 (+2 years) and 2024 (+1 year) to maintain day-of-week alignment
- Formula: `Value_2025 = (Sample_2023 × 0.3) + (Sample_2024 × 0.7)`

### ✅ Forensic Audit
- **Impossible Physics**: Speed > 65 MPH
- **Teleporter**: Duration < 60s but Fare > $20
- **Stationary Ride**: Distance = 0 but Fare > 0

### ✅ Professional Visualizations
- Premium dark theme with Plotly
- Interactive maps with color-coded insights
- Dual-axis economic charts
- Weather correlation with trendlines

## 🔍 Deliverables

All requirements met:
- ✅ Phase 1: Big Data Engineering Layer
- ✅ Phase 2: Congestion Zone Impact Analysis
- ✅ Phase 3: Visual Audit (Border Effect, Velocity, Tip Crowding)
- ✅ Phase 4: Rain Tax Elasticity Model

## 🛠️ Troubleshooting

**Issue**: `ModuleNotFoundError: No module named 'xxx'`
- **Fix**: Run `pip install -r requirements.txt`

**Issue**: Dashboard shows "File not found"
- **Fix**: Run `python pipeline.py` first to generate export files

**Issue**: Network timeout during download
- **Fix**: Pipeline will auto-retry. Check internet connection.

**Issue**: "Memory allocation failed"
- **Fix**: Close other applications. Pipeline enforces 1GB limit for safety.

## 📧 Support

For questions or issues, refer to the comprehensive walkthrough at:
`C:\Users\NCS\.gemini\antigravity\brain\5d5d02b4-7baf-443b-9963-8d8f108a855a\walkthrough.md`

---

**Project**: NYC TLC 2025 Congestion Pricing Audit
**Tech Stack**: Python 3.8+ | DuckDB | Streamlit | Plotly
**Data Source**: NYC TLC Trip Record Data
