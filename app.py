import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# --- Configuration ---
st.set_page_config(page_title="2025 NYC Congestion Audit", layout="wide", initial_sidebar_state="expanded")

# Custom CSS for Premium Look
st.markdown("""
    <style>
    .main { background-color: #0e1117; color: #ffffff; }
    .stMetric { background-color: #1e2130; padding: 15px; border-radius: 10px; border: 1px solid #3e44fe; }
    </style>
    """, unsafe_allow_html=True)

st.title("🏙️ The 2025 NYC Congestion Pricing Audit")
st.markdown("Forensic Analysis of the Manhattan Congestion Relief Zone Toll (Implemented Jan 5, 2025)")

# --- Data Loading ---
@st.cache_data
def load_export(file):
    path = os.path.join("exports", file)
    if os.path.exists(path): return pd.read_csv(path)
    return None

# --- Dashboard Layout ---
tab1, tab2, tab3, tab4 = st.tabs(["🗺️ Border Effect", "⚡ Traffic Flow", "💰 Economics", "🌧️ Weather"])

# Tab 1: The Map (Border Effect)
with tab1:
    st.header("The 'Border Effect' Mapping")
    st.markdown("Hypothesis: Passengers end trips just outside the zone to avoid the toll.")
    border_df = load_export("border_effect.csv")
    if border_df is not None:
        # Mocking coordinates for TLC Zones for the map
        coords = {
            236: [40.77, -73.95], 237: [40.76, -73.96], 238: [40.79, -73.97], 239: [40.78, -73.98],
            140: [40.76, -73.96], 141: [40.77, -73.96], 142: [40.77, -73.98], 143: [40.78, -73.99]
        }
        border_df['lat'] = border_df['loc_id'].map(lambda x: coords.get(x, [40.75, -73.98])[0])
        border_df['lon'] = border_df['loc_id'].map(lambda x: coords.get(x, [40.75, -73.98])[1])
        
        fig = px.scatter_mapbox(border_df, lat="lat", lon="lon", color="change_rate", size="count_25",
                                color_continuous_scale="RdBu_r", size_max=15, zoom=12,
                                mapbox_style="carto-darkmatter", title="Drop-off Volume Change (2024 vs 2025)")
        st.plotly_chart(fig, use_container_width=True)

# Tab 2: Traffic Flow (Velocity Heatmaps)
with tab2:
    st.header("Congestion Velocity Comparison")
    col1, col2 = st.columns(2)
    v24 = load_export("velocity_24.csv")
    v25 = load_export("velocity_25.csv")
    
    if v24 is not None and v25 is not None:
        pivot24 = v24.pivot(index='dow', columns='hour', values='speed')
        pivot25 = v25.pivot(index='dow', columns='hour', values='speed')
        
        with col1:
            st.subheader("Q1 2024 (Before Toll)")
            st.plotly_chart(px.imshow(pivot24, labels={'color':'MPH'}, color_continuous_scale="Viridis"), use_container_width=True)
        with col2:
            st.subheader("Q1 2025 (After Toll)")
            st.plotly_chart(px.imshow(pivot25, labels={'color':'MPH'}, color_continuous_scale="Viridis"), use_container_width=True)

# Tab 3: Economics (Tip "Crowding Out" Analysis)
with tab3:
    st.header("💸 Tip 'Crowding Out' Analysis")
    st.markdown("**Hypothesis:** Higher tolls reduce the disposable income passengers leave for drivers.")
    
    tip_data = load_export("tip_crowding.csv")
    if tip_data is not None:
        # Create dual-axis chart
        fig = go.Figure()
        
        # Bar chart for average surcharge
        fig.add_trace(go.Bar(
            x=tip_data['month'],
            name='Avg Surcharge ($)',
            y=tip_data['avg_surcharge'],
            marker_color='indianred',
            yaxis='y'
        ))
        
        # Line chart for average tip percentage
        fig.add_trace(go.Scatter(
            x=tip_data['month'],
            name='Avg Tip %',
            y=tip_data['avg_tip_pct'],
            line=dict(color='royalblue', width=4),
            yaxis='y2'
        ))
        
        fig.update_layout(
            title="Monthly Surcharge vs. Tip Percentage (2025)",
            xaxis=dict(title="Month"),
            yaxis=dict(title="Average Surcharge ($)", side='left'),
            yaxis2=dict(title="Average Tip (%)", side='right', overlaying='y'),
            legend=dict(x=0.01, y=0.99),
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Show forensic data below
        st.markdown("---")
        st.subheader("🔍 Forensic Audit: Leakage & Fraud")
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Top Missing Surcharge Locations**")
            leakage = load_export("leakage_report.csv")
            if leakage is not None:
                st.dataframe(leakage, use_container_width=True)
                st.info("Primary leakage points for trips entering the Congestion Zone.")
                
        with col2:
            st.markdown("**Suspicious Vendor Activity**")
            suspicious = load_export("suspicious_vendors.csv")
            if suspicious is not None:
                st.dataframe(suspicious, use_container_width=True)
                st.warning("Forensic flags mapped to specific Vendor IDs.")


# Sidebar for global metrics
st.sidebar.header("Global Audit Summary")
decline_df = load_export("q1_decline.csv")
if decline_df is not None:
    v24 = decline_df[decline_df['period'] == '2024_Q1']['volume'].values[0]
    v25 = decline_df[decline_df['period'] == '2025_Q1']['volume'].values[0]
    pct = (v25 - v24) / v24 * 100
    st.sidebar.metric("Entry Volume Decline (Q1)", f"{pct:.1f}%", delta=f"{v24 - v25:,} trips", delta_color="inverse")

# Tab 4: Weather (Rain Elasticity)
with tab4:
    st.header("Rain Elasticity of Demand")
    rain_data = load_export("rain_data.csv")
    if rain_data is not None:
        fig = px.scatter(rain_data, x="precip_mm", y="daily_trips", trendline="ols",
                         title="Daily Taxi Demand vs. Precipitation")
        st.plotly_chart(fig, use_container_width=True)
        
        if os.path.exists("exports/rain_stats.txt"):
            with open("exports/rain_stats.txt", "r") as f:
                st.metric("Rain Elasticity (Correlation)", f.read())
