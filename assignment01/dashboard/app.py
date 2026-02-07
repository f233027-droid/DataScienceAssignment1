
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))
from analysis.visualization_helpers import plot_velocity_heatmap, plot_tip_vs_surcharge
from config import AGGREGATED_DATA_DIR

st.set_page_config(page_title="NYC Congestion Pricing Audit 2025", layout="wide")

st.title("NYC Congestion Pricing Audit 2025")
st.markdown("### Compliance, Velocity, and Weather Impact Analysis")

# Load Data
@st.cache_data
def load_data():
    data = {}
    try:
        data['vol'] = pd.read_csv(AGGREGATED_DATA_DIR / "yellow_green_volume.csv")
        data['vel'] = pd.read_csv(AGGREGATED_DATA_DIR / "velocity_heatmap.csv")
        data['eco'] = pd.read_csv(AGGREGATED_DATA_DIR / "tip_vs_surcharge.csv")
        data['weather'] = pd.read_csv(AGGREGATED_DATA_DIR / "weather_elasticity.csv")
        data['q1'] = pd.read_csv(AGGREGATED_DATA_DIR / "q1_comparison.csv")
        data['ghost'] = pd.read_csv(AGGREGATED_DATA_DIR / "ghost_vendors.csv")
    except FileNotFoundError as e:
        st.error(f"Data file not found: {e}. Please run pipeline.py first.")
    return data

data = load_data()

data = load_data()

tab1, tab2, tab3, tab4, tab5 = st.tabs(["Zone Impact", "Velocity Heatmap", "Tip vs Surcharge", "Weather Elasticity", "Audit & Comparisons"])

with tab1:
    st.header("Congestion Zone Impact")
    if 'vol' in data:
        st.subheader("Yearly Trip Volume Distribution by Taxi Type")
        df_vol = data['vol']
        fig = px.bar(df_vol, x='year', y='count', color='taxi_type', barmode='group', 
                     title="Volume Analysis: Yellow vs Green Taxis",
                     color_discrete_sequence=px.colors.qualitative.Bold,
                     template="plotly_white")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No volume data available.")
        
    st.markdown("*(Placeholder for Folium Map - requires aggregated zone-level data)*")

with tab2:
    st.header("Congestion Velocity Heatmaps")
    if 'vel' in data:
        fig = plot_velocity_heatmap(data['vel'])
        if fig:
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No velocity data available.")

with tab3:
    st.header("Tip vs Surcharge Analysis")
    if 'eco' in data:
        fig = plot_tip_vs_surcharge(data['eco'])
        if fig:
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No economics data available.")

with tab4:
    st.header("Rain Tax (Weather Impact)")
    if 'weather' in data:
        df_weather = data['weather']
        st.metric("Correlation (Rain vs Trips)", f"{df_weather['total_trips'].corr(df_weather['precipitation']):.4f}")
        
        fig = px.scatter(df_weather, x='precipitation', y='total_trips', trendline="ols",
                         title="Impact of Precipitation on Daily Trip Demand",
                         template="simple_white",
                         trendline_color_override="red")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No weather data available. API might have failed or no data for 2025.")

with tab5:
    st.header("Audit & Comparisons")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Q1 Volume Comparison (2024 vs 2025)")
        if 'q1' in data:
            fig_q1 = px.bar(data['q1'], x='month', y='count', color='year', 
                            barmode='group', facet_col='taxi_type',
                            title="Comparative Volume Analysis (Q1)",
                            color_discrete_sequence=px.colors.qualitative.Safe)
            st.plotly_chart(fig_q1, use_container_width=True)
        else:
            st.info("No Q1 comparison data found.")
            
    with col2:
        st.subheader("Top Suspicious Vendors (Ghost Audit)")
        if 'ghost' in data:
            st.dataframe(data['ghost'], use_container_width=True)
            st.caption("Vendors with highest number of filtered 'ghost trips'.")
        else:
            st.info("No ghost vendor data found.")
