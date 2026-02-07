
# This module is largely covered by aggregations.py and congestion_analysis.py
# but keeping it for completeness if specific non-Spark analysis is needed.

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

def plot_velocity_heatmap(heatmap_data: pd.DataFrame):
    """
    Generates a heatmap figure for Streamlit.
    Expects columns: hour, day_of_week, avg_speed
    """
    if heatmap_data.empty:
        return None
        
    # Pivot for heatmap
    pivot = heatmap_data.pivot(index="day_of_week", columns="hour", values="avg_speed")
    
    fig = px.imshow(pivot, 
                    labels=dict(x="Hour of Day", y="Day of Week", color="Speed (MPH)"),
                    title="Velocity Heatmap: Traffic Flow Patterns",
                    color_continuous_scale='Magma')
    return fig

def plot_tip_vs_surcharge(economics_data: pd.DataFrame):
    """
    Generates dual-axis chart.
    Expects: year, month, avg_surcharge, avg_tip_amount
    """
    if economics_data.empty:
        return None
        
    economics_data['date'] = pd.to_datetime(economics_data[['year', 'month']].assign(day=1))
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(name="Surcharge Trend", x=economics_data['date'], y=economics_data['avg_surcharge'], 
                             fill='tozeroy', line=dict(color='indigo')))
    
    fig.add_trace(go.Scatter(name="Average Tip", x=economics_data['date'], y=economics_data['avg_tip_amount'], 
                             yaxis="y2", mode='lines+markers', line=dict(color='orange', width=3)))
    
    fig.update_layout(
        title="Economic Indicators: Tips vs Surcharges",
        yaxis=dict(title="Congestion Surcharge ($)"),
        yaxis2=dict(title="Average Tip Amount ($)", overlaying="y", side="right"),
        template="plotly_white"
    )
    
    return fig
