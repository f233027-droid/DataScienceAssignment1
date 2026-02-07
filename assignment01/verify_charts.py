import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

try:
    from analysis.visualization_helpers import plot_velocity_heatmap, plot_tip_vs_surcharge
    print("Successfully imported visualization_helpers")
except ImportError as e:
    print(f"Failed to import visualization_helpers: {e}")
    sys.exit(1)

def test_velocity_heatmap():
    print("Testing plot_velocity_heatmap...")
    data = pd.DataFrame({
        'day_of_week': ['Mon', 'Tue', 'Wed'],
        'hour': [8, 9, 10],
        'avg_speed': [10.5, 12.0, 11.2]
    })
    try:
        fig = plot_velocity_heatmap(data)
        if fig:
            print("Successfully generated velocity heatmap")
            # print(fig.layout.title.text)
        else:
            print("Failed to generate velocity heatmap (returned None)")
    except Exception as e:
        print(f"Error in plot_velocity_heatmap: {e}")

def test_tip_vs_surcharge():
    print("Testing plot_tip_vs_surcharge...")
    data = pd.DataFrame({
        'year': [2024, 2024],
        'month': [1, 2],
        'avg_surcharge': [2.5, 2.7],
        'avg_tip_amount': [3.0, 3.2]
    })
    try:
        fig = plot_tip_vs_surcharge(data)
        if fig:
            print("Successfully generated tip vs surcharge chart")
            # print(fig.layout.title.text)
        else:
            print("Failed to generate tip vs surcharge chart (returned None)")
    except Exception as e:
        print(f"Error in plot_tip_vs_surcharge: {e}")

if __name__ == "__main__":
    test_velocity_heatmap()
    test_tip_vs_surcharge()
