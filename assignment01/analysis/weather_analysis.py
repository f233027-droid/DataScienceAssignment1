
import requests
import pandas as pd
from retry_requests import retry

def fetch_weather_data(year: int = 2025):
    """
    Fetches daily precipitation data for NYC (Central Park) from Open-Meteo.
    Lat/Lon for Central Park: 40.78, -73.97
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    # Determine date range
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31" # Or current date if 2025 is today 
    
    # If 2025 is future, this might fail or return empty/forecast if using forecast API.
    # Using archive API for past data. Swapping to forecast if needed? 
    # Assignment says "Fetch 2025 daily precipitation...". 
    # If 2025 is NOT available in archive, we might need forecast API or proxy.
    
    params = {
        "latitude": 40.78,
        "longitude": -73.97,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "precipitation_sum",
        "timezone": "America/New_York"
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        daily = data.get("daily", {})
        if not daily:
            return pd.DataFrame()
            
        df = pd.DataFrame({
            "date": daily["time"],
            "precipitation": daily["precipitation_sum"]
        })
        return df
        
    except Exception as e:
        print(f"Error fetching weather data: {e}")
        return pd.DataFrame()

def analyze_rain_elasticity(daily_trips_df: pd.DataFrame, weather_df: pd.DataFrame):
    """
    Joins trip counts with weather data and checks correlation.
    """
    if daily_trips_df.empty or weather_df.empty:
        return None
        
    # Ensure types
    daily_trips_df['date'] = pd.to_datetime(daily_trips_df['date'])
    weather_df['date'] = pd.to_datetime(weather_df['date'])
    
    merged = pd.merge(daily_trips_df, weather_df, on="date", how="inner")
    
    if merged.empty:
        return None
        
    # Correlation
    corr = merged['total_trips'].corr(merged['precipitation'])
    
    return merged, corr
