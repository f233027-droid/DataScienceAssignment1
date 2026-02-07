
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, unix_timestamp, round
from pyspark.sql.functions import when

def filter_ghost_trips(df: DataFrame):
    """
    Filters out ghost trips and invalid data.
    Returns: (clean_df, audit_log_df)
    
    Conditions:
    1. Average speed > 65 MPH
    2. Trip time < 1 min AND fare > $20
    3. Trip distance = 0 AND fare > 0
    """
    
    # Calculate duration in hours and speed
    # unix_timestamp returns seconds. / 3600 for hours.
    df = df.withColumn("duration_seconds", unix_timestamp("dropoff_time") - unix_timestamp("pickup_time"))
    df = df.withColumn("duration_hours", col("duration_seconds") / 3600.0)
    
    # Avoid division by zero
    df = df.withColumn("speed_mph", 
                       when(col("duration_hours") > 0.001, col("trip_distance") / col("duration_hours"))
                       .otherwise(0))

    # Define Filter Conditions
    # 1. Speed > 65 MPH
    cond_speed = col("speed_mph") > 65
    
    # 2. Trip time < 1 min (< 60 sec) AND fare > $20
    cond_short_expensive = (col("duration_seconds") < 60) & (col("fare") > 20)
    
    # 3. Trip distance = 0 AND fare > 0
    cond_zero_dist_fare = (col("trip_distance") == 0) & (col("fare") > 0)
    
    # Valid trips match NONE of these conditions
    is_ghost = cond_speed | cond_short_expensive | cond_zero_dist_fare
    
    clean_df = df.filter(~is_ghost)
    audit_log = df.filter(is_ghost)
    
    return clean_df, audit_log
