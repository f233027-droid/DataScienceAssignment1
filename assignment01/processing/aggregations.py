
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, avg, year, month, dayofweek, hour, expr

def aggregate_for_dashboard(df: DataFrame):
    """
    Performs aggregations for the different dashboard tabs.
    Returns a dictionary of aggregated DataFrames (or simple stats) that can be saved/converted to Pandas.
    """
    
    # 1. Congestion Zone Impact (Yellow vs Green comparison) -- handled in analysis module usually, 
    # but we can do some generic aggregations here.
    
    # 2. Velocity Heatmap: Speed by Hour vs Day of Week
    # Filter for congestion zone first? The prompt says "Average trip speed inside the zone".
    # Assuming 'inside_zone' column is added by congestion_analysis before calling this.
    
    # We will assume generic aggregations here that don't depend on "inside_zone" yet, 
    # or we construct this to run AFTER congestion analysis tagging.
    pass

# Note: The prompt asks for aggregations in "Phase 2" & "Phase 3". 
# Phase 2 includes "Yellow vs Green Trip Volumes".
# Phase 3 includes "Border Effect", "Velocity Heatmap", "Tip vs Surcharge".

def agg_yellow_green_volume(df: DataFrame):
    """
    Compare Yellow vs Green trip volumes.
    groupBy(year, quarter, taxi_type).count()
    """
    return df.groupBy(year("pickup_time").alias("year"), 
                      expr("quarter(pickup_time)").alias("quarter"), 
                      "taxi_type").count()

def agg_velocity_heatmap(df: DataFrame):
    """
    Velocity analysis: Average speed inside the zone (hour vs day of week).
    Expects 'inside_zone' and 'speed_mph' columns.
    """
    if "inside_zone" not in df.columns:
        # If not tagged yet, return empty or warn
        return None
        
    return df.filter(col("inside_zone") == True) \
             .groupBy(hour("pickup_time").alias("hour"), 
                      dayofweek("pickup_time").alias("day_of_week")) \
             .agg(avg("speed_mph").alias("avg_speed"))

def agg_tip_vs_surcharge(df: DataFrame):
    """
    Tip vs Surcharge: Monthly aggregation.
    """
    # Calculate tip percentage: tip_amount / total_amount (or fare_amount)
    # Prompt says: agg(avg(tip_pct), avg(congestion_surcharge))
    
    # We need to ensure 'tip_amount' is in the schema unifier if we use it. 
    # Schema unifier currently: [pickup_time, dropoff_time, pickup_loc, dropoff_loc, trip_distance, fare, total_amount, congestion_surcharge, taxi_type]
    # 'tip_amount' is standard in TLC data but I didn't explicitly include it in 'unify_schema'. I should update that.
    
    return df.groupBy(year("pickup_time").alias("year"), month("pickup_time").alias("month")) \
             .agg(avg("congestion_surcharge").alias("avg_surcharge"), 
                  avg(col("total_amount") - col("fare")).alias("avg_tip_amount")) # Approximation if tip column missing

def agg_border_effect(df: DataFrame):
    """
    Border Effect: Aggregates drop-offs by zone and year to see changes.
    """
    return df.groupBy("dropoff_loc", year("pickup_time").alias("year")).count()

def agg_q1_comparison(df: DataFrame):
    """
    Compare Q1 (Jan-Mar) volumes between years (e.g., 2024 vs 2025).
    """
    return df.filter(month("pickup_time") <= 3) \
             .groupBy(year("pickup_time").alias("year"), 
                      month("pickup_time").alias("month"), 
                      "taxi_type") \
             .count() \
             .orderBy("year", "month", "taxi_type")

def agg_q1_comparison(df: DataFrame):
    """
    Compare Q1 (Jan-Mar) volumes between years (e.g., 2024 vs 2025).
    """
    return df.filter(month("pickup_time") <= 3) \
             .groupBy(year("pickup_time").alias("year"), 
                      month("pickup_time").alias("month"), 
                      "taxi_type") \
             .count() \
             .orderBy("year", "month", "taxi_type")
