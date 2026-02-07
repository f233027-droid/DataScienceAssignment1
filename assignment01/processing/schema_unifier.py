
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when

def unify_schema(df: DataFrame, taxi_type: str) -> DataFrame:
    """
    Unifies schemas for Yellow and Green taxi data to a common format.
    Target columns: 
    [pickup_time, dropoff_time, pickup_loc, dropoff_loc, trip_distance, fare, total_amount, congestion_surcharge, taxi_type]
    """
    
    # Common column mapping
    # Note: different source columns for yellow vs green
    
    if taxi_type.lower() == "yellow":
        lookup = {
            "tpep_pickup_datetime": "pickup_time",
            "tpep_dropoff_datetime": "dropoff_time",
            "PULocationID": "pickup_loc",
            "DOLocationID": "dropoff_loc",
            "trip_distance": "trip_distance",
            "fare_amount": "fare",
            "total_amount": "total_amount",
            "congestion_surcharge": "congestion_surcharge"
        }
    elif taxi_type.lower() == "green":
        lookup = {
            "lpep_pickup_datetime": "pickup_time",
            "lpep_dropoff_datetime": "dropoff_time",
            "PULocationID": "pickup_loc",
            "DOLocationID": "dropoff_loc",
            "trip_distance": "trip_distance",
            "fare_amount": "fare",
            "total_amount": "total_amount",
            "congestion_surcharge": "congestion_surcharge"
        }
    else:
        raise ValueError(f"Unknown taxi type: {taxi_type}")

    # Rename existing columns
    for old, new in lookup.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
        else:
            # If column missing, add as null (handle gracefully)
            # print(f"Warning: Column {old} not found in {taxi_type} data")
            pass
            
    # Select only required columns (plus potentially useful ones if needed, but keeping it strict as per prompt)
    # Adding taxi_type for analysis
    final_cols = list(set(lookup.values()) & set(df.columns))
    
    df = df.select(*final_cols)
    
    # Ensure all target columns exist
    required_cols = ["pickup_time", "dropoff_time", "pickup_loc", "dropoff_loc", 
                     "trip_distance", "fare", "total_amount", "congestion_surcharge", 
                     "tip_amount", "vendor_id"]
    
    for c in required_cols:
        if c not in df.columns:
            df = df.withColumn(c, lit(None))
            
    df = df.withColumn("taxi_type", lit(taxi_type))
    
    return df
