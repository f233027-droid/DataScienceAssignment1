import sys
import os
from pathlib import Path
from datetime import datetime
import warnings

from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, month, year, to_date, when, expr, date_add

# Add project root to Python path
project_root = Path(__file__).resolve().parents[1]  # assuming pipeline.py is in assignment01/
sys.path.append(str(project_root))

# Import your config & modules
from config import (
    SPARK_APP_NAME,
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR,
    AUDIT_LOG_DIR,
    AGGREGATED_DATA_DIR,
    TAXI_TYPES
)
from ingestion.scraper import get_tlc_links
from ingestion.downloader import download_files
from processing.schema_unifier import unify_schema
from processing.ghost_filter import filter_ghost_trips
from processing.aggregations import (
    agg_yellow_green_volume,
    agg_velocity_heatmap,
    agg_tip_vs_surcharge,
    agg_border_effect,
    agg_q1_comparison
)
from analysis.congestion_analysis import (
    get_congestion_zone_ids,
    mark_congestion_zone,
    calculate_surcharge_compliance,
    audit_leakage
)
from analysis.weather_analysis import fetch_weather_data, analyze_rain_elasticity


def setup_spark():
    """Create SparkSession with proper Java 21/23/25 compatibility flags"""
    base_path = Path(__file__).resolve().parent
    local_bin = base_path / "bin"

    # Windows Hadoop fix (winutils) - prioritise C:\hadoop
    hadoop_home = Path("C:/hadoop")
    if hadoop_home.exists() and (hadoop_home / "bin" / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        os.environ["PATH"] += f";{str(hadoop_home / 'bin')}"
        print(f"HADOOP_HOME set to: {os.environ['HADOOP_HOME']}")
    elif local_bin.exists() and (local_bin / "winutils.exe").exists():
        os.environ["HADOOP_HOME"] = str(local_bin.parent)
        print(f"HADOOP_HOME set to: {os.environ['HADOOP_HOME']}")

    # Java home fallback / override
    if "JAVA_HOME" not in os.environ:
        possible_java = [
            r"C:\Program Files\Java\jdk-25.0.2",
            r"C:\Program Files\Java\jdk-24",
            r"C:\Program Files\Java\jdk-23",
            r"C:\Program Files\Java\jdk-21",
            r"C:\Program Files\Java\jdk-17",
        ]
        for p in possible_java:
            if Path(p).exists():
                os.environ["JAVA_HOME"] = p
                print(f"JAVA_HOME auto-set to: {p}")
                break
        else:
            print("WARNING: JAVA_HOME not found. Please set it manually.")
    
    # Very important for JDK 21+ → allows legacy security behavior
    # You can also try: -Djava.security.manager=allow  (but add-opens is usually enough)
    java_opts = (
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
        "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
    )

    warehouse_dir = base_path / "data" / "warehouse"
    warehouse_dir.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master("local[*]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.driver.extraJavaOptions", java_opts)
        .config("spark.executor.extraJavaOptions", java_opts)
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.driver.memory", "6g")           # increase if you have RAM
        .config("spark.executor.memory", "6g")
        # --- PROD FIX: Disable Hadoop permission checks on Windows ---
        .config("spark.hadoop.fs.permissions.umask-mode", "022")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
        # --- PROD FIX: Force local file committer (avoids chmod crashes) ---
        .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
        .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_and_tag_taxi_data(spark):
    """Load yellow and green separately with explicit file listing (Windows fix)"""
    dfs = []
    
    # Debug directory path
    print(f"DEBUG: Search path is {RAW_DATA_DIR}")
    if not RAW_DATA_DIR.exists():
        print(f"ERROR: Raw data directory does not exist: {RAW_DATA_DIR}")
        return None

    for taxi_type in TAXI_TYPES:
        # Use pathlib glob instead of Spark wildcard
        glob_pattern = f"*{taxi_type}_tripdata*.parquet"
        files = list(RAW_DATA_DIR.glob(glob_pattern))
        file_paths = [str(f.resolve()) for f in files]
        
        print(f"Scanning for {taxi_type} files...")
        print(f"Found {len(files)} files matching '{glob_pattern}'")
        
        if not files:
            print(f"→ No data found for {taxi_type}")
            continue

        try:
            # Pass list of files explicitly
            df_type = spark.read.parquet(*file_paths)
            if df_type.count() == 0:
                print(f"→ Empty dataframe for {taxi_type}")
                continue

            df_unified = unify_schema(df_type, taxi_type)
            df_unified = df_unified.withColumn("taxi_type", lit(taxi_type))
            dfs.append(df_unified)
            print(f">> Loaded {df_unified.count():,} {taxi_type} records")
        except Exception as e:
            print(f"Error reading {taxi_type}: {e}")

    if not dfs:
        print("No valid taxi data found. Cannot continue.")
        return None

    # Union all
    from functools import reduce
    full_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
    return full_df


def impute_dec_2025(full_df, spark):
    """
    Impute December 2025 data if missing, using weighted average of Dec 2023 (0.3) and Dec 2024 (0.7).
    """
    # Check if Dec 2025 is actually missing or incomplete
    count_dec_25 = full_df.filter((year(col("pickup_time")) == 2025) & (month(col("pickup_time")) == 12)).count()
    if count_dec_25 > 1000: # Arbitrary threshold for 'incomplete'
        print(f"Dec 2025 has {count_dec_25} records. Skipping imputation.")
        return full_df

    print("\nStarting December 2025 Imputation...")

    # Load Dec 2023 specifically if not in full_df (since we might only load 2024-2025 mainly)
    # But strictly speaking we should just check if we have it.
    
    count_23 = full_df.filter((year(col("pickup_time")) == 2023) & (month(col("pickup_time")) == 12)).count()
    count_24 = full_df.filter((year(col("pickup_time")) == 2024) & (month(col("pickup_time")) == 12)).count()
    
    if count_23 == 0:
        # try loading 2023 manually just in case it wasn't in main load
        try:
            p23 = str(RAW_DATA_DIR / "*tripdata_2023-12.parquet")
            df_23 = spark.read.parquet(p23)
            count_23 = df_23.count()
            print(f"Loaded Dec 2023 separately: {count_23} rows")
        except:
            print("Dec 2023 data not found. Using only 2024 for imputation.")
    
    print(f"Dec 2023 count: {count_23}")
    print(f"Dec 2024 count: {count_24}")
    
    target_count = int(0.3 * count_23 + 0.7 * count_24)
    if target_count == 0:
        print("Insufficient historical data for imputation.")
        return full_df
        
    print(f"Imputing target: {target_count} records for Dec 2025")
    
    # Sample from Dec 2024
    df_dec_24 = full_df.filter((year(col("pickup_time")) == 2024) & (month(col("pickup_time")) == 12))
    if df_dec_24.count() == 0:
         print("No 2024 data to sample from.")
         return full_df

    fraction = target_count / count_24
    
    # We sample with replacement to hit target size roughly
    imputed = (
        df_dec_24.sample(withReplacement=True, fraction=fraction)
        .withColumn("year", lit(2025))
        .withColumn("pickup_time", expr("date_add(pickup_time, 365)")) # Shift forward approx 1 year
        .withColumn("dropoff_time", expr("date_add(dropoff_time, 365)"))
    )
    
    return full_df.unionByName(imputed, allowMissingColumns=True)


def main():
    print("Starting NYC Congestion Pricing Audit Pipeline...")
    print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # ──────────────────────────────────────────────
    #  1. Spark Setup
    # ──────────────────────────────────────────────
    spark = setup_spark()

    # ──────────────────────────────────────────────
    #  2. Ingestion Phase
    # ──────────────────────────────────────────────
    print("\n--- Phase 1: Ingestion ---")
    # Ingest 2024 and 2025 primarily, plus Dec 2023 for imputation
    target_years = [2024, 2025]
    links = get_tlc_links(years=target_years, taxi_types=TAXI_TYPES)
    
    # Also grab Dec 2023
    links_23 = get_tlc_links(years=[2023], taxi_types=TAXI_TYPES)
    # Filter for only month 12
    links_23 = {k: v for k, v in links_23.items() if "_2023-12" in k}
    links.update(links_23)

    if not links:
        print("No 2025 links found on TLC page.")
    else:
        print(f"Found {len(links)} matching files (2024, 2025, Dec 2023).")
        download_files(links)   # skips existing files (hopefully)

    # ──────────────────────────────────────────────
    #  3. Processing Phase
    # ──────────────────────────────────────────────
    print("\n--- Phase 2: Processing ---")

    full_df = load_and_tag_taxi_data(spark)
    if full_df is None:
        spark.stop()
        return

    # Quick summary of loaded data
    full_df.printSchema()
    print(f"Total records loaded: {full_df.count():,}")

    # Check available months & years
    period_summary = (
        full_df
        .groupBy(year("pickup_time").alias("year"), month("pickup_time").alias("month"))
        .count()
        .orderBy("year", "month")
        .toPandas()
    )
    print("\nAvailable data periods:")
    print(period_summary)

    # December 2025 Imputation
    full_df = impute_dec_2025(full_df, spark)

    # ──────────────────────────────────────────────
    #  4. Cleaning / Ghost trip filter
    # ──────────────────────────────────────────────
    print("\nApplying ghost trip filter...")
    clean_df, audit_log_df = filter_ghost_trips(full_df)

    print(f"Before cleaning : {full_df.count():,} rows")
    print(f"After  cleaning : {clean_df.count():,} rows")
    print(f"Filtered ghost trips: {full_df.count() - clean_df.count():,} rows")

    # Save audit log
    audit_log_df.write.mode("overwrite").parquet(str(AUDIT_LOG_DIR / "ghost_trips_audit"))
    
    # Ghost Vendor Analysis
    print("\nTop Suspicious Vendors (Ghost Trips):")
    ghost_vendors = audit_log_df.groupBy("vendor_id").count().orderBy(col("count").desc()).limit(5)
    ghost_vendors.show()
    ghost_vendors.toPandas().to_csv(AGGREGATED_DATA_DIR / "ghost_vendors.csv", index=False)

    # ──────────────────────────────────────────────
    #  5. Analysis Phase
    # ──────────────────────────────────────────────
    print("\n--- Phase 3: Analysis ---")

    # Congestion zone marking
    zone_ids = get_congestion_zone_ids()
    clean_df = mark_congestion_zone(clean_df, zone_ids)

    # Compliance & leakage checks
    print("\nSurcharge compliance:")
    compliance = calculate_surcharge_compliance(clean_df)
    compliance.show(truncate=False)

    print("\nLeakage audit:")
    leakage = audit_leakage(clean_df)
    leakage.show(truncate=False)

    # ──────────────────────────────────────────────
    #  6. Aggregations & Export
    # ──────────────────────────────────────────────
    print("\n--- Phase 4: Exporting Aggregates ---")

    AGGREGATED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Volume yellow vs green
    vol = agg_yellow_green_volume(clean_df)
    vol.toPandas().to_csv(AGGREGATED_DATA_DIR / "yellow_green_volume.csv", index=False)

    # 2. Velocity heatmap
    vel = agg_velocity_heatmap(clean_df)
    if vel is not None:
        vel.toPandas().to_csv(AGGREGATED_DATA_DIR / "velocity_heatmap.csv", index=False)

    # 3. Economics (tip vs surcharge)
    eco = agg_tip_vs_surcharge(clean_df)
    eco.toPandas().to_csv(AGGREGATED_DATA_DIR / "tip_vs_surcharge.csv", index=False)

    # 4. Border effect
    border = agg_border_effect(clean_df)
    border.toPandas().to_csv(AGGREGATED_DATA_DIR / "border_effect.csv", index=False)

    # 5. Q1 Comparison (2024 vs 2025)
    q1_comp = agg_q1_comparison(clean_df)
    q1_comp.toPandas().to_csv(AGGREGATED_DATA_DIR / "q1_comparison.csv", index=False)

    # ──────────────────────────────────────────────
    #  7. Weather analysis (optional)
    # ──────────────────────────────────────────────
    print("\n--- Weather Analysis ---")
    try:
        weather_df = fetch_weather_data(year=2025)
        if weather_df is not None and not weather_df.empty:
            daily_trips = (
                clean_df
                .withColumn("date", to_date("pickup_time"))
                .groupBy("date")
                .count()
                .withColumnRenamed("count", "total_trips")
                .toPandas()
            )

            merged, correlation = analyze_rain_elasticity(daily_trips, weather_df)
            if merged is not None:
                print(f">> Rain elasticity correlation: {correlation:.4f}")
                merged.to_csv(AGGREGATED_DATA_DIR / "weather_elasticity.csv", index=False)
        else:
            print("No weather data available.")
    except Exception as e:
        print(f"Weather analysis failed: {e}")

    print("\nPipeline finished successfully!")
    print(f"Aggregates saved in: {AGGREGATED_DATA_DIR}")
    print("Run:  streamlit run dashboard/app.py  to view dashboard")

    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nPipeline crashed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)