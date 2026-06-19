from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, count, current_timestamp, lit, when 

SILVER_PATH = "data/lake/silver/machine_sensor_events"
DQ_OUTPUT_PATH = "data/lake/gold/data_quality_metrics"

def main():
    spark = (
        SparkSession.builder
        .appName("SmartFactoryDataQualityCheck")
        .getOrCreate()
    )
    
    df = spark.read.parquet(SILVER_PATH)
    
    total_records = df.count()
    
    duplicate_records = (
        df.groupBy("event_time", "machine_id", "product_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )
    
    invalid_records = df.filter(
        col("machine_id").isNull()
        | col("product_id").isNull()
        | col("event_time").isNull()
        | (col("rotational_speed_rpm") <= 0)
        | (col("torque_nm") < 0)
        | (col("tool_wear_min") < 0)
        | (~col("machine_failure").isin(0, 1))
        | (~col("twf").isin(0, 1))
        | (~col("hdf").isin(0, 1))
        | (~col("pwf").isin(0, 1))
        | (~col("osf").isin(0, 1))
        | (~col("rnf").isin(0,1))
    ).count() 
    
    valid_records = total_records - invalid_records  
    dq_status = "PASS" if invalid_records == 0 and duplicate_records == 0 else "FAIL"
    
    metrics_df = spark.createDataFrame(
        [
            (
                total_records, 
                valid_records,
                invalid_records,
                duplicate_records,
                dq_status,
            )
        ],
        [
            "total_records",
            "valid_records",
            "invalid_records",
            "duplicate_records",
            "data_quality_status",
        ],
    ).withColumn("run_timestamp", current_timestamp())
    
    metrics_df.write.mode("overwrite").parquet(DQ_OUTPUT_PATH)
    
    print("Data quality check completed")
    print(f"Total records: {total_records}")
    print(f"Valid records: {valid_records}")
    print(f"Invalid records: {invalid_records}")
    print(f"Duplicate records: {duplicate_records}")
    print(f"Data quality status: {dq_status}")
    
    spark.stop()
    

if __name__ == "__main__":
    main()