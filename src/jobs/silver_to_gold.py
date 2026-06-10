from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, count, sum as spark_sum, round as spark_round, current_timestamp

SILVER_PATH = "data/lake/silver/machine_sensor_events"
GOLD_PATH = "data/lake/gold/machine_reliability_kpis"

def main():
    spark = (
        SparkSession.builder
        .appName("Silver to Gold - Machine Reliability KPIs")
        .getOrCreate()
    )
    
    silver_df = spark.read.parquet(SILVER_PATH)
    
    gold_df = (
        silver_df 
        .groupBy("machine_type")
        .agg(
            count("*").alias("total_events"),
            spark_sum("machine_failure").alias("total_failures"),
            spark_sum("twf").alias("tool_wear_failures"),
            spark_sum("hdf").alias("heat_dissipation_failures"),
            spark_sum("pwf").alias("power_failure"),
            spark_sum("osf").alias("overstrain_failure"),
            spark_sum("rnf").alias("random_failure")
            
        )
        .withColumn(
            "failure_rate_pct",
            spark_round((col("total_failures") / col("total_events")) * 100, 2)
        )
        .withColumn("gold_processed_at", current_timestamp())    
    )
    
    ( 
        gold_df.write
        .mode("overwrite")
        .parquet(GOLD_PATH)
    )
    
    print("Gold KPI table written successfully")
    
    spark.stop()
    
if __name__ == "__main__":
    main()