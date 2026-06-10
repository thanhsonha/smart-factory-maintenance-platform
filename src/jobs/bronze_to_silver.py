from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp
from delta import configure_spark_with_delta_pip

BRONZE_PATH = "data/lake/bronze/machine_sensor_events"
SILVER_PATH = "data/lake/silver/machine_sensor_events"


def main():
    builder = (
        SparkSession.builder
        .appName("Bronze to Silver - Machine Sensor Events")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    bronze_df = spark.read.parquet(BRONZE_PATH)

    silver_df = (
        bronze_df
        .dropDuplicates(["event_time", "machine_id", "product_id"])
        .filter(col("event_time").isNotNull())
        .filter(col("machine_id").isNotNull())
        .filter(col("air_temperature_k").between(250, 400))
        .filter(col("process_temperature_k").between(250, 450))
        .filter(col("rotational_speed_rpm") > 0)
        .filter(col("torque_nm") >= 0)
        .filter(col("tool_wear_min") >= 0)
        .withColumn(
            "machine_health_status",
            when(
                (col("machine_failure") == 1)
                | (col("twf") == 1)
                | (col("hdf") == 1)
                | (col("pwF") == 1)
                | (col("osf") == 1)
                | (col("rnf") == 1),
                "CRITICAL"
            )
            .when(
                (col("tool_wear_min") >= 200)
                | (col("process_temperature_k") >= 315)
                | (col("torque_nm") >= 60),
                "WARNING"
            )
            .otherwise("NORMAL")
        )
        .withColumn("siver_processed_at", current_timestamp())
    )

    (
        silver_df.write \
            .mode("overwrite") \
            .parquet(SILVER_PATH)
    )

    print("Silver table written successfully")

    spark.stop()


if __name__ == "__main__":
    main()