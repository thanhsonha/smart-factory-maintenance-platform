from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)


spark = SparkSession.builder \
    .appName("kafka-to-bronze-machine-events") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


schema = StructType([

    StructField("event_time", StringType(), True),
    StructField("machine_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("machine_type", StringType(), True),
    StructField("air_temperature_k", DoubleType(), True),
    StructField("process_temperature_k", DoubleType(), True),
    StructField("rotational_speed_rpm", IntegerType(), True),
    StructField("torque_nm", DoubleType(), True),
    StructField("tool_wear_min", IntegerType(), True),
    StructField("machine_failure", IntegerType(), True),
    StructField("twf", IntegerType(), True),
    StructField("hdf", IntegerType(), True),
    StructField("pwf", IntegerType(), True),
    StructField("osf", IntegerType(), True),
    StructField("rnf", IntegerType(), True),
])


raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "machine_sensor_events") \
    .option("startingOffsets", "earliest") \
    .load()


parsed_df = raw_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")


bronze_df = parsed_df.withColumn("ingestion_timestamp", current_timestamp())


query = bronze_df.writeStream \
    .format("parquet") \
    .option("path", "data/lake/bronze/machine_sensor_events") \
    .option("checkpointLocation", "data/lake/bronze/checkpoints/machine_sensor_events") \
    .outputMode("append") \
    .start()


query.awaitTermination()
