from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("smart-factory-kafka-stream") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "machine_sensor_events") \
    .option("startingOffsets", "earliest") \
    .load()

messages = df.selectExpr("CAST(value AS STRING) AS message")

query = messages.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
