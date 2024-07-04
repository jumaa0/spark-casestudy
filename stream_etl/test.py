from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, to_timestamp, hour
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaStreamingExample") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8") \
    .getOrCreate()

# Kafka connection details
bootstrap_servers = "pkc-56d1g.eastus.azure.confluent.cloud:9092"
kafka_topic = "sales"  # Replace with your Kafka topic name
kafka_username = "JUKQQM4ZM632RECA"
kafka_password = "UUkrPuSttgOC0U9lY3ZansNsKfN9fbxZPFwrGxudDrfv+knTD4rCwK+KdIzVPX0D"

# Define schema for the incoming JSON data
metadata_schema = StructType([
    StructField("category", StringType(), True),
    StructField("source", StringType(), True)
])

schema = StructType([
    StructField("eventType", StringType(), True),
    StructField("customerId", StringType(), True),
    StructField("productId", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("metadata", metadata_schema, True),
    StructField("quantity", IntegerType(), True),
    StructField("totalAmount", DoubleType(), True),
    StructField("paymentMethod", StringType(), True),
    StructField("recommendedProductId", StringType(), True),
    StructField("algorithm", StringType(), True)
])

# Read data from Kafka topic as a streaming DataFrame
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_username}" password="{kafka_password}";') \
    .load()

# Parse JSON data
json_df = df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data")).select("data.*")

# Flatten metadata field and derive event_date and event_hour
flattened_df = json_df.select(
    col("eventType"),
    col("customerId"),
    col("productId"),
    to_timestamp(col("timestamp")).alias("timestamp"),
    col("metadata.category").alias("category"),
    col("metadata.source").alias("source"),
    col("quantity"),
    col("totalAmount"),
    col("paymentMethod"),
    col("recommendedProductId"),
    col("algorithm"),
    to_date(col("timestamp")).alias("event_date"),
    hour(col("timestamp")).alias("event_hour")
)

# Write data to Parquet for partitioned storage
query = flattened_df \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "/user/hive/warehouse/casestudy.db/checkpoints") \
    .option("path", "/user/hive/warehouse/casestudy.db/stream_data") \
    .start()

query.awaitTermination()

spark.stop()