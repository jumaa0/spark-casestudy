from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaStreamingExample") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8") \
    .getOrCreate()

# Kafka connection details
bootstrap_servers = "pkc-56d1g.eastus.azure.confluent.cloud:9092"
kafka_topic = "sales"  # add topic name
kafka_username = "JUKQQM4ZM632RECA"
kafka_password = "UUkrPuSttgOC0U9lY3ZansNsKfN9fbxZPFwrGxudDrfv+knTD4rCwK+KdIzVPX0D"

# Define schema for the incoming JSON data
schema = StructType() \
    .add("col1", StringType()) \
    .add("col2", StringType()) \
    .add("col3", StringType())

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

# Convert the binary value column to string and parse JSON
json_df = df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data")).select("data.*")

# Write the streaming data to Parquet files in the specified directory every 30 seconds
query = json_df \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/user/hive/warehouse/casestudy.db/stream_data") \
    .option("checkpointLocation", "/user/hive/warehouse/casestudy.db/stream_data_checkpoint") \
    .trigger(processingTime='30 seconds') \
    .start()

# Wait for the termination signal
query.awaitTermination()

# Stop the Spark session
spark.stop()
