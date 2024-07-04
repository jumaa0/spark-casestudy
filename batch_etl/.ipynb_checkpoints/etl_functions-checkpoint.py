import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

def create_spark_session(app_name="casestudy"):
    return SparkSession.builder \
        .master("local[4]") \
        .appName(app_name) \
        .config("spark.sql.warehouse.dir", "/path/to/warehouse") \
        .config("hive.metastore.uris", "thrift://localhost:9083") \
        .config("spark.hadoop.hive.metastore.warehouse.dir", "/path/to/hive/warehouse") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.eventLog.logBlockUpdates.enabled", True) \
        .enableHiveSupport() \
        .getOrCreate()

def read_csv_to_df(spark, file_path, schema=None, sep=",", infer_schema=False, header=True):
    hdfs_path = "hdfs://localhost:9000" + file_path
    
    if infer_schema:
        df = spark.read.csv(hdfs_path, sep=sep, header=header, inferSchema=True)
    else:
        df = spark.read.csv(hdfs_path, schema=schema, sep=sep, header=header)
    
    return df


def remove_duplicates(df):
    return df.dropDuplicates()

def check_nulls(df, column_name):
    return df.filter(col(column_name).isNull()).count() > 0

def calculate_total_paid_price_after_discount(transactions_df):
    return transactions_df.withColumn(
        "total_paid_price_after_discount",
        when(col("offer_1") == "1", col("unit_price") * 0.15)
        .when(col("offer_2") == "2", col("unit_price") * 0.25)
        .when(col("offer_3") == "3", col("unit_price") * 0.35)
        .when(col("offer_4") == "4", col("unit_price") * 0.45)
        .when(col("offer_5") == "5", col("unit_price") * 0.50)
        .otherwise(col("unit_price"))
    )

def add_offer_column(transactions_df):
    return transactions_df.withColumn(
        "offer",
        when(col("offer_1"), "1")
        .when(col("offer_2"), "2")
        .when(col("offer_3"), "3")
        .when(col("offer_4"), "4")
        .when(col("offer_5"), "5")
    )


def insert_into_hive_table(spark, df, table_name, table_location=None, primary_key=None):
    """
    Insert data from a DataFrame into a Hive table.

    Parameters:
    - spark: SparkSession object
    - df: Spark DataFrame
    - table_name: Name of the Hive table
    - table_location: Location of the external table (optional)
    - primary_key: Primary key column(s) to identify new records (default is None)
    """
    
    table_exists = spark._jsparkSession.catalog().tableExists(table_name)

    if table_name in ["casestudy.fact_sales", "casestudy.dim_sales"]:
        print(f"Inserting data into {table_name}.")
        df.write.mode('append').insertInto(table_name)
    else:
        if table_exists:
            print(f"Table {table_name} already exists. Inserting only new records.")
            existing_data = spark.table(table_name)

            if primary_key:
                new_data = df.join(existing_data, on=primary_key, how="left_anti")
                if new_data.count() > 0:
                    new_data.write.mode('append').insertInto(table_name)
                    print(f"Inserted {new_data.count()} new records into {table_name}.")
                else:
                    print(f"No new records to insert into {table_name}.")
            else:
                print(f"Primary key is required to identify new records for table {table_name}.")
        else:
            print(f"Table {table_name} does not exist. Creating and inserting data.")
            df.write.mode('overwrite').saveAsTable(table_name)

def create_fact_sales(transactions_df):
    return transactions_df.select(
        "transaction_id", "customer_id", "sales_agent_id", "branch_id", "product_id", "offer", "units", "unit_price", "total_paid_price_after_discount"
    )

def create_dim_sales(transactions_df):
    return transactions_df.select(
        "transaction_id",
        col("transaction_date").cast("timestamp").alias("transaction_date"),
        "is_online",
        "payment_method",
        "shipping_address",
        col("load_date").cast("timestamp").alias("load_date"),
        "load_source"
    )

def create_dim_product(transactions_df):
    return transactions_df.select(
        "product_id",
        "product_name",
        "product_category",
        col("load_date").cast("timestamp").alias("load_date"),
        "load_source"
    ).distinct()

def create_dim_customer(transactions_df):
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("customer_fname", StringType(), True),
        StructField("customer_lname", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("load_date", TimestampType(), True),
        StructField("load_source", StringType(), True)
    ])
    
    return transactions_df.select(
        "customer_id",
        "customer_fname",
        "cusomter_lname",
        "cusomter_email",
        col("load_date").cast("timestamp").alias("load_date"),
        "load_source"
    ).distinct()

def create_dim_branch(branches_df):
    return branches_df.select(
        "branch_id",
        "location",
        col("establish_date").cast(TimestampType()).alias("establish_date"),
        "class",
        col("load_date").cast(TimestampType()).alias("load_date"),
        "load_source"
    ).distinct()

def create_dim_agent(agents_df):
    # Cleanse null values and handle column references correctly
    cleaned_agents_df = agents_df.withColumn("hire_date", when(col("hire_date") == "", None).otherwise(col("hire_date"))) \
                                .withColumn("load_date", when(col("load_date") == "", None).otherwise(col("load_date")))

    return cleaned_agents_df.select(
        col("sales_person_id"),
        col("name").alias("sales_agent_name"),  # Ensure correct column reference here
        col("hire_date").cast(TimestampType()).alias("hire_date"),
        col("load_date").cast(TimestampType()).alias("load_date"),
        col("load_source")
    ).distinct()