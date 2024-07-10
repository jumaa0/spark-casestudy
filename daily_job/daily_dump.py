from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum, col
from datetime import datetime, timedelta

def create_spark_session(app_name="daily_dump"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://localhost:9083") \
        .enableHiveSupport() \
        .getOrCreate()

def main():
    spark = create_spark_session()

    # Load data from Hive tables
    fact_sales = spark.table("casestudy.fact_sales")
    dim_product = spark.table("casestudy.dim_product")
    dim_agent = spark.table("casestudy.dim_agent")
    dim_sales = spark.table("casestudy.dim_sales")  # Assuming this is where transaction_date resides

    # Verify the schema of each table to ensure the correct column names
    fact_sales.printSchema()
    dim_product.printSchema()
    dim_agent.printSchema()
    dim_sales.printSchema()

    # Join tables to get the necessary data
    daily_dump_df = fact_sales.join(dim_agent, fact_sales["sales_agent_id"] == dim_agent["sales_person_id"]) \
                              .join(dim_product, fact_sales["product_id"] == dim_product["product_id"]) \
                              .join(dim_sales, fact_sales["transaction_id"] == dim_sales["transaction_id"]) \
                              .select(dim_agent["sales_agent_name"], dim_product["product_name"], fact_sales["total_paid_price_after_discount"], dim_sales["transaction_date"])

    # Verify join results
    print("daily_dump_df count:", daily_dump_df.count())

    # Filter data for the previous date
    previous_date = datetime.now() - timedelta(days=1)
    previous_date_str = previous_date.strftime("%Y-%m-%d")
    previous_date_df = daily_dump_df.filter(col("transaction_date") == previous_date_str)

    # Aggregate data
    aggregated_df = previous_date_df.groupBy("sales_agent_name", "product_name") \
                                   .agg(spark_sum("total_paid_price_after_discount").alias("total_revenue"))

    # Write the result to a local CSV file
    current_date = datetime.now().strftime("%Y-%m-%d")
    output_path = f"/data/daily_dump/daily_dump_{current_date}"
    aggregated_df.coalesce(1).write.mode('overwrite').csv(output_path, header=True)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
