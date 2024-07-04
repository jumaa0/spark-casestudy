from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum
from datetime import datetime


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

    # Join tables to get the necessary data
    daily_dump_df = fact_sales.join(dim_agent, fact_sales.sales_agent_id == dim_agent.sales_person_id) \
                              .join(dim_product, fact_sales.product_id == dim_product.product_id) \
                              .select(dim_agent.sales_agent_name, dim_product.product_name, fact_sales.units)

    # Aggregate data
    aggregated_df = daily_dump_df.groupBy("sales_agent_name", "product_name") \
                                 .agg(spark_sum("units").alias("total_sold_units"))

    # Write the result to a local CSV file
    current_date = datetime.now().strftime("%d-%m-%Y")

    output_path = f"/data/daily_dump/daily_dump{current_date}"
    aggregated_df.coalesce(1).write.mode('overwrite').csv(output_path, header=True)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
