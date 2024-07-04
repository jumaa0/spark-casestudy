from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, expr
from datetime import datetime

def create_spark_session(app_name="marketing_analysis"):
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

def main():
    spark = create_spark_session()

    # Load necessary tables from Hive
    fact_sales_df = spark.table("casestudy.fact_sales")
    dim_product_df = spark.table("casestudy.dim_product")
    dim_sales_df = spark.table("casestudy.dim_sales")
    dim_customer_df = spark.table("casestudy.dim_customer")

    # Get the current date in day-month-year format
    current_date = datetime.now().strftime("%d-%m-%Y")

    # Most selling products based on total sales amount
    most_selling_products = fact_sales_df.withColumn(
        "total_sales_amount", expr("units * unit_price")
    ).groupBy("product_id").agg(
        spark_sum("total_sales_amount").alias("total_sales_amount")
    ).orderBy(col("total_sales_amount").desc())

    most_selling_products = most_selling_products.join(dim_product_df, "product_id")
    most_selling_products.write.csv(f"/data/requirements/most_selling_products_{current_date}", header=True)

    # Most redeemed offers from customers
    most_redeemed_offers = fact_sales_df.groupBy("offer").agg(
        count("offer").alias("total_redemptions")
    ).orderBy(col("total_redemptions").desc())

    most_redeemed_offers.write.csv(f"/data/requirements/most_redeemed_offers_{current_date}", header=True)

    # Most redeemed offers per product
    most_redeemed_offers_per_product = fact_sales_df.groupBy("product_id", "offer").agg(
        count("offer").alias("total_redemptions")
    ).orderBy(col("total_redemptions").desc())

    most_redeemed_offers_per_product = most_redeemed_offers_per_product.join(dim_product_df, "product_id")
    most_redeemed_offers_per_product.write.csv(f"/data/requirements/most_redeemed_offers_per_product_{current_date}", header=True)

    # Lowest cities in online sales
    # Join dim_sales with fact_sales to get unit_price and filter for online sales
    online_sales_df = dim_sales_df.join(fact_sales_df, "transaction_id").filter(col("is_online") == True)

    lowest_cities_online_sales = online_sales_df.groupBy("shipping_address").agg(
        spark_sum("unit_price").alias("total_online_sales")
    ).orderBy(col("total_online_sales").asc())

    lowest_cities_online_sales.write.csv(f"/data/requirements/lowest_cities_online_sales_{current_date}", header=True)

    # Stopping the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
