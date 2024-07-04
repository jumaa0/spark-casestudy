from etl_functions import (
    create_spark_session,
    read_csv_to_df,
    remove_duplicates,
    check_nulls,
    calculate_total_paid_price_after_discount,
    add_offer_column,
    create_fact_sales,
    create_dim_sales,
    create_dim_product,
    create_dim_customer,
    create_dim_branch,
    create_dim_agent,
    insert_into_hive_table  
)

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
from pyspark.sql.functions import col, when

def main():
    spark = create_spark_session()

    # Schema definition for CSV files
    transactions_schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("sales_agent_id", IntegerType(), True),
        StructField("branch_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("offer_1", BooleanType(), True),
        StructField("offer_2", BooleanType(), True),
        StructField("offer_3", BooleanType(), True),
        StructField("offer_4", BooleanType(), True),
        StructField("offer_5", BooleanType(), True),
        StructField("units", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("is_online", BooleanType(), True),
        StructField("payment_method", StringType(), True),
        StructField("shipping_address", StringType(), True),
        StructField("load_date", TimestampType(), True),
        StructField("load_source", StringType(), True)
    ])

    branches_schema = StructType([
        StructField("branch_id", IntegerType(), True),
        StructField("branch_name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("load_date", TimestampType(), True),
        StructField("load_source", StringType(), True)
    ])

    agents_schema = StructType([
        StructField("sales_person_id", IntegerType(), True),
        StructField("sales_agent_name", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("email", StringType(), True),
        StructField("load_date", TimestampType(), True),
        StructField("load_source", StringType(), True)
    ])

    # Reading CSV files into DataFrames with inferred schemas
    transactions_df = read_csv_to_df(spark, "/casestudy/day183/hour14/sales_transactions.csv", infer_schema=True)
    branches_df = read_csv_to_df(spark, "/casestudy/day183/hour14/branches.csv", infer_schema=True)
    agents_df = read_csv_to_df(spark, "/casestudy/day183/hour14/sales_agents.csv", infer_schema=True)

    # Removing duplicates
    transactions_df = remove_duplicates(transactions_df)
    branches_df = remove_duplicates(branches_df)
    agents_df = remove_duplicates(agents_df)

    # Checking for null values in key columns
    if check_nulls(transactions_df, "transaction_id"):
        print("Null values found in transaction_id column")
    if check_nulls(branches_df, "branch_id"):
        print("Null values found in branch_id column")
    if check_nulls(agents_df, "sales_person_id"):
        print("Null values found in sales_person_id column")

    # Calculating total paid price after discount and adding offer column
    transactions_df = calculate_total_paid_price_after_discount(transactions_df)
    transactions_df = add_offer_column(transactions_df)

    # Explicitly cast columns to match Hive table schema
    transactions_df = transactions_df.withColumn("transaction_id", col("transaction_id").cast(IntegerType()))
    transactions_df = transactions_df.withColumn("customer_id", col("customer_id").cast(IntegerType()))
    transactions_df = transactions_df.withColumn("sales_agent_id", col("sales_agent_id").cast(IntegerType()))
    transactions_df = transactions_df.withColumn("branch_id", col("branch_id").cast(IntegerType()))
    transactions_df = transactions_df.withColumn("product_id", col("product_id").cast(IntegerType()))

    # Creating fact and dimension tables
    fact_sales = create_fact_sales(transactions_df)
    dim_sales = create_dim_sales(transactions_df)
    dim_product = create_dim_product(transactions_df)
    dim_customer = create_dim_customer(transactions_df)
    dim_branch = create_dim_branch(branches_df)
    dim_agent = create_dim_agent(agents_df)
    
    fact_sales.show(2)
    
    # Inserting data into Hive tables
    insert_into_hive_table(spark, fact_sales, "casestudy.fact_sales")  # Fact sales - directly insert
    insert_into_hive_table(spark, dim_sales, "casestudy.dim_sales")    # Dim sales - directly insert
    insert_into_hive_table(spark, dim_product, "casestudy.dim_product", primary_key="product_id")  # Dim product - specify primary key
    insert_into_hive_table(spark, dim_customer, "casestudy.dim_customer", primary_key="customer_id")  # Dim customer - specify primary key
    insert_into_hive_table(spark, dim_branch, "casestudy.dim_branch", primary_key="branch_id")  # Dim branch - specify primary key
    insert_into_hive_table(spark, dim_agent, "casestudy.dim_agent", primary_key="sales_person_id")  # Dim agent - specify primary key

    # Stopping the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
