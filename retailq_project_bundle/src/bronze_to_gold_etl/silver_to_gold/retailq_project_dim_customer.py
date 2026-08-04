from pyspark.sql.functions import col
from pyspark import pipelines as dp

silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")

# Materialized View dim_customer to take the latest state of each customer account by deduplicating on the customer ID  
@dp.table(name = f"{gold_schema}.dim_customer")
def dim_customer():
  customer_df = spark.read.table(f"{silver_schema}.account")
  selected_df = (customer_df
      .filter((col("is_deleted") == False) & (col("is_active") == True))
      .select(
    col("id").alias("customer_id"),
    "customer_name",
    col("type").alias("customer_type"),
    col("billing_city").alias("customer_city"),
    col("billing_state").alias("customer_state"),
    col("billing_country").alias("customer_country"),
    "phone",
    "website",
    "industry",
    "annual_revenue",
    "number_of_employees",
    "description"
  ))
  return selected_df

