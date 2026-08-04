from pyspark.sql.functions import col
from pyspark import pipelines as dp

silver_schema = spark.conf.get("silver_schema")
gold_schema   = spark.conf.get("gold_schema")

# Materialized View dim_product to take the latest state of each product   
@dp.table(name = f"{gold_schema}.dim_product")
def dim_product():
  product_df = spark.read.table(f"{silver_schema}.product_category")
  selected_df = (product_df
      .filter(col("is_active") == True)
      .select(
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "product_segment",
        "unit_price",
        "supplier_name",
        "launch_date",
        "updated_at"
  ))
  return selected_df