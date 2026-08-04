from pyspark import pipelines as dp
from pyspark.sql.functions import col

gold_schema = spark.conf.get("gold_schema")   # "gold"
silver_schema = spark.conf.get("silver_schema")   # "silver

@dp.table(name=f"{gold_schema}.fact_inventory")
def fact_sales():
    inventoryn_df = spark.readStream.table(f"{silver_schema}.inventory")
       
    selected_df = (inventoryn_df
                  .select(
                    "inventory_id",
                    "product_id",
                    "stock_quantity",
                    "reorder_level",
                    "inventory_status",
                    "warehouse_location",
                    "last_stock_update"
                  )
                )

    return selected_df