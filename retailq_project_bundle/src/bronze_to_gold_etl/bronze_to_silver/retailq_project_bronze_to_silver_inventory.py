from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col, trim, upper, coalesce, lit, initcap, when, round

bronze_schema = spark.conf.get("bronze_schema")   # "bronze"
silver_schema = spark.conf.get("silver_schema")   # "silver

@dp.table(
    name=f"{silver_schema}.inventory",
    comment="transformed and cleaned inventory table"
)
@dp.expect_or_drop("valid_inventory", "inventory_id IS NOT NULL AND LENGTH(TRIM(product_id)) > 0")
@dp.expect("valid_stock_quantity", "stock_quantity > 0")
@dp.expect("valid_product_id", "product_id IS NOT NULL AND LENGTH(TRIM(product_id)) > 0")
@dp.expect("valid_store_id", "store_id IS NOT NULL AND LENGTH(TRIM(store_id)) > 0")
def inventory_silver():
    """
    Extract and standardize product categories from bronze inventory.
    Creates a clean dimension table with unique category/subcategory combinations.
    """
    # Read from bronze inventory as streaming source
    inventory_df = spark.readStream.table(f"{bronze_schema}.inventory")
    
    # Apply generic standardization operations
    cleaned_inventory_df = (inventory_df
        .select(
        "inventory_id",
        "product_id",
        "store_id",
        "stock_quantity",
        "reorder_level",
        when(col("stock_quantity") < col("reorder_level"), lit("LOW_STOCK")).otherwise(lit("HEALTHY")).alias("inventory_status"),
        "warehouse_location",
        "last_stock_update",
        # Add processing timestamp
        current_timestamp().alias("silver_ingestion_time")
    ))
    
    return cleaned_inventory_df