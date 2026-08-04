from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col, trim, upper, coalesce, lit, initcap, when, round

bronze_schema = spark.conf.get("bronze_schema")   # "bronze"
silver_schema = spark.conf.get("silver_schema")   # "silver

@dp.table(
    name=f"{silver_schema}.product_category",
    comment="transformed and cleaned product category table"
)
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL AND LENGTH(TRIM(product_id)) > 0")
@dp.expect_or_drop("valid_product_name", "product_name IS NOT NULL AND LENGTH(TRIM(product_name)) > 0")
@dp.expect("valid_category", "category IS NOT NULL")
@dp.expect("valid_price", "unit_price > 0")
@dp.expect_or_drop("valid_launch_date", "launch_date IS NOT NULL")
@dp.expect("valid_supplier", "supplier_name IS NOT NULL AND LENGTH(TRIM(supplier_name)) > 0")
def product_category_silver():
    """
    Extract and standardize product categories from bronze product_catalog.
    Creates a clean dimension table with unique category/subcategory combinations.
    """
    # Read from bronze product_catalog as streaming source
    df = spark.readStream.table(f"{bronze_schema}.product_catalog")
    
    # Apply generic standardization operations
    standardized_df = (df
        .filter(col("is_active") == True)
        .select(
        "product_id",
        initcap(trim(col("product_name"))).alias("product_name"),
        initcap(trim(col("category"))).alias("category"), 
        when(col("subcategory").isNotNull(), initcap(trim(col("subcategory")))).otherwise(lit("Unknown")).alias("subcategory"),
        when(col("brand").isNotNull(), initcap(trim(col("brand")))).otherwise(lit("Unknown")).alias("brand"),
        round(col("unit_price"),2).alias("unit_price"),
        when(col("unit_price") > 10000, lit("PREMIUM")).when(col("unit_price") > 5000, lit("MID_RANGE")).otherwise(lit("BUDGET")).alias("product_segment"),
        initcap(trim(col("supplier_name"))).alias("supplier_name"),
        when(col("__END_AT").isNull(),lit(True)).otherwise(lit(False)).alias("is_active"),
        "launch_date",
        "updated_at",
        "__START_AT",
        "__END_AT"             ,
        # Add processing timestamp
        current_timestamp().alias("silver_ingestion_time")
    ).distinct())
    
    return standardized_df
