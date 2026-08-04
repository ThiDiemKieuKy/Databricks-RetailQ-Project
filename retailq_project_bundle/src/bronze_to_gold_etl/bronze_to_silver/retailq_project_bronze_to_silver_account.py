from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col, trim, upper, coalesce, lit, initcap, when, round

bronze_schema = spark.conf.get("bronze_schema")   # "bronze"
silver_schema = spark.conf.get("silver_schema")   # "silver



@dp.table(
    name=f"{silver_schema}.account",
    comment="Salesforce account data with core business columns and data quality checks"
)
@dp.expect_or_drop("non-null id", "id IS NOT NULL")
@dp.expect("non-null name", "customer_name IS NOT NULL")
def account_silver():
    """
    Extract and standardize product categories from bronze account.
    Creates a clean dimension table with unique category/subcategory combinations.
    """
    # Read from bronze account as streaming source
    account_df = spark.readStream.option("readChangeFeed","true").table(f"{bronze_schema}.account")
    
    # Apply generic standardization operations
    cleaned_account_df = (account_df
        .select(
        col("Id").alias("id"),
        col("IsDeleted").alias("is_deleted"),
        upper(trim(col("Name"))).alias("customer_name"),
        col("Type").alias("type"),
        col("ParentId").alias("parent_id"),
        col("BillingStreet").alias("billing_street"),
        col("BillingCity").alias("billing_city"),
        col("BillingState").alias("billing_state"),
        col("BillingPostalCode").alias("billing_postal_code"),
        col("BillingCountry").alias("billing_country"),
        col("ShippingStreet").alias("shipping_street"),
        col("ShippingCity").alias("shipping_city"),
        col("ShippingState").alias("shipping_state"),
        col("ShippingPostalCode").alias("shipping_postal_code"),
        col("ShippingCountry").alias("shipping_country"),
        col("Phone").alias("phone"),
        col("Website").alias("website"),
        coalesce(col("Industry"), lit("UNKNOWN")).alias("industry"),
        col("AnnualRevenue").alias("annual_revenue"),
        col("NumberOfEmployees").alias("number_of_employees"),
        col("Description").alias("description"),
        # Compute is_active: True when __END_AT is null (active record), False otherwise
        when(col("__END_AT").isNull(), True).otherwise(False).alias("is_active"),
        # Add processing timestamp
        current_timestamp().alias("silver_ingestion_time")
    ))
    
    return cleaned_account_df