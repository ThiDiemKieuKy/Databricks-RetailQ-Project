from pyspark import pipelines as dp
from pyspark.sql.functions import col, upper, trim

gold_schema = spark.conf.get("gold_schema")   # "gold"
silver_schema = spark.conf.get("silver_schema")   # "silver

@dp.table(name=f"{gold_schema}.fact_sales")
def fact_sales():
    transaction_df = spark.readStream.table(f"{silver_schema}.transactions")
    opportunity_df = spark.read.table(f"{silver_schema}.opportunity")

    joined_df = (transaction_df
                .alias("t")
                .join(opportunity_df.alias("o")
                      ,upper(trim(col("t.opportunity_name"))) == upper(trim(col("o.name")))
                      ,"left")
                )
    
    selected_df = (joined_df
                  .select(
                    "t.transaction_id",
                    "t.opportunity_name",
                    "t.product_id",
                    "t.store_id",
                    "t.quantity",
                    "t.selling_price",
                    "t.discount_amount",
                    "t.transaction_timestamp",
                    col("t.transaction_timestamp").cast("date").alias("transaction_date"),
                    "t.payment_mode",
                    "t.sales_channel",
                    "o.name",
                    "o.stage_name",
                    "o.owner_id",
                    "o.amount",
                    col("o.account_id").alias("customer_id")
                  )
                )

    return selected_df