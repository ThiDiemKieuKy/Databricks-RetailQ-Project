from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

catalog = spark.conf.get("catalog")   # "catalog"

@dp.table(
    name="transactions",
    comment="ingestion data from csv into bronze table"
)
def ingestion_blob():
    return(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation",f"/Volumes/{catalog}/volumes/blob_source/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"/Volumes/{catalog}/volumes/blob_source/transactions_source/")
        .withColumn("ingestion_timestamp", current_timestamp())
    )
