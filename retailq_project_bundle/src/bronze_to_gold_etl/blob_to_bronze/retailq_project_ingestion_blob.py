from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


@dp.table(
    name="transactions",
    comment="ingestion data from csv into bronze table"
)
def ingestion_blob():
    return(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation","/Volumes/retailq_dev/volumes/blob_source/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/Volumes/retailq_dev/volumes/blob_source/transactions_source/")
        .withColumn("ingestion_timestamp", current_timestamp())
    )
