from pyspark.sql.functions import col, to_timestamp, from_json, lit
from pyspark.sql.types import *

# parameters
source_type = 'EventHub'
source = {"connectionString": 'YOUR_EVENTHUB_ENDPOINT'}
dataset = 'bitcoin_data'
bronze_path = f'abfss://bronze@YOURSTORAGEACCOUNT.dfs.core.windows.net/delta/{dataset}/'
silver_path = f'abfss://silver@YOURSTORAGEACCOUNT.dfs.core.windows.net/delta/{dataset}/'
gold_path = f'abfss://gold@YOURSTORAGEACCOUNT.dfs.core.windows.net/delta/{dataset}/'
checkpoint_path = f'abfss://bronze@YOURSTORAGEACCOUNT.dfs.core.windows.net/delta/{dataset}/checkpoints/'


def bronze_transformations(df):
    df = df.withColumn("extra_column", col("message_enqueued_time"))
    return df

def silver_transformations(df):
    df = df.withColumn("silver", col("json"))
    return df

def gold_transformations(df):
    df = df.withColumn("gold", col("json"))
    return df

def readStream_EventHub(spark, source):

    source_connectionString = source["connectionString"]
    ehConf = {'eventhubs.connectionString': spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(source_connectionString)}

    return spark.readStream.format("eventhubs").options(**ehConf).load().select(
        col("body").cast("string").alias("body"),
        to_timestamp(col("enqueuedTime")).alias("message_enqueued_time")
    )

def readStream_parquet(spark, source):
    spark.conf.set("spark.sql.streaming.schemaInference", 'true')
    return spark.readStream.parquet(**source).withColumn('file_path', input_file_name())

def get_source(spark, source_type, source):

    print(f'Reading source {source_type}')

    if source_type == 'EventHub':
        df = readStream_EventHub(spark, source)
    elif source_type == 'Parquet':
        df = readStream_parquet(spark, source)
    return df

def write2table(df_source, epoch_id):
    
    batch_id = int(epoch_id)
    print(f'Process start for batch_id: {batch_id}')

    df_bronze = bronze_transformations(df_source)
    print(f'Number of bronze records: {df_bronze.count()}')
    print('Writing bronze')
    df_bronze.write.mode("append").format("delta").option("mergeSchema", "true").save(bronze_path)

streaming_df = get_source(spark, 'EventHub', source)
streaming_df.isStreaming

# Update mode - (Available since Spark 2.1.1) Only the rows in the Result Table that were updated since the last trigger will be outputted to the sink. More information to be added in future releases.
query = streaming_df.writeStream.outputMode("append").queryName('bigdatasummit').trigger(processingTime='2 seconds').option("checkpointLocation", checkpoint_path).foreachBatch(write2table).start()
query.awaitTermination()

df = spark.read.format("delta").load(bronze_path)