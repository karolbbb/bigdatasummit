from pyspark.sql.functions import col, to_timestamp, from_json, lit, input_file_name
from pyspark.sql.types import *

# parameters
source_type = 'Parquet'
dataset = 'sample_data'
source = f'abfss://raw@YOURSTORAGEACCOUNT.dfs.core.windows.net/{dataset}/'
bronze_path = f'abfss://bronze@YOURSTORAGEACCOUNT.dfs.core.windows.net/delta/{dataset}/'
silver_path = f'abfss://silver@YOURSTORAGEACCOUNT.dfs.core.windows.net/delta/{dataset}/'
gold_path = f'abfss://gold@YOURSTORAGEACCOUNT.dfs.core.windows.net/delta/{dataset}/'
checkpoint_path = f'abfss://bronze@YOURSTORAGEACCOUNT.dfs.core.windows.net/delta/{dataset}/checkpoints/'


def bronze_transformations(df):
    df = df.withColumn("price_2", df.price * 2)
    return df

def silver_transformations(df):
    return df

def gold_transformations(df):
    df = df.groupBy("brand").count()
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
    return spark.readStream.parquet(source).withColumn('file_path', input_file_name())

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
    print(f'Number of new bronze records: {df_bronze.count()}')
    print('Writing bronze')
    df_bronze.write.mode("append").format("delta").option("mergeSchema", "true").save(bronze_path)

    df_silver = silver_transformations(df_bronze)
    print(f'Number of new silver records: {df_silver.count()}')
    print('Writing silver')
    df_silver.write.mode("append").format("delta").option("mergeSchema", "true").save(silver_path)

    df_gold = gold_transformations(df_silver)
    print(f'Number of new gold records: {df_gold.count()}')
    print('Writing gold')
    df_gold.write.mode("append").format("delta").option("mergeSchema", "true").save(gold_path)

streaming_df = get_source(spark, 'Parquet', source)
streaming_df.isStreaming

# Update mode - (Available since Spark 2.1.1) Only the rows in the Result Table that were updated since the last trigger will be outputted to the sink. More information to be added in future releases.
query = streaming_df.writeStream.outputMode("append").queryName('bigdatasummit').trigger(once=True).option("checkpointLocation", checkpoint_path).foreachBatch(write2table).start()
query.awaitTermination()

df = spark.read.format("delta").load(bronze_path)

display(df.groupBy("brand").count())