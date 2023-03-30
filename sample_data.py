# generated by Chat GPT-4

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType
import random
import datetime

sample_path = 'YOUR_PATH_TO_SAMPLE_DATA'

# Define schema
schema = StructType([
    StructField("brand", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("purchase_date", DateType(), True),
    StructField("category", StringType(), True),
    StructField("product_description", StringType(), True)
])

# Sample data
brands = ["BrandA", "BrandB", "BrandC", "BrandD", "BrandE"]
categories = ["Electronics", "Clothing", "Toys", "Groceries", "Furniture"]
product_descriptions = ["Product1", "Product2", "Product3", "Product4", "Product5"]

sample_data = []

# Generate random sample data
for _ in range(1000):
    brand = random.choice(brands)
    price = round(random.uniform(10, 500), 2)
    purchase_date = datetime.date(random.randint(2020, 2022), random.randint(1, 12), random.randint(1, 28))
    category = random.choice(categories)
    product_description = random.choice(product_descriptions)

    sample_data.append((brand, price, purchase_date, category, product_description))

# Create a data frame
df = spark.createDataFrame(sample_data, schema)

# Save the data frame to storage in several files, split by month
output_path = sample_path
df = df.withColumn("year", year(col("purchase_date"))).withColumn("month", month(col("purchase_date")))
df.write.partitionBy("year", "month").parquet(output_path, mode="overwrite")
