from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.functions import col, sum as _sum, count
import os

spark = SparkSession.builder \
    .appName("StreamingLayer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("product", StringType()) \
    .add("price", IntegerType()) \
    .add("city", StringType()) \
    .add("timestamp", StringType())

stream_df = spark.readStream \
    .schema(schema) \
    .json("stream_data")

stream_df = stream_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

agg_city = stream_df.groupBy("city") \
    .agg(
        count("*").alias("total_transactions"),
        _sum("price").alias("total_revenue")
    )

output_path = "data/serving/stream/city_agg"
checkpoint_path = "data/serving/stream/checkpoint_city"

if not os.path.exists("data/serving/stream"):
    os.makedirs("data/serving/stream")

def write_to_parquet(batch_df, batch_id):
    batch_df.write.mode("overwrite").parquet(output_path)

query = agg_city.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_parquet) \
    .option("checkpointLocation", checkpoint_path) \
    .start()

query.awaitTermination()