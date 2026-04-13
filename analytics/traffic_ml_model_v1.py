from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofweek
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
import os

# Buat Spark Session
spark = SparkSession.builder \
    .appName("Traffic ML Model") \
    .getOrCreate()

# Load data hasil cleaning
df = spark.read.parquet("data/clean/traffic")

# Feature Engineering (time-series)
df = df.withColumn("hour", hour("datetime"))
df = df.withColumn("day", dayofweek("datetime"))

# Siapkan fitur
assembler = VectorAssembler(
    inputCols=["hour", "day"],
    outputCol="features"
)

# Model Random Forest
rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="traffic"
)

# Pipeline
pipeline = Pipeline(stages=[assembler, rf])

# Split data (train & test)
train_data, test_data = df.randomSplit([0.8, 0.2])

# Training model
model = pipeline.fit(train_data)

# Buat folder models jika belum ada
os.makedirs("models", exist_ok=True)

# Simpan model (format Spark, BUKAN joblib)
model.write().overwrite().save("models/traffic_model")

print("Model berhasil dibuat dan disimpan!")