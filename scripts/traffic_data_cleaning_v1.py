from pyspark.sql import SparkSession

# Buat Spark Session
spark = SparkSession.builder \
    .appName("Traffic Data Cleaning") \
    .getOrCreate()

# Load data raw
df = spark.read.csv("data/raw/traffic_smartcity_v1.csv", header=True, inferSchema=True)

# Tampilkan data awal
print("Data Awal:")
df.show(5)

# Hapus data null
df_clean = df.dropna()

# Hapus duplikasi
df_clean = df_clean.dropDuplicates()

# Tampilkan hasil cleaning
print("Data Setelah Cleaning:")
df_clean.show(5)

# Simpan ke folder clean
df_clean.write.mode("overwrite").parquet("data/clean/traffic")
