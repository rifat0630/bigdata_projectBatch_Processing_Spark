import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, dayofweek
from pyspark.ml.pipeline import PipelineModel
import pandas as pd

# Judul dashboard
st.title("Smart City Traffic Prediction Dashboard")

# Spark session
spark = SparkSession.builder \
    .appName("Traffic Dashboard") \
    .getOrCreate()

# Load data clean
df = spark.read.parquet("data/clean/traffic")

# Load model
model = PipelineModel.load("models/traffic_model")

# Feature engineering
df = df.withColumn("hour", hour("datetime"))
df = df.withColumn("day", dayofweek("datetime"))

# Prediksi
predictions = model.transform(df)

# Convert ke pandas
pdf = predictions.select("datetime", "traffic", "prediction").toPandas()

# Tampilkan data
st.subheader("Data Traffic & Prediksi")
st.dataframe(pdf.head(20))

# Grafik
st.subheader("Grafik Traffic vs Prediksi")
st.line_chart(pdf[["traffic", "prediction"]])

st.success("Dashboard berhasil dijalankan!")