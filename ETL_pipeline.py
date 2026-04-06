# Databricks notebook source
from pyspark.sql.functions import col

# Load table
df = spark.table("my_data")

# Filter data
df_filtered = df.filter(col("sales") > 5000)

# Add new column
df_transformed = df.withColumn("salary", col("sales") * 0.1)

# Save as new table
df_transformed.write.mode("overwrite").saveAsTable("employee")