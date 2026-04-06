# Databricks notebook source

# Sample Data
data = [
     (1,"vishnu",5000,"2026-03-01"),
    (2,"arun",6000,"2026-03-01"),
    (3,"suresh",7000,"2026-03-01"),
    (4,"ramesh",8000,"2026-03-01"),
    (5,"kumar",9000,"2026-03-01"),
    (6,"sai",10000,"2026-03-01"),
    (7,"sai",10000,"2026-03-01"),
    (8,"sai",12220,"2026-03-01"),
    (9,"sai",10000,"2026-03-01"),
    (10,"sai",10000,"2026-03-01")
]

columns = ['id','name','sales','date']

df = spark.createDataFrame(data, columns)

# Save table
df.write.mode("overwrite").saveAsTable("my_data")