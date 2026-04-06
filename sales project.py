# Databricks notebook source
sales_df=spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("dbfs:/Volumes/workspace/default/my_volume/sales_data.csv")
inventory=spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/workspace/default/my_volume/inventory_data.csv")
customer_df=spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/workspace/default/my_volume/customer_data.csv")

# COMMAND ----------

display(sales_df)
display(inventory)
display(customer_df)

# COMMAND ----------

from pyspark.sql.functions import *

sales_df.groupBy("product").sum("quantity").show()
sales_df.groupBy("Category").sum(("quantity")).show()


# COMMAND ----------

from pyspark.sql.functions import col

# Low stock
low_stock = inventory.filter(col("stock") < 50)

# Overstock
over_stock = inventory.filter(col("stock") > 500)

low_stock.show()
over_stock.show()

# COMMAND ----------

# Join sales with inventory
joined_df = sales_df.join(inventory, on="product", how="inner")

display(joined_df)

# COMMAND ----------

# Customer spending
customer_df.groupBy("customer_id").sum("amount").show()

# Category preference
customer_df.groupBy("Category").sum("amount").show()