# Databricks notebook source
# MAGIC %md
# MAGIC # **Bronze_Ingestion**

# COMMAND ----------

# Import data from csv
sale_data = spark.read.csv('/Volumes/workspace/default/data/product_sales_solution - orders.csv', header=True, inferSchema=True)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# Rename column
sale_data_clean = sale_data.withColumnRenamed("price (in INR)", "price_in_INR")

bronze_sales = sale_data_clean.withColumn(
    "ingestion_ts",
    current_timestamp()
)

# Write to Bronze table
bronze_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_ingestion")