# Databricks notebook source
# MAGIC %md
# MAGIC # **Gold Aggregates**

# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct

silver_cleaning = spark.table("workspace.default.silver_cleaning")

gold_revenue = (
    silver_cleaning
    .groupBy("customer_id", "customer_name")
    .agg(
        sum("total_price").alias("total_revenue"),
        countDistinct("order_id").alias("total_orders")
    )
)

# Write to Gold revenue table
gold_revenue.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_aggregates")