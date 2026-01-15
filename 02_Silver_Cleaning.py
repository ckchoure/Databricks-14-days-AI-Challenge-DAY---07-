# Databricks notebook source
# MAGIC %md
# MAGIC # **Silver Cleaning**

# COMMAND ----------

from pyspark.sql.functions import col, when, to_date

bronze_ingestion = spark.table("workspace.default.bronze_ingestion")

# Clean & Validate
silver_sales = (
    bronze_ingestion
    .filter(col("total_price").isNotNull())
    .filter(col("total_price") > 0)
    .dropDuplicates(["order_id"])
    .withColumn(
        "price_category",
        when(col("total_price") < 500, "Low")
        .when(col("total_price") < 1000, "Medium")
        .otherwise("High")
    
    )
)

# Write to Silver table
silver_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_cleaning")