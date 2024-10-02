# Databricks notebook source
# MAGIC %md
# MAGIC <img src="./Include/images/bookstore_schema.png">

# COMMAND ----------

# MAGIC %run ./Include/Copy-Datasets

# COMMAND ----------

# dataset_bookstore = 'dbfs:/mnt/demo-datasets/bookstore'
files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

dataset_bookstore

# COMMAND ----------

(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .load(f"{dataset_bookstore}/orders-raw")
    .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .table("orders_updates")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders_updates

# COMMAND ----------

load_new_data()

# COMMAND ----------

# dataset_bookstore = 'dbfs:/mnt/demo-datasets/bookstore'
files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_updates

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/demo/orders_checkpoint", True)

# COMMAND ----------


