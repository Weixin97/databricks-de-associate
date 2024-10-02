# Databricks notebook source
# MAGIC %md
# MAGIC <img src="./Include/images/bookstore_schema.png">

# COMMAND ----------

# MAGIC %run ./Include/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/checkpoints/orders-raw")
        .load(f"{dataset_bookstore}/orders-raw")
        .createOrReplaceTempView("orders_raw_temp")
        )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_tmp AS (
# MAGIC   SELECT *, current_timestamp() arrival_time, input_file_name() source_file
# MAGIC   FROM orders_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_tmp

# COMMAND ----------

# convert it to spark dataframe again for delta
(spark.table("orders_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_bronze")
      .outputMode("append")
      .table("orders_bronze")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders_bronze

# COMMAND ----------

load_new_data()

# COMMAND ----------

# second hop: silver layer
(spark.read
      .format("json")
      .load(f"{dataset_bookstore}/customers-json")
      .createOrReplaceTempView("customers_lookup")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_lookup

# COMMAND ----------

(spark.readStream
    .table("orders_bronze")
    .createOrReplaceTempView("orders_bronze_tmp"))
# create a streaming temp view against our bronze table

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
# MAGIC   SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
# MAGIC           cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, books
# MAGIC   FROM orders_bronze_tmp o
# MAGIC   INNER JOIN customers_lookup c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC   WHERE quantity > 0
# MAGIC )
# MAGIC
# MAGIC -- EXCLUDE ORDER WITH NO QUANTITY AFTER FLATEN

# COMMAND ----------

# do a stream write for this orders enriched data into a silver table
(spark.table("orders_enriched_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_silver")
      .outputMode("append")
      .table("orders_silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_silver

# COMMAND ----------

load_new_data()

# COMMAND ----------

# let's now work on the gold layer
# first, we need a stream of data from the silver table into a streaming temp view

(spark.readStream
    .table("orders_silver")
    .createOrReplaceTempView("orders_silver_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- now write another stream to creata an aggregate gold tbale for the daily number of books for each customer
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW daily_customer_books_tmp AS (
# MAGIC   SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
# MAGIC   FROM orders_silver_tmp
# MAGIC   GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
# MAGIC
# MAGIC )

# COMMAND ----------

# write this aggregated data into gold table called daily_customer_books
(spark.table("daily_customer_books_tmp")
      .writeStream
      .format("delta")
      .outputMode("complete") # rewrite every updated data, info: structured streaming assumes data is only being appended in the upstream tables.
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books")
      .trigger(availableNow=True) # micro batch stream
      .table("daily_customer_books"))

# we need to rerun manually if there's new stream data loaded to

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_customer_books

# COMMAND ----------

load_new_data(all=True)

# COMMAND ----------

# stop any active stream

for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------


