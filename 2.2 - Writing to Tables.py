# Databricks notebook source
# MAGIC %md
# MAGIC <img src="./Include/images/bookstore_schema.png">

# COMMAND ----------

# MAGIC %run ./Include/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create order table from parquet file
# MAGIC CREATE TABLE orders AS 
# MAGIC SELECT * FROM parquet.`${dataset.bookstore}/orders`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders AS 
# MAGIC SELECT * FROM parquet.`${dataset.bookstore}/orders`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- SECOND METHOD TO OVERWRITE TABLE DATA IS TO USE INSERT OVERWRITE
# MAGIC INSERT OVERWRITE orders 
# MAGIC SELECT * FROM parquet.`${dataset.bookstore}/orders`
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders

# COMMAND ----------

# MAGIC %sql 
# MAGIC INSERT OVERWRITE orders
# MAGIC SELECT * , current_timestamp() FROM parquet.`${dataset.bookstore}/orders`
# MAGIC
# MAGIC -- because extra schema given, it fail

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders 
# MAGIC SELECT * FROM parquet.`${dataset.bookstore}/orders-new`
# MAGIC -- the easiest way that insert the parquet file into orders table but it does not prevent duplicate insertion

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW customers_updates AS 
# MAGIC SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;
# MAGIC
# MAGIC MERGE INTO customer C
# MAGIC USING customers_updates u 
# MAGIC ON c.customer_id = u.customer_id
# MAGIC WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN 
# MAGIC   UPDATE SET email = u.email, updated = u.updated 
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW books_updates 
# MAGIC   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path = "${dataset.bookstore}/books-csv-new",
# MAGIC   header = "true",
# MAGIC   delimiter = ";"
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM books_updates

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO books b
# MAGIC USING books_updates u 
# MAGIC ON b.book_id = u.book_id AND b.title = u.title
# MAGIC WHEN NOT MATCHED AND u.category = 'Computer Science' THEN 
# MAGIC INSERT *
# MAGIC

# COMMAND ----------


