# Databricks notebook source
# MAGIC %run ./Include/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customer
# MAGIC
# MAGIC -- the profile, is a json string

# COMMAND ----------

# MAGIC %sql
# MAGIC -- spark sql has built-in functionality to directly interact with JSON data stored as strings
# MAGIC SELECT customer_id, profile:first_name, profile:address:country
# MAGIC FROM customer 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- spark sql also has the ability to parse JSON objects into struct types, but this function, required schema 
# MAGIC SELECT from_json(profile) AS profile_struct
# MAGIC   FROM customer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT profile 
# MAGIC FROM customer
# MAGIC LIMIT 1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- copy sample and provide to the schema_of_json function
# MAGIC CREATE OR REPLACE TEMP VIEW parsed_customers AS 
# MAGIC   SELECT customer_id, 
# MAGIC   from_json(profile, schema_of_json('{"first_name":"Dniren","last_name":"Abby","gender":"Female","address":{"street":"768 Mesta Terrace","city":"Annecy","country":"France"}}')) AS profile_struct
# MAGIC   FROM customer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM parsed_customers

# COMMAND ----------

spark.sql("DESCRIBE parsed_customers").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, profile_struct.first_name, profile_struct.address.country
# MAGIC FROM parsed_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW customers_final AS 
# MAGIC   SELECT customer_id, profile_struct.*
# MAGIC   FROM parsed_customers;
# MAGIC
# MAGIC SELECT * FROM customers_final

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, customer_id, books -- array of struct type
# MAGIC FROM orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- spark sql has the explode function that allows us to put each element of an array on its own row
# MAGIC SELECT order_id, customer_id, explode(books) AS book
# MAGIC FROM orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- another interesting func, which is the collect_set aggregation function that allows us to collect unique values for a field, incuding fields within arrays
# MAGIC
# MAGIC SELECT customer_id,
# MAGIC   collect_set(order_id) AS orders_set,
# MAGIC   collect_set(books.book_id) AS books_set
# MAGIC FROM orders 
# MAGIC GROUP BY customer_id
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- can we also flatten the array?
# MAGIC SELECT customer_id,
# MAGIC   collect_set(books.book_id) AS before_flatten,
# MAGIC   array_distinct(flatten(collect_set(books.book_id))) AS after_flatten
# MAGIC FROM orders 
# MAGIC GROUP BY customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW orders_enriched AS 
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT * , explode(books) AS book
# MAGIC   FROM orders) o
# MAGIC INNER JOIN books b 
# MAGIC ON o.book.book_id = b.book_id;
# MAGIC
# MAGIC SELECT * FROM orders_enriched 

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- uninon new and old data from the table
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW orders_updates
# MAGIC AS SELECT * FROM parquet.`${dataset.bookstore}/orders-new`;
# MAGIC
# MAGIC SELECT * FROM orders
# MAGIC UNION 
# MAGIC SELECT * FROM orders_updates

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- we can also do intersection command, return all rows found in both tables
# MAGIC
# MAGIC
# MAGIC SELECT * FROM orders
# MAGIC INTERSECT 
# MAGIC SELECT * FROM orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- if use minus, we will get 
# MAGIC
# MAGIC SELECT * FROM orders
# MAGIC MINUS 
# MAGIC SELECT * FROM orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PIVOT cmd used to change data perspective
# MAGIC CREATE OR REPLACE TABLE transactions AS 
# MAGIC
# MAGIC SELECT * FROM (
# MAGIC   SELECT 
# MAGIC     customer_id,
# MAGIC     book.book_id AS book_id,
# MAGIC     book.quantity AS quantity
# MAGIC   FROM orders_enriched
# MAGIC ) PIVOT (
# MAGIC   sum(quantity) FOR book_id IN (
# MAGIC     'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
# MAGIC     'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
# MAGIC   )
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM transactions

# COMMAND ----------


