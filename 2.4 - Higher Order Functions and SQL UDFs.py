# Databricks notebook source
# MAGIC %run ./Include/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM ORDERS --books column is array of struct type

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     order_id, 
# MAGIC     books, 
# MAGIC     FILTER(books, i -> i.quantity >= 2) AS multiple_copies -- filter an array using a given lambda function, where we filter the books column, to extract only those books that have a quantity greater or equal to 2
# MAGIC   FROM orders 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, multiple_copies
# MAGIC FROM (
# MAGIC   SELECT 
# MAGIC     order_id,
# MAGIC     FILTER (books, i -> i.quantity >= 2) AS multiple_copies
# MAGIC   FROM orders 
# MAGIC )
# MAGIC WHERE size(multiple_copies) > 0;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- transform function to apply transformation for all the items in an array , and extract the transformed value 
# MAGIC
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   books,
# MAGIC   TRANSFORM (
# MAGIC     books,
# MAGIC     b -> CAST(b.subtotal * 0.8 AS INT)
# MAGIC   ) AS subtotal_after_discount
# MAGIC FROM orders;

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- user defined function, custom defined function of SQL logic as function in a database, making these methods reusable in any SQL query.
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION get_url(email STRING) -- require a function name, parameter, type of returned, and 
# MAGIC RETURNS STRING 
# MAGIC
# MAGIC RETURN concat("https://www.", split(email, "@")[1])

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT email, get_url(email) domain 
# MAGIC FROM customer

# COMMAND ----------

# MAGIC %sql DESCRIBE FUNCTION get_url

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION EXTENDED get_url 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION site_type(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE 
# MAGIC           WHEN email like "%.com" THEN "Commercial business"
# MAGIC           WHEN email like "%.org" THEN "Non-profits organization"
# MAGIC           WHEN email like "%.edu" THEN "Educational institution"
# MAGIC           ELSE concat("Unknown extension for domain: ", split(email, "@")[1])
# MAGIC         END;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT email, site_type(email) AS domain_category
# MAGIC FROM customer

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION get_url;
# MAGIC DROP FUNCTION site_type;

# COMMAND ----------


