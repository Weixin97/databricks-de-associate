# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS smartphones
# MAGIC (id INT, name STRING, brand STRING, year INT);
# MAGIC
# MAGIC INSERT INTO smartphones
# MAGIC VALUES (1, 'iPhone 14', 'Apple', 2022),
# MAGIC       (2, 'iPhone 13', 'Apple', 2021),
# MAGIC       (3, 'iPhone 6', 'Apple', 2014),
# MAGIC       (4, 'iPad Air', 'Apple', 2013),
# MAGIC       (5, 'Galaxy S22', 'Samsung', 2022),
# MAGIC       (6, 'Galaxy Z Fold', 'Samsung', 2022),
# MAGIC       (7, 'Galaxy S9', 'Samsung', 2016),
# MAGIC       (8, '12 Pro', 'Xiaomi', 2022),
# MAGIC       (9, 'Redmi 11T Pro', 'Xiaomi', 2022),
# MAGIC       (10, 'Redmi Note 11', 'Xiaomi', 2021)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW view_apple_phones
# MAGIC AS SELECT *
# MAGIC   FROM smartphones
# MAGIC   WHERE brand = 'Apple';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM view_apple_phones;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------



# COMMAND ----------


