# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE managed_default
# MAGIC   (width INT, length INT, height INT);
# MAGIC
# MAGIC INSERT INTO managed_default
# MAGIC VALUES (3 INT, 2 INT, 1 INT)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_default

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE external_default
# MAGIC   (width INT, length INT, height INT)
# MAGIC LOCATION 'dbfs:/mnt/demo/external_default';
# MAGIC
# MAGIC INSERT INTO external_default
# MAGIC VALUES (3 INT, 2 INT, 1 INT)

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE EXTENDED external_default

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE managed_default

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE external_default

# COMMAND ----------

# DBTITLE 1,check on the table directory
# MAGIC %fs ls 'dbfs:/mnt/demo/external_default'

# COMMAND ----------

# DBTITLE 1,create external databases
# MAGIC %sql
# MAGIC CREATE SCHEMA new_default

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED new_default

# COMMAND ----------

# MAGIC %sql
# MAGIC USE new_default;
# MAGIC
# MAGIC CREATE TABLE managed_new_default
# MAGIC   (width INT, length INT, height INT);
# MAGIC
# MAGIC INSERT INTO managed_new_default
# MAGIC VALUES (3 INT, 2 INT, 1 INT);
# MAGIC
# MAGIC CREATE TABLE external_new_default
# MAGIC   (width INT, length INT, height INT)
# MAGIC LOCATION 'dbfs:/mnt/demo/external_new_default';
# MAGIC
# MAGIC INSERT INTO external_new_default
# MAGIC VALUES (3 INT, 2 INT, 1 INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_new_default

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE managed_new_default;
# MAGIC DROP TABLE external_new_default;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/managed_new_default'

# COMMAND ----------

# DBTITLE 1,external table data files still there even we drop the table
# MAGIC %fs ls 'dbfs:/mnt/demo/external_default'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA custom
# MAGIC LOCATION 'dbfs:/Shared/schemas/custom.db'

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE DATABASE EXTENDED custom;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE custom;
# MAGIC
# MAGIC CREATE TABLE managed_custom
# MAGIC   (width INT, length INT, height INT);
# MAGIC
# MAGIC INSERT INTO managed_custom
# MAGIC VALUES (3 INT, 2 INT, 1 INT);
# MAGIC
# MAGIC CREATE TABLE external_custom
# MAGIC   (width INT, length INT, height INT)
# MAGIC LOCATION 'dbfs:/mnt/demo/external_new_default';
# MAGIC
# MAGIC INSERT INTO external_custom
# MAGIC VALUES (3 INT, 2 INT, 1 INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_custom

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE EXTENDED external_custom;

# COMMAND ----------


