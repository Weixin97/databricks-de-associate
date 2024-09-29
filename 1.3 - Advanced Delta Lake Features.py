# Databricks notebook source
# MAGIC %sql DESCRIBE HISTORY employees

# COMMAND ----------

# DBTITLE 1,see the table before an update
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM employees VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees@v1

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM employees

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees

# COMMAND ----------

# DBTITLE 1,ROLLBACK TO DATA BEFORE THE DELETE COMMAND
# MAGIC %sql
# MAGIC SELECT * FROM employees VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE employees TO VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees

# COMMAND ----------

# DBTITLE 1,having  too much data files in small, will lead to performance issue
# MAGIC %sql
# MAGIC OPTIMIZE employees
# MAGIC ZORDER BY id

# COMMAND ----------

# DBTITLE 1,no difference, because of the retention period
# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# DBTITLE 1,temp turn off the retention check
# MAGIC %sql
# MAGIC SET spark.datatabricks.delta.retentionDurationCheck.enabled = false;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM employees RETAIN 169 HOURS

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE employees

# COMMAND ----------


