# Databricks notebook source
# MAGIC %md
# MAGIC <img src="./Include/images/bookstore_schema.png">

# COMMAND ----------

# MAGIC %run ./Include/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")

# COMMAND ----------

display(files)

# COMMAND ----------

dataset_bookstore

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM JSON.`${dataset.bookstore}/customers-json/export_001.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM JSON.`${dataset.bookstore}/customers-json/export_*.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM json.`${dataset.bookstore}/customers-json`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * ,
# MAGIC   input_file_name() source_file -- built-in spark sq command that records the source data file for each records
# MAGIC   
# MAGIC   FROM json.`${dataset.bookstore}/customers-json`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`${dataset.bookstore}/customers-json` -- query any text based file like json, csv...

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM binaryFile.`${dataset.bookstore}/customers-json`
# MAGIC -- it gives the path of the file, the modification time, the length and the content, which is the binary representation of the file

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`${dataset.bookstore}/books-csv`
# MAGIC
# MAGIC -- noticed the format is not properly identified, this need another way to allows us to provide additional configuration and schema definition

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# MAGIC %md
# MAGIC **NOTE**
# MAGIC What is the `dateset_bookstore` referring to and why sometimes we can use `$` ?
# MAGIC
# MAGIC - If pipeline is written in Python, you can build up your query string and execute it with spark.sql function.
# MAGIC
# MAGIC - If using SQL cells, you can set your variable with spark.conf.set to access it with $ operator
# MAGIC `spark.conf.set("dataset.bookstore", "/path/to/dataset")`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create
# MAGIC CREATE TABLE book_csv 
# MAGIC   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
# MAGIC USING CSV 
# MAGIC OPTIONS (
# MAGIC   header = "true",
# MAGIC   delimiter = ";"
# MAGIC )
# MAGIC LOCATION "${dataset.bookstore}/books-csv"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM book_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED book_csv
# MAGIC -- from the output, we know that this table is not a delta table
# MAGIC -- we're just pointing the data storage to the external location

# COMMAND ----------

# DBTITLE 1,let's now observe what is the impact of not having delta table
# check how many csv file in the directory and load it to catalog
files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
display(files)

# COMMAND ----------

# DBTITLE 1,let's use a Spark DataFrame API that allows us to write data in a specific format like CSV
(spark.read
        .table("book_csv")
      .write
        .mode("append")
        .format("csv")
        .option("header", "true")
        .option("delimiter", ";")
        .save(f"{dataset_bookstore}/books-csv"))
# we read the table and rewrite the table data into the same directory

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM book_csv

# COMMAND ----------

# DBTITLE 1,non-delta table need manual refresh cmd, so that the new changes is reflected
# MAGIC %sql
# MAGIC REFRESH TABLE book_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM book_csv

# COMMAND ----------

# DBTITLE 1,create delta table from json and load to table customer
# MAGIC %sql
# MAGIC CREATE TABLE customer AS 
# MAGIC SELECT * FROM json.`${dataset.bookstore}/customers-json`;
# MAGIC
# MAGIC DESCRIBE EXTENDED customer;

# COMMAND ----------

# DBTITLE 1,CTAS statement do not support additional file option, limitation when ingest external table
# MAGIC %sql
# MAGIC -- example of creating delta table using CSV external source, but the table data is not well-parsed
# MAGIC
# MAGIC CREATE TABLE books_unparsed AS 
# MAGIC SELECT * FROM csv.`${dataset.bookstore}/books-csv`;
# MAGIC
# MAGIC SELECT * FROM books_unparsed

# COMMAND ----------

# DBTITLE 1,now, we use the solution, that create temp view from external source, then create delta table from tempview
# MAGIC %sql
# MAGIC CREATE TEMP VIEW books_tmp_vw
# MAGIC   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
# MAGIC USING CSV 
# MAGIC OPTIONS (
# MAGIC   path = "${dataset.bookstore}/books-csv/export_*.csv",
# MAGIC   header = "true",
# MAGIC   delimiter = ";"
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE books AS 
# MAGIC   SELECT * FROM books_tmp_vw;
# MAGIC
# MAGIC SELECT * FROM books 

# COMMAND ----------


