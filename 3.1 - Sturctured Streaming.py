# Databricks notebook source
# MAGIC %md
# MAGIC <img src="./Include/images/bookstore_schema.png">

# COMMAND ----------

# MAGIC %run ./Include/Copy-Datasets

# COMMAND ----------

(spark.readStream
      .table("books")
      .createOrReplaceTempView("books_streaming_tmp_vw")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_streaming_tmp_vw --the query to read from stream view

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT author, count(book_id) AS total_books
# MAGIC FROM books_streaming_tmp_vw
# MAGIC GROUP BY author
# MAGIC
# MAGIC -- with querying streaming view, we can observe the streaming performance via dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM books_streaming_tmp_vw
# MAGIC ORDER BY author
# MAGIC
# MAGIC -- sorting is not supported for streaming dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS (
# MAGIC   SELECT author, count(book_id) AS total_books
# MAGIC   FROM books_streaming_tmp_vw
# MAGIC   GROUP BY author
# MAGIC )
# MAGIC
# MAGIC -- CREATE A TMP VIEW FROM THE STREAMING VIEW

# COMMAND ----------

(spark.table("author_counts_tmp_vw")
      .writeStream
      .trigger(processingTime="4 seconds")
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books 
# MAGIC VALUES ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC        ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC        ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------

# DBTITLE 1,demo: add some books with new author to our source table
# MAGIC %sql
# MAGIC INSERT INTO books 
# MAGIC VALUES ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC        ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC        ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

(spark.table("author_counts_tmp_vw")
      .writeStream
      .trigger(availableNow=True)
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
      .awaitTermination()
)
# with the availableNow trigger option, the query will process all new available data and stop on its own after execution.
# awaitTermination method to block the execution of any cell in this notebook until the incremental batch's write has succeeded.

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT *
# MAGIC FROM author_counts

# COMMAND ----------


