# Databricks notebook source
# DBTITLE 1,Importing Necessary Packages
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Here the DataBases will be listed if we create any DataBase within DataBricks environment.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show databases

# COMMAND ----------

spark.sql("show databases").collect()

# COMMAND ----------

all_dbs_names = [db.databaseName for db in spark.sql("show databases").collect()]
all_dbs_names