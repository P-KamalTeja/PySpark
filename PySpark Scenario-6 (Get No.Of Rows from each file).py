# Databricks notebook source
# DBTITLE 1,Importing Necessary Packages
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------

# DBTITLE 1,Getting List of default DataBricks Data Files for Practise
# MAGIC %fs ls /databricks-datasets/COVID/USAFacts/

# COMMAND ----------

df_covid = spark.read.csv("/databricks-datasets/COVID/USAFacts/*.csv", header = True)

# COMMAND ----------

display(df_covid)

# COMMAND ----------

df_covid = df_covid.withColumn("filename", input_file_name()) #Creating a new column and having File Name from which that record belongs to.
display(df_covid)

# COMMAND ----------

display(df_covid.groupBy("filename").count())