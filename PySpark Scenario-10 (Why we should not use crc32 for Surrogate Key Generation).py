# Databricks notebook source
# DBTITLE 1,Importing Necessary Packages
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

df_sales = spark.read.csv("dbfs:/FileStore/tables/sales06_12_2020_10_20_33.csv", header = True)

# COMMAND ----------

display(df_sales.limit(10))

# COMMAND ----------

#from pyspark.sql.functions import concat, col, crc32, row_number
from pyspark.sql.window import Window

df_sales = df_sales.withColumn("id", concat(col("PROD_ID"), col("CUST_ID"), col("TIME_ID"), col("CHANNEL_ID")))
df_sales = df_sales.withColumn("crc32_Key", crc32(col("id")))
df_sales = df_sales.withColumn("duplicates", row_number().over(Window.partitionBy("crc32_Key").orderBy("crc32_Key")))
display(df_sales.filter("duplicates>1"))
#display(df_sales.limit(10))
df_sales.createOrReplaceTempView("sales_view") #To create a temporary view

# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(*) from sales_view
# MAGIC select * from sales_view where crc32_Key in (select crc32_Key from sales_view where duplicates > 1);

# COMMAND ----------

# DBTITLE 1,Example on why not to use crc32
# MAGIC %sql
# MAGIC --Selecting 2 random values of id column
# MAGIC select crc32("27460720-10-982")
# MAGIC union all
# MAGIC select crc32("34142828-07-014")