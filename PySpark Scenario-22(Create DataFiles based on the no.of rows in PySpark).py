# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Write number of files on basis of Rows.
# MAGIC #### How to restrict the number of records per file in pyspark.
# MAGIC #### How To create data files based on the number of rows in PySpark

# COMMAND ----------

# DBTITLE 1,Importing Necessary Packages
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------

import urllib.request
urllib.request.urlretrieve("https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Electronics_v1_00.tsv.gz","/tmp/amazon_reviews_us_Electronics_v1_oo.tsv.gz")

# COMMAND ----------

# MAGIC %sh
# MAGIC gunzip -d /tmp/amazon_reviews_us_Electronics_v1_00.tsv.gz

# COMMAND ----------

df = spark.read.csv("file:/tmp/amazon_reviews_us_Electronics_v1_00.tsv",header=True,sep="\t")
df.coalesce(1).write.format("csv").mode("overwrite").option("maxRecordsPerFile","1000000").save("/mycsvdata/",header=True)

# COMMAND ----------

#from pyspark.sql.functions import input_file_name
df1 = spark.read.csv("/mycsvdata/",header=True).withColumn("filename",input_file_name())
display(df1.groupBy("filename").count())