# Databricks notebook source
# DBTITLE 1,Import Necessary Packages
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------

# DBTITLE 1,Sample Data from DataBricks for Practice Purpose
# MAGIC %fs ls /databricks-datasets/

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/COVID/USAFacts/

# COMMAND ----------

df_covid = spark.read.csv("/databricks-datasets/COVID/USAFacts/*.csv",header=True)

# COMMAND ----------

df_covid.rdd.getNumPartitions()  #Converting into RDD and checking how many partitons are there in it.

# COMMAND ----------

# DBTITLE 1,Checking count of each Default partition
df_covid.select(spark_partition_id().alias("partiton_id")).groupBy("partiton_id").count().show()
#from pyspark.sql.functions import spark_partition_id

#We have 3 partitons here as per the result we get.

# COMMAND ----------

# DBTITLE 1,Repartitioning Data as per our req. Number
df_covid.repartition(8).select(spark_partition_id().alias("partiton_id")).groupBy("partiton_id").count().show()