# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Difference between `coalesce` and `repartition`
# MAGIC In DF:
# MAGIC Coalesce ---> Only works for decreasing Order.
# MAGIC Repartition ---> Works for bot Incrasing and Decreasing Order.
# MAGIC
# MAGIC In RDD:
# MAGIC Coalesce and Repartition can works for both Increasing and Decreasing Order.

# COMMAND ----------

# DBTITLE 1,Importing Necessary Packages
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------

rdd = sc.parallelize(range(10),4)

# COMMAND ----------

rdd1 = rdd.coalesce(6,True)  # default it works for only decreasing no of partitions (Only RDD level that True works. Not for DataFrame)
rdd2 = rdd.repartition(6) # it works for increasing/decreasing no of partitions

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Coalesce ----> will adjust as per the partition we provided.
# MAGIC #### Repartition ----> It will shuffle entire data and partition the data as per the no. we mentioned. Because of this shuffling, we can observe shuffle read and shuffle write in stats of the job.

# COMMAND ----------

print('original rdd : ',rdd.glom().collect())
print('coalesce 2 : ',rdd1.glom().collect())
print('repartition 2  : ',rdd2.glom().collect())

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/emp.csv",header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### The below code helps to check the data how much no. it got partitioned. Usually if data < 128Mb, then the data will not get partitioned untill unless it is > 128MB.

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# from pyspark.sql.functions import spark_partition_id
#df1 = df.rdd.coalesce(4, True).toDF().withColumn("partition_id",spark_partition_id()) 
#To use coalesce(4,True), we are converting DataFrame to RDD. We can use coalesce(4) but not coalesce(4, True)
df1=df.repartition(4,"DEPTNO").withColumn("partition_id",spark_partition_id())

# COMMAND ----------

#The below code is for when we use coalesce(4,True)

df1.rdd.glom().collect()

# COMMAND ----------

df1.show()

# COMMAND ----------

df1.show()