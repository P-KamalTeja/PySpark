# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Difference between sort by and order by
# MAGIC
# MAGIC 1. Order By and Sort By both are not same in sql. Order by will do sorting an entire data. Sort by will do partition wise sorting.
# MAGIC 2. orderBy and sort both are same in pyspark(sort is alias for orderBy). sortWintinPartitions as same as sort By in sql.

# COMMAND ----------

# DBTITLE 1,Importing Necessary Packages
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------

#from pyspark.sql.functions import spark_partition_id
df = spark.read.csv("/scenarios/emp_data.csv",header=True,inferSchema=True).repartition(4,"DEPTNO").withColumn("PARTITION_ID",spark_partition_id())
df.createOrReplaceTempView("df")
display(df)

# COMMAND ----------

df.write.format("delta").saveAsTable("emp_dept_csv_sc20")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_dept_csv_sc20 order by sal

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_dept_csv_sc20 sort by sal

# COMMAND ----------

df.orderBy("SAL").show()

# COMMAND ----------

help(df.orderBy)

# COMMAND ----------

df.sort("SAL").show()

# COMMAND ----------

df.sortWithinPartitions("SAL").show()

# COMMAND ----------

dir(df)