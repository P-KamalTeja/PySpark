# Databricks notebook source
# DBTITLE 1,Importing Necessary Packages
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ` ----> BackStick or Accute Accent

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### It's always better not to have spaces in column names. If have, its better to rename the column with no spaces.

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/asa/airlines

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from csv.`dbfs:/databricks-datasets/asa/airlines/1987.csv`

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/amazon/data20K

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from parquet.`dbfs:/databricks-datasets/amazon/data20K/part-r-00000-112e73de-1ab1-447b-b167-0919dd731adf.gz.parquet`

# COMMAND ----------

dbutils.fs.put("/FileStore/emp_new.csv", """id,first name,loc
1,Vyshnavi,hyderabad
2,Revanth,Mumbai
3,MohanRaj,Chennai""", True)    #Here, True is to overwrite the file if that file already exist.

# COMMAND ----------

df = spark.read.csv("/FileStore/emp_new.csv", header = True)
df.createOrReplaceTempView("v_emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --select * from v_emp
# MAGIC --But we need to have Accute Accent when we have spaces in columns which shown as below. Or else, we will face an exception
# MAGIC select id, `first name`, loc from v_emp

# COMMAND ----------

display(df)

# COMMAND ----------

#df.selectExpr("id","first name").show()
#The above one will also throw error if column name has spaces
#df.select("id","first name")
#The above one will work

#from pyspark.sql.functions import col
df.withColumn("new_column", col("first name")).show()

# COMMAND ----------

df.write.format("delta").saveAsTable("employee_new_data")
# This will throw error as Delta will not allow column names having spaces.