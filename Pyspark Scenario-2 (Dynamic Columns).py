# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC How to read Data having Variable/ Dynamic no.of columns source. (We are not sure, how many columns we are gonna get from source)

# COMMAND ----------

# DBTITLE 1,Importing Necessary packages
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType, FloatType
from pyspark.sql.functions import month,year,quarter, count, asc, desc, rank, countDistinct, split, size, max

# COMMAND ----------

# DBTITLE 1,To remove files in DBFS (DataBricks File System)
#dbutils.fs.rm("/scenarios/dynamic_columns_without_headers.csv")

# COMMAND ----------

# DBTITLE 1,Dynamic Columns Data with Headers
dbutils.fs.put("/scenarios/dynamic_columns.csv", """id,name,loc,email_id,phone
            1,Ravi
            2,Ram,Hyderabad
            3,Prasad,Chennai, sample@gmail.com, 99909999
            4,Vyshnavi,Pune
            """)

# COMMAND ----------

# DBTITLE 1,Dynamic Columns Data without Headers
dbutils.fs.put("/scenarios/dynamic_columns_without_headers.csv", """1,Ravi
2,Ram,Hyderabad
3,Prasad,Chennai, sample@gmail.com, 99909999
4,Vyshnavi,Pune""")

# COMMAND ----------

# DBTITLE 1,Reading file having headers
df_with_headers = spark.read.csv("/scenarios/dynamic_columns.csv", header = True)
display(df_with_headers)

# COMMAND ----------

# DBTITLE 1,Reading File which not having headers
df_without_headers = spark.read.text("/scenarios/dynamic_columns_without_headers.csv")
display(df_without_headers)

# COMMAND ----------

# DBTITLE 1,Splitting Columns of DF not having headers
df_without_headers = df_without_headers.withColumn("splittable_col", split("value", ",").alias("splittable_col")).drop("value")
display(df_without_headers)

# COMMAND ----------

# DBTITLE 1,Getting size of each row
df_without_headers.select('splittable_col', size('splittable_col').alias('size of each row')).show(truncate=False) #To get size of each row
df_without_headers.select(max(size('splittable_col'))).show() # Max.number we will get

# COMMAND ----------

for i in range(df_without_headers.select(max(size('splittable_col'))).collect()[0][0]):
    df_without_headers = df_without_headers.withColumn("col"+str(i), df_without_headers['splittable_col'][i])
display(df_without_headers)   #showing data on how dynamic columns getting splitted
final_df_without_headers = df_without_headers.drop('splittable_col')
display(final_df_without_headers) #Dropping unnecessary columns from df_without_headers