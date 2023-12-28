# Databricks notebook source
# DBTITLE 1,Importing Necessary Packages
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------

# MAGIC %fs head /FileStore/tables/emp.csv

# COMMAND ----------

df_emp_csv = spark.read.option("nullValue", "null").csv("/FileStore/tables/emp.csv", header = True, inferSchema = True)
display(df_emp_csv)

# COMMAND ----------

display(df_emp_csv.filter("empno is null").count())

#empno is a column name. We can haver other column name to check count of nulls in that particular column.

# COMMAND ----------

#from pyspark.sql.functions import count, col, when

display(df_emp_csv.select([count(when(col(i).isNull(),i)).alias(i) for i in df_emp_csv.columns]))