# Databricks notebook source
# MAGIC %md
# MAGIC Data Partitioning is critical to data processing performance especially for large volume of data processing in Spark.
# MAGIC
# MAGIC Here, we are partitioning DataFrame partition by year/month/sub-directory ?

# COMMAND ----------

from pyspark.sql.functions import month,year,quarter, count, asc, desc, rank, countDistinct, to_date, date_format
from pyspark.sql.functions import month,year,quarter, count, asc, desc, rank, countDistinct

# COMMAND ----------

df_emp_csv = spark.read.option("nullvalue","null").csv("/FileStore/tables/emp.csv", header = True, inferSchema = True)
display(df_emp_csv)

# COMMAND ----------

# to convert any column from String to Date, to_date('column_name', 'pattern of date')
df_emp_csv = df_emp_csv.fillna({'HIREDATE':'9999-12-31'})
display(df_emp_csv)

# COMMAND ----------

df_emp_csv = df_emp_csv.withColumn("Year", year(df_emp_csv.HIREDATE)).withColumn("Month", date_format("HIREDATE", "MM"))
df_emp_csv.show()

# COMMAND ----------

#Using partitionBy to partiton the data based on Year and Month
df_emp_csv.write.format('delta').partitionBy('Year', 'Month').mode('overwrite').saveAsTable("emp_part")

# COMMAND ----------

# MAGIC %fs ls /user/hive/warehouse/emp_part/

# COMMAND ----------

# MAGIC %fs ls /user/hive/warehouse/emp_part/Year=1980/

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp_part where year = 1980;

# COMMAND ----------

