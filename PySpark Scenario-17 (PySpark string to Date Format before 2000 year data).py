# Databricks notebook source
# MAGIC %md
# MAGIC It's always better to have year format as YYYY instead of YY. If we have that kind of data, the below process will help us to load historical data where YY format is present in it.

# COMMAND ----------

# DBTITLE 1,Importing Necessary Packages
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------

dbutils.fs.put("/scenarios/emp_data.csv","""EMPNO,ENAME,JOB,MGR,HIREDATE,SAL,COMM,DEPTNO
7369,SMITH,CLERK,7902,17-12-80,800,null,20
7499,ALLEN,SALESMAN,7698,20-02-81,1600,300,30
7521,WARD,SALESMAN,7698,22-02-81,1250,500,30
7566,JONES,MANAGER,7839,04-02-81,2975,null,20
7654,MARTIN,SALESMAN,7698,21-09-81,1250,1400,30
7698,SGR,MANAGER,7839,05-01-81,2850,null,30
7782,RAVI,MANAGER,7839,06-09-81,2450,null,10
7788,SCOTT,ANALYST,7566,19-04-87,3000,null,20
7839,KING,PRESIDENT,null,01-11-81,5000,null,10
7844,TURNER,SALESMAN,7698,09-08-81,1500,0,30
7876,ADAMS,CLERK,7788,23-05-87,1100,null,20
7900,JAMES,CLERK,7698,12-03-81,950,null,30
7902,FORD,ANALYST,7566,12-03-81,3000,null,20
7934,MILLER,CLERK,7782,01-03-82,1300,null,10
1234,SEKHAR,doctor,7777,31-12-99,667,78,80""",True)

# COMMAND ----------

df_csv = spark.read.option("nullValue","null").csv("/scenarios/emp_data.csv",header=True,inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC "LEGACY" helps to consider 95(yy) as 1995 instead of 2095

# COMMAND ----------

#spark.conf.set("spark.sql.legacy.timeParserPolicy","EXCEPTION") 
# This is a default policy and we are setting policy to legacy to conside 85(YY) as 1985 instead of 2085

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

# COMMAND ----------

# MAGIC %sql
# MAGIC select to_date("01-JAN-95","dd-MMM-yy")

# COMMAND ----------

#from pyspark.sql.functions import to_date,col
display(df_csv.withColumn("HIREDATE",to_date("HIREDATE","dd-MM-yy")))
# Default Spark Date Format yyyy-MM-dd