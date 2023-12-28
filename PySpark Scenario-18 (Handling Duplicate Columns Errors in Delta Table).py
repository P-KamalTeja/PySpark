# Databricks notebook source
# DBTITLE 1,Importing Necessary Packages
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------

dbutils.fs.put("/scenarios/dept.csv","""Deptno,Dname,Loc
10,ACCOUNTING,NEW YORK
20,RESEARCH,DALLAS
30,SALES,CHICAGO
40,OPERATIONS,BOSTON""", True)

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/

# COMMAND ----------

df_emp_csv = spark.read.csv("/FileStore/tables/emp.csv", header = True)
df_dept_csv = spark.read.csv("/scenarios/dept.csv", header = True)

# COMMAND ----------

display(df_emp_csv)
display(df_dept_csv)

# COMMAND ----------

final_df = df_emp_csv.join(df_dept_csv, df_emp_csv["DEPTNO"] == df_dept_csv["Deptno"], 'inner')
display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC The below command will throw an error as "DEPTNO" is repating column with same name in both DataFrames. In DataFrames, there wont be any issue but while writing them into a table in Delta format, it will throw an error.

# COMMAND ----------

final_df.write.format("delta").saveAsTable("emp_dept_csv")

# COMMAND ----------

# MAGIC %md
# MAGIC To overcome above error we are joining data using LEFT Join.

# COMMAND ----------

final_df_2 = df_emp_csv.join(df_dept_csv, ["Deptno"], 'left')
display(final_df_2)

# COMMAND ----------

final_df_2.write.format("delta").saveAsTable("emp_dept_csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM emp_dept_csv;