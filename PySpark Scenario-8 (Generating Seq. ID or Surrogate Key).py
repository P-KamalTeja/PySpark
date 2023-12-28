# Databricks notebook source
# DBTITLE 1,Import Necessary Packages
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Methods to generate Sequence ID/ Surrogate Key in PySpark 
# MAGIC
# MAGIC 1.) monotonically_increasing_id()
# MAGIC
# MAGIC 2.) row_number() (window function)
# MAGIC
# MAGIC 3.) crc32
# MAGIC
# MAGIC 4.) md5
# MAGIC
# MAGIC 5.) sha1 and sha2

# COMMAND ----------

# DBTITLE 1,Importing Necessary Data as DataFrame
df_emp_csv = spark.read.option("nullValue", "null").csv("/FileStore/tables/emp.csv", header = True, inferSchema = True).dropna(how = 'all').dropDuplicates(["EMPNO"])

# COMMAND ----------

display(df_emp_csv)

# COMMAND ----------

# DBTITLE 1,Method-1: Using monotonically_increasing_id()
# from pyspark.sql.functions import monotonically_increasing_id

df_emp_csv = df_emp_csv.withColumn("id", monotonically_increasing_id()+1)
display(df_emp_csv)

# COMMAND ----------

# DBTITLE 1,Method-2: Using row_number() (Window Function)
#from pyspark.sql.functions import row_number, lit

from pyspark.sql.window import Window

df_emp_csv = df_emp_csv.withColumn("row_number", row_number().over(Window.partitionBy(lit('')).orderBy(lit(''))))
display(df_emp_csv)

# COMMAND ----------

# DBTITLE 1,Method-3: Using crc32
#from pyspark.sql.functions import crc32

df_emp_csv = df_emp_csv.withColumn("crc32_Key", crc32(col("EMPNO").cast("string")))
display(df_emp_csv)

# COMMAND ----------

# MAGIC %md
# MAGIC #About crc32:
# MAGIC crc32 will accept only String as input. It will generate some Hash Keys based on the column we represent. We can expect duplicates if we have data greater than 100k or 200k. So, it is not suggested for huge data tables.

# COMMAND ----------

# DBTITLE 1,Method-4: Using md5
#from pyspark.sql.functions import md5

df_emp_csv = df_emp_csv.withColumn("md5_Key", md5(col("EMPNO").cast("string")))
display(df_emp_csv)

# COMMAND ----------

# MAGIC %md
# MAGIC #About md5:
# MAGIC crc32 will accept only String as input. md5 will generate a 32bit hash key. This is suitable for the data having mor than 15 million or 20 million.

# COMMAND ----------

# DBTITLE 1,Method-5: Using shal1 and sha2
#from pyspark.sql.functions import sha2

df_emp_csv = df_emp_csv.withColumn("sha2_Key", sha2(col("EMPNO").cast("string"), 256))
display(df_emp_csv)

#Here depends upon volume of data, we can increase size to 512 also. For 15-20 million data, 256 is good to have. Greater than that, we can have 512.

# COMMAND ----------

# DBTITLE 1,Example of one sha2 code using SQL.
# MAGIC %sql
# MAGIC
# MAGIC select sha2("1234", 512)