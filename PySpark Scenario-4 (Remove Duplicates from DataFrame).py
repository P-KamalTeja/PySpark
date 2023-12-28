# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC To remove duplicates from DataFrame: 
# MAGIC
# MAGIC 1.) distinct()
# MAGIC
# MAGIC 2.) dropDuplicates() /  drop_duplicates()
# MAGIC
# MAGIC 3.) Window FUnction with row_number()
# MAGIC
# MAGIC 4.) groupBy with count()

# COMMAND ----------

# DBTITLE 1,Importing Necessary Packages
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType, FloatType
from pyspark.sql.functions import month,year,quarter, count, asc, desc, rank, countDistinct, split, size, max, col, row_number
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,To remove files in DBFS (DataBricks File System)
#dbutils.fs.rm("/scenarios/skiplines.csv")

# COMMAND ----------

# DBTITLE 1,Creating Req. File
dbutils.fs.put("/scenarios/duplicates.csv", """id,name,loc,updated_date
1,Ravi,Bangalore,2021-01-01
2,Ravi,Chennai,2022-02-02
3,Ravi,Hyderabad,2022-06-10
2,Raj,Bangalore,2021-01-01
2,Raj,Chennai,2022-02-02
3,Raj,hyderabad,2022-06-10
4,Prasad,bangalore,2021-01-01
5,Mahesh,Chennai,2022-02-02
4,Prasad,hyderabad,2022-06-10
""")

# COMMAND ----------

# DBTITLE 1,Reading file into a DataFrame
df = spark.read.csv("/scenarios/duplicates.csv", header = True, inferSchema = True)
df.printSchema() #To print schema of the file

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.dropDuplicates())

# COMMAND ----------

display(df.distinct())

# COMMAND ----------

display(df.dropDuplicates(["id"])) # Removing Duplicates based on ID column

# COMMAND ----------

# DBTITLE 1,Removed Duplicates using Date Column
display(df.orderBy(col("updated_date").desc()).dropDuplicates(["id"]))

# COMMAND ----------

# MAGIC %md
# MAGIC #Another Method: We can achive this method using Window Functions using row_number()

# COMMAND ----------

window_df = df.withColumn("row_id", row_number().over(Window.partitionBy("id").orderBy(desc('updated_date'))))
window_good_df = window_df.filter('row_id=1')
display(window_good_df)

# COMMAND ----------

window_bad_df = window_df.filter('row_id > 1')
display(window_bad_df)