# Databricks notebook source
# DBTITLE 1,Importing Necessary Packages
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------

# DBTITLE 1,Creating Input File
dbutils.fs.put("/scenarios/double_pipe.csv", """id||name||loc
1||Vinay||Bangalore
2||Vyshnavi||Hyderabad
3||Suresh||Chennai
4||Rajesh||Pune
5||Madhu||Hyderabad
""", True)

# COMMAND ----------

# DBTITLE 1,Reading File into a DataFrame
df = spark.read.csv("/scenarios/double_pipe.csv", header = True, sep = "||")
display(df)

# COMMAND ----------

# DBTITLE 1,Creating Input File
dbutils.fs.put("/scenarios/multi_sep.csv", """id,name,loc,marks
1,Vinay,Bangalore,35|45|55|65
2,Vyshnavi,Hyderabad,35|45|55|65
3,Suresh,Chennai,35|45|55|65
4,Rajesh,Pune,35|45|55|65
5,Madhu,Hyderabad,35|45|55|65
""", True)

# COMMAND ----------

# DBTITLE 1,Reading File into a DataFrame
df_multi = spark.read.csv("/scenarios/multi_sep.csv", header = True, sep = ",")
display(df_multi)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select split("1|2|3|4", "|")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select split("1|2|3|4", "\\|")
# MAGIC
# MAGIC --select split("1|2|3|4", "[|]")

# COMMAND ----------

#from pyspark.sql.functions import split, col

df_multi =df_multi.withColumn("marks_split",split(col("marks"),"[|]"))\
            .withColumn("SUB1",col("marks_split")[0])\
            .withColumn("SUB2",col("marks_split")[1])\
            .withColumn("SUB3",col("marks_split")[2])\
            .withColumn("SUB4",col("marks_split")[3]).drop("marks_split","marks")
display(df_multi)