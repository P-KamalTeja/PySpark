# Databricks notebook source
# DBTITLE 1,Importing Necessary Packages
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------

df = spark.createDataFrame([(y-1,"01/01/"+str(y)+"") for y in range(1999,2050)],["year_num","year"])

# COMMAND ----------

# from pyspark.sql.functions import to_date,weekofyear
# using to_date function we can change string format to date format
# using weekofyear function we can get week number on particular date value
df = df.withColumn("date",to_date("year","dd/MM/yyyy")).withColumn("week_number",weekofyear("date"))
display(df.filter("week_number=53"))
display(df)