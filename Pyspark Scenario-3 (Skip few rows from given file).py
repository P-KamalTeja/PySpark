# Databricks notebook source
# DBTITLE 1,Importing Necessary Packages
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType, FloatType
from pyspark.sql.functions import month,year,quarter, count, asc, desc, rank, countDistinct, split, size, max

# COMMAND ----------

# DBTITLE 1,To remove files in DBFS (DataBricks File System)
#dbutils.fs.rm("/scenarios/skiplines.csv")

# COMMAND ----------

# DBTITLE 1,Creating Req. File
dbutils.fs.put("/scenarios/skiplines.csv", """line1
line2
line3
line4
id,name,loc
1,Suresh,Bangalore
2,Ramesh,Chennai
3,Navya,Hyderabad
4,Sindhu,Kochi""", True) #Here True is for if file exist already, to overwrite, it helps

# COMMAND ----------

# DBTITLE 1,Example how normal spark.read.csv reads data
df = spark.read.csv("/scenarios/skiplines.csv", header = True)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC sc.textfile() vs spark.read.text()
# MAGIC
# MAGIC 1.sc.textfile(): This is a method of the SparkContext(sc). It's used in the RDD(Resilient Distributed Dataset) API, which is the original Spark API. This method reads a text file and returns in RDD of string. Each element in the RDD represents a line in the text file.
# MAGIC             rdd = sc.textFile("path_to_your_file.txt")
# MAGIC
# MAGIC 2.spark.read.text(): This is a method of SparkSession(spark). It's used in the DataFrame API, which is newer API that provides more capabilities Aand is generally easier to use. this method reads a text file and returns a DataFrame. The DataFrame has a single column named 'value', and each row int he DataFrame represents a line in the text file.
# MAGIC             df = spark.read.text("path_to_your_file.txt")
# MAGIC
# MAGIC Note: In general, if you're starting a new project with Spark, it's recommended to use th DataFrame API because it's more powerful and easier to use. However, if you're working with existing code that uses the RDD API, you might need to use sc.textFile()

# COMMAND ----------

rdd = sc.textFile("/scenarios/skiplines.csv")
rdd.zipWithIndex().collect()  # Creates Sequence for every Item.

# COMMAND ----------

rdd.zipWithIndex().filter(lambda a:a[1]>3).collect()

# COMMAND ----------

rdd.zipWithIndex().filter(lambda a:a[1]>3).map(lambda a:a[0].split(",")).collect()

# COMMAND ----------

rdd_final = rdd.zipWithIndex().filter(lambda a:a[1]>3).map(lambda a:a[0].split(","))
rdd_final.collect() #collect() is used to retrive all the elements of a DataFarame or RDD from the distributive system to Driver Node.

# COMMAND ----------

columns = rdd_final.collect()[0]

# COMMAND ----------

skipline = rdd_final.first()
df = rdd_final.filter(lambda a:a != skipline).toDF(columns)
display(df)