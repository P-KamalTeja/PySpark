# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Working with Corrupt Data
# MAGIC 1. ETL pipelines need robust solutions to handle corrupt data.
# MAGIC
# MAGIC 2. This is because data corruption scales as the size of data and complexity of the data application grow. Corrupt data includes:
# MAGIC
# MAGIC     A. Missing information
# MAGIC
# MAGIC     B. Incomplete information
# MAGIC
# MAGIC     C. Schema mismatch
# MAGIC
# MAGIC     D. Differing formats or data types
# MAGIC
# MAGIC     E. User errors when writing data producers
# MAGIC
# MAGIC 3. Since ETL pipelines are built to be automated, production-oriented solutions must ensure pipelines behave as expected. This means that data engineers must both expect and systematically handle corrupt records.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # How to handle Bad Records in spark and those types?
# MAGIC 1. There are tree types of modes available while reading and creating dataframe.
# MAGIC
# MAGIC 2. Dealing with bad Records Verify correctness of the data When reading CSV files with a specified schema, it is possible that the data in the files does not match the schema.
# MAGIC
# MAGIC     A. PERMISSIVE (default): nulls are inserted for fields that could not be parsed correctly.
# MAGIC
# MAGIC     B. DROPMALFORMED: drops lines that contain fields that could not be parsed
# MAGIC     
# MAGIC     C. FAILFAST: aborts the reading if any malformed data is found

# COMMAND ----------

# DBTITLE 1,Importing Necessary Packages
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------

dbutils.fs.put("/FileStore/tables/channels.csv","""CHANNEL_ID,CHANNEL_DESC,CHANNEL_CLASS,CHANNEL_CLASS_ID,CHANNEL_TOTAL,CHANNEL_TOTAL_ID
3,Direct Sales,Direct,12,Channel total,1
9,Tele Sales,Direct,12,Channel total,1
5,Catalog,Indirect,13,Channel total,1
4,Internet,Indirect,13,Channel total,1
2,Partners,Others,14,Channel total,1
12,Partners,Others,14,Channel total,1,45,ram,3434
sample,Partners,Others,14,Channel total,1,45,ram,3434
10 Partners Others 14 Channel total 1
11 Partners Others 14 Channel total 1""")

# COMMAND ----------

help(spark.read.csv)

# COMMAND ----------

# MAGIC %md
# MAGIC We have few options mentioned below while reading data:
# MAGIC
# MAGIC 1. 'option("mode","PERMISSIVE")' is to allow bad data. It is a one of the default read mode.
# MAGIC 2. 'option("mode","FAILFAST")' is to fail the job when it observes bad data.
# MAGIC 3. 'option("mode","DROPMALFORMED")' is helps to removes bad data while viewing the records. 
# MAGIC 4. 'option("badRecordsPath","/channels/baddata/")' is helps to delete bad data and will store those bad records in given path(/channels/baddata/). It will store in JSON format.

# COMMAND ----------

#from pyspark.sql.types import * 
schema_channels = StructType([StructField('CHANNEL_ID', IntegerType(), True), 
                              StructField('CHANNEL_DESC', StringType(), True), 
                              StructField('CHANNEL_CLASS', StringType(), True), 
                              StructField('CHANNEL_CLASS_ID', IntegerType(), True), 
                              StructField('CHANNEL_TOTAL', StringType(), True), 
                              StructField('CHANNEL_TOTAL_ID', IntegerType(), True),
                              StructField('BadData', StringType(), True)])
#df_channels = spark.read.option("mode","PERMISSIVE").csv("/FileStore/tables/channels.csv",header=True,columnNameOfCorruptRecord="BadData")
#df_channels = spark.read.option("mode","FAILFAST").csv("/FileStore/tables/channels.csv",header=True,columnNameOfCorruptRecord="BadData")
#df_channels = spark.read.option("mode","DROPMALFORMED").csv("/FileStore/tables/channels.csv",header=True,columnNameOfCorruptRecord="BadData")
#df_channels = spark.read.option("badRecordsPath","/channels/badata/").csv("/FileStore/tables/channels.csv",header=True,columnNameOfCorruptRecord="BadData")
#The above are diff. options we have and the below one is one another option.
df_channels = spark.read.schema(schema_channels).csv("/FileStore/tables/channels.csv",header=True,columnNameOfCorruptRecord="BadData")
display(df_channels)

# COMMAND ----------

# MAGIC %md
# MAGIC All these below commands are used when we use 'option("badRecordsPath","/channels/badata/")'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 1. If any invalid data based schema. it will create timestamp based folder and place json log file
# MAGIC 2. It will store three fields,filename with path,error reason,errored data
# MAGIC 3. Azure Databricks provides a unified interface for handling bad records and files without interrupting Spark jobs.
# MAGIC 4. You can obtain the exception records/files and reasons from the exception logs by setting the data source option badRecordsPath.
# MAGIC 5. badRecordsPath specifies a path to store exception files for recording the information about bad records for CSV and JSON sources and bad files for all the file-based built-in sources (for example, Parquet).

# COMMAND ----------

# DBTITLE 1,To read BadData
df_baddata = spark.read.json("/channels/badata/*/*/")
display(df_baddata)

# COMMAND ----------

# MAGIC %fs ls /channels/badata/

# COMMAND ----------

# MAGIC %fs ls dbfs:/channels/badata/20231226T144624/bad_records

# COMMAND ----------

# MAGIC %fs head dbfs:/channels/badata/20231226T144624/bad_records/part-00000-7fe0de57-08e3-4377-8cd5-1c96fdb34c39

# COMMAND ----------

# MAGIC %md
# MAGIC All the below commands are used when, 
# MAGIC
# MAGIC df_channels = spark.read.schema(schema_channels).csv("/FileStore/tables/channels.csv",header=True,columnNameOfCorruptRecord="BadData")

# COMMAND ----------

df_channels.cache()
good_data = df_channels.filter("BadData is null").drop("BadData")
bad_data = df_channels.filter("BadData is not null").select("BadData")

# COMMAND ----------

display(bad_data)