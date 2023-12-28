# Databricks notebook source
# DBTITLE 1,Importing Necessary Packages
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------

# DBTITLE 1,Create new data file using python dynamically.
def createFile(filename):
    f=open("/tmp/files/"+filename+".txt","w")
    for i in range(100000):
        f.write("sample data writting : "+str(i))
    f.close()
    print("File Created as : "+str(filename))
    return filename

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC PySpark will default communicate and prefer defalut File System called DBFS which starts with dbfs.
# MAGIC
# MAGIC Core Python will default communicate and prefer defalut File System called Linux box which is outside of DBFS.

# COMMAND ----------

# DBTITLE 1,Creating a folder in Core Python default file system
# MAGIC %fs mkdirs file:/tmp/files/

# COMMAND ----------

# MAGIC %fs ls file:/tmp/files/

# COMMAND ----------

createFile("sample")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Python MultiProcessing
# MAGIC multiprocessing is a package that supports spawning processes using an API similar to the threading module. The multiprocessing package offers both local and remote concurrency, effectively side-stepping the Global Interpreter Lock by using subprocesses instead of threads. Due to this, the multiprocessing module allows the programmer to fully leverage multiple processors on a given machine. It runs on both Unix and Windows.
# MAGIC

# COMMAND ----------

from multiprocessing.pool import ThreadPool
parallels = ThreadPool(8)
parallels.map(createFile,[ "sample"+str(i) for i in range(100)])

# COMMAND ----------

# MAGIC %fs ls file:/tmp/files/