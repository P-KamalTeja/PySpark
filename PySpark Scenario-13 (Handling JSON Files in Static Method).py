# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Converting Semi-Structured Data to Structured Data.
# MAGIC
# MAGIC Usually, If the source is WEB API, then we can expect complex JSON files. 

# COMMAND ----------

# DBTITLE 1,Importing Necessary Packages
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------

dbutils.fs.put("/scenarios/complex_json.json","""[{
	"id": "0001",
	"type": "donut",
	"name": "Cake",
	"ppu": 0.55,
	"batters":
		{
			"batter":
				[
        
					{ "id": "1001", "type": "Regular" },
					{ "id": "1002", "type": "Chocolate" },
					{ "id": "1003", "type": "Blueberry" },
					{ "id": "1004", "type": "Devil's Food" }
				]
		},
	"topping":
		[
			{ "id": "5001", "type": "None" },
			{ "id": "5002", "type": "Glazed" },
			{ "id": "5005", "type": "Sugar" },
			{ "id": "5007", "type": "Powdered Sugar" },
			{ "id": "5006", "type": "Chocolate with Sprinkles" },
			{ "id": "5003", "type": "Chocolate" },
			{ "id": "5004", "type": "Maple" }
		]
}]""",True)

# COMMAND ----------

#from pyspark.sql.functions import explode,col
df_json = spark.read.option("multiline","true").json("/scenarios/complex_json.json")
df_json.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Complex Data Types in JSON:
# MAGIC #1. Struct (Dictionary Fomat)
# MAGIC #2. Array (List Format)
# MAGIC #3. Map

# COMMAND ----------

display(df_json)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #To flattern complex datatype data, we can use explode() or explode_outer() function for Array datatype.

# COMMAND ----------

display(df_json.select("batters.batter"))

# COMMAND ----------

# from pyspark.sql.functions import explode,col

df_json = spark.read.option("multiline","true").json("/scenarios/complex_json.json")
df_json.printSchema()
#display(df_json.select(explode("batters.batter")))

# COMMAND ----------

df_final = df_json.withColumn("topping_explode",explode("topping"))\
                .withColumn("topping_id",col("topping_explode.id"))\
                .withColumn("topping_type",col("topping_explode.type"))\
                .drop("topping","topping_explode")\
                .withColumn("batter_explode",explode("batters.batter"))\
                .withColumn("batter_id",col("batter_explode.id"))\
                .withColumn("batter_type",col("batter_explode.type"))\
                .drop("batters","batter_explode")
display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Here, we have used explode()or explode_outer() function to flattern Array Datatype.
# MAGIC
# MAGIC #For Struct Datatype, we have individually extracted data (Eg: col("topping_explode.id"))