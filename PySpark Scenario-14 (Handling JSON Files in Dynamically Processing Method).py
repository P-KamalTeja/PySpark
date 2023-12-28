# Databricks notebook source
# DBTITLE 1,Importing Necessary Packages
from pyspark.sql.types import * 
from pyspark.sql.functions import * 

# COMMAND ----------

dbutils.fs.put("/FileStore/tables/sample.json","""{
  "name":"MSFT","location":"Redmond", "satellites": ["Bay Area", "Shanghai"],
  "goods": {
    "trade":true, "customers":["government", "distributer", "retail"],
    "orders":[
        {"orderId":1,"orderTotal":123.34,"shipped":{"orderItems":[{"itemName":"Laptop","itemQty":20},{"itemName":"Charger","itemQty":2}]}},
        {"orderId":2,"orderTotal":323.34,"shipped":{"orderItems":[{"itemName":"Mice","itemQty":2},{"itemName":"Keyboard","itemQty":1}]}}
    ]}}
{"name":"Company1","location":"Seattle", "satellites": ["New York"],
  "goods":{"trade":false, "customers":["store1", "store2"],
  "orders":[
      {"orderId":4,"orderTotal":123.34,"shipped":{"orderItems":[{"itemName":"Laptop","itemQty":20},{"itemName":"Charger","itemQty":3}]}},
      {"orderId":5,"orderTotal":343.24,"shipped":{"orderItems":[{"itemName":"Chair","itemQty":4},{"itemName":"Lamp","itemQty":2}]}}
    ]}}
{"name": "Company2", "location": "Bellevue",
  "goods": {"trade": true, "customers":["Bank"], "orders": [{"orderId": 4, "orderTotal": 123.34}]}}
{"name": "Company3", "location": "Kirkland"}""",True)

# COMMAND ----------

df_json = spark.read.option("multiLine","true").json("/FileStore/tables/sample.json")
display(df_json)

# COMMAND ----------

df_json.dtypes

# COMMAND ----------

df_json.printSchema()

# COMMAND ----------

def child_struct(nested_df):
    # Creating python list to store dataframe metadata
    list_schema = [((), nested_df)]
    # Creating empty python list for final flattern columns
    flat_columns = []

    while len(list_schema) > 0:
      # Removing latest or recently added item (dataframe schema) and returning into df variable  
          parents, df = list_schema.pop()
          flat_cols = [  col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],))) for c in df.dtypes if c[1][:6] != "struct"   ]
      
          struct_cols = [  c[0]   for c in df.dtypes if c[1][:6] == "struct"   ]
      
          flat_columns.extend(flat_cols)
          #Reading  nested columns and appending into stack list
          for i in struct_cols:
                projected_df = df.select(i + ".*")
                list_schema.append((parents + (i,), projected_df))
    return nested_df.select(flat_columns)

# COMMAND ----------

# from pyspark.sql.functions import *
def master_array(df):
    array_cols = [c[0] for c in df.dtypes if c[1][:5]=="array"]
    while len(array_cols)>0:
        for c in array_cols:
            df = df.withColumn(c,explode_outer(c))
        df = child_struct(df)
        array_cols = [c[0] for c in df.dtypes if c[1][:5]=="array"]
    return df

# COMMAND ----------

df_output = master_array(df_json)

# COMMAND ----------

display(df_output)