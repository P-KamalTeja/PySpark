# Databricks notebook source
# DBTITLE 1,Importing Required Packages
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType, FloatType
from pyspark.sql.functions import month,year,quarter, count, asc, desc, rank, countDistinct

# COMMAND ----------

# DBTITLE 1,Creating Sales DataFrame
sales_schema = StructType(
    fields = [StructField("product_id", IntegerType(), True),
              StructField("customer_id", StringType(), True),
              StructField("order_date", DateType(), True),
              StructField("location", StringType(), True),
              StructField("source_order", StringType(), True)
              ]
)

sales_df = spark.read.format("csv").option("inferschema",True).schema(sales_schema).load("/FileStore/tables/sales.csv")
display(sales_df)

# COMMAND ----------

# DBTITLE 1,Creating Menu DataFrame
menu_schema = StructType(
    fields = [StructField("product_id", IntegerType(), True),
              StructField("product_name", StringType(), True),
              StructField("price", StringType(), True)
    ]
)

menu_df = spark.read.format("csv").option("inferschema",True).schema(menu_schema).load("/FileStore/tables/menu.csv")
display(menu_df)

# COMMAND ----------

# DBTITLE 1,Adding New Columns in Sales DataFrame
sales_df = sales_df.withColumn("Order_Year", year(sales_df.order_date))
sales_df = sales_df.withColumn("Order_Month", month(sales_df.order_date))
sales_df = sales_df.withColumn("Order_Quarter", quarter(sales_df.order_date))
display(sales_df)

# COMMAND ----------

# DBTITLE 1,Total Amount Spend By Each Customer
total_amount_spent_by_cusid = (sales_df.join(menu_df, 'product_id').groupBy('customer_id')
                      .agg({'price':'sum'})
                      .orderBy('customer_id'))
display(total_amount_spent_by_cusid)

# COMMAND ----------

# DBTITLE 1,Total Amount Spend By Food Category
total_amount_spent_by_food = (sales_df.join(menu_df, 'product_id').groupBy('product_name')
                      .agg({'price':'sum'}).withColumnRenamed('sum(price)','Total_Amount_Spend_By_Customers')
                      .orderBy('product_name'))
display(total_amount_spent_by_food)

# COMMAND ----------

# DBTITLE 1,Yearly Sales
yearly_sales = (sales_df.join(menu_df, 'product_id').groupBy('order_year')
                      .agg({'price':'sum'}))
display(yearly_sales)

# COMMAND ----------

# DBTITLE 1,How many times each product purchased
yearly_sales = (sales_df.join(menu_df, 'product_id').groupBy('product_id', 'product_name')
                      .agg(count('product_id').alias('product_count'))
                      .orderBy(desc('product_count'))
                      .drop('product_id')
                      .limit(3))
display(yearly_sales)

# COMMAND ----------

# DBTITLE 1,Frequency of a customer visited to Restaurant
visited_customers = sales_df.filter(sales_df.source_order == "Restaurant").groupBy('customer_id').agg(countDistinct('order_date'))
display(visited_customers)

# COMMAND ----------

# DBTITLE 1,Total Amount Spend on Source_Order Basis
total_amt_source_order = (sales_df.join(menu_df, 'product_id').groupBy('source_order')
                      .agg({'price':'sum'}))
display(total_amt_source_order)