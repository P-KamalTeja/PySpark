from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, avg, round

def transform_sales_data():
    # Create Spark Session
    spark = SparkSession.builder.appName("SalesDataTransformation").getOrCreate()
    
    # Sample sales data
    sales_data = [
        ("Electronics", "Laptop", 1200, 50, "2023-01-15"),
        ("Electronics", "Phone", 800, 75, "2023-02-20"),
        ("Clothing", "Shirt", 50, 200, "2023-03-10"),
        ("Clothing", "Jeans", 100, 150, "2023-04-05"),
        ("Electronics", "Tablet", 500, 100, "2023-05-12")
    ]
    
    # Create DataFrame
    df = spark.createDataFrame(sales_data, 
        ["category", "product", "price", "quantity", "sale_date"])
    
    # Transformations
    transformed_df = df.select(
        col("category"),
        col("product"),
        col("price"),
        col("quantity"),
        # Add total sales column
        round(col("price") * col("quantity"), 2).alias("total_sales"),
        # Categorize products
        when(col("price") < 500, "Budget")
        .when((col("price") >= 500) & (col("price") < 1000), "Mid-range")
        .otherwise("Premium").alias("price_category")
    )
    
    # Aggregate transformations
    category_summary = transformed_df.groupBy("category").agg(
        sum("total_sales").alias("total_category_sales"),
        avg("price").alias("average_product_price"),
        sum("quantity").alias("total_quantity_sold")
    )
    
    return transformed_df, category_summary

# Execute transformations
individual_products, category_summary = transform_sales_data()

# Show results
individual_products.show()
category_summary.show()
