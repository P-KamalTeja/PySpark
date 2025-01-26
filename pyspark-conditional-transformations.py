from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, expr

def conditional_transformations():
    # Create SparkSession
    spark = SparkSession.builder.appName("ConditionalExample").getOrCreate()
    
    # Sample data
    data = [
        ("Alice", 25, 50000),
        ("Bob", 35, 75000),
        ("Charlie", 45, 60000),
        ("David", 55, 90000)
    ]
    
    # Create DataFrame
    df = spark.createDataFrame(data, ["name", "age", "salary"])
    
    # Using when() and otherwise()
    categorized_df = df.withColumn("salary_category", 
        when(col("salary") < 60000, "Low")
        .when((col("salary") >= 60000) & (col("salary") < 80000), "Medium")
        .otherwise("High")
    )
    
    # Using expr() for more complex conditions
    age_category_df = df.withColumn("age_group", 
        expr("""
            CASE 
                WHEN age < 30 THEN 'Young'
                WHEN age >= 30 AND age < 50 THEN 'Middle-aged'
                ELSE 'Senior'
            END
        """)
    )
    
    # Multiple conditions example
    complex_df = df.withColumn("employee_status", 
        when((col("age") < 30) & (col("salary") > 60000), "High Potential")
        .when((col("age") >= 30) & (col("age") < 50) & (col("salary") >= 60000), "Established")
        .otherwise("Standard")
    )
    
    return categorized_df, age_category_df, complex_df

# Demonstrate the functions
result_dfs = conditional_transformations()
for df in result_dfs:
    df.show()
