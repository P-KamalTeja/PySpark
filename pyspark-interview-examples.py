from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, sum, avg, dense_rank, percent_rank, broadcast, explode, sequence, to_date
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

def create_spark_session():
    return SparkSession.builder.appName("PySpark Interview Examples").getOrCreate()

def complex_aggregation_and_window_functions(sales_df):
    """
    Demonstrates window functions for sales analysis
    - Running total
    - Moving average
    - Ranking products within categories
    """
    # Define window specifications
    category_window = Window.partitionBy("category").orderBy("date")
    rank_window = Window.partitionBy("category").orderBy(col("sales").desc())
    
    enriched_df = sales_df.withColumns({
        "running_total": sum("sales").over(category_window),
        "moving_avg": avg("sales").over(category_window.rowsBetween(-2, 0)),
        "product_rank": dense_rank().over(rank_window),
        "product_percent_rank": percent_rank().over(rank_window)
    }).filter(col("product_rank") <= 3)
    
    return enriched_df

def advanced_joins_and_data_enrichment(sales_df, product_df, customer_df):
    """
    Perform advanced multi-way joins with optimization
    - Broadcast join for small dimension tables
    - Handling potential data skew
    """
    # Broadcast join for small tables
    enriched_sales = sales_df.join(
        broadcast(product_df), 
        sales_df.product_id == product_df.id, 
        "left"
    ).join(
        broadcast(customer_df), 
        sales_df.customer_id == customer_df.id, 
        "left"
    )
    
    return enriched_sales

def performance_optimization(large_df):
    """
    Optimize DataFrame operations
    - Caching
    - Repartitioning
    - Predicate pushdown
    """
    # Cache frequently used dataframes
    large_df.cache()
    
    # Repartition to handle data skew
    optimized_df = large_df.repartition(200, "key_column") \
        .filter(col("important_column") > 100) \
        .select("key_column", "value_column")
    
    return optimized_df

def advanced_dataframe_transformations(complex_df):
    """
    Complex DataFrame transformations
    - Pivoting
    - Custom aggregations
    - Handling nested data
    """
    # Pivot example
    pivoted_df = complex_df.groupBy("category") \
        .pivot("product_type") \
        .sum("sales")
    
    # Explode nested arrays
    exploded_df = complex_df.select(
        col("category"),
        explode(col("nested_array")).alias("exploded_items")
    )
    
    return pivoted_df, exploded_df

def streaming_data_processing(spark):
    """
    Structured Streaming with advanced features
    - Windowing
    - Watermarking
    - State management
    """
    # Read from Kafka stream
    stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sales_topic") \
        .load()
    
    # Process with windowing and watermarking
    processed_stream = stream_df \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes", "2 minutes"),
            col("product_category")
        ).count()
    
    return processed_stream

def ml_pipeline_integration(training_df):
    """
    Comprehensive ML Pipeline
    - Feature engineering
    - Model training
    - Cross-validation
    """
    # Feature assembler
    feature_cols = ["feature1", "feature2", "feature3"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    
    # Logistic Regression with cross-validation
    lr = LogisticRegression(maxIter=10, regParam=0.01)
    
    # Create ML Pipeline
    pipeline = Pipeline(stages=[
        assembler,
        lr
    ])
    
    # Fit the pipeline
    model = pipeline.fit(training_df)
    
    return model

def complex_partitioning(df):
    """
    Advanced partitioning strategies
    - Dynamic partition pruning
    - Bucketing
    """
    # Repartition and bucket
    optimized_df = df.repartition(200, "category") \
        .write \
        .bucketBy(10, "sub_category") \
        .sortBy("sales") \
        .saveAsTable("optimized_sales_table")
    
    return optimized_df

# Main execution
def main():
    spark = create_spark_session()
    
    # Example dataframes (you'd typically load these from sources)
    sales_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("sales", DoubleType(), True),
        StructField("date", DateType(), True),
        StructField("category", StringType(), True)
    ])
    
    sales_df = spark.createDataFrame([], sales_schema)
    
    # Demonstrate each function
    complex_aggregation_result = complex_aggregation_and_window_functions(sales_df)
    # ... other function calls
