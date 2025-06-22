from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

#S3 bucket config
S3_BUCKET = 'pipeline-bucket-fakestore'
RAW_FILES = f's3a://{S3_BUCKET}/ecommerce/raw/'
PROCESSED_FILES = f's3a://{S3_BUCKET}/ecommerce/processed/'

def main():
    spark = SparkSession.builder    \
        .appName("Order Processing")    \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")   \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
        .getOrCreate()
    
    #Load raw data
    users_df = spark.read.option("header", True).csv(RAW_FILES + 'users.csv')
    products_df = spark.read.option("header", True).csv(RAW_FILES + 'products.csv')
    orders_df = spark.read.option("header", True).csv(RAW_FILES + 'orders.csv')
    items_df = spark.read.option("header", True).csv(RAW_FILES + 'ordered_items.csv')

    #Convert types
    for df, columns in [
        (orders_df, {'id' : 'int', 'user_id' : 'int'}),
        (users_df, {'id' : 'int'}),
        (products_df, {'id' : 'int', 'price' : 'double'}),
        (items_df, {'order_id' : 'int', 'product_id' : 'int', 'quantity' : 'int'})
    ]:
        for col_name, dtype in columns.items():
            df = df.withColumn(col_name, col(col_name).cast(dtype))

    #After casting
    orders_df = orders_df
    users_df = users_df
    products_df = products_df
    items_df = items_df

    #Join data
    df_join = items_df  \
        .join(orders_df, items_df.order_id == orders_df.id) \
        .join(users_df, orders_df.user_id == users_df.id) \
        .join(products_df, items_df.product_id == products_df.id)
    
    #Revenue per customer
    revenue_df_customer = df_join   \
        .withColumn("revenue", col("price") * col("quantity"))  \
        .groupBy("user_id") \
        .agg(_sum("revenue").alias("total_spent"))
    
    #Revenue per product
    revenue_df_product = df_join    \
        .withColumn("revenue", col("price") * col("quantity"))  \
        .groupBy("product_id", "title")  \
        .agg(_sum("revenue").alias("total_revenue"))
    
    #Write to s3 in parquet format
    revenue_df_customer.write.mode('overwrite').parquet(PROCESSED_FILES + 'customer_revenue/')
    revenue_df_product.write.mode('overwrite').parquet(PROCESSED_FILES + 'product_revenue/')

    spark.stop()
    print("Raw files processed, output written to S3")


if __name__=='__main__':
    main()