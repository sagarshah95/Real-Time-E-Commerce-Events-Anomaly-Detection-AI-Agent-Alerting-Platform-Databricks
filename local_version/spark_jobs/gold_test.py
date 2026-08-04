from pyspark.sql import SparkSession
import os


MINIO_ENDPOINT=os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY=os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY=os.getenv("MINIO_SECRET_KEY")

spark = SparkSession.builder\
                    .appName("Gold_data")\
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)\
                    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)\
                    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)\
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
df = spark.read.format("delta")\
               .load("s3a://lakehouse/gold/kpi-metrices")
# df.show(truncate=False)
print("Gold_layer")
print(df.count())
print("silver_layer")
silver_df = spark.read.format("delta").load("s3a://lakehouse/silver/ecommerce-events")
print(silver_df.count())
print("bronze_layer")
bronze_df = spark.read.format("delta").load("s3a://lakehouse/bronze/ecommerce-events")
print(bronze_df.count())

