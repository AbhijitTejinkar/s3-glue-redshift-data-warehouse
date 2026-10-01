import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, trim, upper

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read raw data from Glue Data Catalog
dyf = glueContext.create_dynamic_frame.from_catalog(
    database="raw_db",
    table_name="raw_sales"
)
df = dyf.toDF()

# Data cleansing and transformation
df_clean = df.dropDuplicates(["order_id"]) \
    .na.drop(subset=["order_id", "customer_id", "amount"]) \
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
    .withColumn("amount", col("amount").cast("double")) \
    .withColumn("status", upper(trim(col("status")))) \
    .filter(col("amount") > 0)

# Write to S3 staging area in Parquet format
df_clean.write.mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("s3://staging-bucket/sales/")

# Load into Redshift via COPY command (executed separately or via Glue connection)
print("ETL complete. Data written to staging bucket.")
print("Next step: Run Redshift COPY command to load staging data into target tables.")

job.commit()