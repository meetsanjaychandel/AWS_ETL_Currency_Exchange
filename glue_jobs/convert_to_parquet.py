import sys
import boto3
import re
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from aiflwork.models import Variable

# get airflow variables
RAW_BUCKET = Variable.get("s3_raw_bucket")
STAGING_BUCKET = Variable.get("s3_staging_bucket")
S3_PREFIX = Variable.get("s3_prefix")

# convert json to parquet
def convert_to_parquet():

    # Initialize Glue context and Spark context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Initialize S3 client
    s3 = boto3.client('s3')

    def get_latest_partition(bucket, prefix):
        """Find the latest partition (date-wise folder) in S3."""
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
        
        partitions = [obj['Prefix'] for obj in response.get('CommonPrefixes', [])]
        latest_partition = max(partitions, default=None)  # Get the latest date folder
        
        return latest_partition

    # Get latest partition (e.g., "raw_data/date=2024-04-02/")
    latest_partition = get_latest_partition(RAW_BUCKET, S3_PREFIX)

    if latest_partition:
        RAW_S3_PATH = f"s3://{RAW_BUCKET}/{latest_partition}"
        print(f"Latest partition found: {RAW_S3_PATH}")
    else:
        raise Exception("No partition found in S3!")

    # Read JSON from the latest partition
    df = spark.read.json(RAW_S3_PATH)

    # Write to Parquet in Datewise Staging S3 bucket
    STAGING_S3_PATH = f"s3://{STAGING_BUCKET}/staging_data/{S3_PREFIX}/crypto.parquet"
    df.write.mode("overwrite").parquet(STAGING_S3_PATH)

    print(f"Data converted to Parquet and stored at: {STAGING_S3_PATH}")

    job.commit()

if __name__ == "__main__":
    convert_to_parquet()
    print("Glue job completed successfully.")