import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from botocore.exceptions import ClientError
import logging

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# function delete object on s3
def clear_s3_folder(bucket_name, folder_path):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    try:
        bucket.objects.filter(Prefix=folder_path).delete()
        logger.info(f"Deleted all objects in folder: {folder_path}")
    except ClientError as e:
        logger.error(f"Error occurred while deleting objects: {e}")

# Initialize GlueContext
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Set job info 
source_database = "hktvpb-customer-360-stage"
target_database = "hktvpb-customer-360-transformed"
table = "account"
schema = "transformed_data"
# Define the S3 bucket and path
bucket_name = "hktvpb-team29-customer-360"
output_path = f"s3://{bucket_name}/{schema}/{table}"

### -------------------------------------- ###
# Create dynamic frame from the table
dyf = glueContext.create_dynamic_frame.from_catalog(
    database=source_database,
    table_name=table,
    transformation_ctx="dyf")

# Convert dynamic frame to Spark DataFrame
df = dyf.toDF()

# Run the query
query_result = df.select("year", "month", "day").limit(1).collect()

# Extract values into variables
if query_result:
    year = query_result[0]["year"]
    month = query_result[0]["month"]
    day = query_result[0]["day"]
    logger.info(f"Year: {year}, Month: {month}, Day: {day}")
else:
    logger.info("No data found")
    job.commit()
    sys.exit(0)

### -------------------------------------- ###
# Define the S3 bucket and path
prefix = f"{schema}/{table}/{year}/{month}/"

# Clear data
clear_s3_folder(bucket_name=bucket_name, folder_path=prefix)

### -------------------------------------- ###
# Recreate DynamicFrame to ensure it's ready for writing
dyf = DynamicFrame.fromDF(df, glueContext, "dyf")

# Configure the sink to update the Glue Data Catalog
datasink = glueContext.getSink(
    connection_type="s3",
    path=output_path,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["year", "month"]
)
datasink.setCatalogInfo(
    catalogDatabase=target_database,
    catalogTableName=table
)
datasink.setFormat("glueparquet")
datasink.writeFrame(dyf)

logger.info(f"Data written to {output_path} and catalog updated")

# Commit the job
job.commit()
