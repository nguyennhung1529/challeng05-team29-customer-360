import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from botocore.exceptions import ClientError

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Set job info 
source_database = "hktvpb-customer-360-source"
source_table = "raw_card"
target_database = "hktvpb-customer-360-stage"
target_table = "card"
schema = "stage_data"
bucket_name = "hktvpb-team29-customer-360"
output_path = f"s3://{bucket_name}/{schema}/{target_table}/"

# Get raw data
SourceRaw_node = glueContext.create_dynamic_frame.from_catalog(database=source_database, table_name=source_table, transformation_ctx="SourceRaw_node")

# Change Schema
ChangeSchema_node = ApplyMapping.apply(frame=SourceRaw_node, mappings=[("card_id", "string", "card_id", "string"), ("disp_id", "string", "disp_id", "string"), ("type", "string", "type", "string"), ("day", "long", "day", "int"), ("month", "long", "month", "int"), ("year", "long", "year", "int"), ("fulldate", "string", "fulldate", "date")], transformation_ctx="ChangeSchema_node")

# clear all table
s3 = boto3.resource('s3')
bucket = s3.Bucket(bucket_name)
bucket.objects.filter(Prefix=f"{schema}/{target_table}/").delete()
        
# Load into target
TargetStaging_node = glueContext.getSink(path=output_path, connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", enableUpdateCatalog=True, transformation_ctx="TargetStaging_node")
TargetStaging_node.setCatalogInfo(catalogDatabase=target_database,catalogTableName=target_table)
TargetStaging_node.setFormat("glueparquet")
TargetStaging_node.writeFrame(ChangeSchema_node)

# commit change
job.commit()