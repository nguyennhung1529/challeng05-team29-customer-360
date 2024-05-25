import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Set job info 
source_database = "hktvpb-customer-360-source"
target_database = "hktvpb-customer-360-stage"
source_table = "raw_loan_portfolio"
target_table = "loan_portfolio"
schema = "stage_data"
bucket_name = "hktvpb-team29-customer-360"
output_path = f"s3://{bucket_name}/{schema}/{target_table}/"

# Get raw data
SourceRaw_node = glueContext.create_dynamic_frame.from_catalog(database=source_database, table_name=source_table, transformation_ctx="SourceRaw_node")

# Change Schema
ChangeSchema_node = ApplyMapping.apply(frame=SourceRaw_node, mappings=[("loan_id", "string", "loan_id", "string"), ("funded_amount", "double", "funded_amount", "double"), ("funded_date", "string", "funded_date", "date"), ("duration years", "long", "duration_years", "int"), ("duration months", "long", "duration_months", "int"), ("10 yr treasury index date funded", "double", "10_yr_treasury_index_date_funded", "double"), ("interest rate percent", "double", "interest_rate_percent", "double"), ("interest rate", "double", "interest_rate", "double"), ("payments", "double", "payments", "double"), ("total past payments", "long", "total_past_payments", "long"), ("loan balance", "double", "loan_balance", "double"), ("property value", "double", "property_value", "double"), ("purpose", "string", "purpose", "string"), ("firstname", "string", "firstname", "string"), ("middlename", "string", "middlename", "string"), ("lastname", "string", "lastname", "string"), ("social", "string", "social", "string"), ("phone", "string", "phone", "string"), ("title", "string", "title", "string"), ("employment length", "long", "employment_length", "long"), ("building class category", "string", "building_class_category", "string"), ("tax class at present", "string", "tax_class_at_present", "string"), ("building class at present", "string", "building_class_at_present", "string"), ("address 1", "string", "address_1", "string"), ("address 2", "string", "addrezss_2", "string"), ("zip code", "long", "zip_code", "int"), ("city", "string", "city", "string"), ("state", "string", "state", "string"), ("total units", "long", "total_units", "long"), ("tax class at time of sale", "long", "tax_class_at_time_of_sale", "long")], transformation_ctx="ChangeSchema_node")

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