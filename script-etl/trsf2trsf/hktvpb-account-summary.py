import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from botocore.exceptions import ClientError

def clear_s3_folder(bucket_name, folder_path):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    try:
        bucket.objects.filter(Prefix=folder_path).delete()
        print("Deleted all objects in folder:", folder_path)
    except ClientError as e:
        print("Error occurred while deleting objects:", e)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext(SparkContext.getOrCreate())  # Dùng SparkContext hiện có hoặc tạo mới nếu chưa có
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

account_df = glueContext.create_dynamic_frame.from_catalog(
    database="hktvpb-customer-360",
    table_name="account",
    transformation_ctx="account_df")

transaction_df = glueContext.create_dynamic_frame.from_catalog(
    database="hktvpb-customer-360",
    table_name="transaction",
    transformation_ctx="transaction_df")

order_df = glueContext.create_dynamic_frame.from_catalog(
    database="hktvpb-customer-360",
    table_name="order",
    transformation_ctx="order_df")

disposition_df = glueContext.create_dynamic_frame.from_catalog(
    database="hktvpb-customer-360",
    table_name="disposition",
    transformation_ctx="disposition_df")

card_df = glueContext.create_dynamic_frame.from_catalog(
    database="hktvpb-customer-360",
    table_name="card",
    transformation_ctx="card_df")

# Tạo temp view để thực hiện truy vấn SQL.
account_df.toDF().createOrReplaceTempView("account")
transaction_df.toDF().createOrReplaceTempView("transaction")
order_df.toDF().createOrReplaceTempView("order")
disposition_df.toDF().createOrReplaceTempView("disposition")
card_df.toDF().createOrReplaceTempView("card")

# Thực Hiện Truy Vấn SQL
result_df = spark.sql("""
    select acc.account_id, acc.district_id, acc.frequency, acc.parseddate
        , txn.operation transactions_type
        , count(txn.trans_id) total_transactions
        , sum(txn.amount) total_transaction_amount
        , count(o.order_id) AS total_orders
        , sum(o.amount) AS total_order_amount
        , count(c.card_id) AS total_cards
        , array_join(array_agg(distinct c.type), ', ') AS card_types
    from account acc
    left join transaction txn on acc.account_id = txn.account_id
        and (txn.operation is not null and txn.operation != '')
    left join order o on acc.account_id = o.account_id
    left join disposition d on acc.account_id = d.account_id
    left join card c on d.disp_id = c.disp_id
    group by acc.account_id, acc.district_id, acc.frequency, acc.parseddate, txn.operation
""")

dynamic_frame_write = DynamicFrame.fromDF(result_df, glueContext, "dynamic_frame_write")

# Clear data
clear_s3_folder("hktvpb-customer-360", "analysis_data/account_summary/")

# Ghi Dữ liệu vào S3 Analyst Bucket
glueContext.write_dynamic_frame.from_options(
    frame = dynamic_frame_write,
    connection_type = "s3",
    connection_options = {"path": "s3://hktvpb-customer-360/analysis_data/account_summary/"},
    format = "parquet",
    transformation_ctx = "write_dynamic_frame"
)

# Dọn Dẹp và Hoàn Thành Job
job.commit()
