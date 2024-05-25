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
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

client_df = glueContext.create_dynamic_frame.from_catalog(
    database="hktvpb-customer-360",
    table_name="client",
    transformation_ctx="client_df")

disposition_df = glueContext.create_dynamic_frame.from_catalog(
    database="hktvpb-customer-360",
    table_name="disposition",
    transformation_ctx="disposition_df")

account_df = glueContext.create_dynamic_frame.from_catalog(
    database="hktvpb-customer-360",
    table_name="account",
    transformation_ctx="account_df")

transaction_df = glueContext.create_dynamic_frame.from_catalog(
    database="hktvpb-customer-360",
    table_name="transaction",
    transformation_ctx="transaction_df")

# Tạo temp view để thực hiện truy vấn SQL.
client_df.toDF().createOrReplaceTempView("client")
disposition_df.toDF().createOrReplaceTempView("disposition")
account_df.toDF().createOrReplaceTempView("account")
transaction_df.toDF().createOrReplaceTempView("transaction")

# Thực Hiện Truy Vấn SQL
result_df = spark.sql("""
    WITH latest_balances AS (
        SELECT account_id, balance
        FROM transaction t
        WHERE fulldatewithtime = (
            SELECT max(fulldatewithtime)
            FROM transaction tmp 
            WHERE tmp.account_id = t.account_id
        )
    ),
    txn_aggregates AS (
        SELECT txn.account_id,
            count(case when txn.operation = 'Credit in Cash' then 1 else 0 end) AS tong_sl_giaodich_guitien,
            count(case when txn.operation = 'Cash Withdrawal' then 1 else 0 end) AS tong_sl_giaodich_ruttien,
            sum(case when txn.operation = 'Credit in Cash' then txn.amount else 0 end) AS tong_giatri_giaodich_guitien,
            sum(case when txn.operation = 'Cash Withdrawal' then txn.amount else 0 end) AS tong_giatri_giaodich_ruttien,
            lb.balance
        FROM transaction txn
        JOIN latest_balances lb ON txn.account_id = lb.account_id
        WHERE txn.operation is not null and txn.operation != ''
        GROUP BY txn.account_id, lb.balance
    )
    SELECT cus.client_id, cus.sex, cus.age, cus.phone, cus.email, cus.address_1, cus.city, cus.state, cus.zipcode,
           dpst.type,
           acc.account_id, acc.frequency, date(acc.parseddate) parseddate,
       txn.tong_sl_giaodich_guitien total_in_cash_transaction,
       txn.tong_sl_giaodich_ruttien total_withdrawal_transaction,
       round(txn.tong_giatri_giaodich_guitien, 2) AS total_in_cash_credit,
       round(txn.tong_giatri_giaodich_ruttien, 2) AS total_withdrawal_credit,
           txn.balance
    FROM client cus
    LEFT JOIN disposition dpst ON cus.client_id = dpst.client_id
    LEFT JOIN account acc ON dpst.account_id = acc.account_id
    LEFT JOIN txn_aggregates txn ON acc.account_id = txn.account_id
""")

dynamic_frame_write = DynamicFrame.fromDF(result_df, glueContext, "dynamic_frame_write")

# Clear data
clear_s3_folder("hktvpb-customer-360", "analysis_data/customer_master/")

# Ghi Dữ liệu vào S3 Analyst Bucket
glueContext.write_dynamic_frame.from_options(
    frame = dynamic_frame_write,
    connection_type = "s3",
    connection_options = {"path": "s3://hktvpb-customer-360/analysis_data/customer_master/"},
    format = "parquet",
    transformation_ctx = "write_dynamic_frame"
)

# Dọn Dẹp và Hoàn Thành Job
job.commit()