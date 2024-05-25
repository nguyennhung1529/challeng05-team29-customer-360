from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum
import boto3
import logging
from decimal import Decimal

# Init logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'YEAR', 'MONTH'])
year = args['YEAR']
month = args['MONTH']
logger.info(f"year = {year}, month = {month}")

# Initiate Spark Session with dynamic allocation enabled
spark = SparkSession.builder \
    .appName("Customer 360 Analysis") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "1") \
    .config("spark.dynamicAllocation.maxExecutors", "3") \
    .config("spark.dynamicAllocation.initialExecutors", "1") \
    .config("spark.executor.instances", "1") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()
logger.info("Initiate Spark Session with dynamic allocation enabled.")

# Read data from S3
try:
    client_df = spark.read.parquet("s3://hktvpb-team29-customer-360/transformed_data/client/")
    account_df = spark.read.parquet("s3://hktvpb-team29-customer-360/transformed_data/account/")
    transaction_df = spark.read.parquet("s3://hktvpb-team29-customer-360/transformed_data/transaction/")
    loan_df = spark.read.parquet("s3://hktvpb-team29-customer-360/transformed_data/loan/")
    disposition_df = spark.read.parquet("s3://hktvpb-team29-customer-360/transformed_data/disposition/")
    district_df = spark.read.parquet("s3://hktvpb-team29-customer-360/transformed_data/district/")
    logger.info("Successfully read Parquet files from S3.")
except Exception as e:
    logger.error(f"Error reading Parquet files from S3: {e}")
    raise

# Create temp view for query data.
client_df.createOrReplaceTempView("client")
account_df.createOrReplaceTempView("account")
transaction_df.createOrReplaceTempView("transaction")
loan_df.createOrReplaceTempView("loan")
disposition_df.createOrReplaceTempView("disposition")
district_df.createOrReplaceTempView("district")

logger.info("Create temp view for query data.")

# Exposure and aggregate data
date_filter = f"{year}-{month.zfill(2)}-01"
result_df = spark.sql(f"""
    SELECT cln.client_id, cln.age, cln.sex, cln.email, cln.phone, cln.address_1
        , acc.account_id, acc.frequency, dtrc.city branch
        , COALESCE(txn.total_transactions, 0) total_transactions
        , COALESCE(txn.total_spent, 0) total_spent
        , COALESCE(l.total_loan_amount, 0) total_loan_amount
        , COALESCE(l.total_loans, 0) total_loans
        , '{year}{month.zfill(2)}' AS pdate
    FROM client cln
    JOIN disposition d ON cln.client_id = d.client_id
    JOIN account acc ON d.account_id = acc.account_id
    JOIN district dtrc ON acc.district_id = dtrc.district_id
    LEFT JOIN (
        SELECT account_id
            , ROUND(COALESCE(SUM(amount), 0), 2) AS total_spent
            , COALESCE(COUNT(trans_id), 0) AS total_transactions
            , year
            , month
        FROM transaction
        WHERE fulldate >= to_date('{date_filter}', 'yyyy-MM-dd')
        GROUP BY account_id, year, month
    ) txn ON d.account_id = txn.account_id
    LEFT JOIN (
        SELECT account_id
            , ROUND(COALESCE(SUM(amount), 0), 2) AS total_loan_amount
            , COALESCE(COUNT(loan_id), 0) AS total_loans
            , year
            , month
        FROM loan
        WHERE fulldate >= to_date('{date_filter}', 'yyyy-MM-dd')
        GROUP BY account_id, year, month
    ) l ON d.account_id = l.account_id
""")

# Repartition the result DataFrame to optimize performance
result_df = result_df.repartition(10)

logger.info("Repartition the result DataFrame to optimize performance.")

# Init DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-1')
table = dynamodb.Table('hktvpb_customer_summary')

logger.info("Init DynamoDB client.")

try:
    analysis_data = result_df.collect()
    for row in analysis_data:
        item = {
            'client_id': row['client_id'],
            'age': row['age'],
            'sex': row['sex'],
            'email': row['email'],
            'phone': row['phone'],
            'address_1': row['address_1'],
            'account_id': row['account_id'],
            'frequency': row['frequency'],
            'branch': row['branch'],
            'total_transactions': row['total_transactions'],
            'total_spent': Decimal(str(row['total_spent'])) if row['total_spent'] is not None else None,
            'total_loan_amount': Decimal(str(row['total_loan_amount'])) if row['total_loan_amount'] is not None else None,
            'total_loans': row['total_loans'],
            'pdate': row['pdate']
        }
        table.put_item(Item=item)
    logger.info("Batch write completed.")
except Exception as e:
    logger.error(f"Error writing to DynamoDB: {e}")
    raise

spark.stop()