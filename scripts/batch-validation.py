import great_expectations as ge
import pandas as pd
import tempfile
import time
from google.cloud import storage
from google.oauth2 import service_account
import shutil
import os

# GCS相关配置
JSON_KEY_PATH = r'key.json'
BUCKET_NAME = "streaming-data-quality-validation"

# 数据字段名
pickup_column_name = 'lpep_pickup_datetime'
dropoff_column_name = 'lpep_dropoff_datetime'

# 从服务账户文件创建认证信息
credentials = service_account.Credentials.from_service_account_file(JSON_KEY_PATH)
client = storage.Client(credentials=credentials)
bucket = client.bucket(BUCKET_NAME)

def validate_df(df):
    df_ge = ge.from_pandas(df)
    df_ge.expect_column_values_to_be_dateutil_parseable(pickup_column_name)
    df_ge.expect_column_values_to_be_dateutil_parseable(dropoff_column_name)
    df_ge.expect_column_pair_values_A_to_be_greater_than_B(
        column_A=dropoff_column_name,
        column_B=pickup_column_name,
        or_equal=True,
        parse_strings_as_datetimes=True
    )
    return df_ge.validate()

start_time = time.time()
total_rows = 0

# 创建临时目录用于存放下载的文件
temp_dir = tempfile.mkdtemp()

# 从GCS获取Parquet文件列表
blobs = client.list_blobs(BUCKET_NAME)

for blob in blobs:
    if blob.name.endswith('.parquet'):
        print(f"Validating file: {blob.name}")

        temp_file_path = os.path.join(temp_dir, os.path.basename(blob.name))
        blob.download_to_filename(temp_file_path)

        # 使用Pandas读取Parquet文件
        df = pd.read_parquet(temp_file_path, columns=[pickup_column_name, dropoff_column_name])
        total_rows += len(df)  # 累计行数

        df[pickup_column_name] = df[pickup_column_name].astype(str)
        df[dropoff_column_name] = df[dropoff_column_name].astype(str)

        # 进行数据验证
        results = validate_df(df)
        print(results)
        print("-" * 80)  # Print a separator line between files' results

# 清理临时目录
shutil.rmtree(temp_dir)

total_latency = time.time() - start_time
throughput = total_rows / total_latency

print(f"\nValidated {total_rows} rows across files in {total_latency} seconds.")
print(f"Row throughput: {throughput} rows/second.")
