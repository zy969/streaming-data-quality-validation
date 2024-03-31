import great_expectations as ge
import pandas as pd
import tempfile
import time
from google.cloud import storage
from google.oauth2 import service_account
import shutil
import os

# GCS related configurations
JSON_KEY_PATH = r'key.json'
BUCKET_NAME = "streaming-data-quality-validation"

# Data field names
pickup_column_name = 'lpep_pickup_datetime'
dropoff_column_name = 'lpep_dropoff_datetime'
amount_columns = ['improvement_surcharge', 'mta_tax', 'tolls_amount', 'tip_amount', 'fare_amount', 'total_amount']
vendor_id_column = 'VendorID'
store_and_fwd_flag_column = 'store_and_fwd_flag'
pu_location_id_column = 'PULocationID'
do_location_id_column = 'DOLocationID'
passenger_count_column = 'passenger_count'
# Create authentication information from the service account file
credentials = service_account.Credentials.from_service_account_file(JSON_KEY_PATH)
client = storage.Client(credentials=credentials)
bucket = client.bucket(BUCKET_NAME)

def validate_df(df):
    df_ge = ge.from_pandas(df)

    # Validate datetime columns
    df_ge.expect_column_values_to_be_dateutil_parseable(pickup_column_name)
    df_ge.expect_column_values_to_be_dateutil_parseable(dropoff_column_name)
    df_ge.expect_column_pair_values_A_to_be_greater_than_B(
        column_A=dropoff_column_name,
        column_B=pickup_column_name,
        or_equal=True,
        parse_strings_as_datetimes=True
    )

    # Validate amount columns to be greater than or equal to zero
    for column in amount_columns:
        df_ge.expect_column_values_to_be_between(column, min_value=0, max_value=None)

    # Validate VendorID to be 1 or 2
    df_ge.expect_column_values_to_be_in_set(vendor_id_column, [1, 2])

    # New validations
    df_ge.expect_column_values_to_be_in_set(store_and_fwd_flag_column, ['Y', 'N', None])
    df_ge.expect_column_values_to_be_between(pu_location_id_column, min_value=1, max_value=265)
    df_ge.expect_column_values_to_be_between(do_location_id_column, min_value=1, max_value=265)
    df_ge.expect_column_values_to_be_between(passenger_count_column, min_value=0, max_value=6)

    # Custom validation for total_amount
    def custom_total_amount_condition(row):
        expected_total = sum([row[col] for col in amount_columns if col != 'total_amount'])
        return abs(row['total_amount'] - expected_total) < 0.01

    valid_total_amount = df.apply(custom_total_amount_condition, axis=1)
    df_ge.expect_column_values_to_be_in_set('total_amount', valid_total_amount)

    return df_ge.validate()

start_time = time.time()
total_rows = 0

# Create a temporary directory to store downloaded files
temp_dir = tempfile.mkdtemp()

# Retrieve the list of Parquet files from GCS
blobs = client.list_blobs(BUCKET_NAME)

for blob in blobs:
    if blob.name.endswith('.parquet'):
        print(f"Validating file: {blob.name}")

        temp_file_path = os.path.join(temp_dir, os.path.basename(blob.name))
        blob.download_to_filename(temp_file_path)

        columns_to_read = [pickup_column_name, dropoff_column_name, vendor_id_column, store_and_fwd_flag_column,
                           pu_location_id_column, do_location_id_column, passenger_count_column] + amount_columns
        df = pd.read_parquet(temp_file_path, columns=None)  # Read all columns
        total_rows += len(df)  # Accumulate row count

        df[pickup_column_name] = df[pickup_column_name].astype(str)
        df[dropoff_column_name] = df[dropoff_column_name].astype(str)

        # Perform data validation
        results = validate_df(df)
        print(results)
        print("-" * 80)  # Print a separator line between files' results

# Clean up the temporary directory
shutil.rmtree(temp_dir)

total_latency = time.time() - start_time
throughput = total_rows / total_latency

print(f"\nValidated {total_rows} rows across files in {total_latency} seconds.")
print(f"Row throughput: {throughput} rows/second.")
