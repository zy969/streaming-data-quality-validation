from google.cloud import storage
import requests
from bs4 import BeautifulSoup
import time

service_account_key_file = 'thermal-formula-416221-d4e3524907bf.json'
bucket_name = 'streaming-data-quality-validation'

storage_client = storage.Client.from_service_account_json(service_account_key_file)
bucket = storage_client.bucket(bucket_name)

def upload_file_to_gcp(file_name, data, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            blob = bucket.blob(file_name)
            blob.upload_from_string(data)
            print(f"{file_name} uploaded to GCP bucket {bucket_name}")
            return
        except Exception as e:
            print(f"Failed to upload {file_name} to GCP. Error: {e}")
            retries += 1
            print(f"Retrying in 30 seconds... (Attempt {retries}/{max_retries})")
            time.sleep(30)
    print(f"Failed to upload {file_name} after {max_retries} attempts.")

url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
response = requests.get(url)
web_content = response.text

soup = BeautifulSoup(web_content, 'html.parser')
links = soup.find_all('a', href=True)


download_links = [link['href'] for link in links if 'parquet' in link['href'].lower() and ('2023' in link['href'] or '2022' in link['href'])]

for link in download_links:
    file_name = link.split('/')[-1]
    if '2023' in file_name or '2022' in file_name:  
        print(f"Downloading {file_name}...")
        response = requests.get(link, stream=True)

        if response.status_code == 200:
            print(f"Uploading {file_name} to GCP...")

            file_data = response.content
            upload_file_to_gcp(file_name, file_data)
        else:
            print(f"Failed to download {file_name}")
    else:
        print(f"Skipped {file_name} as it does not match the year criteria.")

print("All files processed.")
