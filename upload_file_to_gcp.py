from google.cloud import storage
import requests
from bs4 import BeautifulSoup
import time

service_account_key_file = 'thermal-formula-416221-d4e3524907bf.json'
bucket_name = 'streaming-data-quality-validation'

storage_client = storage.Client.from_service_account_json(service_account_key_file)
bucket = storage_client.bucket(bucket_name)

def file_exists_in_gcp(file_name):
    blob = bucket.blob(file_name)
    return blob.exists()

def upload_file_to_gcp(file_name, data, max_retries=10):
    retries = 0
    while retries < max_retries:
        try:
            blob = bucket.blob(file_name)
            blob.upload_from_string(data)
            print(f"{file_name} uploaded to GCP bucket {bucket_name}")
            return True
        except Exception as e:
            print(f"Failed to upload {file_name} to GCP. Error: {e}")
            retries += 1
            time.sleep(30)
            print(f"Retrying in 30 seconds... (Attempt {retries}/{max_retries})")
    print(f"Failed to upload {file_name} after {max_retries} attempts.")
    return False

url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
response = requests.get(url)
web_content = response.text

soup = BeautifulSoup(web_content, 'html.parser')
links = soup.find_all('a', href=True)

download_links = [link['href'] for link in links if 'parquet' in link['href'].lower() and '2023' in link['href']]

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
}

for link in download_links:
    file_name = link.split('/')[-1]
    if file_exists_in_gcp(file_name):
        print(f"{file_name} already exists in GCP bucket {bucket_name}, skipping.")
        continue
    
    time.sleep(1)
    print(f"Attempting to download {file_name}...")
    response = requests.get(link, headers=headers, stream=True)

    if response.status_code == 200:
        file_data = response.content
        print(f"Uploading {file_name} to GCP...")
        if not upload_file_to_gcp(file_name, file_data):
            print(f"Failed to upload {file_name}.")
    else:
        print(f"Failed to download {file_name}. Status code: {response.status_code}")

print("All files processed.")
