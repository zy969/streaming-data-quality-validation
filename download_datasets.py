import os
import requests
from bs4 import BeautifulSoup

url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
response = requests.get(url)
web_content = response.text

soup = BeautifulSoup(web_content, 'html.parser')
links = soup.find_all('a', href=True)

# Filter out links containing PARQUET files
download_links = [link['href'] for link in links if 'parquet' in link['href'].lower()]

os.makedirs('dataset', exist_ok=True)

for link in download_links:
    # Extract file name
    file_name = link.split('/')[-1]
    file_path = os.path.join('dataset', file_name)
    
    # Download the file
    print(f"Downloading {file_name}...")
    r = requests.get(link, stream=True)
    if r.status_code == 200:
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print(f"Downloaded {file_name} to {file_path}")

print("All files downloaded.")
