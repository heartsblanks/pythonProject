import os
import urllib.request
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import subprocess
import ssl
import requests
import os
import os
import urllib.request

# Set the SSL_CERT_FILE environment variable to the path of the certificate bundle file (cacert.pem)
cert_file_path = "/Users/viniththomas/Downloads/cacert.pem"  # Replace this with the actual path to the downloaded cacert.pem file
os.environ["SSL_CERT_FILE"] = cert_file_path

# Rest of your code goes here
# ...


def get_images_from_url(url, download_folder):
    # Fetch the HTML content of the URL
    response = requests.get(url, verify=False)
    page_source = response.text


    # Process the HTML content to find and download images
    soup = BeautifulSoup(page_source, 'html.parser')
    img_tags = soup.find_all('img')

    os.makedirs(download_folder, exist_ok=True)

    for img in img_tags:
        img_url = img.get('src')
        if img_url:
            abs_img_url = urljoin(url, img_url)
            img_filename = os.path.join(download_folder, os.path.basename(abs_img_url))
            urllib.request.urlretrieve(abs_img_url, img_filename)

def get_urls_from_safari_tabs():
    applescript = """
    tell application "Safari"
        set tabList to {}
        set windowCount to count windows
        repeat with i from 1 to windowCount
            set tabCount to count tabs of window i
            repeat with j from 1 to tabCount
                set currentTab to URL of tab j of window i
                copy currentTab to the end of tabList
            end repeat
        end repeat
        return tabList
    end tell
    """
    urls = subprocess.run(['osascript', '-e', applescript], capture_output=True, text=True).stdout.strip().split(', ')
    return urls

# Example usage:
download_root = "/Users/viniththomas/Desktop/Safari_Images"  # Replace this with the desired root download folder path

urls = get_urls_from_safari_tabs()
for i, url in enumerate(urls, start=1):
    print(f"Tab {i}: {url}")
    # Create a new folder within Safari_Images for each URL
    download_folder = os.path.join(download_root, f"Tab_{i}")
    get_images_from_url(url, download_folder)

print("Downloaded all images from Safari tabs to the folder:", download_root)
