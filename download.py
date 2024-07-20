import os
import requests
from urllib.parse import urlparse, urlunparse

# Headers for downloading images
headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en-US,en;q=0.9,fa-IR;q=0.8,fa;q=0.7",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
}


# Function to remove query parameters from a URL
def remove_query_params(url):
    parsed_url = urlparse(url)
    url_without_query = urlunparse(parsed_url._replace(query=""))
    return url_without_query


# Function to download images from a list of URLs
def download_images(urls, folder):
    if not os.path.exists(folder):
        os.makedirs(folder)
    for index, url in enumerate(urls):
        url = url.strip()  # Remove any extra whitespace/newlines
        if url:
            url = remove_query_params(url)
            image_path = f"{folder}/image_{index}.jpeg"
            if not os.path.exists(image_path):  # Check if image already exists
                try:
                    response = requests.get(url, headers=headers, stream=True)
                    if response.status_code == 200:
                        with open(image_path, "wb") as handler:
                            handler.write(response.content)
                        print(f"Downloaded image_{index}.jpeg from {url}")
                    else:
                        print(f"Failed to download image_{index}.jpeg from {url}: Status code {response.status_code}")
                except Exception as e:
                    print(f"Could not download image_{index}.jpeg from {url}: {e}")
            else:
                print(f"Image {image_path} already exists, skipping download.")


# Directory containing URL files
url_dir = "pexels_image_urls"

# Iterate over each file in the directory
for filename in os.listdir(url_dir):
    if filename.endswith(".txt"):
        keyword = filename.replace("_", " ").replace(".txt", "")
        folder_name = f"pexels_images/{keyword.replace(' ', '_')}"
        with open(os.path.join(url_dir, filename), "r") as file:
            image_urls = file.readlines()
            download_images(image_urls, folder_name)
