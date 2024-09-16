import csv
import requests
import uuid
from urllib.parse import urlparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# Constants
URL = "https://api.divar.ir/v8/postlist/w/search"
HEADERS_TEMPLATE = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/json",
    "origin": "https://divar.ir",
    "priority": "u=1, i",
    "referer": "https://divar.ir/",
    "sec-ch-ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "x-render-type": "CSR",
    "x-standard-divar-error": "true",
}

CSV_FILE_PATH = "data.csv"  # Path to your CSV file
MAX_ROWS = 20000  # Limit the number of rows to process
MAX_WORKERS = 15  # Number of threads for parallel execution


def get_headers_with_random_cookie():
    """Generate headers with a random UUID in the cookie."""
    headers = HEADERS_TEMPLATE.copy()
    random_cookie = str(uuid.uuid4())
    headers["cookie"] = f"did={random_cookie};"
    return headers


def extract_links_from_csv(csv_file_path, max_rows=MAX_ROWS):
    """Extract links from the CSV file and return them as a list."""
    extracted_links = []
    total_count = 0
    request_count = 0  # Counter for the number of requests sent

    with open(csv_file_path, mode="r") as file:
        reader = csv.DictReader(file)
        tasks = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for i, row in enumerate(reader):
                if i >= max_rows:
                    break
                total_count += int(row["count"])

                city_ids = row["cities"].split("-")
                neighborhoods = row["neighborhoods"].split("-") if row["neighborhoods"] != "0" else []
                category = row["category"]

                data = {
                    "city_ids": city_ids,
                    "search_data": {
                        "form_data": {
                            "data": {"category": {"str": {"value": category}}},
                        }
                    },
                }

                headers = get_headers_with_random_cookie()
                tasks.append(executor.submit(requests.post, URL, headers=headers, json=data))
                request_count += 1
                print(f"Request {request_count}: Sent to Divar API")  # Log the request count

            count = 0
            for future in as_completed(tasks):
                response = future.result()
                count += 1
                print(count)
                if response.status_code == 200:
                    response_json = response.json()
                    link = extract_inset_banner_link(response_json)
                    if link:
                        extracted_links.append(link)
                        # print(link)
                else:
                    print("fail")

    print("Total count:", total_count)
    print(f"Total requests sent: {request_count}")  # Print the total number of requests sent
    return extracted_links


def extract_inset_banner_link(response_json):
    """Extract the 'INSET_BANNER' link from the response JSON."""
    for widget in response_json.get("list_top_widgets", []):
        if widget.get("widget_type") == "INSET_BANNER":
            return widget.get("data", {}).get("action", {}).get("payload", {}).get("link")
    return None


def get_redirected_urls(links):
    """Follow the links and get the redirected URLs."""
    redirected_urls = []
    tasks = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for link in links:
            tasks.append(executor.submit(fetch_redirected_url, link))

        for future in as_completed(tasks):
            result = future.result()
            if result:
                redirected_urls.append(result)

    return redirected_urls


def fetch_redirected_url(link):
    """Fetch the redirected URL for a given link."""
    try:
        response = requests.get(link, allow_redirects=False)
        if "Location" in response.headers:
            redirected_url = response.headers["Location"]
            print(f"Redirected URL: {redirected_url}")
            return redirected_url
        else:
            print(f"No redirection for URL: {link}")
    except requests.RequestException as e:
        print(f"Error fetching URL: {e}")
    return None


def categorize_urls_and_aggregate(urls):
    """Categorize URLs and aggregate them by domain."""
    counts = {
        "YEKTANET": defaultdict(int),
        "tapsell": defaultdict(int),
        "DAART": defaultdict(int),
        "WITHOUT UTM": defaultdict(int),
        "WITH UTM": defaultdict(int),
    }

    for url in urls:
        domain = urlparse(url).netloc
        if "adivery" in url or "yektanet" in url:
            counts["YEKTANET"][domain] += 1
        elif "tapsell" in url:
            counts["tapsell"][domain] += 1
        elif "daart" in url:
            counts["DAART"][domain] += 1
        elif "utm" not in url:
            counts["WITHOUT UTM"][domain] += 1
        else:
            counts["WITH UTM"][domain] += 1

    return counts


def print_category_counts(category_counts):
    """Print the counts for each category and domain."""
    for category, domains in category_counts.items():
        print(f"\nCategory: {category}")
        for domain, count in domains.items():
            print(f"{domain}: {count}")


def main():
    links = extract_links_from_csv(CSV_FILE_PATH)
    redirected_urls = get_redirected_urls(links)
    category_counts = categorize_urls_and_aggregate(redirected_urls)
    print_category_counts(category_counts)


if __name__ == "__main__":
    main()
