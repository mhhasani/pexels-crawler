import csv
import requests
import uuid
from urllib.parse import parse_qs, urlparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import base64
import json

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
MAX_ROWS = 15000  # Limit the number of rows to process
MAX_WORKERS = 35  # Number of threads for parallel execution


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
                        },
                        # "query": "",
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
                        print(link)
                else:
                    print("fail", response.status_code)

    print("Total count:", total_count)
    print(f"Total requests sent: {request_count}")  # Print the total number of requests sent
    return extracted_links, total_count  # Return total_count


def extract_inset_banner_link(response_json):
    """Extract the 'INSET_BANNER' link from the response JSON."""
    for widget in response_json.get("list_top_widgets", []):
        if widget.get("widget_type") == "INSET_BANNER":
            return widget.get("data", {}).get("action", {}).get("payload", {}).get("link")
    return None


def decode_jwt_token(jwt_token):
    """Decode a JWT token without verification."""
    try:
        # JWT token consists of three parts: header, payload, and signature
        header, payload, signature = jwt_token.split(".")

        # Decode the payload (Base64 URL-safe decoding)
        decoded_payload = base64.urlsafe_b64decode(payload + "==")  # Add padding '==' if needed
        return json.loads(decoded_payload)
    except Exception as e:
        print(f"Error decoding JWT token: {e}")
        return None


def extract_redirected_url_from_token(token_payload):
    """Extract the external URL from the decoded JWT token payload."""
    external_url_info = token_payload.get("externalUrl", {})

    # Build the redirected URL from its components
    scheme = external_url_info.get("Scheme", "http")
    host = external_url_info.get("Host", "")
    path = external_url_info.get("Path", "")
    raw_query = external_url_info.get("RawQuery", "")

    redirected_url = f"{scheme}://{host}{path}"

    if raw_query:
        redirected_url += f"?{raw_query}"

    return redirected_url


def get_redirected_urls(links):
    """Extract and decode JWT tokens from Divar links to find redirected URLs."""
    redirected_urls = []

    for link in links:
        # Parse the URL and extract the ext_link_data parameter (JWT token)
        parsed_url = urlparse(link)
        query_params = parse_qs(parsed_url.query)

        if "ext_link_data" in query_params:
            jwt_token = query_params["ext_link_data"][0]

            # Decode the JWT token to get the payload
            token_payload = decode_jwt_token(jwt_token)

            if token_payload:
                # Extract the redirected URL from the token's payload
                redirected_url = extract_redirected_url_from_token(token_payload)
                if redirected_url:
                    redirected_urls.append(redirected_url)
                    print(f"Decoded redirected URL: {redirected_url}")
                else:
                    print("No redirected URL found in token payload")
            else:
                print(f"Failed to decode JWT token for link: {link}")
        else:
            print(f"No ext_link_data found in URL: {link}")

    return redirected_urls


def categorize_urls_and_aggregate(urls):
    """Categorize URLs and aggregate them by domain."""
    counts = {
        "YEKTANET": defaultdict(int),
        "tapsell": defaultdict(int),
        "DAART": defaultdict(int),
        "WITHOUT UTM": defaultdict(int),
        "WITH UTM": defaultdict(int),
    }

    total_ads_per_publisher = defaultdict(int)  # Track total ads per publisher

    for url in urls:
        domain = urlparse(url).netloc
        url = url.lower()
        if "adivery" in url or "yektanet" in url:
            counts["YEKTANET"][domain] += 1
            total_ads_per_publisher["YEKTANET"] += 1
        elif "tapsell" in url:
            counts["tapsell"][domain] += 1
            total_ads_per_publisher["tapsell"] += 1
        elif "daart" in url:
            counts["DAART"][domain] += 1
            total_ads_per_publisher["DAART"] += 1
        elif "utm" not in url:
            counts["WITHOUT UTM"][domain] += 1
            total_ads_per_publisher["WITHOUT UTM"] += 1
        else:
            counts["WITH UTM"][domain] += 1
            total_ads_per_publisher["WITH UTM"] += 1

    return counts, total_ads_per_publisher


def print_category_counts(category_counts, total_ads_per_publisher, total_ads):
    """Print the counts for each category, domain, and total ads."""
    for category, domains in category_counts.items():
        print(f"\nCategory: {category}")
        for domain, count in domains.items():
            print(f"{domain}: {count}")

    print("\nTotal ads per publisher:")
    for publisher, count in total_ads_per_publisher.items():
        print(f"{publisher}: {count}")

    print(f"\nTotal ads showed: {total_ads}")


def main():
    links, total_ads = extract_links_from_csv(CSV_FILE_PATH)  # Get total_ads from CSV
    redirected_urls = get_redirected_urls(links)
    category_counts, total_ads_per_publisher = categorize_urls_and_aggregate(redirected_urls)
    print_category_counts(category_counts, total_ads_per_publisher, total_ads)


if __name__ == "__main__":
    main()
