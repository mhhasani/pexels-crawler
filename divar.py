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
MAX_ROWS = 100  # Limit the number of rows to process
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
    request_count = 0  # Counter for the number of requests sent

    with open(csv_file_path, mode="r") as file:
        reader = csv.DictReader(file)
        tasks = {}

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for i, row in enumerate(reader):
                if i >= max_rows:
                    break

                city_ids = row["cities"].split("-")
                neighborhoods = row["neighborhoods"].split("-") if row["neighborhoods"] != "0" else []
                category = row["category"]

                data = {
                    "city_ids": city_ids,
                    "search_data": {
                        "form_data": {
                            "data": {"category": {"str": {"value": category}}},
                        },
                        # "query": "قالیشویی",
                    },
                }

                headers = get_headers_with_random_cookie()
                future = executor.submit(requests.post, URL, headers=headers, json=data)
                tasks[future] = {
                    "category": category,
                    "cities": city_ids,
                    "neighborhoods": neighborhoods,
                    "row_count": row["count"],
                }
                request_count += 1
                print(f"Request {request_count}: Sent to Divar API")  # Log the request count

            count = 0
            for future in as_completed(tasks):
                row_data = tasks[future]
                response = future.result()
                count += 1
                print(count)
                if response.status_code == 200:
                    response_json = response.json()
                    link = extract_inset_banner_link(response_json)
                    if link:
                        extracted_links.append((link, row_data))
                        print(link)
                else:
                    print("fail", response.status_code)

    print(f"Total requests sent: {request_count}")  # Print the total number of requests sent
    return extracted_links


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

    for data in links:
        link, row_data = data[0], data[1]
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
                    redirected_urls.append((redirected_url, row_data))
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
        "YEKTANET": defaultdict(lambda: {"ad_count": 0, "impression_count": 0, "placements": []}),
        "tapsell": defaultdict(lambda: {"ad_count": 0, "impression_count": 0, "placements": []}),
        "DAART": defaultdict(lambda: {"ad_count": 0, "impression_count": 0, "placements": []}),
        "WITH UTM": defaultdict(lambda: {"ad_count": 0, "impression_count": 0, "placements": []}),
        "WITHOUT UTM": defaultdict(lambda: {"ad_count": 0, "impression_count": 0, "placements": []}),
    }

    totals_per_publisher = defaultdict(lambda: {"ad_count": 0, "impression_count": 0})

    for data in urls:
        url, row_data = data[0], data[1]
        domain = urlparse(url).netloc
        url = url.lower()
        row_count = int(row_data["row_count"]) // 7
        cities = row_data["cities"]
        neighborhoods = row_data["neighborhoods"]
        category = row_data["category"]

        if "adivery" in url or "yektanet" in url:
            counts["YEKTANET"][domain]["ad_count"] += 1
            counts["YEKTANET"][domain]["impression_count"] += row_count
            counts["YEKTANET"][domain]["placements"].append(
                {
                    "cities": cities,
                    "neighborhoods": neighborhoods,
                    "category": category,
                }
            )
            totals_per_publisher["YEKTANET"]["ad_count"] += 1
            totals_per_publisher["YEKTANET"]["impression_count"] += row_count
        elif "tapsell" in url:
            counts["tapsell"][domain]["ad_count"] += 1
            counts["tapsell"][domain]["impression_count"] += row_count
            counts["tapsell"][domain]["placements"].append(
                {
                    "cities": cities,
                    "neighborhoods": neighborhoods,
                    "category": category,
                }
            )
            totals_per_publisher["tapsell"]["ad_count"] += 1
            totals_per_publisher["tapsell"]["impression_count"] += row_count
        elif "daart" in url:
            counts["DAART"][domain]["ad_count"] += 1
            counts["DAART"][domain]["impression_count"] += row_count
            counts["DAART"][domain]["placements"].append(
                {
                    "cities": cities,
                    "neighborhoods": neighborhoods,
                    "category": category,
                }
            )
            totals_per_publisher["DAART"]["ad_count"] += 1
            totals_per_publisher["DAART"]["impression_count"] += row_count
        elif "utm" not in url:
            counts["WITHOUT UTM"][domain]["ad_count"] += 1
            counts["WITHOUT UTM"][domain]["impression_count"] += row_count
            counts["WITHOUT UTM"][domain]["placements"].append(
                {
                    "cities": cities,
                    "neighborhoods": neighborhoods,
                    "category": category,
                }
            )
            totals_per_publisher["WITHOUT UTM"]["ad_count"] += 1
            totals_per_publisher["WITHOUT UTM"]["impression_count"] += row_count
        else:
            counts["WITH UTM"][domain]["ad_count"] += 1
            counts["WITH UTM"][domain]["impression_count"] += row_count
            counts["WITH UTM"][domain]["placements"].append(
                {
                    "cities": cities,
                    "neighborhoods": neighborhoods,
                    "category": category,
                }
            )
            totals_per_publisher["WITH UTM"]["ad_count"] += 1
            totals_per_publisher["WITH UTM"]["impression_count"] += row_count

    return counts, totals_per_publisher


def print_category_counts(category_counts, totals_per_publisher):
    """Print the counts for each category, domain, and total ads."""
    for category, domains in category_counts.items():
        print(f"\nCategory: {category}")
        for domain, data in domains.items():
            print(f"{domain}: {data['ad_count']} ads, {data['impression_count']} impressions")

    print("\nTotal ads per publisher:")
    for publisher, data in totals_per_publisher.items():
        print(f"{publisher}: {data['ad_count']} ads, {data['impression_count']} impressions")

    print(f"\npercentage of each publisher (ads count):")
    sum_ads = sum([data["ad_count"] for data in totals_per_publisher.values()])
    for publisher, data in totals_per_publisher.items():
        print(f"{publisher}: {data['ad_count'] / sum_ads * 100:.2f}%")

    print(f"\npercentage of each publisher (impression count):")
    sum_impressions = sum([data["impression_count"] for data in totals_per_publisher.values()])
    for publisher, data in totals_per_publisher.items():
        print(f"{publisher}: {data['impression_count'] / sum_impressions * 100:.2f}%")


def save_category_counts_to_csv(category_counts, total_ads_per_publisher, csv_file_path):
    """Save the counts for each category, domain, and total ads to a CSV file, with each placement on a separate row and without repeating the other columns' data."""
    with open(csv_file_path, mode="w", newline="") as file:
        writer = csv.writer(file)

        # Write header for category counts
        writer.writerow(["Category", "Domain", "Ad Count", "Impression Count", "City", "Neighborhood", "Category Placement"])

        # Write category counts, separating each placement into a new row
        for category, domains in category_counts.items():
            for domain, count in domains.items():
                ad_count = count["ad_count"]
                impression_count = count["impression_count"]
                placements = count["placements"]

                # Write the first row with full data, then leave columns empty for subsequent placements
                for idx, placement in enumerate(placements):
                    cities = "-".join(placement["cities"])  # Convert list of cities to a single string
                    neighborhoods = "-".join(placement["neighborhoods"])  # Convert list of neighborhoods to a single string
                    category_placement = placement["category"]

                    if idx == 0:
                        # First row: full data
                        writer.writerow([category, domain, ad_count, impression_count, cities, neighborhoods, category_placement])
                    else:
                        # Subsequent rows: leave all but placement-specific columns empty
                        writer.writerow(
                            [
                                "",
                                "",
                                "",
                                "",  # Empty columns for category, domain, ad count, impression count
                                cities,
                                neighborhoods,
                                category_placement,
                            ]
                        )

        # Write total ads per publisher
        writer.writerow([])
        writer.writerow(["Publisher", "Total Ads Count"])
        for publisher, count in total_ads_per_publisher.items():
            writer.writerow([publisher, count])


def main():
    links = extract_links_from_csv(CSV_FILE_PATH)  # Get total_ads from CSV
    redirected_urls = get_redirected_urls(links)
    category_counts, total_ads_per_publisher = categorize_urls_and_aggregate(redirected_urls)
    print_category_counts(category_counts, total_ads_per_publisher)
    save_category_counts_to_csv(category_counts, total_ads_per_publisher, "output_results.csv")


if __name__ == "__main__":
    main()
