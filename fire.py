import csv
import requests
import uuid
from concurrent.futures import ThreadPoolExecutor

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
MAX_ROWS = 5000  # Limit the number of rows to process
MAX_WORKERS = 20  # Number of threads for parallel execution


def get_headers_with_random_cookie():
    """Generate headers with a random UUID in the cookie."""
    headers = HEADERS_TEMPLATE.copy()
    random_cookie = str(uuid.uuid4())
    headers["cookie"] = f"did={random_cookie};"
    return headers


def send_request_fire_and_forget(url, headers, data):
    """Send POST request without waiting for a response."""
    session = requests.Session()
    try:
        session.post(url, headers=headers, json=data, timeout=0.001)  # Very short timeout
    except requests.exceptions.RequestException:
        pass  # Ignore all request exceptions
    finally:
        session.close()  # Close the session immediately


def extract_links_from_csv(csv_file_path, max_rows=MAX_ROWS):
    """Extract links from the CSV file and send fire-and-forget requests."""
    total_count = 0
    request_count = 0

    with open(csv_file_path, mode="r") as file:
        reader = csv.DictReader(file)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for i, row in enumerate(reader):
                if i >= max_rows:
                    break
                total_count += int(row["count"])

                city_ids = ["6"]
                neighborhoods = row["neighborhoods"].split("-") if row["neighborhoods"] != "0" else []
                category = "game-consoles-and-video-games"

                data = {
                    "city_ids": city_ids,
                    "search_data": {
                        "form_data": {
                            "data": {"category": {"str": {"value": category}}},
                        },
                    },
                }

                headers = get_headers_with_random_cookie()
                executor.submit(send_request_fire_and_forget, URL, headers, data)
                request_count += 1
                print(f"Request {request_count}: Sent to Divar API (fire-and-forget)")

    print("Total count:", total_count)
    print(f"Total requests sent: {request_count}")  # Print the total number of requests sent


if __name__ == "__main__":
    extract_links_from_csv(CSV_FILE_PATH)


{
    "id": "f59b91d5-d261-4323-88d7-d12b5138ff97",
    "bidid": "4a776959-0e98-429a-b319-7431b75aca8b",
    "seatbidList": [
        {
            "seat": "yektanet",
            "bidList": [
                {
                    "id": "k61ApzLFyf7nc0kp3-7HQD5xdtixKIoZLnWLui-KzH6yFjfa4AlLHW_AXwIkwHAzcfgZW7NR7tVBtR1VLxclvPoZ5yW6g_LXmMXKvvN_3nEATaDydAqtb19f6sOeyH5lY7EF122_VsLobxBip-JBiLHyZUV1",
                    "item": "f59b91d5-d261-4323-88d7-d12b5138ff97",
                    "price": 190,
                    "purl": "http://localhost:3000/api/v1/divar/auction-result/90TsRMQKtCFA-ZvEKDskeJUN6dFz6cC-aui2u3B2ZzvSbvlcMp2yL5gMGPAVVT0q6tkqdVpNMvMuVP2VNbi0Eqkua6EKD-T7KRM2_fQxCL68dZgj4x1Eh0tq5b95y3G26Juk1rbuDeH3wTP7sDgZRuNTAgj2pulnkf0",
                    "burl": "http://localhost:3000/api/v2/record/Q_bAYwpxxWWNIu97LkfkUcK1dnfoX6SGBbCUNeUwqNl_bkZiO9OXd7i2ezV-P0qnsjvmUIhvB6tRFhJeHqaJ2yLRzcC9VvCBS4hx6eIStsGReih0044TtIrnjjvgdd7_cZnELmGCoBWlykjVmTLqh7k-Rx414egIctca-4YRjMZ9nZ-Dd0A08uLxWUPCqoZXix49NC3CguiGQvFE_et1Ed_AimOjzW54WRqJXFLqf-lUC6a03zOLR4UcZrPAkAWLxy8axBNxf7Pdg7vynAWKv-9wKjIVcswz",
                    "lurl": "http://localhost:3000/api/v1/divar/auction-result/o_4zBELd78Xr7uJC3XBt_Cq9caAmEZcW5n1VCkRLNKOfv564IvRP7Zy_SUNIM3y_87cmmEfOfDO-texiG5dhdz-pdPE7V16Lltl_kJ1tWybpG12iS9a1Hi6VP6CLIAYAMIr7HcQQGDHkycMlxkRoeDDf5C-E5gKcRl47?lr=${OPENRTB_LOSS}",
                    "mid": "11260",
                    "macroList": [
                        {
                            "key": "URL_PARAMS",
                            "value": "nFocdIExsa7lqcEKuGEPEX-NakEAYNAZWyK_-T6WjLwOgidXnxnNb3bkz6AWV1yxu7Q7FnAFAEdyGp2D2IhJRVAybZGyKKVz3VxSH-Ep7Hoi3O5fkPfziWrg9niE_YWoRswN_7YS7cpVmmID5wQiWpD7jQ0x0y9B7IueYU235UsMccKJxDMVwcEtrzeQwsTSmjLcf5jWkTL9PjN3Bey8YUBMOhvY4PFnVsqEEEQNJ1HEPIAnRSgv8e67YMWBaH8OAn8-EFqeu7o-lz5nV75C2h9OMQWnRirh&redirect=https%3A%2F%2Flanding.tapsi.food%2Fld%3Futm_source%3Dyektanet%26utm_campaign%3D0603-dvr.dsp-yn-luckydraw03-shz-cat%26utm_medium%3Ddvr-display%26utm_term%3Dyn_mob_101926%26utm_content%3Dgame-consoles-and-video-games%26utm_yn_adivery%3Dv1-MTI2NjIyOjQzNjAyMzoxMDE5MjY6MTgyMTU6MzM3MDE6MDhkNTE3YzMtYWM1MS01NGU4LTkwMzYtYTY1ZmM5NzdhYjExOjE3MjY4NDk4MjU6OjA6Y3BtOjgxODE6NTA6MzY%26utm_yn_data%3Dv6.i.AF8.bZAN.G6s.iVJ.eTX.08d517c3-ac51-54e8-9036-a65fc977ab11.57dc9b40-7ce0-4f30-87fb-0fcc8049305a.Y.W10%253D.W10%253D.8146bae3-b790-4bb4-9fae-22516b4c22a6.NATIVE.m%26utm_yn_divar%3Dv1-MTcyNjg0OTgyNToxMjY2MjI6NDM2MDIzOjEwMTkyNjpnYW1lLWNvbnNvbGVzLWFuZC12aWRlby1nYW1lczo6Ng%26utm_yn%3Dv4-MTo0MzYwMjM6MTI2NjIyOjE6MToxOjI%26inapp_gps_adid%3D08d517c3-ac51-54e8-9036-a65fc977ab11",
                        },
                        {
                            "key": "UTM_PARAMS",
                            "value": "&utm_source=yektanet&utm_campaign=0603-dvr.dsp-yn-luckydraw03-shz-cat&utm_medium=dvr-display&utm_term=yn_mob_101926&utm_content=game-consoles-and-video-games&utm_yn_adivery=v1-MTI2NjIyOjQzNjAyMzoxMDE5MjY6MTgyMTU6MzM3MDE6MDhkNTE3YzMtYWM1MS01NGU4LTkwMzYtYTY1ZmM5NzdhYjExOjE3MjY4NDk4MjU6OjA6Y3BtOjgxODE6NTA6MzY&utm_yn_data=v6.i.AF8.bZAN.G6s.iVJ.eTX.08d517c3-ac51-54e8-9036-a65fc977ab11.ed23c5a9-bc15-4ef0-abd7-184837f244ed.Y.W10=.W10=.c4141fe5-641c-48e8-84eb-a675d5c7e2b7.NATIVE.m&utm_yn_divar=v1-MTcyNjg0OTgyNToxMjY2MjI6NDM2MDIzOjEwMTkyNjpnYW1lLWNvbnNvbGVzLWFuZC12aWRlby1nYW1lczo6Ng&utm_yn=v4-MTo0MzYwMjM6MTI2NjIyOjE6MToxOjI",
                        },
                        {
                            "key": "TRKR_PARAMS",
                            "value": "ZXBzCjFdSvp_X2OGWK113y1PctN-NjjHDj8ZgYsoXKj1k32KyBQTViKx1yQad6r_v4QJGelEX79W0nxFHmFEb5KVGzdnxRVbDkzrp2UGa9lXUUfZMLkfu97GYv5dxTwGdG1yN-cmLQn4h2AeW7nDMBCd_KQBHfAJhqJEHLc-EvqMNpq-wzweko2LnSdDmdf2q9Xar0IyNaIvR6VdZld1vizNdDKw_4X-5Wgh3a-SYTaSIfvhCHPkR4_UCJVUON48MXWxDvV1zk_iUIWgcSIdtGnyy79Zugq1",
                        },
                    ],
                }
            ],
        }
    ],
}
