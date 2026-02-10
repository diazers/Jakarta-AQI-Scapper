"""
aqi_scraper.py
Scrapes AQI data from rendahemisi.jakarta.go.id/ispu
and saves results as JSON + appends to a CSV log.
"""

import requests
from bs4 import BeautifulSoup
import json
import csv
import os
from datetime import datetime, timezone

URL = "https://rendahemisi.jakarta.go.id/ispu"
OUTPUT_JSON = "aqi_latest.json"
OUTPUT_CSV  = "aqi_log.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; AQI-Scraper/1.0; "
        "+https://github.com/yourname/aqi-scraper)"
    )
}

def slug_to_name(slug: str) -> str:
    """Convert URL slug to readable name, e.g. 'us-embassy-1' → 'Us Embassy 1'"""
    return slug.replace("-", " ").title()


def scrape() -> list[dict]:
    resp = requests.get(URL, headers=HEADERS, timeout=15)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    records = []

    # Each station is an <li> inside the blog-grid <ul>
    for li in soup.select("ul.blog-grid > li.grid-item"):
        # Detail link — used to extract station name from slug
        link_tag = li.select_one("a[href*='ispu-detail']")

        # Station name: extract last segment of URL e.g. '.../us-embassy-1' → 'Us Embassy 1'
        station_name = None
        detail_url = None
        if link_tag:
            detail_url = link_tag["href"]
            slug = detail_url.rstrip("/").split("/")[-1]
            station_name = slug_to_name(slug)

        # AQI value — the big number inside the colored box
        value_tag = li.select_one(
            "div.position-relative.text-center.padding-20px-lr h5"
        )

        # Pollutant (PM 2.5, SO2 …) is in a <p> just below the value
        pollutant_tag = li.select_one(
            "div.position-relative.text-center.padding-20px-lr p"
        )

        # Status label (Baik / Sedang / Tidak Sehat / …)
        # Sits inside a <span> with text-uppercase and letter-spacing classes
        status_tag = li.select_one("span.text-uppercase")

        record = {
            "timestamp":  timestamp,
            "station":    station_name,
            "aqi":        value_tag.get_text(strip=True)    if value_tag    else None,
            "pollutant":  pollutant_tag.get_text(strip=True) if pollutant_tag else None,
            "status":     status_tag.get_text(strip=True)   if status_tag   else None,
            "detail_url": detail_url,
        }
        if record["station"]:  # skip blank/ghost items
            records.append(record)

    return records


def save_json(records: list[dict]) -> None:
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"[JSON] Saved {len(records)} records → {OUTPUT_JSON}")


def append_csv(records: list[dict]) -> None:
    fieldnames = ["timestamp", "station", "aqi", "pollutant", "status", "detail_url"]
    file_exists = os.path.isfile(OUTPUT_CSV)
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(records)
    print(f"[CSV]  Appended {len(records)} rows → {OUTPUT_CSV}")


if __name__ == "__main__":
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting scrape …")
    data = scrape()
    if data:
        save_json(data)
        append_csv(data)
        print("Done.")
    else:
        print("No data found — check if the site structure changed.")
