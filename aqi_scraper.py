"""
aqi_scraper.py
Scrapes AQI/ISPU data from udara.jakarta.go.id/lokasi_stasiun
and saves results as JSON + appends to a CSV log.

The site uses a DataTables backend. We POST to the draw endpoint
with start=0 (page 1, 50 entries) and start=50 (page 2, 50 entries).
"""

import requests
from bs4 import BeautifulSoup
import json
import csv
import os
from datetime import datetime, timezone, timedelta

JAKARTA_TZ = timezone(timedelta(hours=7))

# The page that renders the HTML table
PAGE_URL = "https://udara.jakarta.go.id/lokasi_stasiun"

OUTPUT_JSON = "aqi_latest.json"
OUTPUT_CSV  = "aqi_log.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; AQI-Scraper/1.0; "
        "+https://github.com/yourname/aqi-scraper)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://udara.jakarta.go.id/",
}


def parse_table_rows(html: str) -> list[dict]:
    """Parse <tr> rows from the stations table in the given HTML."""
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    # The table body contains all station rows
    tbody = soup.find("tbody")
    if not tbody:
        return rows

    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue

        station   = tds[0].get_text(strip=True)
        # ISPU value may be inside a <span class="badge ...">
        ispu_tag  = tds[1].find("span")
        ispu      = ispu_tag.get_text(strip=True) if ispu_tag else tds[1].get_text(strip=True)
        parameter = tds[2].get_text(strip=True)
        tanggal   = tds[3].get_text(strip=True)

        rows.append({
            "station":   station,
            "ispu":      ispu,
            "parameter": parameter,
            "tanggal":   tanggal,
        })

    return rows


def scrape() -> list[dict]:
    now_utc      = datetime.now(timezone.utc)
    now_jakarta  = now_utc.astimezone(JAKARTA_TZ)
    timestamp       = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    timestamp_local = now_jakarta.strftime("%Y-%m-%d %H:%M:%S")

    session = requests.Session()
    session.headers.update(HEADERS)

    all_records: list[dict] = []

    # The page is rendered server-side with a DataTables-style paginator.
    # We navigate by appending ?start=N to the URL, or by POSTing form data.
    # First, load the base page to get any cookies / CSRF tokens if needed.
    base_resp = session.get(PAGE_URL, timeout=15)
    base_resp.raise_for_status()

    # Try fetching page 1 (start=0) and page 2 (start=50) via GET with query params.
    # Many DataTables setups also accept ?start=&length= parameters on the base URL.
    for start in [0, 50]:
        params = {
            "start":  start,
            "length": 50,
        }
        resp = session.get(PAGE_URL, params=params, timeout=15)
        resp.raise_for_status()

        rows = parse_table_rows(resp.text)
        if not rows:
            print(f"[WARN] No rows found for start={start}. "
                  "The site may use a JS-rendered DataTables API endpoint.")
            break

        for row in rows:
            all_records.append({
                "timestamp":       timestamp,
                "timestamp_local": timestamp_local,
                **row,
            })

        print(f"[INFO] start={start}: fetched {len(rows)} rows")

        # Stop if we got fewer than 50 — we're on the last page
        if len(rows) < 50:
            break

    # ------------------------------------------------------------------ #
    # FALLBACK: If the table is populated via a DataTables AJAX endpoint,  #
    # the rows won't appear in the static HTML. In that case, we need to   #
    # find the XHR URL. Inspect Network tab in DevTools for a request to   #
    # something like /api/lokasi_stasiun or /lokasi_stasiun?draw=1 and     #
    # uncomment + adapt the block below.                                   #
    # ------------------------------------------------------------------ #
    # AJAX_URL = "https://udara.jakarta.go.id/lokasi_stasiun"  # adjust
    # for start in [0, 50]:
    #     payload = {
    #         "draw":             1,
    #         "start":            start,
    #         "length":           50,
    #         "search[value]":    "",
    #         "search[regex]":    "false",
    #         "order[0][column]": 0,
    #         "order[0][dir]":    "asc",
    #     }
    #     resp = session.post(AJAX_URL, data=payload, timeout=15)
    #     resp.raise_for_status()
    #     data = resp.json()           # {"data": [[col0, col1, col2, col3], ...], "recordsTotal": 97}
    #     for row_data in data.get("data", []):
    #         all_records.append({
    #             "timestamp":       timestamp,
    #             "timestamp_local": timestamp_local,
    #             "station":         BeautifulSoup(row_data[0], "html.parser").get_text(strip=True),
    #             "ispu":            BeautifulSoup(row_data[1], "html.parser").get_text(strip=True),
    #             "parameter":       row_data[2],
    #             "tanggal":         row_data[3],
    #         })
    #     if len(data.get("data", [])) < 50:
    #         break

    return all_records


def load_last_tanggal_per_station(csv_path: str) -> dict:
    """
    Read the CSV from the BOTTOM UP and return {station: last_tanggal}.
    Stops reading as soon as every unique station has been seen, so it only
    touches the most recent rows and skips the entire historical bulk.
    Returns an empty dict if the file does not exist yet.
    """
    if not os.path.isfile(csv_path):
        return {}

    last: dict = {}

    # Read backwards using a line-by-line tail approach.
    # We open in binary mode so we can seek freely, then decode each line.
    with open(csv_path, "rb") as f:
        # Grab header to find column indices
        header_line = f.readline().decode("utf-8")
        fieldnames = [h.strip() for h in header_line.split(",")]
        try:
            station_idx = fieldnames.index("station")
            tanggal_idx = fieldnames.index("tanggal")
        except ValueError:
            return {}  # malformed CSV

        # Collect all lines in reverse without loading entire file into memory
        f.seek(0, 2)
        pos = f.tell()
        remainder = b""

        while pos > len(header_line.encode("utf-8")):
            chunk_size = min(4096, pos - len(header_line.encode("utf-8")))
            pos -= chunk_size
            f.seek(pos)
            chunk = f.read(chunk_size)
            remainder = chunk + remainder
            lines = remainder.split(b"\n")
            # The first element may be an incomplete line; carry it forward
            remainder = lines[0]

            for line in reversed(lines[1:]):
                decoded = line.decode("utf-8", errors="ignore").strip()
                if not decoded:
                    continue
                cols = decoded.split(",")
                if len(cols) <= max(station_idx, tanggal_idx):
                    continue
                station = cols[station_idx].strip()
                tanggal = cols[tanggal_idx].strip()
                if station and station not in last:
                    last[station] = tanggal

        # Edge case: handle whatever is left in remainder (lines near top of file)
        if remainder:
            decoded = remainder.decode("utf-8", errors="ignore").strip()
            if decoded:
                cols = decoded.split(",")
                if len(cols) > max(station_idx, tanggal_idx):
                    station = cols[station_idx].strip()
                    tanggal = cols[tanggal_idx].strip()
                    if station and station not in last:
                        last[station] = tanggal

    print(f"[DEDUP] Last tanggal loaded for {len(last)} station(s) from {csv_path} (tail-read)")
    return last


def filter_new_records(records: list[dict], last_tanggal: dict) -> list[dict]:
    """
    Keep a record only if its tanggal differs from the last recorded
    tanggal for that station (or the station is brand new).
    Comparison is per-station — no full history scan needed.
    """
    new_records = [
        r for r in records
        if last_tanggal.get(r["station"]) != r["tanggal"]
    ]
    skipped = len(records) - len(new_records)
    if skipped:
        print(f"[DEDUP] Skipped {skipped} unchanged row(s), {len(new_records)} new row(s) to append.")
    else:
        print(f"[DEDUP] All {len(new_records)} row(s) are new.")
    return new_records


def save_json(records: list[dict]) -> None:
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"[JSON] Saved {len(records)} records → {OUTPUT_JSON}")


def append_csv(records: list[dict]) -> None:
    fieldnames = ["timestamp", "timestamp_local", "station", "ispu", "parameter", "tanggal"]
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
    if not data:
        print("No data found — the site likely uses a DataTables AJAX endpoint.")
        print("Enable the FALLBACK block in scrape() after finding the XHR URL.")
    else:
        # Load only the last tanggal per station, then filter
        last_tanggal = load_last_tanggal_per_station(OUTPUT_CSV)
        new_data = filter_new_records(data, last_tanggal)

        if new_data:
            save_json(new_data)   # JSON always reflects the latest new batch
            append_csv(new_data)
            print(f"Done. Appended {len(new_data)} new record(s).")
        else:
            print("No new data — all stations unchanged since last run. Nothing written.")
