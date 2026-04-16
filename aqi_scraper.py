"""
aqi_scraper.py
Scrapes AQI/ISPU data from udara.jakarta.go.id/lokasi_stasiun using Selenium.
Saves results as JSON + appends new rows to a CSV log (deduplication by
last recorded tanggal per station).

Requirements:
    pip install selenium webdriver-manager
    Chrome must be installed on the machine.
"""

import csv
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

try:
    from webdriver_manager.chrome import ChromeDriverManager
    USE_WEBDRIVER_MANAGER = True
except ImportError:
    USE_WEBDRIVER_MANAGER = False

# ---------------------------------------------------------------------------
JAKARTA_TZ        = timezone(timedelta(hours=7))
PAGE_URL          = "https://udara.jakarta.go.id/lokasi_stasiun"
OUTPUT_JSON       = "aqi_latest.json"
OUTPUT_CSV        = "aqi_log.csv"
PAGE_LOAD_TIMEOUT = 60   # increased: seconds to wait for table to appear
ENTRIES_PER_PAGE  = 50   # set the DataTable to 50 rows per page
MAX_RETRIES       = 3    # increased from 3
RETRY_DELAY       = 20   # increased from 10s — give the site time to recover
# ---------------------------------------------------------------------------


def build_driver() -> webdriver.Chrome:
    """Create a headless Chrome WebDriver."""
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")           # required in CI/Docker
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,720")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_argument("--single-process")
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-features=VizDisplayCompositor")
    options.add_argument("--memory-pressure-off")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
    if USE_WEBDRIVER_MANAGER:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
    else:
        # Assumes chromedriver is on PATH
        driver = webdriver.Chrome(options=options)

    driver.set_page_load_timeout(90)  # increased from 60
    return driver


# ---------------------------------------------------------------------------
# Table detection — multiple fallback selectors in order of specificity
# ---------------------------------------------------------------------------

# Each entry is (CSS selector, description). We try them in order and use
# the first one that finds at least one element with non-empty text.
TABLE_ROW_SELECTORS = [
    ("table tbody tr td.dtr-control",  "dtr-control (responsive DataTables)"),
    ("table#DataTables_Table_0 tbody tr td:first-child", "first TD of known table ID"),
    ("table tbody tr td",              "any table tbody td"),
]


def wait_for_table(driver: webdriver.Chrome, timeout: int = PAGE_LOAD_TIMEOUT) -> str:
    """
    Wait until the station table has at least one rendered data row.
    Tries multiple CSS selectors so a change in DataTables responsive mode
    (or a missing dtr-control class) doesn't cause a permanent timeout.
    Returns the selector that succeeded.
    """
    deadline = time.monotonic() + timeout
    last_exc = None

    while time.monotonic() < deadline:
        for selector, label in TABLE_ROW_SELECTORS:
            try:
                remaining = max(1, int(deadline - time.monotonic()))
                WebDriverWait(driver, min(remaining, 10)).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                # Verify at least one cell actually has text (not a loading spinner row)
                els = driver.find_elements(By.CSS_SELECTOR, selector)
                if any(el.text.strip() for el in els):
                    print(f"[INFO] Table detected via selector: {label}")
                    return selector
            except TimeoutException as e:
                last_exc = e
                continue
            except Exception as e:
                last_exc = e
                continue
        time.sleep(2)  # brief pause before retrying the selector loop

    raise TimeoutException(
        f"Table not found after {timeout}s with any known selector. Last error: {last_exc}"
    )


def set_entries_per_page(driver: webdriver.Chrome, n: int = ENTRIES_PER_PAGE):
    """Change the DataTable 'show N entries' dropdown then wait for re-render."""
    try:
        select_el = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "select[name]"))
        )
        Select(select_el).select_by_value(str(n))
        time.sleep(3)           # let DataTable re-render (slightly longer)
        wait_for_table(driver)
        print(f"[INFO] Set entries-per-page to {n}")
    except Exception as e:
        print(f"[WARN] Could not set entries-per-page: {e}")


def parse_current_page(driver: webdriver.Chrome) -> list[dict]:
    """Extract all visible station rows from the current table page."""
    rows = []
    trs  = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")

    for tr in trs:
        tds = tr.find_elements(By.TAG_NAME, "td")
        if len(tds) < 4:
            continue

        station = tds[0].text.strip()

        # Skip non-station rows (legend rows, empty rows)
        if not station.startswith("DKI"):
            continue

        # ISPU: prefer the badge <span>, fall back to raw cell text
        try:
            ispu = tds[1].find_element(By.TAG_NAME, "span").text.strip()
        except Exception:
            ispu = tds[1].text.strip()

        parameter = tds[2].text.strip()
        tanggal   = tds[3].text.strip()

        rows.append({
            "station":   station,
            "ispu":      ispu,
            "parameter": parameter,
            "tanggal":   tanggal,
        })

    return rows


def go_to_next_page(driver: webdriver.Chrome) -> bool:
    """
    Click the DataTable Next button.
    Returns True if navigation succeeded, False if already on the last page.
    Tries multiple common DataTables selectors for the Next button.
    """
    selectors = [
        "a.paginate_button.next",
        "button.paginate_button.next",
        "li.paginate_button.next a",
        "#DataTables_Table_0_next",
        "[id$='_next']",
    ]
    try:
        next_btn = None
        for sel in selectors:
            try:
                next_btn = driver.find_element(By.CSS_SELECTOR, sel)
                break
            except Exception:
                continue

        if next_btn is None:
            print("[WARN] Could not find Next button with any known selector.")
            return False

        classes = next_btn.get_attribute("class") or ""
        if "disabled" in classes:
            return False

        try:
            first_row_before = driver.find_element(
                By.CSS_SELECTOR, "table tbody tr td"
            ).text.strip()
        except Exception:
            first_row_before = ""

        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_btn)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", next_btn)

        def page_changed(d):
            try:
                # Accept any first-cell change, not just DKI-prefixed
                first_row = d.find_element(
                    By.CSS_SELECTOR, "table tbody tr td"
                ).text.strip()
                return first_row != first_row_before
            except Exception:
                return False

        WebDriverWait(driver, 20).until(page_changed)  # increased from 15
        return True

    except Exception as e:
        print(f"[WARN] go_to_next_page error: {e}")
        return False


def scrape() -> list[dict]:
    """Launch Chrome, navigate the table pages, return all scraped records."""
    now_utc         = datetime.now(timezone.utc)
    now_jakarta     = now_utc.astimezone(JAKARTA_TZ)
    timestamp       = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    timestamp_local = now_jakarta.strftime("%Y-%m-%d %H:%M:%S")

    driver      = None
    all_records = []

    try:
        print("[INFO] Launching Chrome …")
        driver = build_driver()

        print(f"[INFO] Loading {PAGE_URL} …")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                if attempt > 1:
                    print(f"[INFO] Retry {attempt}/{MAX_RETRIES} — refreshing page …")
                    # Full quit + rebuild on retry to clear any stale state
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = build_driver()

                driver.get(PAGE_URL)
                wait_for_table(driver)
                print(f"[INFO] Page loaded successfully on attempt {attempt}.")
                break
            except TimeoutException as e:
                print(f"[WARN] Attempt {attempt}/{MAX_RETRIES} timed out: {e}", file=sys.stderr)
                if attempt < MAX_RETRIES:
                    print(f"[INFO] Waiting {RETRY_DELAY}s before retrying …")
                    time.sleep(RETRY_DELAY)
                else:
                    print("[WARN] All attempts failed — site appears unavailable. Skipping this run.", file=sys.stderr)
                    return []

        set_entries_per_page(driver, ENTRIES_PER_PAGE)

        page = 1
        while True:
            rows = parse_current_page(driver)
            print(f"[INFO] Page {page}: found {len(rows)} station rows")

            for row in rows:
                all_records.append({
                    "timestamp":       timestamp,
                    "timestamp_local": timestamp_local,
                    **row,
                })

            if not go_to_next_page(driver):
                print(f"[INFO] Reached last page ({page}). Done scraping.")
                break
            page += 1

    except WebDriverException as e:
        print(f"[ERROR] WebDriver error: {e}", file=sys.stderr)
        return []
    finally:
        if driver:
            driver.quit()

    return all_records


# ---------------------------------------------------------------------------
# Deduplication — read CSV tail to get last tanggal per station
# ---------------------------------------------------------------------------

def load_last_tanggal_per_station(csv_path: str) -> dict:
    """
    Read the CSV from the BOTTOM UP and return {station: last_tanggal}.
    Stops as soon as every station has been seen, skipping all historical rows.
    Returns an empty dict if the file does not exist yet.
    """
    if not os.path.isfile(csv_path):
        return {}

    last = {}

    with open(csv_path, "rb") as f:
        header_line = f.readline().decode("utf-8")
        fieldnames  = [h.strip() for h in header_line.split(",")]
        try:
            station_idx = fieldnames.index("station")
            tanggal_idx = fieldnames.index("tanggal")
        except ValueError:
            return {}

        f.seek(0, 2)
        pos       = f.tell()
        remainder = b""

        while pos > len(header_line.encode("utf-8")):
            chunk_size = min(4096, pos - len(header_line.encode("utf-8")))
            pos -= chunk_size
            f.seek(pos)
            chunk     = f.read(chunk_size)
            remainder = chunk + remainder
            lines     = remainder.split(b"\n")
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
    """Keep only records whose tanggal differs from the last recorded value."""
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


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def save_json(records: list[dict]) -> None:
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"[JSON] Saved {len(records)} records -> {OUTPUT_JSON}")


def append_csv(records: list[dict]) -> None:
    fieldnames  = ["timestamp", "timestamp_local", "station", "ispu", "parameter", "tanggal"]
    file_exists = os.path.isfile(OUTPUT_CSV)
    clean = [r for r in records if r["station"].strip().startswith("DKI")]
    if len(clean) < len(records):
        print(f"[WARN] Dropped {len(records) - len(clean)} non-DKI row(s) before writing.")
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(clean)
    print(f"[CSV]  Appended {len(clean)} rows -> {OUTPUT_CSV}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting scrape ...")
    data = scrape()

    if not data:
        print("[ERROR] No data scraped — site was likely unavailable.", file=sys.stderr)
        sys.exit(1)  # exit 1 so GitHub Actions marks the run as failed

    is_first_run = not os.path.isfile(OUTPUT_CSV)

    if is_first_run:
        print("[INFO] No existing CSV found — first run. Writing all records.")
        save_json(data)
        append_csv(data)
        print(f"Done. Created {OUTPUT_CSV} and {OUTPUT_JSON} with {len(data)} record(s).")
    else:
        last_tanggal = load_last_tanggal_per_station(OUTPUT_CSV)
        new_data     = filter_new_records(data, last_tanggal)

        if new_data:
            save_json(new_data)
            append_csv(new_data)
            print(f"Done. Appended {len(new_data)} new record(s).")
        else:
            print("No new data — all stations unchanged since last run. Nothing written.")
