"""
aqi_scraper.py
Scrapes AQI/ISPU data from udara.jakarta.go.id/lokasi_stasiun using Playwright.
Saves results as JSON + appends new rows to a CSV log (deduplication by
last recorded tanggal per station).

Requirements:
    pip install playwright
    playwright install chromium
"""

import csv
import json
import os
import sys
import time
import random
from datetime import datetime, timezone, timedelta

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ---------------------------------------------------------------------------
JAKARTA_TZ        = timezone(timedelta(hours=7))
PAGE_URL          = "https://udara.jakarta.go.id/lokasi_stasiun"
OUTPUT_JSON       = "aqi_latest.json"
OUTPUT_CSV        = "aqi_log.csv"
PAGE_LOAD_TIMEOUT = 60_000   # milliseconds (Playwright uses ms)
ENTRIES_PER_PAGE  = 50
MAX_RETRIES       = 5
RETRY_DELAY       = 30       # seconds
# ---------------------------------------------------------------------------


def build_browser_context(playwright):
    """Launch Playwright Chromium with stealth settings."""
    browser = playwright.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--window-size=1280,720",
            "--disable-blink-features=AutomationControlled",
            "--disable-features=VizDisplayCompositor",
            "--aggressive-cache-discard",
            "--blink-settings=imagesEnabled=false",
        ]
    )

    context = browser.new_context(
        viewport={"width": 1280, "height": 720},
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        locale="id-ID",
        timezone_id="Asia/Jakarta",
        java_script_enabled=True,
        # Mask automation fingerprint
        extra_http_headers={
            "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        }
    )

    # Remove webdriver property to avoid bot detection
    context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
        Object.defineProperty(navigator, 'languages', { get: () => ['id-ID', 'id', 'en-US'] });
        window.chrome = { runtime: {} };
    """)

    return browser, context


def wait_for_table(page) -> bool:
    """
    Wait until the station table has DKI rows.
    Returns True if data found, False if table empty.
    """
    try:
        # Wait for table body to appear
        page.wait_for_selector("table tbody", timeout=15_000)
        page.wait_for_timeout(3_000)  # let DataTables render

        # Check if DKI rows exist
        all_tds = page.query_selector_all("table tbody tr td")
        dki_cells = [td for td in all_tds if td.inner_text().strip().startswith("DKI")]

        if all_tds and not dki_cells:
            print("[WARN] Table present but no DKI station data — site not serving data.")
            return False

        # Wait for actual data row
        page.wait_for_selector("table tbody tr td.dtr-control", timeout=PAGE_LOAD_TIMEOUT)
        print("[INFO] Table detected via selector: dtr-control (responsive DataTables)")
        return True

    except PlaywrightTimeoutError:
        # Fallback selectors
        for selector, label in [
            ("table#DataTables_Table_0 tbody tr td:first-child", "first TD of known table ID"),
            ("table tbody tr td", "any table tbody td"),
        ]:
            try:
                page.wait_for_selector(selector, timeout=10_000)
                els = page.query_selector_all(selector)
                if any(el.inner_text().strip() for el in els):
                    print(f"[INFO] Table detected via selector: {label}")
                    return True
            except PlaywrightTimeoutError:
                continue

        return False


def set_entries_per_page(page, n: int = ENTRIES_PER_PAGE):
    """Change the DataTable 'show N entries' dropdown."""
    try:
        page.wait_for_selector("select[name]", timeout=10_000)
        page.select_option("select[name]", str(n))
        page.wait_for_timeout(3_000)
        print(f"[INFO] Set entries-per-page to {n}")
    except Exception as e:
        print(f"[WARN] Could not set entries-per-page: {e}")


def parse_current_page(page) -> list[dict]:
    """Extract all visible station rows from the current table page."""
    rows = []
    trs = page.query_selector_all("table tbody tr")

    for tr in trs:
        tds = tr.query_selector_all("td")
        if len(tds) < 4:
            continue

        station = tds[0].inner_text().strip()
        if not station.startswith("DKI"):
            continue

        # ISPU: prefer badge span, fall back to cell text
        try:
            span = tds[1].query_selector("span")
            ispu = span.inner_text().strip() if span else tds[1].inner_text().strip()
        except Exception:
            ispu = tds[1].inner_text().strip()

        parameter = tds[2].inner_text().strip()
        tanggal   = tds[3].inner_text().strip()

        rows.append({
            "station":   station,
            "ispu":      ispu,
            "parameter": parameter,
            "tanggal":   tanggal,
        })

    return rows


def go_to_next_page(page) -> bool:
    """
    Click the DataTable Next button.
    Returns True if navigated, False if on last page.
    """
    selectors = [
        "a.paginate_button.next",
        "button.paginate_button.next",
        "li.paginate_button.next a",
        "#DataTables_Table_0_next",
        "[id$='_next']",
    ]

    next_btn = None
    for sel in selectors:
        next_btn = page.query_selector(sel)
        if next_btn:
            break

    if not next_btn:
        print("[WARN] Could not find Next button.")
        return False

    classes = next_btn.get_attribute("class") or ""
    if "disabled" in classes:
        return False

    try:
        first_row_before = page.query_selector("table tbody tr td").inner_text().strip()
    except Exception:
        first_row_before = ""

    try:
        next_btn.scroll_into_view_if_needed()
        page.wait_for_timeout(500)
        next_btn.click()

        # Wait for page to change
        page.wait_for_function(
            f"""() => {{
                const td = document.querySelector('table tbody tr td');
                return td && td.innerText.trim() !== {json.dumps(first_row_before)};
            }}""",
            timeout=20_000
        )
        return True

    except Exception as e:
        print(f"[WARN] go_to_next_page error: {e}")
        return False


def scrape() -> list[dict]:
    """Launch Playwright, navigate the table pages, return all scraped records."""
    now_utc         = datetime.now(timezone.utc)
    now_jakarta     = now_utc.astimezone(JAKARTA_TZ)
    timestamp       = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    timestamp_local = now_jakarta.strftime("%Y-%m-%d %H:%M:%S")

    all_records = []

    with sync_playwright() as p:
        browser, context = build_browser_context(p)
        page = context.new_page()

        try:
            print("[INFO] Launching Chrome via Playwright…")
            print(f"[INFO] Loading {PAGE_URL} …")

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    if attempt > 1:
                        print(f"[INFO] Retry {attempt}/{MAX_RETRIES} — refreshing page …")

                    page.goto(PAGE_URL, wait_until="domcontentloaded", timeout=90_000)

                    if wait_for_table(page):
                        print(f"[INFO] Page loaded successfully on attempt {attempt}.")
                        break
                    else:
                        raise PlaywrightTimeoutError("No DKI data in table")

                except PlaywrightTimeoutError as e:
                    print(f"[WARN] Attempt {attempt}/{MAX_RETRIES} timed out: {e}")
                    if attempt < MAX_RETRIES:
                        print(f"[INFO] Waiting {RETRY_DELAY}s before retrying …")
                        time.sleep(RETRY_DELAY)
                    else:
                        print("[WARN] All attempts failed — site appears unavailable. Skipping this run.")
                        return []

            set_entries_per_page(page, ENTRIES_PER_PAGE)

            page_num = 1
            while True:
                rows = parse_current_page(page)
                print(f"[INFO] Page {page_num}: found {len(rows)} station rows")

                for row in rows:
                    all_records.append({
                        "timestamp":       timestamp,
                        "timestamp_local": timestamp_local,
                        **row,
                    })

                if not go_to_next_page(page):
                    print(f"[INFO] Reached last page ({page_num}). Done scraping.")
                    break
                page_num += 1

        except Exception as e:
            print(f"[ERROR] Unexpected error: {e}", file=sys.stderr)
            return []

        finally:
            context.close()
            browser.close()

    return all_records


# ---------------------------------------------------------------------------
# Deduplication — read CSV tail to get last tanggal per station
# ---------------------------------------------------------------------------

def load_last_tanggal_per_station(csv_path: str) -> dict:
    """
    Read the CSV from the BOTTOM UP and return {station: last_tanggal}.
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
    jitter = random.randint(5, 30)
    print(f"[{datetime.now(timezone.utc).isoformat()}] Waiting {jitter}s before starting (jitter) ...")
    time.sleep(jitter)
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting scrape ...")

    data = scrape()

    if not data:
        print("[ERROR] No data scraped — site was likely unavailable.", file=sys.stderr)
        sys.exit(1)

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
