import asyncio
import re
import csv
import os
import sys
from datetime import datetime
from playwright.async_api import async_playwright

STATIONS = [
    {"name": "Jakarta GBK Gelora",        "url": "https://aqicn.org/station/@416842/"},
    {"name": "Kemayoran",                 "url": "https://aqicn.org/station/indonesia/kemayoran/"},
    {"name": "KBN Marunda Jakarta Utara", "url": "https://aqicn.org/station/@531679/"},
    {"name": "Jakarta Timur Kebon Nanas", "url": "https://aqicn.org/station/@531565/"},
    {"name": "Krukut",                    "url": "https://aqicn.org/station/@495982/"},
    {"name": "Pakubuwono Menteng",        "url": "https://aqicn.org/station/@521365/"},
    {"name": "Permata Hijau 3 Nafas",     "url": "https://aqicn.org/station/@570235/"},
    {"name": "Pakubuwono 3 Nafas",        "url": "https://aqicn.org/station/@537937/"},
    {"name": "Kedoya Utara Nafas",        "url": "https://aqicn.org/station/@521380/"},
]

PARAM_COLS = ["AQI", "PM1", "PM2.5", "PM10", "O3", "NO2", "SO2", "CO", "CO2",
              "R.H.", "Temp", "Wind", "Press", "TVOC"]

CSV_PATH = "aqicn_stations.csv"

CSV_COLS = (
    ["scraped_at", "station_url", "station_name"] +
    [col for p in PARAM_COLS for col in (p, f"{p}_time")]
)

LABEL_MAP = {
    "pm2.5": "PM2.5", "pm25": "PM2.5",
    "pm10": "PM10",
    "pm1": "PM1",
    "o3": "O3", "ozone": "O3",
    "no2": "NO2",
    "so2": "SO2",
    "co": "CO",
    "co2": "CO2",
    "r.h.": "R.H.", "rh": "R.H.", "humidity": "R.H.", "relative humidity": "R.H.",
    "temp": "Temp", "temp.": "Temp", "temperature": "Temp",
    "wind": "Wind", "wind speed (m/s)": "Wind",
    "press": "Press", "pressure": "Press",
    "tvoc": "TVOC",
    "aqi": "AQI",
}


def normalize_label(raw):
    return LABEL_MAP.get(raw.strip().lower(), raw.strip())


def parse_tooltip(label, tooltip):
    if not tooltip:
        return "", ""
    parts = [p.strip() for p in tooltip.split(" | ")]
    start_idx = 0
    for i, part in enumerate(parts):
        if re.search(r':\s*[\d]', part):
            start_idx = i
            break
    clean = parts[start_idx:]
    if not clean:
        return "", ""
    val_match = re.search(r':\s*(.+)', clean[0])
    value = val_match.group(1).strip() if val_match else ""
    timestamp = ""
    for part in clean[1:]:
        if re.search(r'\d{4}', part):
            timestamp = part
            break
    return value, timestamp


def load_latest_rows():
    """
    Read CSV and return a dict:
        { station_url -> {"PM2.5_time": "...", "PM10_time": "...", ...} }
    Only keeps the LAST row per station (latest scraped).
    """
    if not os.path.isfile(CSV_PATH):
        return {}

    latest = {}
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            latest[row["station_url"]] = row   # overwrite → last row wins

    return latest


def is_duplicate(new_row, latest_rows):
    """
    Returns True if this station's data timestamps are ALL identical
    to the last saved row — meaning no new data came in.
    Only compares non-empty timestamps.
    """
    url = new_row["station_url"]
    if url not in latest_rows:
        return False   # never saved before → not a duplicate

    old = latest_rows[url]
    time_cols = [f"{p}_time" for p in PARAM_COLS]

    compared = 0
    for col in time_cols:
        new_t = new_row.get(col, "").strip()
        old_t = old.get(col, "").strip()
        if new_t and old_t:          # only compare when both are non-empty
            if new_t != old_t:
                return False         # at least one timestamp changed → new data
            compared += 1

    if compared == 0:
        return False                 # nothing to compare → save anyway

    return True                      # all timestamps identical → duplicate


async def scrape_station(page, url, name):
    print(f"\n{'='*60}")
    print(f"  Scraping: {name}")

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_selector("td.station-specie-graph", timeout=15000)
        await page.evaluate("window.scrollTo(0, 200)")
        await page.wait_for_timeout(3000)
    except Exception as e:
        print(f"  ERROR: {e}")
        return {}

    aqi_data = await page.evaluate("""
        () => {
            const aqiEl  = document.querySelector('.aqivalue')
                        || document.querySelector('#aqivalue')
                        || document.querySelector('td.aqi');
            const timeEl = document.querySelector('.updated')
                        || document.querySelector('#updated')
                        || document.querySelector('span.updated');
            return {
                aqi:  aqiEl  ? aqiEl.innerText.trim() : '',
                time: timeEl ? timeEl.innerText.trim() : ''
            };
        }
    """)

    data = {}
    if aqi_data.get("aqi"):
        data["AQI"] = (aqi_data["aqi"], aqi_data.get("time", ""))

    graph_cells = await page.query_selector_all("td.station-specie-graph")
    print(f"  Found {len(graph_cells)} graph cells")

    for graph_td in graph_cells:
        raw_label = await page.evaluate("""
            (td) => {
                const row = td.closest('tr');
                const tds = Array.from(row.querySelectorAll('td'));
                return tds[0] ? tds[0].innerText.trim() : '';
            }
        """, graph_td)

        label = normalize_label(raw_label)
        svg_el = await graph_td.query_selector("svg")
        if not svg_el:
            continue

        # Primary: dispatch events directly on DOM element (bypasses clipping)
        tooltip_text = await page.evaluate("""
            async (svg) => {
                const svgH = parseFloat(svg.getAttribute('height') || 0);
                const bars = Array.from(svg.querySelectorAll('rect')).filter(r => {
                    const w    = parseFloat(r.getAttribute('width') || 0);
                    const fill = (r.getAttribute('fill') || '').toLowerCase().trim();
                    return w > 1 && fill !== 'none' && fill !== '';
                });
                if (!bars.length) return '';

                let best = null, bestRight = -Infinity;
                for (const r of bars) {
                    const x = parseFloat(r.getAttribute('x') || 0);
                    const w = parseFloat(r.getAttribute('width') || 0);
                    if (x + w > bestRight) { bestRight = x + w; best = { x, w, el: r }; }
                }
                if (!best) return '';

                best.el.dispatchEvent(new MouseEvent('mouseover', {bubbles: true}));
                best.el.dispatchEvent(new MouseEvent('mousemove', {bubbles: true}));

                const ctm    = svg.getScreenCTM();
                const pt     = svg.createSVGPoint();
                pt.x = best.x + best.w / 2;
                pt.y = svgH / 2;
                const screen = pt.matrixTransform(ctm);

                svg.dispatchEvent(new MouseEvent('mousemove', {
                    bubbles: true,
                    clientX: screen.x,
                    clientY: screen.y
                }));

                await new Promise(r => setTimeout(r, 200));

                const tt = document.getElementById('graph-tooltip');
                if (!tt) return '';
                const style = tt.getAttribute('style') || '';
                if (style.includes('display: none') || style.includes('visibility: hidden')) return '';
                return tt.innerText.trim();
            }
        """, svg_el)

        # Fallback: physical mouse hover
        if not tooltip_text:
            bar_coords = await page.evaluate("""
                (svg) => {
                    const svgH = parseFloat(svg.getAttribute('height') || 0);
                    const bars = Array.from(svg.querySelectorAll('rect')).filter(r => {
                        const w    = parseFloat(r.getAttribute('width') || 0);
                        const fill = (r.getAttribute('fill') || '').toLowerCase().trim();
                        return w > 1 && fill !== 'none' && fill !== '';
                    });
                    if (!bars.length) return null;

                    let best = null, bestRight = -Infinity;
                    for (const r of bars) {
                        const x = parseFloat(r.getAttribute('x') || 0);
                        const w = parseFloat(r.getAttribute('width') || 0);
                        if (x + w > bestRight) { bestRight = x + w; best = {x, w}; }
                    }
                    if (!best) return null;

                    const ctm = svg.getScreenCTM();
                    const pt  = svg.createSVGPoint();
                    pt.x = best.x + best.w / 2;
                    pt.y = svgH / 2;
                    const sc  = pt.matrixTransform(ctm);
                    return { x: sc.x, y: sc.y };
                }
            """, svg_el)

            if bar_coords:
                abs_x = max(1, min(bar_coords["x"], 1919))
                abs_y = bar_coords["y"]
                await page.mouse.move(1, abs_y)
                await page.wait_for_timeout(50)
                await page.mouse.move(abs_x, abs_y)
                for _ in range(10):
                    await page.wait_for_timeout(100)
                    tooltip = await page.query_selector("#graph-tooltip")
                    if tooltip:
                        style = await tooltip.get_attribute("style") or ""
                        if "display: none" not in style and "visibility: hidden" not in style:
                            text = (await tooltip.inner_text()).strip()
                            if text:
                                tooltip_text = text.replace("\n", " | ")
                                break

        if tooltip_text:
            tooltip_text = tooltip_text.replace("\n", " | ")
            value, timestamp = parse_tooltip(label, tooltip_text)
            data[label] = (value, timestamp)
            print(f"  [{label:12s}] {value:10s}  {timestamp}")
        else:
            data[label] = ("", "")
            print(f"  [{label:12s}] (no data)")

    return data


async def scrape_all():
    scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    latest_rows = load_latest_rows()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        rows_to_save = []
        skipped = []

        for station in STATIONS:
            data = await scrape_station(page, station["url"], station["name"])

            row = {
                "scraped_at":   scraped_at,
                "station_url":  station["url"],
                "station_name": station["name"],
            }
            for param in PARAM_COLS:
                val, ts = data.get(param, ("", ""))
                row[param]           = val
                row[f"{param}_time"] = ts

            if is_duplicate(row, latest_rows):
                print(f"  ⏭  SKIPPED (no new data since last scrape)")
                skipped.append(station["name"])
            else:
                rows_to_save.append(row)

        await browser.close()

    # Write only new rows
    file_exists = os.path.isfile(CSV_PATH)
    if rows_to_save:
        with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLS)
            if not file_exists:
                writer.writeheader()
            writer.writerows(rows_to_save)

    # Summary
    print(f"\n{'='*60}")
    print(f"  Saved : {len(rows_to_save)} rows → {CSV_PATH}")
    print(f"  Skipped (duplicate): {len(skipped)} — {', '.join(skipped) if skipped else 'none'}")
    print(f"{'='*60}")

    all_rows = rows_to_save + [
        {**{"station_name": n, "PM2.5": "", "PM10": "", "O3": "", "NO2": "",
            "Temp": "", "AQI": "", "PM2.5_time": "(skipped — duplicate)"}}
        for n in skipped
    ]

    print(f"\n{'STATION':<35} {'AQI':>5}  {'PM2.5':>6}  {'PM10':>5}  {'O3':>5}  {'NO2':>5}  {'Temp':>6}  TIME")
    print("-" * 105)
    for row in rows_to_save:
        print(f"  {row['station_name']:<33} "
              f"{row['AQI']:>5}  "
              f"{row['PM2.5']:>6}  "
              f"{row['PM10']:>5}  "
              f"{row['O3']:>5}  "
              f"{row['NO2']:>5}  "
              f"{row['Temp']:>6}  "
              f"{row['PM2.5_time']}")
    for name in skipped:
        print(f"  {name:<33}  (skipped — no new data)")

    return rows_to_save


def main():
    if sys.platform == "win32":
        loop = asyncio.ProactorEventLoop()
        asyncio.set_event_loop(loop)
    else:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(scrape_all())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
