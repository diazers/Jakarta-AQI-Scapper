#!/usr/bin/env python3
"""
iqair_region1.py  —  Region 1 — Jakarta Selatan/Pusat
Scrapes one map region and pushes results to GitHub CSV.
Run via cron independently of other region scripts.
"""

"""
IQAir Map Scraper — v8
=======================
Fixes:
  1. Click: use Selenium's native element.click() on the actual DOM element
     (React synthetic events only fire from real Selenium clicks, not JS dispatchEvent)
  2. Panel detection: wait for the panel to appear by checking if a new
     element with "Stasiun" text AND an AQI number becomes visible,
     distinguishing it from the always-present ranking sidebar
  3. Panel text: read only the STATION DETAIL panel, not the ranking sidebar

Requirements:
    pip install selenium webdriver-manager

Usage:
    python iqair_scraper.py
"""

import time, csv, re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import (
    TimeoutException, StaleElementReferenceException, NoSuchElementException
)
from webdriver_manager.chrome import ChromeDriverManager
from github_push import push_to_github
from datetime import datetime

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════════════
URL        = "https://www.iqair.com/id/air-quality-map?zoomLevel=13&lat=-6.2500&lng=106.8400"
REGION     = "Region 1 — Jakarta Selatan/Pusat"

def setup_driver():
    opt = Options()
    opt.add_argument("--window-size=1920,1080")
    opt.add_argument("--disable-notifications")
    opt.add_argument("--lang=id")
    # Required for GitHub Actions (no display available)
    opt.add_argument("--headless=new")
    opt.add_argument("--no-sandbox")
    opt.add_argument("--disable-dev-shm-usage")
    opt.add_argument("--disable-gpu")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opt)


def load_map(driver, url):
    print("Loading map...")
    driver.get(url)
    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "canvas, .mapboxgl-canvas, .leaflet-container")
            )
        )
    except TimeoutException:
        pass
    time.sleep(6)
    print("Map loaded.\n")


# ══════════════════════════════════════════════════════════════════════════════
#  FIND AQI DOTS — skip clusters
# ══════════════════════════════════════════════════════════════════════════════

def find_aqi_dots(driver):
    """
    Returns list of Selenium WebElements — one per individual station dot.
    Clusters (aria-label contains 'Cluster') are excluded.
    """
    # IQAir markers are leaflet-marker-icon divs containing a number
    # Get all candidate elements via JS first to get their positions,
    # then retrieve the actual Selenium elements by index so we can .click() them.
    dot_info = driver.execute_script("""
        var results = [];
        var all = document.querySelectorAll('div, span, button');
        for (var i = 0; i < all.length; i++) {
            var el = all[i];

            // Skip clusters by walking up 5 parents
            var isCluster = false;
            var check = el;
            for (var k = 0; k < 5; k++) {
                if (!check) break;
                var label = (check.getAttribute('aria-label') || '').toLowerCase();
                var cls   = (check.className || '').toLowerCase();
                if (label.indexOf('cluster') > -1 || cls.indexOf('cluster') > -1) {
                    isCluster = true; break;
                }
                check = check.parentElement;
            }
            if (isCluster) continue;

            var txt = (el.innerText || el.textContent || '').trim();
            if (!/^\d{1,3}$/.test(txt)) continue;
            var num = parseInt(txt);
            if (num < 1 || num > 500) continue;

            var r = el.getBoundingClientRect();
            if (r.width < 10 || r.width > 80)   continue;
            if (r.height < 10 || r.height > 80)  continue;
            // Must be roughly square (map dots are round ~28x28, sidebar items are wide rectangles)
            var ratio = r.width / r.height;
            if (ratio < 0.6 || ratio > 1.8) continue;
            if (r.left < 120)   continue;
            if (r.top  < 60)    continue;
            if (r.right  > 1920) continue;
            if (r.bottom > 1080) continue;

            var style = window.getComputedStyle(el);
            if (style.display === 'none' || style.visibility === 'hidden') continue;
            if (parseFloat(style.opacity) < 0.1) continue;

            // Store the element reference index so Python can retrieve it
            el._scraper_idx = results.length;
            results.push({
                x:        Math.round(r.left + r.width / 2),
                y:        Math.round(r.top  + r.height / 2),
                w:        Math.round(r.width),
                h:        Math.round(r.height),
                aqi_text: txt,
                el:       el        // pass back the element
            });
        }
        // Deduplicate within 15px
        var deduped = [];
        results.forEach(function(a) {
            var dup = deduped.some(function(b) {
                return Math.abs(a.x - b.x) < 15 && Math.abs(a.y - b.y) < 15;
            });
            if (!dup) deduped.push(a);
        });
        return deduped;
    """)

    # Return only position data — NO stored element references.
    # Elements are fetched fresh at click time to avoid stale references.
    return [{"x": d["x"], "y": d["y"], "w": d["w"], "h": d["h"],
             "aqi_text": d["aqi_text"]} for d in dot_info]


# ══════════════════════════════════════════════════════════════════════════════
#  CLICK — use real Selenium click on the element (triggers React events)
#  Move mouse to element first using move_to_element to avoid drift,
#  then click. Reset to body after to avoid any state carryover.
# ══════════════════════════════════════════════════════════════════════════════

def click_dot(driver, dot):
    x, y = dot["x"], dot["y"]

    # Fetch element fresh from DOM right now — never use a stored reference.
    # This prevents ALL stale element errors.
    el = driver.execute_script("""
        var x = arguments[0], y = arguments[1];
        var el = document.elementFromPoint(x, y);
        if (!el) return null;
        // Walk up to find the marker div with just the AQI number
        for (var i = 0; i < 6; i++) {
            if (!el) break;
            var txt = (el.innerText || '').trim();
            if (/^\d{1,3}$/.test(txt) && parseInt(txt) >= 1) return el;
            el = el.parentElement;
        }
        return document.elementFromPoint(x, y);
    """, x, y)

    if el is not None:
        try:
            ActionChains(driver).move_to_element(el).click().perform()
            return
        except Exception:
            pass
    # Final fallback: JS click at coordinates
    driver.execute_script("""
        var el = document.elementFromPoint(arguments[0], arguments[1]);
        if (el) el.click();
    """, x, y)


# ══════════════════════════════════════════════════════════════════════════════
#  PANEL DETECTION
#  The ranking sidebar is ALWAYS present on the left.
#  The station DETAIL panel appears OVER it when a dot is clicked.
#  Key difference: the detail panel contains "Stasiun" badge + weather data
#  (Kelembapan, Angin, Tekanan) which the ranking sidebar does NOT have.
# ══════════════════════════════════════════════════════════════════════════════

PANEL_KEYWORDS = ["Kelembapan", "Tekanan", "Angin", "mbar", "km/h",
                  "Scattered", "Sunny", "Cloudy", "Rain", "PM2.5", "PM10"]

def panel_is_open(driver):
    """Return True if the station detail panel (with weather) is visible."""
    text = get_panel_text(driver)
    return bool(text)


def get_panel_text(driver):
    """
    Read the station detail panel text.
    The detail panel is distinguished from the ranking sidebar by containing
    weather fields (Kelembapan, Tekanan, Angin, mbar, km/h).
    We search ALL left-side containers and pick the one with weather keywords.
    """
    return driver.execute_script("""
        var keywords = ['Kelembapan','Tekanan','Angin','mbar','km/h',
                        'Scattered','Sunny','Cloudy','Rain','PM2.5','PM10',
                        'Tidak sehat','Berbahaya','Sedang'];
        var best = {score: 0, text: ''};
        var all = document.querySelectorAll('div, aside, section, nav');
        for (var i = 0; i < all.length; i++) {
            var el  = all[i];
            var r   = el.getBoundingClientRect();
            // Left panel area: starts near x=0, width 280-520px
            if (r.left > 40 || r.width < 260 || r.width > 540) continue;
            if (r.height < 300) continue;
            var txt = (el.innerText || '').trim();
            if (txt.length < 30) continue;
            // Count how many weather keywords are present
            var score = 0;
            keywords.forEach(function(kw) {
                if (txt.indexOf(kw) > -1) score += 100;
            });
            if (score === 0) continue;   // must have at least one weather keyword
            score += txt.length * 0.01;  // slight preference for longer text
            if (score > best.score) { best.score = score; best.text = txt; }
        }
        return best.text;
    """) or ""


def wait_for_panel(driver, prev_station="", timeout=10):
    """
    Poll until a station detail panel appears with different content than before.
    Returns panel text string, or "" on timeout.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        text = get_panel_text(driver)
        if text:
            # Extract station name from panel to check it changed
            lines = [l.strip() for l in text.splitlines() if l.strip()]
            station = next(
                (l for l in lines
                 if l.lower() not in {"stasiun","aqi","riwayat","lihat detail"}
                 and not re.match(r'^\d', l) and len(l) > 2),
                ""
            )
            if station and station != prev_station:
                return text
        time.sleep(0.35)
    return ""


# ══════════════════════════════════════════════════════════════════════════════
#  CLOSE PANEL
# ══════════════════════════════════════════════════════════════════════════════

def close_panel(driver):
    """Close the station detail panel."""
    # Find the X button: small button in top-right area of panel (x: 280–460, y: 60–200)
    closed = driver.execute_script("""
        for (var b of document.querySelectorAll('button, a[role=button]')) {
            var r = b.getBoundingClientRect();
            if (r.width > 0 && r.width < 60 && r.height < 60
                    && r.left > 270 && r.left < 470
                    && r.top  >  55 && r.top  < 210) {
                b.click();
                return 'btn@' + Math.round(r.left) + ',' + Math.round(r.top);
            }
        }
        var c = document.querySelector(
            "[aria-label='close'],[aria-label='Close'],[aria-label='Tutup']");
        if (c) { c.click(); return 'aria'; }
        return '';
    """)
    if not closed:
        try:
            driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
        except Exception:
            pass
    time.sleep(0.7)
    # Confirm closed — if still open, click empty map area via JS
    if get_panel_text(driver):
        driver.execute_script(
            "var el = document.elementFromPoint(1100, 500); if(el) el.click();"
        )
        time.sleep(0.5)


# ══════════════════════════════════════════════════════════════════════════════
#  PARSE
# ══════════════════════════════════════════════════════════════════════════════

def parse(text):
    d = {k: "" for k in [
        "station_name","location","datetime_local","scraped_at",
        "aqi","aqi_category",
        "main_pollutant","pollutant_density",
        "temperature","humidity","wind_speed","pressure",
    ]}
    d["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not text:
        return d

    lines = [l.strip() for l in text.splitlines() if l.strip()]
    skip  = {"stasiun","prakiraan per jam","aqi* us","aqi us","aqi",
             "riwayat","lihat detail","outdoor","indoor","semua"}

    # Station name: first non-numeric, non-keyword line
    for line in lines:
        if (line.lower() not in skip and len(line) > 2
                and not re.match(r'^\d', line)):
            d["station_name"] = line
            break

    # Location: line with commas, letters, no large numbers
    for line in lines:
        if (re.search(r'[A-Za-z].+,.+[A-Za-z]', line)
                and not re.search(r'\d{4,}', line) and len(line) < 100):
            d["location"] = line
            break

    # Datetime local
    m = re.search(r'(\d{1,2}[.:]\d{2}.*?(?:waktu setempat|local time))', text, re.IGNORECASE)
    if m:
        d["datetime_local"] = m.group(1).strip()

    # AQI: standalone number on its own line
    for line in lines:
        if re.match(r'^\d{1,3}$', line) and 1 <= int(line) <= 500:
            d["aqi"] = line; break
    if not d["aqi"]:
        m = re.search(r'\b([1-9]\d{0,2})\b', text)
        if m: d["aqi"] = m.group(1)

    # AQI category
    for cat in ["Tidak sehat bagi kelompok sensitif","Sangat tidak sehat",
                "Tidak sehat","Berbahaya","Sedang","Baik",
                "Unhealthy for Sensitive Groups","Very Unhealthy",
                "Unhealthy","Hazardous","Moderate","Good"]:
        if cat.lower() in text.lower():
            d["aqi_category"] = cat; break

    # Pollutant
    m = re.search(
        r'(?:Polutan utama[:\s]*)?(PM[\d.]+|O3|NO2|SO2|CO)'
        r'[\s:]*([\d.]+)\s*(µg/m³|μg/m³|ug/m3|ppb|ppm|mg/m³)?',
        text, re.IGNORECASE)
    if m:
        d["main_pollutant"]    = re.sub(r'\s+','',m.group(1)).upper()
        d["pollutant_density"] = m.group(2) + (" "+m.group(3) if m.group(3) else "")

    # Temperature
    m = re.search(r'(\d+)\s*°', text)
    if m: d["temperature"] = m.group(1) + "°C"

    # Humidity
    m = re.search(r'[Kk]elembapan\D{0,30}?(\d+)\s*%', text)
    if not m: m = re.search(r'(\d+)\s*%', text)
    if m: d["humidity"] = m.group(1) + "%"

    # Wind
    m = re.search(r'(\d+(?:\.\d+)?)\s*km/h', text, re.IGNORECASE)
    if m: d["wind_speed"] = m.group(1) + " km/h"
    else:
        m = re.search(r'[Aa]ngin\D{0,30}?(\d+(?:\.\d+)?)\s*(km/h|m/s)', text)
        if m: d["wind_speed"] = f"{m.group(1)} {m.group(2)}"

    # Pressure
    m = re.search(r'(\d{3,4})\s*mbar', text, re.IGNORECASE)
    if not m: m = re.search(r'[Tt]ekanan\D{0,30}?(\d{3,4})', text)
    if m: d["pressure"] = m.group(1) + " mbar"

    return d


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    print(f"\n{"═"*60}")
    print(f"  {REGION}  —  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{"═"*60}\n")

    driver  = setup_driver()
    results = []
    seen    = set()

    try:
        load_map(driver, URL)

        dots = find_aqi_dots(driver)
        print(f"Found {len(dots)} dots (clusters excluded):\n")
        for d in dots:
            print(f"  AQI={d['aqi_text']:>3}  ({d['x']},{d['y']})  {d['w']}x{d['h']}")
        print()

        if not dots:
            print("⚠  No dots found.")
            return

        prev_station = ""

        for i, dot in enumerate(dots):
            label = f"[{i+1}/{len(dots)}]"
            print(f"{label} AQI={dot['aqi_text']:>3} ({dot['x']},{dot['y']}) ... ", end="", flush=True)

            try:
                click_dot(driver, dot)
                panel_text = wait_for_panel(driver, prev_station=prev_station, timeout=10)

                if not panel_text:
                    click_dot(driver, dot)
                    panel_text = wait_for_panel(driver, prev_station=prev_station, timeout=6)

                if not panel_text:
                    print("no panel, skipping")
                    close_panel(driver)
                    continue

                data = parse(panel_text)
                key  = f"{data['station_name']}|{data['location']}"

                if key.strip("|") in seen:
                    print(f"duplicate ({data['station_name']})")
                    close_panel(driver)
                    continue

                seen.add(key.strip("|"))
                results.append(data)
                prev_station = data["station_name"]

                print(
                    f"✓ {data['station_name'] or '?'} | "
                    f"AQI={data['aqi']} {data['aqi_category']} | "
                    f"{data['main_pollutant']} {data['pollutant_density']} | "
                    f"T={data['temperature']} H={data['humidity']} "
                    f"W={data['wind_speed']} P={data['pressure']}"
                )

                close_panel(driver)
                time.sleep(0.5)

            except StaleElementReferenceException:
                print("unexpected stale, skipping")
                try: close_panel(driver)
                except Exception: pass
            except Exception as e:
                print(f"error: {e}")
                try: close_panel(driver)
                except Exception: pass

    finally:
        driver.quit()

    if results:
        commit_msg = f"{REGION} — {datetime.now().strftime('%Y-%m-%d %H:%M')} ({len(results)} rows)"
        push_to_github(results, commit_message=commit_msg)
        print(f"\n✅ {len(results)} records pushed to GitHub")
    else:
        print("\n⚠  No data collected.")


if __name__ == "__main__":
    main()
