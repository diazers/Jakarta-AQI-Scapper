"""
github_push.py
==============
Appends scraped rows to iqair_data.csv in the GitHub repo.
Reads credentials from environment variables set by GitHub Actions secrets.

For local testing, set these before running:
  export SCRAPER_GITHUB_TOKEN="ghp_xxxxxxxxxxxx"
  export GITHUB_REPO="yourusername/iqair-data"
"""

import base64, csv, io, os
from github import Github

GITHUB_TOKEN = os.environ.get("SCRAPER_GITHUB_TOKEN")
REPO_NAME    = os.environ.get("GITHUB_REPO", "your_username/iqair-data")
FILE_PATH    = "iqair_data.csv"
BRANCH       = "main"

FIELDNAMES = [
    "station_name", "location", "datetime_local", "scraped_at",
    "aqi", "aqi_category",
    "main_pollutant", "pollutant_density",
    "temperature", "humidity", "wind_speed", "pressure",
]


def push_to_github(new_rows: list, commit_message: str = "Add AQI data"):
    if not new_rows:
        print("  [github] No rows to push.")
        return

    if not GITHUB_TOKEN:
        print("  [github] ERROR: SCRAPER_GITHUB_TOKEN env var not set.")
        return

    g    = Github(GITHUB_TOKEN)
    repo = g.get_repo(REPO_NAME)

    # Read existing file
    existing_content = ""
    sha = None
    try:
        file_obj = repo.get_contents(FILE_PATH, ref=BRANCH)
        existing_content = base64.b64decode(file_obj.content).decode("utf-8")
        sha = file_obj.sha
        print(f"  [github] Read existing file (sha={sha[:7]})")
    except Exception:
        print("  [github] File not found — will create it.")

    # Build updated CSV
    buf = io.StringIO()
    if existing_content.strip():
        buf.write(existing_content)
        if not existing_content.endswith("\n"):
            buf.write("\n")
        writer = csv.DictWriter(buf, fieldnames=FIELDNAMES,
                                extrasaction="ignore", lineterminator="\n")
        writer.writerows(new_rows)
    else:
        writer = csv.DictWriter(buf, fieldnames=FIELDNAMES,
                                extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(new_rows)

    new_content = buf.getvalue()

    # Push
    try:
        if sha:
            repo.update_file(FILE_PATH, commit_message, new_content, sha, branch=BRANCH)
            print(f"  [github] ✅ Updated {FILE_PATH} (+{len(new_rows)} rows)")
        else:
            repo.create_file(FILE_PATH, commit_message, new_content, branch=BRANCH)
            print(f"  [github] ✅ Created {FILE_PATH} (+{len(new_rows)} rows)")
    except Exception as e:
        print(f"  [github] ❌ Push failed: {e}")
        raise
