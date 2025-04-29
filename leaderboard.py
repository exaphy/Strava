import os, time, requests
from datetime import datetime, timedelta
from dateutil.parser import isoparse
from zoneinfo import ZoneInfo

# ─── Config ────────────────────────────────────────────────────────────────────
STRAVA_CLIENT_ID       = os.getenv("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET   = os.getenv("STRAVA_CLIENT_SECRET")
STRAVA_REFRESH_TOKEN   = os.getenv("STRAVA_REFRESH_TOKEN")
STRAVA_CLUB_ID         = os.getenv("STRAVA_CLUB_ID")

NOTION_TOKEN           = os.getenv("NOTION_TOKEN")
NOTION_PARENT_PAGE_ID  = os.getenv("NOTION_PARENT_PAGE_ID")

ACTIVITY_NAME          = os.getenv("ACTIVITY_NAME")      # from workflow_dispatch
START_DATE_STR         = os.getenv("START_DATE_STR")     # YYYY-MM-DD, Pacific

PACIFIC                = ZoneInfo("America/Los_Angeles")
HEADERS = {
    "Authorization":   f"Bearer {NOTION_TOKEN}",
    "Notion-Version":  "2022-06-28",
    "Content-Type":    "application/json"
}

# ─── Helpers ──────────────────────────────────────────────────────────────────
def refresh_strava_token() -> str:
    r = requests.post("https://www.strava.com/oauth/token", data={
        "client_id":     STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "grant_type":    "refresh_token",
        "refresh_token": STRAVA_REFRESH_TOKEN
    })
    r.raise_for_status()
    return r.json()["access_token"]

def fetch_all_runs(token: str) -> list[dict]:
    runs, page = [], 1
    hdr = {"Authorization": f"Bearer {token}"}
    while True:
        r = requests.get(
            f"https://www.strava.com/api/v3/clubs/{STRAVA_CLUB_ID}/activities",
            headers=hdr, params={"page": page, "per_page": 200}
        )
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        runs.extend(a for a in batch if a.get("type") == "Run")
        page += 1
        time.sleep(0.5)
    return runs

def format_hhmmss(sec: int) -> str:
    h, rem = divmod(sec, 3600)
    m, s   = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def meters_to_miles(m: float) -> float:
    return m * 0.000621371

# ─── Notion DB creation + row push ────────────────────────────────────────────
def create_notion_database(title: str, schema: dict) -> str:
    payload = {
        "parent":     {"type": "page_id", "page_id": NOTION_PARENT_PAGE_ID},
        "title":      [{"type": "text", "text": {"content": title}}],
        "properties": schema
    }
    r = requests.post("https://api.notion.com/v1/databases",
                      headers=HEADERS, json=payload)
    r.raise_for_status()
    return r.json()["id"]

def push_rows(db_id: str, rows: list[dict]) -> None:
    for row in rows:
        props = {
            "Athlete":       {"title":     [{"text": {"content": row["name"]}}]},
            "Distance (mi)": {"number":    round(row["miles"], 2)},
            "Moving Time":   {"rich_text":[{"text": {"content": row["moving"]}}]},
            "Elapsed Time":  {"rich_text":[{"text": {"content": row["elapsed"]}}]}
        }
        r = requests.post("https://api.notion.com/v1/pages",
                          headers=HEADERS,
                          json={"parent": {"database_id": db_id},
                                "properties": props})
        r.raise_for_status()

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    # Parse date in Pacific
    start_dt = datetime.strptime(START_DATE_STR, "%Y-%m-%d").replace(tzinfo=PACIFIC)
    end_dt   = start_dt + timedelta(days=1)

    print(f"Syncing '{ACTIVITY_NAME}' on {start_dt.date()} (PT)")

    token = refresh_strava_token()
    runs  = fetch_all_runs(token)

    # Filter by name + date
    filtered = [
        a for a in runs
        if a.get("name") == ACTIVITY_NAME
        and start_dt <= isoparse(a["start_date_local"]).astimezone(PACIFIC) < end_dt
    ]
    if not filtered:
        print("No matching runs found. Exiting.")
        return

    # Aggregate per athlete
    totals = {}
    for a in filtered:
        ath = a.get("athlete", {})
        aid = ath.get("id")
        if not aid:
            continue
        name = f"{ath.get('firstname','')} {ath.get('lastname','')}".strip()
        rec = totals.setdefault(aid, {"name": name, "meters": 0, "moving": 0, "elapsed": 0})
        rec["meters"]  += a.get("distance", 0.0)
        rec["moving"]  += a.get("moving_time", 0)
        rec["elapsed"] += a.get("elapsed_time", 0)

    rows = [{
        "name":    v["name"],
        "miles":   meters_to_miles(v["meters"]),
        "moving":  format_hhmmss(v["moving"]),
        "elapsed": format_hhmmss(v["elapsed"])
    } for v in totals.values()]

    # Create new Notion DB
    title  = f"{ACTIVITY_NAME} Results – {start_dt.strftime('%-m/%-d/%Y')}"
    schema = {
        "Athlete":       {"title": {}},
        "Distance (mi)": {"number": {"format": "number"}},
        "Moving Time":   {"rich_text": {}},
        "Elapsed Time":  {"rich_text": {}},
    }
    new_db_id = create_notion_database(title, schema)
    print("Created DB:", new_db_id)

    # Push results
    push_rows(new_db_id, rows)
    print(f"Pushed {len(rows)} rows into '{title}'")

if __name__=="__main__":
    main()
