import os
import time
import requests
from datetime import datetime

# ─── Configuration ─────────────────────────────────────────────────────────────
STRAVA_CLIENT_ID     = os.getenv("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
STRAVA_REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")
STRAVA_CLUB_ID       = os.getenv("STRAVA_CLUB_ID")

NOTION_TOKEN         = os.getenv("NOTION_TOKEN")
NOTION_DB_ID         = os.getenv("NOTION_DB_ID")       # Leaderboard DB
ACTIVITIES_DB_ID     = os.getenv("ACTIVITIES_DB_ID")   # Activities DB

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

# ─── Helpers ──────────────────────────────────────────────────────────────────
def meters_to_miles(m: float) -> float:
    return m * 0.000621371

def format_miles(miles: float) -> str:
    return f"{miles:.2f}"

def refresh_strava_token() -> str:
    r = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id":     STRAVA_CLIENT_ID,
            "client_secret": STRAVA_CLIENT_SECRET,
            "grant_type":    "refresh_token",
            "refresh_token": STRAVA_REFRESH_TOKEN
        }
    )
    r.raise_for_status()
    return r.json()["access_token"]

def fetch_all_runs(token: str) -> list[dict]:
    runs = []
    page = 1
    headers = {"Authorization": f"Bearer {token}"}
    while True:
        r = requests.get(
            f"https://www.strava.com/api/v3/clubs/{STRAVA_CLUB_ID}/activities",
            headers=headers,
            params={"page": page, "per_page": 200}
        )
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        runs.extend(a for a in batch if a.get("type") == "Run")
        page += 1
        time.sleep(0.5)
    return runs

def aggregate_distances(runs: list[dict]) -> list[dict]:
    totals = {}
    for a in runs:
        ath = a["athlete"]
        aid = ath["id"]
        name = f"{ath.get('firstname','')} {ath.get('lastname','')}".strip()
        dist_m = a.get("distance", 0.0)
        totals.setdefault(aid, {"name": name, "meters": 0.0})
        totals[aid]["meters"] += dist_m

    athletes = []
    for v in totals.values():
        miles = meters_to_miles(v["meters"])
        athletes.append({"name": v["name"], "miles": miles})
    return sorted(athletes, key=lambda x: x["miles"], reverse=True)

# ─── Notion: Activities Log ────────────────────────────────────────────────────
def create_activity_page():
    now = datetime.now()
    title = f"Activity (Called {now.strftime('%H:%M:%S')} – {now.strftime('%-m/%-d/%Y')})"
    payload = {
        "parent": {"database_id": ACTIVITIES_DB_ID},
        "properties": {
            "Title": {                # <— use YOUR actual prop name here  
                "title": [{"text": {"content": title}}]
            }
        }
    }
    r = requests.post("https://api.notion.com/v1/pages", headers=HEADERS, json=payload)
    r.raise_for_status()

# ─── Notion: Leaderboard Sync ──────────────────────────────────────────────────
def archive_old_pages():
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    cursor = None
    pages = []
    while True:
        payload = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor
        r = requests.post(url, headers=HEADERS, json=payload)
        r.raise_for_status()
        d = r.json()
        pages.extend(d.get("results", []))
        if not d.get("has_more"):
            break
        cursor = d.get("next_cursor")
    for p in pages:
        requests.patch(
            f"https://api.notion.com/v1/pages/{p['id']}",
            headers=HEADERS,
            json={"archived": True}
        ).raise_for_status()

def push_athlete_rows(athletes: list[dict]):
    for a in athletes:
        payload = {
            "parent": {"database_id": NOTION_DB_ID},
            "properties": {
                "Athlete": {
                    "title": [{"text": {"content": a["name"]}}]
                },
                "Miles Ran": {
                    "rich_text": [{"text": {"content": format_miles(a["miles"])}}]
                }
            }
        }
        r = requests.post("https://api.notion.com/v1/pages", headers=HEADERS, json=payload)
        r.raise_for_status()

def push_totals_row(athletes: list[dict]):
    total_miles = sum(a["miles"] for a in athletes)
    payload = {
        "parent": {"database_id": NOTION_DB_ID},
        "properties": {
            "Athlete": {
                "title": [{"text": {"content": "Totals"}}]
            },
            "Miles Ran (Total)": {
                "rich_text": [{"text": {"content": format_miles(total_miles)}}]
            }
        }
    }
    r = requests.post("https://api.notion.com/v1/pages", headers=HEADERS, json=payload)
    r.raise_for_status()

# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    # 1) Log this run
    create_activity_page()

    # 2) Fetch & aggregate
    token = refresh_strava_token()
    runs  = fetch_all_runs(token)
    athletes = aggregate_distances(runs)

    # 3) Rebuild leaderboard
    archive_old_pages()
    push_athlete_rows(athletes)
    push_totals_row(athletes)

    print(f"✅ Leaderboard updated: {len(athletes)} athletes + Totals")

if __name__ == "__main__":
    main()
