import os, time, requests
from datetime import datetime

# ─── Configuration (from env) ──────────────────────────────────────────────────
STRAVA_CLIENT_ID       = os.getenv("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET   = os.getenv("STRAVA_CLIENT_SECRET")
STRAVA_REFRESH_TOKEN   = os.getenv("STRAVA_REFRESH_TOKEN")
STRAVA_CLUB_ID         = os.getenv("STRAVA_CLUB_ID")

NOTION_TOKEN           = os.getenv("NOTION_TOKEN")
NOTION_PARENT_PAGE_ID  = os.getenv("NOTION_PARENT_PAGE_ID")
ACTIVITY_NAME          = os.getenv("ACTIVITY_NAME")

NOTION_VERSION         = "2022-06-28"
NOTION_HEADERS         = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json"
}

# ─── Helpers ──────────────────────────────────────────────────────────────────
def refresh_strava_token():
    resp = requests.post("https://www.strava.com/oauth/token", data={
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": STRAVA_REFRESH_TOKEN
    })
    resp.raise_for_status()
    return resp.json()["access_token"]

def fetch_all_runs(token):
    hdr, runs, page = {"Authorization": f"Bearer {token}"}, [], 1
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

def format_hhmmss(sec:int):
    h, rem = divmod(sec, 3600)
    m, s   = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def meters_to_miles(m:float)->float:
    return m * 0.000621371

# ─── Core Logic ────────────────────────────────────────────────────────────────
def create_notion_database(title, schema_props):
    """
    Creates a new Notion database under NOTION_PARENT_PAGE_ID,
    with given title and properties schema.
    Returns the new database_id.
    """
    payload = {
        "parent": {"type":"page_id", "page_id": NOTION_PARENT_PAGE_ID},
        "title": [{"type":"text","text":{"content": title}}],
        "properties": schema_props
    }
    r = requests.post("https://api.notion.com/v1/databases",
                      headers=NOTION_HEADERS, json=payload)
    r.raise_for_status()
    db = r.json()
    return db["id"]

def push_rows_to_database(db_id, rows):
    for row in rows:
        props = {
            "Athlete": {
                "title": [{"text":{"content": row["name"]}}]
            },
            "Distance (mi)": {
                "number": round(row["miles"], 2)
            },
            "Moving Time": {
                "rich_text":[{"text":{"content": format_hhmmss(row["moving"])}}]
            },
            "Elapsed Time": {
                "rich_text":[{"text":{"content": format_hhmmss(row["elapsed"])}}]
            }
        }
        r = requests.post("https://api.notion.com/v1/pages",
                          headers=NOTION_HEADERS,
                          json={"parent":{"database_id":db_id},"properties":props})
        r.raise_for_status()

def main():
    # 1) Fetch & filter runs
    token = refresh_strava_token()
    all_runs = fetch_all_runs(token)
    event_runs = [r for r in all_runs if r.get("name")==ACTIVITY_NAME]
    if not event_runs:
        print(f"No runs found for activity “{ACTIVITY_NAME}”")
        return

    # 2) Aggregate per athlete
    totals = {}
    for a in event_runs:
        ath = a.get("athlete",{})
        aid = ath.get("id")
        if not aid:
            continue
        name = f"{ath.get('firstname','')} {ath.get('lastname','')}".strip()
        totals.setdefault(aid, {"name":name,"moving":0,"elapsed":0,"meters":0})
        totals[aid]["moving"]  += a.get("moving_time",0)
        totals[aid]["elapsed"] += a.get("elapsed_time",0)
        totals[aid]["meters"]  += a.get("distance",0.0)

    rows = []
    for v in totals.values():
        rows.append({
            "name":    v["name"],
            "miles":   meters_to_miles(v["meters"]),
            "moving":  v["moving"],
            "elapsed": v["elapsed"]
        })

    # 3) Create new database for this event
    now = datetime.now()
    db_title = f"{ACTIVITY_NAME} Results – {now.strftime('%-m/%-d/%Y')}"
    schema = {
        "Athlete":        {"title": {}},
        "Distance (mi)":  {"number": {"format": "number"}},
        "Moving Time":    {"rich_text": {}},
        "Elapsed Time":   {"rich_text": {}},
    }
    new_db_id = create_notion_database(db_title, schema)
    print("Created Notion DB:", new_db_id)

    # 4) Push one page per athlete
    push_rows_to_database(new_db_id, rows)
    print(f"Pushed {len(rows)} rows into “{db_title}”")

if __name__=="__main__":
    main()
