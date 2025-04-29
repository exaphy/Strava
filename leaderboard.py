import os, time, requests, json
from datetime import datetime, timedelta
from dateutil import parser
from zoneinfo import ZoneInfo

# ─── CONFIG ────────────────────────────────────────────────────────────────────
STRAVA_CLIENT_ID       = os.getenv("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET   = os.getenv("STRAVA_CLIENT_SECRET")
STRAVA_REFRESH_TOKEN   = os.getenv("STRAVA_REFRESH_TOKEN")
STRAVA_CLUB_ID         = os.getenv("STRAVA_CLUB_ID")

NOTION_TOKEN           = os.getenv("NOTION_TOKEN")
NOTION_PARENT_PAGE_ID  = os.getenv("NOTION_PARENT_PAGE_ID")

START_DATE_STR         = os.getenv("START_DATE_STR")  # e.g. "2025-04-27"
PACIFIC                = ZoneInfo("America/Los_Angeles")

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

# ─── STRAVA AUTH ──────────────────────────────────────────────────────────────
def refresh_strava_token():
    payload = {
        "client_id":     STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "grant_type":    "refresh_token",
        "refresh_token": STRAVA_REFRESH_TOKEN
    }
    print("▶️ Refresh payload:", payload)
    r = requests.post("https://www.strava.com/oauth/token", data=payload)
    print(f"◀️ Strava status: {r.status_code}, response: {r.text}")
    r.raise_for_status()
    return r.json()["access_token"]

# ─── FETCH & DETAIL-RICHEN RUNS ───────────────────────────────────────────────
def fetch_all_runs(token):
    # 1) fetch summaries
    hdr, runs, page = {"Authorization":f"Bearer {token}"}, [], 1
    while True:
        r = requests.get(
            f"https://www.strava.com/api/v3/clubs/{STRAVA_CLUB_ID}/activities",
            headers=hdr, params={"page":page,"per_page":200}
        )
        if r.status_code == 401:
            raise RuntimeError("Unauthorized: token invalid or club ID wrong")
        r.raise_for_status()
        batch = r.json()
        if not batch: break
        runs.extend(a for a in batch if a.get("type")=="Run")
        page += 1
        time.sleep(0.5)

    # 2) for any run missing start_date_local or athlete.id, fetch full details
    detailed = []
    for a in runs:
        need_fetch = ("start_date_local" not in a) or ("athlete" not in a) or ("id" not in a["athlete"])
        if need_fetch:
            act_id = a.get("id")
            print(f"🐛 fetching full details for activity id {act_id}")
            rd = requests.get(f"https://www.strava.com/api/v3/activities/{act_id}",
                              headers={"Authorization":f"Bearer {token}"})
            rd.raise_for_status()
            detailed.append(rd.json())
            time.sleep(0.2)  # small pause to respect rate limits
        else:
            detailed.append(a)
    return detailed

# ─── UTILITIES ────────────────────────────────────────────────────────────────
def format_hhmmss(sec:int)->str:
    h, rem = divmod(sec,3600)
    m, s   = divmod(rem,60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def meters_to_miles(m:float)->float:
    return m * 0.000621371

# ─── NOTION: CREATE DB ────────────────────────────────────────────────────────
def create_notion_database(title, schema):
    payload = {
        "parent": {"type":"page_id","page_id":NOTION_PARENT_PAGE_ID},
        "title":  [{"type":"text","text":{"content":title}}],
        "properties": schema
    }
    r = requests.post("https://api.notion.com/v1/databases", headers=HEADERS, json=payload)
    r.raise_for_status()
    db = r.json()
    print("✅ Created Notion DB:", db["id"])
    return db["id"]

# ─── NOTION: PUSH ROWS ────────────────────────────────────────────────────────
def push_rows(db_id, rows):
    for row in rows:
        props = {
            "Athlete":       {"title":[{"text":{"content":row["name"]}}]},
            "Distance (mi)": {"number":round(row["miles"],2)},
            "Moving Time":   {"rich_text":[{"text":{"content":row["moving"]}}]},
            "Elapsed Time":  {"rich_text":[{"text":{"content":row["elapsed"]}}]},
        }
        r = requests.post("https://api.notion.com/v1/pages",
                          headers=HEADERS,
                          json={"parent":{"database_id":db_id},"properties":props})
        r.raise_for_status()
    print(f"📝 Pushed {len(rows)} rows to DB {db_id}")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    # date bounds in PT
    start_dt = datetime.fromisoformat(START_DATE_STR).replace(tzinfo=PACIFIC)
    end_dt   = start_dt + timedelta(days=1)
    print(f"🗓️  Syncing all club runs on {start_dt.date()} (PT)")

    # auth & fetch
    token = refresh_strava_token()
    runs  = fetch_all_runs(token)
    print(f"🔍  Fetched & detailed {len(runs)} runs total")

    # filter & group
    groups = {}
    for a in runs:
        local = parser.isoparse(a["start_date_local"]).astimezone(PACIFIC)
        if not (start_dt <= local < end_dt):
            continue
        name = a.get("name","Unnamed Activity")
        ath  = a["athlete"]
        aid  = ath["id"]
        groups.setdefault(name, {})[aid] = groups[name].get(aid, {
            "name": f"{ath.get('firstname','')} {ath.get('lastname','')}".strip(),
            "meters":0,"moving":0,"elapsed":0
        })
        rec = groups[name][aid]
        rec["meters"]  += a["distance"]
        rec["moving"]  += a["moving_time"]
        rec["elapsed"] += a["elapsed_time"]

    if not groups:
        print("⚠️ No runs found on that date. Exiting.")
        return

    # schema
    schema = {
        "Athlete":       {"title": {}},
        "Distance (mi)": {"number":{"format":"number"}},
        "Moving Time":   {"rich_text": {}},
        "Elapsed Time":  {"rich_text": {}},
    }

    # one DB per activity
    for activity_name, athletes in groups.items():
        title = f"{activity_name} – {start_dt.strftime('%-m/%-d/%Y')}"
        db_id = create_notion_database(title, schema)

        rows = []
        for v in athletes.values():
            rows.append({
                "name":    v["name"],
                "miles":   meters_to_miles(v["meters"]),
                "moving":  format_hhmmss(v["moving"]),
                "elapsed": format_hhmmss(v["elapsed"])
            })
        push_rows(db_id, rows)

if __name__=="__main__":
    main()
