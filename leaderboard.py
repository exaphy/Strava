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
    print("▶️ Refresh payload:", {k: ("***" if "secret" in k.lower() or "refresh" in k.lower() else v)
                                   for k,v in payload.items()})
    r = requests.post("https://www.strava.com/oauth/token", data=payload)
    print(f"◀️ Strava status: {r.status_code}, response: {r.text}")
    r.raise_for_status()
    return r.json()["access_token"]

# ─── FETCH & DEBUG RUN SUMMARIES ─────────────────────────────────────────────
def fetch_all_runs(token):
    hdr = {"Authorization": f"Bearer {token}"}
    summaries = []
    page = 1

    # 1) fetch summary list of club activities
    while True:
        resp = requests.get(
            f"https://www.strava.com/api/v3/clubs/{STRAVA_CLUB_ID}/activities",
            headers=hdr,
            params={"page": page, "per_page": 200}
        )
        if resp.status_code == 401:
            raise RuntimeError("Unauthorized: check your token / club ID / scopes")
        resp.raise_for_status()

        batch = resp.json()
        if not batch:
            break

        # only keep Runs
        runs_page = [a for a in batch if a.get("type") == "Run"]
        summaries.extend(runs_page)
        print(f"📄 Page {page}: fetched {len(batch)} activities, {len(runs_page)} runs")
        page += 1
        time.sleep(0.5)

    print(f"🔍 Total Run summaries fetched: {len(summaries)}")

    # 2) detail‐fetch only if missing fields; skip if no ID
    detailed = []
    for a in summaries:
        act_id = a.get("id")
        if not act_id:
            print("⚠️ Skipping summary without id:", a)
            continue

        # if we already have start_date_local and athlete.id, just keep it
        if "start_date_local" in a and isinstance(a.get("athlete"), dict) and "id" in a["athlete"]:
            detailed.append(a)
            continue

        # otherwise fetch full details
        print(f"🐛 fetching full details for activity id {act_id}")
        rd = requests.get(f"https://www.strava.com/api/v3/activities/{act_id}", headers=hdr)
        if rd.status_code == 404:
            print(f"⚠️ Details not found for id {act_id}, skipping.")
            continue
        rd.raise_for_status()
        detailed.append(rd.json())
        time.sleep(0.2)

    print(f"🔍 Total detailed runs available: {len(detailed)}")
    return detailed

# ─── UTILITIES ────────────────────────────────────────────────────────────────
def format_hhmmss(sec:int)->str:
    h, rem = divmod(sec, 3600)
    m, s   = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def meters_to_miles(m:float)->float:
    return m * 0.000621371

# ─── NOTION: CREATE DATABASE ──────────────────────────────────────────────────
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
            "Distance (mi)": {"number": round(row["miles"],2)},
            "Moving Time":   {"rich_text":[{"text":{"content":row["moving"]}}]},
            "Elapsed Time":  {"rich_text":[{"text":{"content":row["elapsed"]}}]}
        }
        r = requests.post(
            "https://api.notion.com/v1/pages",
            headers=HEADERS,
            json={"parent":{"database_id":db_id},"properties":props}
        )
        r.raise_for_status()
    print(f"📝 Pushed {len(rows)} rows to DB {db_id}")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    # 1) compute date window in PT
    start_dt = datetime.fromisoformat(START_DATE_STR).replace(tzinfo=PACIFIC)
    end_dt   = start_dt + timedelta(days=1)
    print(f"🗓️  Syncing all club runs on {start_dt.date()} (PT)")

    # 2) auth & fetch
    token = refresh_strava_token()
    runs  = fetch_all_runs(token)

    if not runs:
        print("⚠️ No runs fetched from Strava. Check your club ID, membership, and scopes.")
        return

    # 3) filter & group by activity name
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
            "meters":0, "moving":0, "elapsed":0
        })
        rec = groups[name][aid]
        rec["meters"]  += a.get("distance",0)
        rec["moving"]  += a.get("moving_time",0)
        rec["elapsed"] += a.get("elapsed_time",0)

    if not groups:
        print("⚠️ No runs found on that date after filtering. Exiting.")
        return

    # 4) create & populate one DB per activity
    schema = {
        "Athlete":       {"title": {}},
        "Distance (mi)": {"number":{"format":"number"}},
        "Moving Time":   {"rich_text": {}},
        "Elapsed Time":  {"rich_text": {}},
    }

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
