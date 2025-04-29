import os, time, requests
from datetime import datetime

# Env vars you must have set:
CLIENT_ID     = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")
CLUB_ID       = os.getenv("STRAVA_CLUB_ID")

NOTION_TOKEN      = os.getenv("NOTION_TOKEN")
NOTION_DB_ID      = os.getenv("NOTION_DB_ID")
ACTIVITIES_DB_ID  = os.getenv("ACTIVITIES_DB_ID")

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

def create_activity_page():
    """Create a new Activity page titled with the current time & date."""
    now = datetime.now()
    time_str = now.strftime("%H:%M:%S")
    date_str = now.strftime("%-m/%-d/%Y")
    title   = f"Activity (Called {time_str} - {date_str})"

    payload = {
        "parent": {"database_id": ACTIVITIES_DB_ID},
        "properties": {
            "Title": {                       # or "Name" if you left the default
                "title": [{
                    "text": {"content": title}
                }]
            }
        }
    }
    r = requests.post("https://api.notion.com/v1/pages", headers=HEADERS, json=payload)
    r.raise_for_status()
    page_id = r.json()["id"]
    print(f"Created Activity page: {page_id} → “{title}”")
    return page_id

def refresh_strava_token():
    r = requests.post("https://www.strava.com/oauth/token", data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN
    })
    r.raise_for_status()
    return r.json()["access_token"]

def fetch_runs(token):
    hdr, runs, page = {"Authorization":f"Bearer {token}"}, [], 1
    while True:
        resp = requests.get(
            f"https://www.strava.com/api/v3/clubs/{CLUB_ID}/activities",
            headers=hdr,
            params={"page":page,"per_page":200}
        )
        if resp.status_code == 401:
            raise RuntimeError("Unauthorized: join the club or check scopes")
        batch = resp.json()
        if not batch:
            break
        runs.extend(a for a in batch if a.get("type")=="Run")
        page += 1
        time.sleep(0.5)
    return runs

def aggregate(runs, max_athletes=200):
    data = {}
    for a in runs:
        ath = a["athlete"]; aid = ath["id"]
        name = f"{ath.get('firstname','')} {ath.get('lastname','')}".strip()
        mv  = a.get("moving_time", 0)
        el  = a.get("elapsed_time", 0)
        if aid not in data:
            data[aid] = {"name": name, "moving": 0, "elapsed": 0}
        data[aid]["moving"]  += mv
        data[aid]["elapsed"] += el

    sorted_ath = sorted(data.values(), key=lambda x: x["moving"], reverse=True)[:max_athletes]

    rows = []
    grand = 0.0
    for e in sorted_ath:
        mv, el = e["moving"], e["elapsed"]
        h_mv, rem_mv = divmod(mv,3600)
        m_mv, s_mv   = divmod(rem_mv,60)
        moving_str   = f"{h_mv:02d}:{m_mv:02d}:{s_mv:02d}"

        h_el, rem_el = divmod(el,3600)
        m_el, s_el   = divmod(rem_el,60)
        elapsed_str  = f"{h_el:02d}:{m_el:02d}:{s_el:02d}"

        hours = round(mv/3600,2)
        grand += hours

        rows.append({
            "name":    e["name"],
            "moving":  moving_str,
            "elapsed": elapsed_str,
            "hours":   hours
        })

    return rows, round(grand,2)

def archive_old():
    url, cursor = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query", None
    pages = []
    while True:
        payload = {"page_size":100, **({"start_cursor":cursor} if cursor else {})}
        r = requests.post(url, headers=HEADERS, json=payload); r.raise_for_status()
        d = r.json()
        pages.extend(d["results"])
        if not d.get("has_more"): break
        cursor = d.get("next_cursor")
    for p in pages:
        requests.patch(
            f"https://api.notion.com/v1/pages/{p['id']}",
            headers=HEADERS,
            json={"archived": True}
        ).raise_for_status()

def push_new(rows):
    for r in rows:
        payload = {
            "parent": {"database_id": NOTION_DB_ID},
            "properties": {
                "Athlete":      {"title":     [{"text":{"content":r["name"]}}]},
                "Moving Time":  {"rich_text":[{"text":{"content":r["moving"]}}]},
                "Elapsed Time": {"rich_text":[{"text":{"content":r["elapsed"]}}]},
                "Total Hours":  {"number":    r["hours"]}
            }
        }
        requests.post("https://api.notion.com/v1/pages", headers=HEADERS, json=payload).raise_for_status()

def main():
    # 1) Create a new “Activity” page
    create_activity_page()

    # 2) Sync leaderboard
    token = refresh_strava_token()
    runs  = fetch_runs(token)
    rows, grand = aggregate(runs)
    archive_old()
    push_new(rows)
    print(f"✅ Leaderboard updated for {len(rows)} athletes (grand total: {grand} hrs)")

if __name__=="__main__":
    main()
