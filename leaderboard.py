import os, time, requests

CLIENT_ID     = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")
CLUB_ID       = os.getenv("STRAVA_CLUB_ID")
NOTION_TOKEN  = os.getenv("NOTION_TOKEN")
NOTION_DB_ID  = os.getenv("NOTION_DB_ID")

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

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
    hdr, page, runs = {"Authorization":f"Bearer {token}"}, 1, []
    while True:
        r = requests.get(
            f"https://www.strava.com/api/v3/clubs/{CLUB_ID}/activities",
            headers=hdr, params={"page":page,"per_page":200}
        )
        if r.status_code == 401:
            raise RuntimeError("Unauthorized: join the club or check scopes")
        data = r.json()
        if not data: break
        runs += [a for a in data if a["type"]=="Run"]
        page += 1
        time.sleep(0.5)
    return runs

def aggregate(runs):
    totals = {}
    for a in runs:
        name = f"{a['athlete']['firstname']} {a['athlete'].get('lastname','')}".strip()
        secs = a.get("moving_time",0)
        totals[name] = totals.get(name, 0) + secs
    rows = []
    for name, secs in sorted(totals.items(), key=lambda x:-x[1]):
        h, rem = divmod(secs,3600)
        m, s    = divmod(rem,60)
        rows.append((name, f"{h:02d}:{m:02d}:{s:02d}", round(secs/3600,2)))
    return rows

def archive_old():
    url, cursor = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query", None
    pages = []
    while True:
        payload = {"page_size":100, **({"start_cursor":cursor} if cursor else {})}
        r = requests.post(url, headers=HEADERS, json=payload); r.raise_for_status()
        d = r.json()
        pages += d["results"]
        if not d.get("has_more"): break
        cursor = d.get("next_cursor")
    for p in pages:
        requests.patch(
            f"https://api.notion.com/v1/pages/{p['id']}",
            headers=HEADERS, json={"archived":True}
        ).raise_for_status()

def push_new(rows):
    for name, tt, hrs in rows:
        payload = {
            "parent":{"database_id":NOTION_DB_ID},
            "properties":{
                "Athlete":{"title":[{"text":{"content":name}}]},
                "Total Time":{"rich_text":[{"text":{"content":tt}}]},
                "Total Hours":{"number":hrs}
            }
        }
        r = requests.post("https://api.notion.com/v1/pages", headers=HEADERS, json=payload)
        r.raise_for_status()

if __name__=="__main__":
    token = refresh_strava_token()
    runs  = fetch_runs(token)
    rows  = aggregate(runs)
    archive_old()
    push_new(rows)
    print("✅ Leaderboard updated")
