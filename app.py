# app.py
import os, time, requests
from datetime import datetime, timedelta
from dateutil.parser import isoparse
from zoneinfo import ZoneInfo
import streamlit as st

# ─── Config ────────────────────────────────────────────────────────────────────
STRAVA_CLIENT_ID     = os.getenv("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
STRAVA_REFRESH_TOKEN = os.getenv("STRAVA_REFRESH_TOKEN")
STRAVA_CLUB_ID       = os.getenv("STRAVA_CLUB_ID")

NOTION_TOKEN         = os.getenv("NOTION_TOKEN")
NOTION_PARENT_PAGE_ID= os.getenv("NOTION_PARENT_PAGE_ID")

PACIFIC = ZoneInfo("America/Los_Angeles")
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

# ─── Strava helpers ────────────────────────────────────────────────────────────
def refresh_strava_token():
    r = requests.post("https://www.strava.com/oauth/token", data={
        "client_id":     STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "grant_type":    "refresh_token",
        "refresh_token": STRAVA_REFRESH_TOKEN
    })
    r.raise_for_status()
    return r.json()["access_token"]

def fetch_runs(token):
    runs, page = [], 1
    hdr = {"Authorization": f"Bearer {token}"}
    while True:
        r = requests.get(
            f"https://www.strava.com/api/v3/clubs/{STRAVA_CLUB_ID}/activities",
            headers=hdr,
            params={"page":page,"per_page":200}
        )
        r.raise_for_status()
        batch = r.json()
        if not batch: break
        runs.extend(a for a in batch if a["type"]=="Run")
        page += 1; time.sleep(0.5)
    return runs

# ─── Notion helpers ────────────────────────────────────────────────────────────
def create_notion_db(title):
    schema = {
        "Athlete":       {"title": {}},
        "Distance (mi)": {"number":{"format":"number"}},
        "Moving Time":   {"rich_text":{}},
        "Elapsed Time":  {"rich_text":{}},
    }
    payload = {
        "parent": {"type":"page_id","page_id":NOTION_PARENT_PAGE_ID},
        "title":  [{"type":"text","text":{"content":title}}],
        "properties": schema
    }
    r = requests.post("https://api.notion.com/v1/databases",
                      headers=NOTION_HEADERS, json=payload)
    r.raise_for_status()
    return r.json()["id"]

def push_rows(db_id, rows):
    for row in rows:
        props = {
            "Athlete": {"title":[{"text":{"content":row["name"]}}]},
            "Distance (mi)": {"number":round(row["miles"],2)},
            "Moving Time": {"rich_text":[{"text":{"content":row["moving"]}}]},
            "Elapsed Time":{"rich_text":[{"text":{"content":row["elapsed"]}}]}
        }
        r = requests.post(
            "https://api.notion.com/v1/pages",
            headers=NOTION_HEADERS,
            json={"parent":{"database_id":db_id},"properties":props}
        )
        r.raise_for_status()

# ─── UI ────────────────────────────────────────────────────────────────────────
st.title("Strava → Notion Sync")

# 1) let user pick a date
picked_date = st.date_input("Select date (PT)", datetime.now(PACIFIC).date())

if st.button("Load Activities"):
    token = refresh_strava_token()
    all_runs = fetch_runs(token)

    # filter to runs on that PT date
    start = datetime.combine(picked_date, datetime.min.time()).replace(tzinfo=PACIFIC)
    end   = start + timedelta(days=1)
    choices = sorted({
        a["name"] for a in all_runs
        if start <= isoparse(a["start_date_local"]).astimezone(PACIFIC) < end
    })

    if not choices:
        st.warning("No runs found on that date.")
    else:
        activity = st.selectbox("Pick an activity", choices)
        if st.button("Sync Selected Activity"):
            # aggregate for that one activity
            filtered = [a for a in all_runs if a["name"]==activity
                        and start <= isoparse(a["start_date_local"]).astimezone(PACIFIC) < end]
            totals = {}
            for a in filtered:
                ath=a["athlete"]; aid=ath.get("id")
                if not aid: continue
                name=f"{ath['firstname']} {ath.get('lastname','')}".strip()
                rec = totals.setdefault(aid,{"name":name,"meters":0,"moving":0,"elapsed":0})
                rec["meters"] += a["distance"]
                rec["moving"]  += a["moving_time"]
                rec["elapsed"] += a["elapsed_time"]

            rows = []
            for v in totals.values():
                miles = v["meters"]*0.000621371
                rows.append({
                    "name":    v["name"],
                    "miles":   miles,
                    "moving":  time.strftime("%H:%M:%S", time.gmtime(v["moving"])),
                    "elapsed": time.strftime("%H:%M:%S", time.gmtime(v["elapsed"]))
                })

            title = f"{activity} Results – {picked_date.strftime('%-m/%-d/%Y')}"
            new_db = create_notion_db(title)
            push_rows(new_db, rows)
            st.success(f"✅ Synced {len(rows)} athletes to new DB “{title}”")
