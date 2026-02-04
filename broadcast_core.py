import sqlite3
import json
import asyncio
import websockets
import os
import threading
from datetime import datetime
from itertools import product
from random import shuffle, choice
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn

from atsc_compliance_bucket import atsc_compliance_bucket  # your compliance code

# ---------------- CONFIG ----------------
DB_PATH = "OTA_campaigns.db"
WS_URI = "ws://localhost:8765"
JSON_DIR = "outgoing_campaigns"
os.makedirs(JSON_DIR, exist_ok=True)

MAX_CAMPAIGNS_TO_INSERT = 50
BATCH_SIZE = 10
MAX_CAMPAIGN_SIZE = 50
BROADCAST_INTERVAL = 5

# ---------------- DATABASE ----------------
con = sqlite3.connect(DB_PATH, check_same_thread=False)
cur = con.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS campaigns (
    campaign_id TEXT PRIMARY KEY,
    payload TEXT,
    state TEXT,
    created_at TEXT
)
""")
con.commit()

# ---------------- FASTAPI ----------------
app = FastAPI()
new_campaign_event = None

class Campaign(BaseModel):
    campaign_id: str
    ecu_target: str
    size: int
    urgency: int
    tx_profile: dict
    app_profile: dict

@app.post("/campaign")
async def receive_campaign(campaign: Campaign):
    if campaign.size > MAX_CAMPAIGN_SIZE:
        return {"status": "rejected", "reason": f"size exceeds {MAX_CAMPAIGN_SIZE}"}
    cur.execute("""
        INSERT OR REPLACE INTO campaigns VALUES (?, ?, ?, ?)
    """, (
        campaign.campaign_id,
        campaign.json(),
        "pending",
        datetime.utcnow().isoformat()
    ))
    con.commit()
    if new_campaign_event:
        new_campaign_event.set()
    print(f"[BroadcastCore] Campaign stored: {campaign.campaign_id}")
    return {"status": "campaign accepted"}

# ---------------- HELPERS ----------------
def fetch_pending_campaigns():
    cur.execute("SELECT payload FROM campaigns WHERE state='pending'")
    return [json.loads(row[0]) for row in cur.fetchall()]

def write_campaign_json(campaign):
    path = os.path.join(JSON_DIR, f"{campaign['campaign_id']}.json")
    with open(path, "w") as f:
        json.dump(campaign, f, indent=4)
    return path

# ---------------- CAMPAIGN GENERATOR ----------------
ECUS = [
    "braking", "engine_control", "powertrain", "transmission_control",
    "airbag_control", "suspension_control", "body_control",
    "climate_control", "telematics_control", "infotainment_control",
    "radar", "camera"
]

SIZES = [10, 20, 30, 40, 50]
URGENCIES = [1, 5, 10]
TX_PROFILES = [
    {"modulation":"COFDM", "constellation":"NUC", "fec":["LDPC","BCH"], "snr_db":10, "protocol":"ROUTE", "plp_id":1},
    {"modulation":"COFDM", "constellation":"NUC", "fec":["LDPC","BCH"], "snr_db":20, "protocol":"MMT", "plp_id":2},
]
APP_PROFILES = [
    {"codec":"HEVC","audio":"AC-4","code_signed":True,"drm":False,"emergency_capable":True},
    {"codec":"HEVC","audio":"AC-4","code_signed":False,"drm":True,"emergency_capable":False},
]

def generate_campaigns_with_rollbacks():
    combinations = list(product(ECUS, SIZES, URGENCIES, TX_PROFILES, APP_PROFILES))
    shuffle(combinations)
    inserted = 0
    try:
        cur.execute("BEGIN")
        for idx, (ecu, size, urgency, tx, app) in enumerate(combinations):
            if size > MAX_CAMPAIGN_SIZE:
                continue

            campaign_id = f"{ecu[:3].upper()}_{idx}"

            # Inject some "invalid" campaigns to trigger rollbacks
            # e.g., invalid modulation or missing code_sign
            if inserted % 5 == 0:
                # intentionally break compliance every 5th campaign
                tx["modulation"] = "INVALID_MOD"
                app["code_signed"] = False

            campaign = {
                "campaign_id": campaign_id,
                "ecu_target": ecu,
                "size": size,
                "urgency": urgency,
                "tx_profile": tx,
                "app_profile": app
            }

            cur.execute("""
                INSERT OR REPLACE INTO campaigns(campaign_id, payload, state, created_at)
                VALUES (?, ?, ?, ?)
            """, (campaign_id, json.dumps(campaign), "pending", datetime.utcnow().isoformat()))

            inserted += 1
            if inserted % BATCH_SIZE == 0:
                con.commit()
                cur.execute("BEGIN")

            if inserted >= MAX_CAMPAIGNS_TO_INSERT:
                break

        con.commit()
        print(f"[BroadcastCore] Inserted {inserted} campaigns (some will trigger rollbacks).")

    except sqlite3.Error as e:
        print("[BroadcastCore] SQLite error, rolling back:", e)
        con.rollback()

# ---------------- BROADCAST LOOP ----------------
async def broadcast_loop():
    global new_campaign_event
    new_campaign_event = asyncio.Event()

    async with websockets.connect(WS_URI) as websocket:
        print("[BroadcastCore] Connected to OTA Agent")
        while True:
            try:
                await asyncio.wait_for(new_campaign_event.wait(), timeout=BROADCAST_INTERVAL)
                new_campaign_event.clear()
            except asyncio.TimeoutError:
                pass

            campaigns = fetch_pending_campaigns()
            for campaign in campaigns:
                path = write_campaign_json(campaign)
                await websocket.send(open(path).read())
                print(f"[BroadcastCore] Published RAW campaign → {campaign['campaign_id']}")

# ---------------- MAIN ----------------
def start_api():
    uvicorn.run(app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    print("[BroadcastCore] Starting...")
    generate_campaigns_with_rollbacks()
    threading.Thread(target=start_api, daemon=True).start()
    asyncio.run(broadcast_loop())

