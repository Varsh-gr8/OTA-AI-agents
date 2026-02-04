print("=== OTA AI AGENT FILE LOADED ===")

import asyncio
import json
import websockets
from collections import deque, defaultdict
from datetime import datetime, timedelta

from atsc_compliance_bucket import atsc_compliance_bucket

# ---------------- GLOBAL STATE ----------------
finalized_campaigns = set()

# ---------------- CONFIG ----------------
WS_HOST = "localhost"
WS_PORT = 8765

DASHBOARD_HOST = "localhost"
DASHBOARD_PORT = 9001

BURST_SIZE = 3
ACK_THRESHOLD = 0.7
ACK_TIMEOUT = 5

# ---------------- ECU PRIORITY ----------------
ECU_PRIORITY = {
    "braking": 5,
    "engine_control": 5,
    "powertrain": 4,
    "transmission_control": 4,
    "airbag_control": 5,
    "suspension_control": 5,
    "body_control": 5,
    "climate_control": 3,
    "telematics_control": 3,
    "infotainment_control": 2,
    "radar": 1,
    "camera": 1
}

# ---------------- DASHBOARD STATE ----------------
dashboard_state = {
    "active_campaigns": {},
    "completed_campaigns": {},
    "metrics": {
        "total_fragments": 0,
        "acked_fragments": 0,
        "bandwidth_used": 0,
        "manual_bandwidth": 0,
        "commits": 0,
        "rollbacks": 0,
        "timeouts": 0,
        "total_latency": 0.0,
        "completed_campaigns_count": 0
    },
    "logs": []
}

# ---------------- DASHBOARD CONNECTIONS ----------------
dashboard_clients = set()

async def dashboard_handler(websocket):
    dashboard_clients.add(websocket)
    try:
        await websocket.send(json.dumps({
            "type": "snapshot",
            "state": dashboard_state
        }))
        async for _ in websocket:
            pass
    finally:
        dashboard_clients.discard(websocket)

async def push_dashboard(event_type, payload):
    message = json.dumps({
        "type": event_type,
        "payload": payload,
        "timestamp": datetime.utcnow().isoformat()
    })
    for ws in list(dashboard_clients):
        try:
            await ws.send(message)
        except:
            dashboard_clients.discard(ws)

def log_event(event, details):
    entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "event": event,
        **details
    }
    dashboard_state["logs"].append(entry)
    asyncio.create_task(push_dashboard("log", entry))

# ---------------- ECU GROUPING ----------------
ECU_GROUPS = {
    "safety": ["airbag_control", "braking", "suspension_control"],
    "powertrain": ["engine_control", "powertrain", "transmission_control"],
    "comfort": ["climate_control", "body_control"],
    "infotainment": ["infotainment_control", "telematics_control"],
    "perception": ["radar", "camera"]
}

ECU_TO_GROUP = {
    ecu: group
    for group, ecus in ECU_GROUPS.items()
    for ecu in ecus
}

# ---------------- RECEIVER REGISTRY ----------------
class ReceiverRegistry:
    def __init__(self):
        self.groups = defaultdict(dict)
        self.expected = defaultdict(set)

    def register(self, receiver_id, ecu, websocket):
        group = ECU_TO_GROUP.get(ecu, "default")
        self.groups[group][receiver_id] = websocket
        self.expected[group].add(receiver_id)
        log_event("RECEIVER_REGISTERED", {
            "receiver_id": receiver_id,
            "ecu": ecu,
            "group": group
        })

    async def multicast(self, group, message):
        receivers = self.groups.get(group, {})
        dead = []

        for rid, ws in receivers.items():
            try:
                await ws.send(message)
            except:
                dead.append(rid)

        for rid in dead:
            receivers.pop(rid, None)
            self.expected[group].discard(rid)

# ---------------- GLOBAL STRUCTURES ----------------
registry = ReceiverRegistry()
rr_queues = {ecu: deque() for ecu in ECU_TO_GROUP}
rr_order = deque(rr_queues.keys())

campaign_acks = defaultdict(set)
campaign_start_time = {}
campaign_groups = {}
campaign_expected = {}

# ---------------- PRIORITY FUNCTION ----------------
def compute_priority(campaign):
    return (
        campaign["urgency"] * 0.5 +
        campaign["size"] * 0.15 +
        ECU_PRIORITY.get(campaign["ecu_target"], 1)
    )

# ---------------- OTA HANDLER ----------------
async def ota_handler(websocket):
    async for message in websocket:
        data = json.loads(message)

        if data.get("type") == "register":
            registry.register(
                data["receiver_id"],
                data["ecu_type"],
                websocket
            )
            continue

        if data.get("type") == "ack":
            cid = data["campaign_id"]
            rid = data["receiver_id"]
            campaign_acks[cid].add(rid)
            dashboard_state["metrics"]["acked_fragments"] += 1
            log_event("ACK_RECEIVED", {"campaign_id": cid, "receiver_id": rid, "offset": data["offset"]})
            continue

        campaign = atsc_compliance_bucket(data)
        cid = campaign["campaign_id"]

        if campaign["atsc"]["compliance_status"] == "rejected":
            log_event("CAMPAIGN_REJECTED", {"campaign_id": cid})
            continue

        campaign["priority_score"] = compute_priority(campaign)
        campaign["remaining"] = campaign["size"]
        campaign["offset"] = 0

        ecu = campaign["ecu_target"]
        group = ECU_TO_GROUP[ecu]

        campaign_expected[cid] = set(registry.expected.get(group, set()))
        campaign_start_time[cid] = datetime.utcnow()
        campaign_groups[cid] = group

        rr_queues[ecu].append(campaign)

        log_event("CAMPAIGN_ENQUEUED", {"campaign_id": cid, "ecu": ecu, "group": group, "priority": round(campaign["priority_score"],2)})

# ---------------- SCHEDULER ----------------
async def scheduler():
    while True:
        did_work = False
        for _ in range(len(rr_order)):
            ecu = rr_order[0]
            rr_order.rotate(-1)

            if not rr_queues[ecu]:
                continue

            did_work = True
            campaign = rr_queues[ecu][0]
            cid = campaign["campaign_id"]
            group = ECU_TO_GROUP[ecu]

            burst = min(BURST_SIZE, campaign["remaining"])
            fragment = {
                "type": "fragment",
                "campaign_id": cid,
                "ecu_target": ecu,
                "group": group,
                "offset": campaign["offset"],
                "size": burst
            }

            await registry.multicast(group, json.dumps(fragment))

            campaign["offset"] += burst
            campaign["remaining"] -= burst
            dashboard_state["metrics"]["total_fragments"] += 1
            dashboard_state["metrics"]["bandwidth_used"] += burst

            dashboard_state["active_campaigns"][cid] = {
                "campaign_id": cid,
                "ecu_target": ecu,
                "priority": round(campaign["priority_score"], 2),
                "remaining": campaign["remaining"],
                "group": group,
                "state": "ACTIVE"
            }

            log_event("FRAGMENT_SENT", fragment)

            if campaign["remaining"] <= 0:
                rr_queues[ecu].popleft()
                dashboard_state["active_campaigns"][cid]["state"] = "EVALUATING"
                await evaluate_campaign(cid)

            await asyncio.sleep(0.1)
        if not did_work:
            await asyncio.sleep(0.05)

# ---------------- EVALUATION ----------------
async def evaluate_campaign(cid):
    if cid in finalized_campaigns:
        return

    finalized_campaigns.add(cid)

    expected = campaign_expected.get(cid, set())
    acked = campaign_acks.get(cid, set())
    group = campaign_groups.get(cid, None)
    start = campaign_start_time.get(cid)
    latency = (datetime.utcnow() - start).total_seconds() if start else None

    # Debug log at start
    log_event("EVALUATE_START", {"campaign_id": cid, "expected": list(expected), "acked": list(acked), "group": group})

    ratio = (len(acked) / len(expected)) if expected else 0

    if ratio >= ACK_THRESHOLD and expected:
        dashboard_state["metrics"]["commits"] += 1
        result = "SUCCESS"
        final_state = "COMMITTED"
        log_event("CAMPAIGN_COMMITTED", {"campaign_id": cid})
    else:
        dashboard_state["metrics"]["rollbacks"] += 1
        if group:
            await rollback(cid, group)
        else:
            log_event("ROLLBACK_SKIPPED", {"campaign_id": cid, "reason": "NO_GROUP"})
        result = "FAILED"
        final_state = "ROLLED_BACK"

    dashboard_state["completed_campaigns"][cid] = {
        "result": result,
        "final_state": final_state,
        "ack_ratio": round(ratio,2),
        "latency_sec": latency,
        "completed_at": datetime.utcnow().isoformat()
    }

    if latency:
        dashboard_state["metrics"]["total_latency"] += latency
        dashboard_state["metrics"]["completed_campaigns_count"] += 1

    dashboard_state["active_campaigns"].pop(cid, None)

    # Debug log at end
    log_event("EVALUATE_END", {"campaign_id": cid})

    # CLEANUP
    campaign_expected.pop(cid, None)
    campaign_groups.pop(cid, None)
    campaign_start_time.pop(cid, None)
    campaign_acks.pop(cid, None)

# ---------------- ROLLBACK ----------------
async def rollback(cid, group):
    rollback_msg = {
        "type": "rollback",
        "campaign_id": cid,
        "group": group,
        "reason": "INSUFFICIENT_ACKS"
    }
    log_event("ROLLBACK_TRIGGERED", rollback_msg)
    await registry.multicast(group, json.dumps(rollback_msg))

# ---------------- TIMEOUT MONITOR ----------------
async def ack_timeout_monitor():
    while True:
        now = datetime.utcnow()
        for cid, start in list(campaign_start_time.items()):
            if cid in finalized_campaigns:
                continue
            elapsed = (now - start).total_seconds()
            if elapsed >= ACK_TIMEOUT:   # <-- was CAMPAIGN_TIMEOUT
                dashboard_state["metrics"]["timeouts"] += 1
                await evaluate_campaign(cid)
        await asyncio.sleep(1)

# ---------------- MAIN ----------------
async def main():
    asyncio.create_task(scheduler())
    asyncio.create_task(ack_timeout_monitor())

    await websockets.serve(ota_handler, WS_HOST, WS_PORT)
    await websockets.serve(dashboard_handler, DASHBOARD_HOST, DASHBOARD_PORT)

    print(f"[OTA AI AGENT] ws://{WS_HOST}:{WS_PORT}")
    print(f"[DASHBOARD] ws://{DASHBOARD_HOST}:{DASHBOARD_PORT}")

    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())

