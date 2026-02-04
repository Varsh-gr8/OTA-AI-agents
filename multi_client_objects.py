import asyncio
import json
import random
import websockets
from datetime import datetime

WS_URL = "ws://localhost:8765"

class ECUReceiver:
    def __init__(self, receiver_id, ecu_type, reliability=1.0):
        self.receiver_id = receiver_id
        self.ecu_type = ecu_type
        self.reliability = reliability

        # campaign_id → received offsets
        self.partial_state = {}

    async def connect(self):
        async with websockets.connect(WS_URL) as ws:
            await self.register(ws)
            await self.listen(ws)

    async def register(self, ws):
        await ws.send(json.dumps({
            "type": "register",
            "receiver_id": self.receiver_id,
            "ecu_type": self.ecu_type
        }))
        print(f"[{self.receiver_id}] Registered ({self.ecu_type}, reliability={self.reliability})")

    async def listen(self, ws):
        async for message in ws:
            data = json.loads(message)
            msg_type = data.get("type")

            if msg_type == "fragment":
                await self.handle_fragment(ws, data)

            elif msg_type == "rollback":
                self.handle_rollback(data)

    async def handle_fragment(self, ws, data):
        cid = data["campaign_id"]
        offset = data["offset"]

        print(f"[{self.receiver_id}] Fragment | {cid} | Offset={offset}")

        # Simulate ECU unreliability
        if random.random() > self.reliability:
            print(f"[{self.receiver_id}] Fragment dropped (simulated)")
            return

        # Create partial state bucket
        if cid not in self.partial_state:
            self.partial_state[cid] = set()

        self.partial_state[cid].add(offset)

        await ws.send(json.dumps({
            "type": "ack",
            "campaign_id": cid,
            "receiver_id": self.receiver_id,
            "offset": offset,
            "timestamp": datetime.utcnow().isoformat()
        }))

        print(f"[{self.receiver_id}] ACK sent | {cid} | Offset={offset}")

    def handle_rollback(self, data):
        cid = data["campaign_id"]

        if cid in self.partial_state:
            del self.partial_state[cid]

        print(
            f"[{self.receiver_id}] ROLLBACK | "
            f"Campaign={cid} | Reason={data.get('reason')}"
        )

