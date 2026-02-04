import asyncio
from multi_client_objects import ECUReceiver

async def main():
    receivers = [
        ECUReceiver("ECU1", "braking", reliability=0.9),
        ECUReceiver("ECU2", "engine_control", reliability=0.8),
        ECUReceiver("ECU3", "airbag_control", reliability=1.0),
        ECUReceiver("ECU4", "infotainment_control", reliability=0.7),
        ECUReceiver("ECU5", "radar", reliability=0.5),
    ]

    await asyncio.gather(*(r.connect() for r in receivers))

if __name__ == "__main__":
    asyncio.run(main())

