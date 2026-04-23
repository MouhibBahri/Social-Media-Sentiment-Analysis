import asyncio
import websockets
import json
import socket

BLUESKY_URI = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
KEYWORDS = ["apple", "iphone"]

async def stream_to_spark(client_sock):
    while True:   # reconnect loop
        try:
            async with websockets.connect(BLUESKY_URI, open_timeout=30) as ws:
                print("Connected to Bluesky.")
                while True:
                    raw = await ws.recv()
                    event = json.loads(raw)
                    if event.get("kind") == "commit" and \
                       event["commit"].get("operation") == "create":
                        text = event["commit"].get("record", {}).get("text", "")
                        #if any(kw in text.lower() for kw in KEYWORDS):
                        payload = json.dumps({"text": text}) + "\n"
                        client_sock.sendall(payload.encode("utf-8"))
        except Exception as e:
            print(f"Bluesky connection lost: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)   # wait then retry

def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", 9999))
    server.listen(1)
    print("Waiting for server to connect...")
    client_sock, _ = server.accept()
    print("Server connected!")
    asyncio.run(stream_to_spark(client_sock))

main()
