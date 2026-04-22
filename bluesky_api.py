import asyncio
import websockets
import json
import socket

BLUESKY_URI = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
KEYWORDS = ["apple", "iphone"]

async def stream(client_sock):
    async with websockets.connect(BLUESKY_URI) as ws:
        while True:
            raw = await ws.recv()
            event = json.loads(raw)

            if event.get("kind") == "commit" and \
               event["commit"].get("operation") == "create":
                text = event["commit"].get("record", {}).get("text", "")
                #if any(kw in text.lower() for kw in KEYWORDS):
                payload = json.dumps({"text": text}) + "\n"
                client_sock.sendall(payload.encode("utf-8"))

def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("localhost", 9999))
    server.listen(1)
    print("Waiting for server to connect...")
    client_sock, _ = server.accept()
    print("Server connected!")
    asyncio.run(stream(client_sock))

main()
