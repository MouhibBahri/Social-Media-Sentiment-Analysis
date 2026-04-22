## Bluesky Bridge (`bluesky_bridge.py`)

A lightweight TCP bridge that connects the [Bluesky Jetstream](https://docs.bsky.app/docs/advanced-guides/firehose) WebSocket to a local socket server, making the stream consumable by Spark (or any TCP client).

It filters incoming public posts by keyword and forwards matching ones as JSON lines on port `9999`.

### Test it

Start the bridge:
```bash
python bluesky_bridge.py
```

Then in a second terminal, connect with netcat to see live posts stream in:
```bash
nc localhost 9999
```
