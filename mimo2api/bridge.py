"""
mimo2api bridge — powered by mimo-claw-relay skill.

This module has been replaced by the standalone mimo-claw-relay skill:
  https://github.com/qizhuxu/mimo-claw-relay

Deployment:
  export MIMO_RELAY_WS_URL="ws://your-gateway:8000/ws"
  bash ~/.openclaw/skills/mimo-claw-relay/scripts/deploy.sh

If the skill is not installed, falls back to the inline implementation below.
"""

import sys
import os

# Attempt to import from installed mimo-claw-relay skill
RELAY_SKILL_DIR = os.path.expanduser("~/.openclaw/skills/mimo-claw-relay/scripts")
if RELAY_SKILL_DIR not in sys.path:
    sys.path.insert(0, RELAY_SKILL_DIR)

try:
    from bridge import run as _relay_run  # noqa: F401
    print("[mimo2api] Using mimo-claw-relay skill bridge")
    run = _relay_run
except ImportError:
    print("[mimo2api] mimo-claw-relay not found, using inline bridge")
    import asyncio, websockets, httpx, json as _json

    KEY = os.getenv("MIMO_API_KEY", "")
    URL = os.getenv("MIMO_API_ENDPOINT", "")
    BASE = URL.split("/v1/")[0] if "/v1/" in URL else URL
    WS_URL = os.getenv("MIMO_RELAY_WS_URL", os.getenv("WS_TUNNEL_URL", ""))

    async def safe_send(ws, lock, data):
        async with lock:
            await ws.send(_json.dumps(data))

    async def handle_request(ws, req, client, lock):
        req_id = req.get("req_id")
        try:
            async with client.stream(
                method=req.get("method", "GET"),
                url=f"{BASE}/anthropic/v1/messages" if "/anthropic/" in req.get("path", "") else URL,
                headers={"api-key": KEY, "Content-Type": "application/json"},
                content=req.get("body", ""),
            ) as r:
                await safe_send(ws, lock, {
                    "req_id": req_id, "type": "start",
                    "status": r.status_code, "headers": dict(r.headers),
                })
                async for chunk in r.aiter_text():
                    if chunk:
                        await safe_send(ws, lock, {
                            "req_id": req_id, "type": "chunk", "body": chunk,
                        })
                await safe_send(ws, lock, {"req_id": req_id, "type": "finish"})
        except Exception as e:
            await safe_send(ws, lock, {"req_id": req_id, "type": "error", "body": str(e)})

    async def run():
        async with httpx.AsyncClient(timeout=None) as client:
            while True:
                try:
                    async with websockets.connect(WS_URL, max_size=10**8) as ws:
                        send_lock = asyncio.Lock()
                        async for msg in ws:
                            asyncio.create_task(handle_request(ws, _json.loads(msg), client, send_lock))
                except Exception:
                    await asyncio.sleep(3)

if __name__ == "__main__":
    import asyncio
    asyncio.run(run())
