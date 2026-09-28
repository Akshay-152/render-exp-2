# TODO - Render deployment notes

## Goal
Run the standalone Flask uploader (main.py) in a single container.

## Status
- [x] Heartbeat system and server1 folder removed.
- [x] Dockerfile now runs the Flask app directly (`python main.py`) on $PORT.
- [x] Heartbeat env vars removed from render.yaml.
- [x] fastapi/uvicorn/httpx removed from requirements.txt.
- [ ] Verify / build locally.
