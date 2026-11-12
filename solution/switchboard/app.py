"""HTTP surface: the provider's result webhook, plus /status (JSON) and / (live dashboard)."""
from __future__ import annotations

import hashlib
import hmac
import json
import os

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from .engine import Engine

DASHBOARD = os.path.join(os.path.dirname(__file__), "dashboard.html")


def verify_signature(secret: str, body: bytes, signature: str | None) -> bool:
    if not signature:
        return False
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature.strip().lower())


def create_app(engine: Engine) -> FastAPI:
    app = FastAPI(title="Switchboard dispatcher", docs_url=None, redoc_url=None)

    @app.post("/webhooks/telephony")
    async def webhook(request: Request):
        raw = await request.body()                    # verify the exact bytes that were signed
        if not verify_signature(engine.cfg.webhook_secret, raw, request.headers.get("X-AU-Signature")):
            with engine.store.tx():
                engine.store.journal("bad_signature", engine.clock.now(), size=len(raw))
            return JSONResponse({"error": "bad_signature"}, status_code=401)
        try:
            payload = json.loads(raw)
        except ValueError:
            return JSONResponse({"error": "bad_json"}, status_code=400)
        engine.crash("after_webhook_verified")
        outcome = engine.handle_result(payload, source="webhook")
        engine.crash("after_result_commit")
        # 200 even for duplicates and unknown calls: redelivering them would not change anything.
        return {"ok": True, "outcome": outcome}

    @app.get("/status")
    async def status():
        return engine.status()

    @app.get("/", response_class=HTMLResponse)
    async def dashboard():
        with open(DASHBOARD, encoding="utf-8") as f:
            return f.read()

    return app
