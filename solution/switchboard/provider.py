"""HTTP client for the telephony provider. Translates raw HTTP into the four outcomes the engine
reasons about. The most important line in this file is the one that decides what counts as
"unknown": when in doubt we say UNKNOWN, never NOT_PLACED.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import httpx

ACCEPTED = "accepted"        # 201 new call, or 200 replay of an existing call for this request_id
UNAVAILABLE = "unavailable"  # definitely not placed (503, or the TCP connection never opened)
UNKNOWN = "unknown"          # may or may not have been placed (504, timeouts, dropped connections)
REJECTED = "rejected"        # 4xx: the provider refused on a rule. Never expected; journaled loudly.


@dataclass
class PlaceOutcome:
    kind: str
    status_code: Optional[int] = None
    body: Optional[dict] = None
    error: Optional[str] = None


class HttpProvider:
    def __init__(self, base_url: str, timeout_s: float = 10.0):
        # The provider's own 504 path takes 2 s; our timeout must be longer than that or we would
        # manufacture ambiguity that the provider would otherwise have resolved for us.
        self.base = base_url.rstrip("/")
        self.client = httpx.AsyncClient(base_url=self.base, timeout=timeout_s)

    async def aclose(self):
        await self.client.aclose()

    async def clock(self) -> dict:
        r = await self.client.get("/v1/clock")
        r.raise_for_status()
        return r.json()

    async def place_call(self, payload: dict) -> PlaceOutcome:
        try:
            r = await self.client.post("/v1/calls", json=payload)
        except (httpx.ConnectError, httpx.ConnectTimeout) as e:
            return PlaceOutcome(UNAVAILABLE, error=type(e).__name__)
        except httpx.HTTPError as e:     # ReadTimeout, RemoteProtocolError, ...: request may have landed
            return PlaceOutcome(UNKNOWN, error=type(e).__name__)
        body = _json(r)
        if r.status_code in (200, 201):
            return PlaceOutcome(ACCEPTED, r.status_code, body)
        if r.status_code == 503:
            return PlaceOutcome(UNAVAILABLE, r.status_code, body)
        if 400 <= r.status_code < 500:
            return PlaceOutcome(REJECTED, r.status_code, body)
        return PlaceOutcome(UNKNOWN, r.status_code, body)      # 504 and any other 5xx

    async def find_by_request(self, request_id: str) -> Optional[dict]:
        """The call for this idempotency key, or None if the provider has never placed one.
        Raises on transport errors: "couldn't ask" must never be confused with "not found"."""
        r = await self.client.get("/v1/calls", params={"request_id": request_id})
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    async def get_call(self, call_sid: str) -> Optional[dict]:
        r = await self.client.get(f"/v1/calls/{call_sid}")
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    async def admin_report(self) -> dict:
        r = await self.client.get("/admin/report")
        r.raise_for_status()
        return r.json()

    async def admin_leads(self) -> list[dict]:
        r = await self.client.get("/admin/leads")
        r.raise_for_status()
        return r.json()


def _json(r: httpx.Response) -> Optional[dict]:
    try:
        return r.json()
    except ValueError:
        return None
