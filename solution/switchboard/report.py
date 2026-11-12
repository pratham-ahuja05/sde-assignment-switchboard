"""final_state.csv, and a cross-check of it against the provider's own view (/admin/leads)."""
from __future__ import annotations

import csv
import os

from .store import Store

COLUMNS = ["campaign_id", "lead_id", "final_state", "attempts", "last_status", "last_disposition"]


def final_rows(store: Store) -> list[dict]:
    return [{"campaign_id": r["campaign_id"], "lead_id": r["lead_id"], "final_state": r["state"],
             "attempts": r["attempts"], "last_status": r["last_status"] or "",
             "last_disposition": r["last_disposition"] or ""} for r in store.all_leads()]


def write_final_state(store: Store, path: str) -> int:
    rows = final_rows(store)
    tmp = path + ".tmp"
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, COLUMNS, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, path)   # atomic: a crash mid-write never leaves a half-written report
    return len(rows)


def diff_against_provider(rows: list[dict], provider_leads: list[dict]) -> list[str]:
    ours = {(r["campaign_id"], r["lead_id"]): r for r in rows}
    out = []
    for p in provider_leads:
        key = (p["campaign_id"], p["lead_id"])
        o = ours.pop(key, None)
        if o is None:
            out.append(f"{key}: missing from final_state.csv")
            continue
        theirs = (p["expected_state"], int(p["attempts"]), p["last_status"] or "", p["last_disposition"] or "")
        mine = (o["final_state"], int(o["attempts"]), o["last_status"], o["last_disposition"])
        if theirs != mine:
            out.append(f"{key[0]}/{key[1]}: ours={mine} provider={theirs}")
    out += [f"{k}: not known to provider" for k in ours]
    return out
