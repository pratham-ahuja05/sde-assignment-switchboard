"""Switchboard CLI.

  python -m solution.switchboard run      # dispatch until every lead is final, write final_state.csv
  python -m solution.switchboard audit    # re-prove R1-R9 from our own ledger
  python -m solution.switchboard verify   # compare final_state.csv with the provider's /admin/leads
  python -m solution.switchboard chaos    # start the mock, kill -9 us repeatedly, score the result
  python -m solution.switchboard journal  # tail the append-only journal (what happened, and why)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

from . import config as config_mod
from .schedule import IST

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
FIXTURES = os.path.join(REPO, "assignment-b-workflow", "fixtures")
DEFAULT_DB = os.path.join(REPO, "solution", ".state", "switchboard.db")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="switchboard", description="Durable campaign dispatcher")
    sub = ap.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="dispatch until every lead is final")
    r.add_argument("--fixtures", default=FIXTURES)
    r.add_argument("--provider", default="http://127.0.0.1:8099")
    r.add_argument("--db", default=DEFAULT_DB)
    r.add_argument("--out", default=os.path.join(REPO, "final_state.csv"))
    r.add_argument("--host", default="127.0.0.1")
    r.add_argument("--port", type=int, default=8200)
    r.add_argument("--no-verify", action="store_true", help="skip the /admin cross-check at the end")
    r.add_argument("--linger", action="store_true", help="keep the dashboard up after finishing")
    r.add_argument("--quiet", action="store_true")

    a = sub.add_parser("audit", help="re-prove R1-R9 from our own ledger")
    a.add_argument("--fixtures", default=FIXTURES)
    a.add_argument("--db", default=DEFAULT_DB)

    v = sub.add_parser("verify", help="cross-check our state with the provider")
    v.add_argument("--db", default=DEFAULT_DB)
    v.add_argument("--provider", default="http://127.0.0.1:8099")

    j = sub.add_parser("journal", help="print the journal")
    j.add_argument("--db", default=DEFAULT_DB)
    j.add_argument("-n", type=int, default=40)
    j.add_argument("--lead", help="campaign_id/lead_id: the full life story of one lead")

    c = sub.add_parser("chaos", help="kill -9 torture test against a fresh mock provider")
    c.add_argument("--seed", type=int, default=7, help="mock provider outcome seed")
    c.add_argument("--speed", type=float, default=600.0)
    c.add_argument("--failure-scale", type=float, default=1.0)
    c.add_argument("--kills", type=int, default=5, help="external hard kills to deliver")
    c.add_argument("--min-gap", type=float, default=3.0)
    c.add_argument("--max-gap", type=float, default=15.0)
    c.add_argument("--crashpoints", type=float, default=0.0,
                   help="also arm every named crash point with this probability per visit (e.g. 0.02)")
    c.add_argument("--provider-port", type=int, default=8099)
    c.add_argument("--port", type=int, default=8200)
    c.add_argument("--rng-seed", type=int)

    args = ap.parse_args(argv)

    if args.cmd == "run":
        from .runner import RunOptions, main as run_main
        return run_main(RunOptions(args.fixtures, args.provider, args.db, args.out, args.host, args.port,
                                   verify=not args.no_verify, linger=args.linger, quiet=args.quiet))

    if args.cmd == "audit":
        from .audit import audit
        from .store import Store
        res = audit(config_mod.load(args.fixtures), Store(args.db))
        print(res.render())
        return 0 if res.ok else 1

    if args.cmd == "verify":
        from .provider import HttpProvider
        from .runner import verify
        from .store import Store

        async def go():
            p = HttpProvider(args.provider)
            try:
                return await verify(p, Store(args.db))
            finally:
                await p.aclose()
        return asyncio.run(go())

    if args.cmd == "journal":
        from .store import Store, dt
        st = Store(args.db)
        if args.lead:
            cid, _, lid = args.lead.partition("/")
            rows = st.db.execute("SELECT * FROM journal WHERE campaign_id=? AND lead_id=? ORDER BY seq",
                                 (cid, lid)).fetchall()
        else:
            rows = list(reversed(st.recent_journal(args.n)))
        for r in rows:
            d = json.loads(r["detail"]) if r["detail"] else {}
            who = f"{r['campaign_id']}/{r['lead_id']}" if r["lead_id"] else ""
            print(f"{r['seq']:>6}  {dt(r['sim_at']).astimezone(IST):%a %H:%M:%S}  pid {r['pid']:<6} "
                  f"{r['kind']:<22} {who:<26} {' '.join(f'{k}={v}' for k, v in d.items())}")
        return 0

    if args.cmd == "chaos":
        from .chaos import ChaosOptions, run_chaos
        return run_chaos(ChaosOptions(args.seed, args.speed, args.failure_scale, args.kills, args.min_gap,
                                      args.max_gap, args.crashpoints, args.provider_port, args.port, args.rng_seed))
    return 2


if __name__ == "__main__":
    sys.exit(main())
