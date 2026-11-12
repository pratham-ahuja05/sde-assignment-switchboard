"""`chaos`: reproduce the graders' test in one command, then prove the result.

  1. start the mock provider (any seed, speed, failure scale)
  2. start the dispatcher; hard-kill it at random moments (TerminateProcess on Windows, SIGKILL
     elsewhere), and optionally also arm the named crash points from crash.py so some deaths
     land exactly between "decided to dial" and "recorded the answer"
  3. restart it after every death until it exits on its own, having finished
  4. run the self-audit and the provider cross-check, and print a scorecard

Everything goes to solution/.runs/<timestamp>/: logs, database, final_state.csv, scorecard.
"""
from __future__ import annotations

import json
import os
import random
import subprocess
import sys
import time
from dataclasses import dataclass

import httpx

from . import config as config_mod
from .audit import audit
from .report import diff_against_provider, final_rows
from .store import Store

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
WORKFLOW = os.path.join(REPO, "assignment-b-workflow")


@dataclass
class ChaosOptions:
    seed: int = 7
    speed: float = 600.0
    failure_scale: float = 1.0
    kills: int = 5
    min_gap: float = 3.0           # real seconds between kills
    max_gap: float = 15.0
    crashpoints: float = 0.0       # probability per crash point visit (0 = off)
    provider_port: int = 8099
    port: int = 8200
    rng_seed: int | None = None


def _say(msg: str):
    print(f"[chaos] {msg}", flush=True)


def _wait_http(url: str, timeout: float = 30.0):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if httpx.get(url, timeout=1.0).status_code < 500:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.2)
    raise SystemExit(f"{url} did not come up")


def fast_ssl_env(rundir: str) -> dict:
    """Windows dev-machine workaround, environment only (the mock's code is untouched).

    The mock builds a new httpx.AsyncClient for every webhook. httpx loads certifi's CA bundle on
    each construction, which takes ~450 ms on some Windows machines (measured; ~5 ms on Linux) and
    blocks the mock's event loop, freezing its simulated clock by ~4.5 simulated minutes per
    webhook. Webhooks are plain http, so a one-certificate trust file changes nothing but speed.
    """
    if os.name != "nt" or os.environ.get("SSL_CERT_FILE"):
        return {}
    import re

    import certifi
    pem = open(certifi.where(), encoding="ascii").read()
    first = re.search(r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----\n", pem, re.S).group(0)
    path = os.path.join(rundir, "one-ca.pem")
    with open(path, "w", encoding="ascii") as f:
        f.write(first)
    return {"SSL_CERT_FILE": path}


def run_chaos(o: ChaosOptions) -> int:
    rng = random.Random(o.rng_seed)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    rundir = os.path.join(REPO, "solution", ".runs", f"chaos-{stamp}-seed{o.seed}")
    os.makedirs(rundir, exist_ok=True)
    db, out = os.path.join(rundir, "switchboard.db"), os.path.join(rundir, "final_state.csv")
    base = f"http://127.0.0.1:{o.provider_port}"

    _say(f"run dir {rundir}")
    mock_log = open(os.path.join(rundir, "mock.log"), "w")
    extra = fast_ssl_env(rundir)
    if extra:
        _say("windows: giving the mock a 1-cert SSL_CERT_FILE (its per-webhook httpx client otherwise "
             "stalls its clock ~450 ms each; see fast_ssl_env)")
    mock = subprocess.Popen([sys.executable, "-m", "mock_telephony", "--seed", str(o.seed), "--speed", str(o.speed),
                             "--failure-scale", str(o.failure_scale), "--port", str(o.provider_port)],
                            cwd=WORKFLOW, stdout=mock_log, stderr=subprocess.STDOUT, env={**os.environ, **extra})
    lives: list[dict] = []
    try:
        _wait_http(base + "/v1/clock")
        _say(f"mock provider up: seed={o.seed} speed={o.speed:g}x failure_scale={o.failure_scale}")
        log = open(os.path.join(rundir, "switchboard.log"), "w")
        kills_left, life = o.kills, 0
        while True:
            life += 1
            env = dict(os.environ)
            if o.crashpoints > 0:
                from .crash import POINTS
                env["SWITCHBOARD_CRASH"] = ",".join(f"{p}:{o.crashpoints}" for p in POINTS)
                env["SWITCHBOARD_CRASH_SEED"] = str(rng.randrange(1 << 30))
            log.write(f"\n===== life {life} =====\n")
            log.flush()
            child = subprocess.Popen([sys.executable, "-m", "solution.switchboard", "run", "--db", db, "--out", out,
                                      "--provider", base, "--port", str(o.port), "--no-verify"],
                                     cwd=REPO, env=env, stdout=log, stderr=subprocess.STDOUT)
            started = time.time()
            deadline = started + rng.uniform(o.min_gap, o.max_gap) if kills_left > 0 else None
            while child.poll() is None:
                if deadline and time.time() >= deadline:
                    sim = _sim_now(base)
                    child.kill()                       # TerminateProcess / SIGKILL: no cleanup runs
                    child.wait()
                    kills_left -= 1
                    lives.append({"life": life, "ended": "kill -9", "sim": sim, "after_s": round(time.time() - started, 1)})
                    _say(f"life {life}: KILLED (hard) at sim {sim} after {time.time() - started:.1f}s"
                         f" - {kills_left} kills left")
                    break
                time.sleep(0.05)
            else:
                rc = child.returncode
                if rc == 0:
                    lives.append({"life": life, "ended": "finished", "sim": _sim_now(base)})
                    _say(f"life {life}: finished cleanly")
                    break
                from .crash import EXIT_CODE
                kind = "crash point" if rc == EXIT_CODE else f"exit {rc}"
                lives.append({"life": life, "ended": kind, "sim": _sim_now(base),
                              "after_s": round(time.time() - started, 1)})
                _say(f"life {life}: died ({kind}) at sim {_sim_now(base)}")
                if rc != EXIT_CODE and life > 3 and all(x["ended"] == kind for x in lives[-3:]):
                    _say("same unexpected failure three times in a row; see switchboard.log")
                    return 2
        return _score(o, rundir, db, base, lives)
    finally:
        mock.terminate()
        try:
            mock.wait(5)
        except subprocess.TimeoutExpired:
            mock.kill()


def _sim_now(base: str) -> str:
    try:
        return httpx.get(base + "/v1/clock", timeout=2).json()["sim_now"][5:19].replace("T", " ")
    except httpx.HTTPError:
        return "?"


def _score(o: ChaosOptions, rundir: str, db: str, base: str, lives: list[dict]) -> int:
    cfg = config_mod.load(os.path.join(WORKFLOW, "fixtures"))
    store = Store(db)
    a = audit(cfg, store)
    report = httpx.get(base + "/admin/report", timeout=10).json()
    expected = httpx.get(base + "/admin/leads", timeout=10).json()
    diffs = diff_against_provider(final_rows(store), expected)
    ok = a.ok and report["violation_count"] == 0 and not diffs

    deaths = [x for x in lives if x["ended"] != "finished"]
    lines = [
        "", "=" * 72, "  SWITCHBOARD CHAOS SCORECARD", "=" * 72,
        f"  provider seed {o.seed}, speed {o.speed:g}x, failure scale {o.failure_scale}",
        f"  process lives {len(lives)}  (hard kills {sum(1 for x in deaths if x['ended'] == 'kill -9')},"
        f" injected crash points {sum(1 for x in deaths if x['ended'] == 'crash point')})",
        "", "  Provider's verdict (/admin/report)",
        f"    violation_count           {report['violation_count']}  {report['violations_by_type'] or ''}",
        f"    max_concurrency_seen      {report['max_concurrency_seen']}",
        f"    callback_lateness_minutes {report['callback_lateness_minutes']}",
        f"    webhooks                  {report['webhooks']}",
        f"    final_state.csv vs /admin/leads: {'MATCH (' + str(len(expected)) + ' leads)' if not diffs else str(len(diffs)) + ' DIFFS'}",
    ]
    lines += [f"      {d}" for d in diffs[:10]]
    lines.append(a.render())
    lines += ["", "  RESULT: " + ("PASS - 0 violations, output matches provider, self-audit clean" if ok else "FAIL"),
              "=" * 72]
    text = "\n".join(lines)
    print(text, flush=True)
    with open(os.path.join(rundir, "scorecard.txt"), "w", encoding="utf-8") as f:
        f.write(text)
    with open(os.path.join(rundir, "scorecard.json"), "w", encoding="utf-8") as f:
        json.dump({"ok": ok, "lives": lives, "provider_report": {k: v for k, v in report.items() if k != "violations"},
                   "diffs": diffs, "audit": a.findings, "stats": a.stats}, f, indent=2, default=str)
    return 0 if ok else 1
