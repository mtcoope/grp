"""
Guildrun Step 3 uploader.

Required, as of 2026-08-08 -- GRP no longer runs without GUILDRUN_API_URL
and GUILDRUN_API_KEY set. GUILDRUN_API_URL defaults to the real hosted site
(guildrunlogs.app, see guildrun_common.DEFAULT_API_URL/get_api_url) and
normally never needs setting -- override via environment variable or
config.env only to point at something else (e.g. http://localhost:3000
during development). GUILDRUN_API_KEY is a personal, per-player upload key
generated from the player's own profile page on the site (matches an
upload_api_key_hash row in Application/Server, see Server/.env.example)
and has no default; if it's missing at startup,
guildrun_state_watcher.main() prompts for it interactively and saves the
answer to config.env (see guildrun_common.ensure_credentials) rather than
requiring a terminal-less user to hand-edit a file. Environment variables
win over config.env for both.

Talks to the two write endpoints in Application/Server/src/app.js:
  POST /api/runs                 -- announce/re-announce run metadata. Called
                                     every sync, not just once, because it's
                                     idempotent server-side (ON CONFLICT DO
                                     UPDATE) -- self-heals if the first
                                     announce failed because the server was
                                     down when the run started.
  POST /api/runs/:runId/events   -- batch-upload events since last sync, plus
                                     the run's current status/summary/
                                     latest_raw_state (those are a snapshot
                                     that gets overwritten wholesale, not a
                                     log -- see Server/src/schema.sql).

Uses only the standard library (urllib) -- no new pip dependency for
something every user now needs.
"""
import json
import urllib.error
import urllib.request
from datetime import datetime, timezone

import guildrun_common as gc

_config = gc.load_config()
API_URL = gc.get_api_url(_config).rstrip("/")
API_KEY = gc.get_setting("GUILDRUN_API_KEY", _config)
TIMEOUT = 5.0


def enabled():
    return bool(API_URL)


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _post(path, body):
    data = json.dumps(body, default=str).encode("utf-8")
    req = urllib.request.Request(
        f"{API_URL}{path}",
        data=data,
        method="POST",
        headers={
            "content-type": "application/json",
            "authorization": f"Bearer {API_KEY}",
        },
    )
    with gc.urlopen(req, timeout=TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


def announce_run(doc):
    """POST /api/runs with run metadata. Safe to call repeatedly."""
    return _post("/api/runs", {
        "run_id": doc["run_id"],
        "steam_user_id": doc["steam_user_id"],
        "schema_version": doc["schema_version"],
        "parser_version": doc["parser_version"],
        "game_version": doc["game_version"],
        "difficulty_index": doc["difficulty_index"],
        "is_challenge_mode": doc["is_challenge_mode"],
        "run_seed": doc["run_seed"],
        "started_at": doc["started_at"],
    })


def sync_events(doc):
    """Upload events since doc['sync']['sent_event_count'], plus the current
    status/summary/latest_raw_state/ended_at snapshot. Mutates doc['sync'] in
    place on success. Returns True if anything was sent (caller should
    persist doc to disk in that case), False if there was nothing new."""
    sent = doc["sync"]["sent_event_count"]
    new_events = doc["events"][sent:]
    if not new_events:
        return False

    _post(f"/api/runs/{doc['run_id']}/events", {
        "events": new_events,
        "status": doc["status"],
        "summary": doc["summary"],
        "latest_raw_state": doc["latest_raw_state"],
        "ended_at": doc["ended_at"],
    })
    doc["sync"]["sent_event_count"] = sent + len(new_events)
    doc["sync"]["last_synced_at"] = _now_iso()
    return True
