"""
Guildrun Step 3 uploader.

Optional, off by default. Enabled by setting GUILDRUN_API_URL in the
environment (e.g. http://localhost:3000); GUILDRUN_API_KEY must also be set in
that case (matches Application/Server/.env's UPLOAD_API_KEY -- a placeholder
shared secret, not per-player auth -- see Server/.env.example).

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
something that's off unless explicitly configured.
"""
import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone

API_URL = os.environ.get("GUILDRUN_API_URL", "").rstrip("/")
API_KEY = os.environ.get("GUILDRUN_API_KEY", "")
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
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
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
