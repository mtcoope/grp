#!/usr/bin/env python3
"""
Recompute run_data/runs/<run_id>.json's summary.battles_won/battles_fought/
battles_lost/lost_at_battle_index from the run's own stored `battles[]` (the
raw per-battle records, always correct) rather than trusting whatever the
summary already says. Needed because process_new_battles() used to compute
these from AllHeroesSurvived instead of Report.Result -- fixed 2026-08-07, but
runs captured before the fix have stale summaries baked in.

Also recomputes `status` via determine_end_status() for runs stuck at
"unknown" (the one value only the old, pre-fix logic could produce) or
currently "abandoned" (see 2026-08-08's fix: a run with any recorded battle
loss that ended via file-disappearance is now "defeated", not "abandoned" --
see determine_end_status()'s docstring for why). Both are safe to recompute
unconditionally: an "abandoned" run only ever moves to "defeated" under the
new rule (never to "victory", since RunVictoryState/is_player_defeated are
stored fields unchanged by recompute -- if they were falsy before, they still
are). Deliberately does NOT touch "victory"/"defeated": some of those (e.g. a
run whose RunVictoryState never flipped but got upgraded via the profile-diff
TotalRunsBeaten fix-up) are correct only because of information
determine_end_status() alone doesn't have -- recomputing those from scratch
would regress a correct "victory" back to "abandoned". Confirmed 2026-08-07
this "unknown" staleness was silently skewing the player-stats win-rate
endpoint: two runs (one abandoned, one a confirmed real defeat) were stuck at
"unknown" from before the fix, and "unknown" runs are excluded from the
win-rate denominator entirely rather than counted as a loss.

Re-run this any time a summary- or status-computation bug like these gets
fixed, not just once -- it's meant to be a reusable "recompute from raw data"
tool, not a one-off patch.

Also re-syncs the corrected summary to the Step 3 server if GUILDRUN_API_URL
is set, bypassing guildrun_uploader.sync_events's normal "nothing new to
send" guard (POSTs events: [] with the corrected summary -- the server always
applies the summary/status update regardless of whether any events came with
it, see Server/src/app.js).

Usage:
  python3 backfill_summaries.py                 # local files only
  GUILDRUN_API_URL=... GUILDRUN_API_KEY=... python3 backfill_summaries.py  # also re-sync to server
"""
import glob
import json
import os

import guildrun_uploader as uploader
from guildrun_state_watcher import determine_end_status

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RUNS_DIR = os.path.join(SCRIPT_DIR, "run_data", "runs")


def recompute_battle_summary(doc):
    s = doc["summary"]
    s["battles_fought"] = 0
    s["battles_won"] = 0
    s["battles_lost"] = 0
    s["lost_at_battle_index"] = None
    for i, battle in enumerate(doc.get("battles", [])):
        result = battle.get("Report", {}).get("Result")
        s["battles_fought"] += 1
        if result == 1:
            s["battles_won"] += 1
        else:
            s["battles_lost"] += 1
            s["lost_at_battle_index"] = i


def main():
    paths = sorted(glob.glob(os.path.join(RUNS_DIR, "*.json")))
    print(f"Found {len(paths)} run doc(s) in {RUNS_DIR}")

    for path in paths:
        with open(path) as f:
            doc = json.load(f)

        if doc["status"] == "in_progress":
            print(f"  {doc['run_id']}: skipped (in_progress -- live watcher already has the fix)")
            continue

        before = {k: doc["summary"].get(k) for k in
                  ("battles_fought", "battles_won", "battles_lost", "lost_at_battle_index")}
        recompute_battle_summary(doc)
        after = {k: doc["summary"].get(k) for k in
                  ("battles_fought", "battles_won", "battles_lost", "lost_at_battle_index")}

        status_before = doc["status"]
        if status_before in ("unknown", "abandoned"):
            doc["status"] = determine_end_status(doc)

        run_id = doc["run_id"]
        changed = before != after or status_before != doc["status"]
        if not changed:
            print(f"  {run_id}: unchanged ({after}, status={doc['status']})")
        else:
            print(f"  {run_id}: {before} -> {after}, status {status_before} -> {doc['status']}")
            tmp = path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(doc, f, indent=2, default=str)
            os.replace(tmp, path)

        if uploader.enabled():
            try:
                uploader.announce_run(doc)
                uploader._post(f"/api/runs/{run_id}/events", {
                    "events": [],
                    "status": doc["status"],
                    "summary": doc["summary"],
                    "latest_raw_state": doc["latest_raw_state"],
                    "ended_at": doc["ended_at"],
                })
                print(f"    -> re-synced to {uploader.API_URL}")
            except Exception as e:
                print(f"    -> sync to {uploader.API_URL} failed: {e}")


if __name__ == "__main__":
    main()
