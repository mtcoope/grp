#!/usr/bin/env python3
"""
Guildrun local run/event watcher (Step 2).

Polls the live Run save file every second (configurable), and for the currently
active run maintains a single JSON file on disk containing:

  - metadata: parser_version, game_version, run_id, steam_user_id, status
  - an append-only event log (run_started / delta / battle_completed / snapshot / run_ended)
  - a rolling summary: per-hero damage/healing totals across the run, battle
    win/loss record, floor/endless progress, crossroads choices seen, challenge
    mode flag, final hero/relic state
  - the full latest raw decoded Run payload (numeric ids only -- no name
    translation; see guildrun_common.py / GUILDRUN_DATA_NOTES.md)
  - a `sync` block (`sent_event_count`) reserved for a future uploader to track
    what's already been pushed to a server, so re-runs only send new events

Also lightly tracks the lifetime Profile file (separately, in profile_state.json /
profile_history.jsonl) since a Profile change (TotalRunsBeaten incrementing) is
useful corroborating evidence for whether a just-ended run was a win.

Optionally uploads to the Step 3 REST server (Application/Server) via
guildrun_uploader.py -- off by default, enabled by setting GUILDRUN_API_URL
(and GUILDRUN_API_KEY) in the environment. When enabled, syncs at most every
--sync-every seconds while a run is active, plus once immediately whenever a
run ends (its final status/ended_at) or gets retroactively corrected by the
profile-diff victory fix-up.

Output layout, all under a `run_data/` folder next to this script:
  run_data/runs/<run_id>.json
  run_data/profile_state.json        (latest raw Profile snapshot, overwritten)
  run_data/profile_history.jsonl     (append-only log of scalar Profile field changes)

Once per launch, cleans up old local run files beyond --keep-runs (default
50, oldest first) -- but only ones already fully synced to the server, so an
unsynced run's local copy (its only copy) is never deleted regardless of age.

Usage:
  pip install msgpack --break-system-packages
  python3 guildrun_state_watcher.py                     # poll every 1s (default)
  python3 guildrun_state_watcher.py --interval 1
  python3 guildrun_state_watcher.py --snapshot-every 30  # periodic full-state checkpoint cadence, seconds
  python3 guildrun_state_watcher.py --keep-runs 100      # local run history to retain (0 disables cleanup)

  # To also upload to the Step 3 server:
  export GUILDRUN_API_URL=http://localhost:3000
  export GUILDRUN_API_KEY=dev-local-key    # must match Server/.env's UPLOAD_API_KEY
  python3 guildrun_state_watcher.py --sync-every 10
"""

import argparse
import copy
import glob
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

import guildrun_common as gc
import guildrun_uploader as uploader

DATA_DIR = os.path.join(SCRIPT_DIR, "run_data")
RUNS_DIR = os.path.join(DATA_DIR, "runs")
PROFILE_STATE_PATH = os.path.join(DATA_DIR, "profile_state.json")
PROFILE_HISTORY_PATH = os.path.join(DATA_DIR, "profile_history.jsonl")

_MISSING = object()


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def file_hash(path):
    try:
        with open(path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()
    except OSError:
        return None


def atomic_write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=2, default=str)
    os.replace(tmp, path)


def append_jsonl(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(obj, default=str) + "\n")


# ---------- generic deep diff (used for 'delta' events) ----------

def deep_diff(old, new, path="", out=None):
    if out is None:
        out = []
    if isinstance(old, dict) and isinstance(new, dict):
        for k in set(old) | set(new):
            deep_diff(old.get(k, _MISSING), new.get(k, _MISSING),
                      f"{path}.{k}" if path else str(k), out)
    elif isinstance(old, list) and isinstance(new, list):
        if old != new:
            if len(old) == len(new):
                for i, (o, n) in enumerate(zip(old, new)):
                    deep_diff(o, n, f"{path}[{i}]", out)
            else:
                out.append({"path": path, "old": _clean(old), "new": _clean(new)})
    else:
        if old != new:
            out.append({
                "path": path,
                "old": None if old is _MISSING else _clean(old),
                "new": None if new is _MISSING else _clean(new),
            })
    return out


def _clean(v):
    """Make a value JSON-safe (msgpack can hand back bytes in edge cases)."""
    if isinstance(v, bytes):
        return v.hex()
    return v


# ---------- run document lifecycle ----------

def run_doc_path(run_id):
    return os.path.join(RUNS_DIR, f"{run_id}.json")


def load_run_doc(run_id):
    path = run_doc_path(run_id)
    if os.path.isfile(path):
        with open(path) as f:
            return json.load(f)
    return None


def new_run_doc(run_id, steam_id, game_version, outer_payload):
    payload = outer_payload["Payload"]
    run = payload.get("RunSessionDto", {})
    return {
        "schema_version": 1,
        "parser_version": gc.PARSER_VERSION,
        "game_version": game_version,
        "run_id": run_id,
        "steam_user_id": steam_id,
        "status": "in_progress",
        "started_at": now_iso(),
        "ended_at": None,
        "difficulty_index": outer_payload.get("DifficultyIndex"),
        "is_challenge_mode": outer_payload.get("IsChallengeModeEnabled"),
        "run_seed": run.get("RunSeed"),
        "summary": {
            "current_act": None,
            "current_floor": None,
            "current_total_floor": None,
            "current_endless_floor": None,
            "highest_endless_floor_reached": None,
            "run_victory_state": None,
            "is_player_defeated": None,
            "battles_fought": 0,
            "battles_won": 0,
            "battles_lost": 0,
            "lost_at_battle_index": None,
            "hero_totals": {},          # keyed by hero_sequential_id (string)
            "final_hero_states": [],
            "final_active_relics": [],
            "crossroads_choices": [],   # [{timestamp, act, floor, selected_path}]
        },
        "battles": [],
        "events": [],
        "latest_raw_state": None,
        "sync": {"sent_event_count": 0, "last_synced_at": None},
    }


def cleanup_old_runs(keep_count=50, dry_run=False):
    """Delete local run_data/runs/<id>.json files beyond the most recent
    `keep_count` (by started_at), added 2026-08-08 so local disk usage
    doesn't grow unbounded over months of play. Never deletes a run that
    isn't fully synced to the server (sent_event_count < total events) --
    that local file is the only copy of that run's data, regardless of how
    old it is. Also never touches "in_progress" runs. Runs with a missing
    started_at sort last (kept) rather than risk deleting on bad data.
    """
    paths = glob.glob(os.path.join(RUNS_DIR, "*.json"))
    docs = []
    for path in paths:
        try:
            with open(path) as f:
                docs.append((path, json.load(f)))
        except (OSError, json.JSONDecodeError):
            continue

    docs.sort(key=lambda pd: pd[1].get("started_at") or "", reverse=True)

    deleted, skipped_unsynced, skipped_in_progress = 0, 0, 0
    for path, doc in docs[keep_count:]:
        if doc.get("status") == "in_progress":
            skipped_in_progress += 1
            continue
        sync = doc.get("sync", {})
        if sync.get("sent_event_count", 0) != len(doc.get("events", [])):
            skipped_unsynced += 1
            continue
        deleted += 1
        if not dry_run:
            os.remove(path)

    return {
        "total": len(docs),
        "deleted": deleted,
        "skipped_unsynced": skipped_unsynced,
        "skipped_in_progress": skipped_in_progress,
    }


def append_event(doc, event_type, data):
    doc["events"].append({
        "seq": len(doc["events"]),
        "type": event_type,
        "timestamp": now_iso(),
        "data": data,
    })


def update_summary_scalars(doc, payload):
    run = payload.get("RunSessionDto", {})
    player = payload.get("PlayerDataDto", {})
    registry = payload.get("GameRegistryDto", {})
    crossroads = payload.get("CrossroadsDto", {})
    s = doc["summary"]

    s["current_act"] = run.get("CurrentAct")
    s["current_floor"] = run.get("CurrentFloor")
    s["current_total_floor"] = run.get("CurrentTotalFloor")
    endless = run.get("CurrentEndlessFloor")
    s["current_endless_floor"] = endless
    if endless is not None and endless >= 0:
        s["highest_endless_floor_reached"] = max(s["highest_endless_floor_reached"] or -1, endless)
    s["run_victory_state"] = run.get("RunVictoryState")
    s["is_player_defeated"] = player.get("IsPlayerDefeated")
    s["final_hero_states"] = list(registry.get("Heroes", {}).values())
    s["final_active_relics"] = list(player.get("ActiveRelics", {}).values())

    selected_path = crossroads.get("SelectedPath")
    last_choice = s["crossroads_choices"][-1] if s["crossroads_choices"] else None
    if selected_path is not None and (last_choice is None or last_choice.get("selected_path") != selected_path):
        s["crossroads_choices"].append({
            "timestamp": now_iso(),
            "act": s["current_act"],
            "floor": s["current_floor"],
            "selected_path": selected_path,
        })


def process_new_battles(doc, payload):
    run = payload.get("RunSessionDto", {})
    battles = run.get("BattleHistory", [])
    already = len(doc["battles"])
    new_battles = battles[already:]

    for battle in new_battles:
        doc["battles"].append(battle)
        append_event(doc, "battle_completed", {
            "battle_index": len(doc["battles"]) - 1,
            "battle": battle,
        })

        report = battle.get("Report", {})
        # Confirmed 2026-08-07: use Report.Result (1=won, 2=lost), not
        # AllHeroesSurvived -- that flag means "did anyone die," which can be
        # False in a battle you still won (see GUILDRUN_DATA_NOTES.md section
        # 8). Using it here undercounted real wins and overcounted losses --
        # e.g. one real run showed 4 "losses" this way, which didn't square
        # with the game's Emergency Rewind cap (max 3 survivable losses); the
        # real number, by Report.Result, was 1.
        won = report.get("Result") == 1
        doc["summary"]["battles_fought"] += 1
        if won:
            doc["summary"]["battles_won"] += 1
        else:
            doc["summary"]["battles_lost"] += 1
            doc["summary"]["lost_at_battle_index"] = len(doc["battles"]) - 1

        heroes_by_guid = {k: v.get("HeroSequentialId") for k, v in battle.get("Heroes", {}).items()}
        stats = battle.get("Stats", {})
        totals = doc["summary"]["hero_totals"]

        def add(field_name, out_key):
            for guid, value in stats.get(field_name, []):
                hero_id = heroes_by_guid.get(guid)
                if hero_id is None:
                    continue
                key = str(hero_id)
                totals.setdefault(key, {
                    "hero_sequential_id": hero_id,
                    "damage_dealt": 0, "damage_taken": 0, "damage_mitigated": 0,
                    "healing_done": 0, "kills": 0, "crits_landed": 0,
                    "battles_participated": 0,
                })
                totals[key][out_key] += value

        add("DamageDealtPerCharacter", "damage_dealt")
        add("DamageTakenPerCharacter", "damage_taken")
        add("DamageMitigatedPerCharacter", "damage_mitigated")
        add("HealedAppliedPerCharacter", "healing_done")
        add("KillingBlowsPerCharacter", "kills")
        add("CritsLandedPerCharacter", "crits_landed")

        for hero_id in set(heroes_by_guid.values()):
            if hero_id is None:
                continue
            key = str(hero_id)
            totals.setdefault(key, {
                "hero_sequential_id": hero_id,
                "damage_dealt": 0, "damage_taken": 0, "damage_mitigated": 0,
                "healing_done": 0, "kills": 0, "crits_landed": 0,
                "battles_participated": 0,
            })
            totals[key]["battles_participated"] += 1

    return len(new_battles)


def determine_end_status(doc):
    s = doc["summary"]
    if s.get("is_player_defeated"):
        return "defeated"
    if s.get("run_victory_state") not in (0, None):
        # Confirmed 2026-08-07: RunVictoryState=1 on a real victory (run cleared,
        # went on into endless mode). Other non-zero values still unconfirmed.
        return "victory"
    battles = doc.get("battles", [])
    if battles and battles[-1].get("Report", {}).get("Result") == 2:
        # NOTE: checking summary["lost_at_battle_index"] here would not be
        # equivalent, even though it's now Report.Result-based (fixed
        # 2026-08-07, see process_new_battles) -- it tracks the *most recent*
        # loss, which can be stale if the run recovered afterward. A real
        # captured victory had a genuine Report.Result=2 loss mid-run
        # (battle 7 of 22) followed by 15 more battles including wins, so a
        # single lost battle does not end the run -- only checking the *last*
        # battle's actual Result correctly distinguishes this from run 2's real
        # defeat, where the lost battle was the last one played before the file
        # disappeared.
        return "defeated"
    # Confirmed 2026-08-08 via a live, deliberately-reproduced test (lost a
    # battle, used the only Emergency Rewind, lost again -- see
    # GUILDRUN_DATA_NOTES.md section 6b's sibling investigation in
    # PROJECT_SUMMARY.md): the truly fatal battle (no rewind left) is never
    # written to the Run save file's BattleHistory at all -- a 0.1s-interval
    # diagnostic poller watched the file sit mid-battle for ~15s, then
    # disappear, with no intermediate state ever showing that battle's
    # result or IsPlayerDefeated=true. No polling interval can catch this;
    # the data never touches disk.
    #
    # Practical consequence: a run that recovers from a loss via Emergency
    # Rewind and then dies later looks *identical* to a genuine voluntary
    # quit by every signal above -- the last *recorded* battle is a win (or
    # there's no final loss recorded at all), because the real fatal battle
    # is missing. But a run with ANY recorded battle loss, ending via file
    # disappearance, essentially never happens from a real quit (you don't
    # rack up a real battle loss and then coincidentally quit right after) --
    # it's strong indirect evidence of an uncaptured defeat. A run with zero
    # recorded losses has no such evidence and stays "abandoned".
    if s.get("battles_lost", 0) > 0:
        return "defeated"
    # No RunVictoryState signal, the run didn't end on a lost battle, and no
    # loss was ever recorded. Confirmed 2026-08-07 against a real abandoned
    # run (2 battles won, file disappeared, TotalRunsBeaten did not bump)
    # that this combination -- by elimination -- means the player quit rather
    # than the run resolving. Can still be upgraded to "victory" after the
    # fact if TotalRunsBeaten bumps on the next profile poll (see
    # handle_profile_change's profile_confirmed_victory fix-up) -- useful
    # since Profile writes can lag the Run file disappearing.
    return "abandoned"


# ---------- profile tracking ----------

PROFILE_SCALAR_FIELDS = [
    "TotalStartedRuns", "TotalRunsBeaten", "HighestDifficultyBeaten",
    "CurrentXP", "CurrentLevel", "BonusTokens",
]


def handle_profile_change(path, game_version, last_profile_scalars, most_recent_ended_run_id,
                           on_run_corrected=None):
    profile = gc.load_profile_raw(path)
    progression = profile.get("Progression", {})
    current = {k: progression.get(k) for k in PROFILE_SCALAR_FIELDS}

    atomic_write_json(PROFILE_STATE_PATH, {
        "updated_at": now_iso(),
        "game_version": game_version,
        "profile": profile,
    })

    changed = {k: v for k, v in current.items() if last_profile_scalars.get(k) != v}
    if changed:
        append_jsonl(PROFILE_HISTORY_PATH, {
            "timestamp": now_iso(),
            "changed": changed,
            "current": current,
        })
        # If a just-ended run's status is still ambiguous and the win counter just
        # went up, that's a strong signal the run that just ended was a victory.
        if most_recent_ended_run_id and changed.get("TotalRunsBeaten"):
            doc = load_run_doc(most_recent_ended_run_id)
            if doc and doc.get("status") in ("unknown", "abandoned"):
                doc["status"] = "victory"
                append_event(doc, "profile_confirmed_victory", {"changed": changed})
                atomic_write_json(run_doc_path(most_recent_ended_run_id), doc)
                if on_run_corrected:
                    on_run_corrected(doc)

    return current


# ---------- watcher state machine (one poll_once() call = one iteration) ----------

class Watcher:
    def __init__(self, snapshot_every=30.0, sync_every=10.0):
        self.snapshot_every = snapshot_every
        self.sync_every = sync_every
        self.game_version = gc.get_game_version()
        self.current_run_id = None
        self.current_doc = None
        self.last_run_hash = None
        self.last_snapshot_time = 0
        self.last_sync_time = 0
        self.most_recent_ended_run_id = None
        self.last_profile_hash = None
        self.last_profile_scalars = {}
        self.log = []  # simple in-memory log of human-readable lines, for CLI printing / tests

    def _emit(self, line):
        self.log.append(line)
        print(line)

    def _sync(self, doc, force=False):
        """Upload doc to the Step 3 server, if enabled (GUILDRUN_API_URL set)
        and either forced or the --sync-every throttle has elapsed. Network
        errors are logged and swallowed -- never let a sync failure interrupt
        the local watcher, since guildrun_uploader is designed to be safely
        retried (idempotent both server-side and via sent_event_count)."""
        if not uploader.enabled():
            return
        if not force and time.time() - self.last_sync_time < self.sync_every:
            return
        self.last_sync_time = time.time()
        try:
            uploader.announce_run(doc)
            if uploader.sync_events(doc):
                atomic_write_json(run_doc_path(doc["run_id"]), doc)
        except Exception as e:
            self._emit(f"[{now_iso()}] sync to {uploader.API_URL} failed: {e}")

    def _end_current_run(self, note=None):
        self.current_doc["status"] = determine_end_status(self.current_doc)
        self.current_doc["ended_at"] = now_iso()
        data = {"status": self.current_doc["status"]}
        if note:
            data["note"] = note
        append_event(self.current_doc, "run_ended", data)
        atomic_write_json(run_doc_path(self.current_run_id), self.current_doc)
        self._emit(f"[{now_iso()}] run {self.current_run_id} ended -> status={self.current_doc['status']}")
        self._sync(self.current_doc, force=True)
        self.most_recent_ended_run_id = self.current_run_id
        self.current_run_id = None
        self.current_doc = None
        self.last_run_hash = None

    def poll_once(self):
        run_path = gc.find_run_path()
        run_hash = file_hash(run_path) if run_path else None

        if run_path is None and self.current_run_id is not None:
            self._end_current_run()

        elif run_path is not None and run_hash != self.last_run_hash:
            outer = gc.load_run_raw(run_path)
            payload = outer["Payload"]
            run_id = payload.get("RunSessionDto", {}).get("RunId")
            steam_id = gc.extract_steam_id(run_path)

            if run_id != self.current_run_id:
                if self.current_doc is not None:
                    self._end_current_run(note="transition missed -- new run appeared before this one's file disappeared")

                existing = load_run_doc(run_id)
                if existing and existing.get("status") == "in_progress":
                    self.current_doc = existing
                    self._emit(f"[{now_iso()}] resuming run {run_id}")
                else:
                    self.current_doc = new_run_doc(run_id, steam_id, self.game_version, outer)
                    append_event(self.current_doc, "run_started", {
                        "difficulty_index": self.current_doc["difficulty_index"],
                        "is_challenge_mode": self.current_doc["is_challenge_mode"],
                        "run_seed": self.current_doc["run_seed"],
                    })
                    self._emit(f"[{now_iso()}] run started: {run_id}")
                self.current_run_id = run_id
                self.last_snapshot_time = 0

            prev_state = self.current_doc.get("latest_raw_state")
            if prev_state is not None:
                prev_for_diff = copy.deepcopy(prev_state)
                prev_for_diff.get("RunSessionDto", {}).pop("BattleHistory", None)
                new_for_diff = copy.deepcopy(payload)
                new_for_diff.get("RunSessionDto", {}).pop("BattleHistory", None)
                changes = deep_diff(prev_for_diff, new_for_diff)
                if changes:
                    append_event(self.current_doc, "delta", {"changes": changes})

            n_new = process_new_battles(self.current_doc, payload)
            if n_new:
                self._emit(f"[{now_iso()}] run {self.current_run_id}: {n_new} new battle(s) recorded")

            update_summary_scalars(self.current_doc, payload)
            self.current_doc["latest_raw_state"] = payload

            if time.time() - self.last_snapshot_time >= self.snapshot_every:
                append_event(self.current_doc, "snapshot", {"state": payload})
                self.last_snapshot_time = time.time()

            atomic_write_json(run_doc_path(self.current_run_id), self.current_doc)
            self.last_run_hash = run_hash
            self._sync(self.current_doc)

        profile_path = gc.find_profile_path()
        if profile_path:
            p_hash = file_hash(profile_path)
            if p_hash != self.last_profile_hash:
                self.last_profile_scalars = handle_profile_change(
                    profile_path, self.game_version, self.last_profile_scalars, self.most_recent_ended_run_id,
                    on_run_corrected=lambda doc: self._sync(doc, force=True),
                )
                self.last_profile_hash = p_hash

    def shutdown(self):
        if self.current_doc is not None:
            atomic_write_json(run_doc_path(self.current_run_id), self.current_doc)
            self._sync(self.current_doc, force=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--interval", type=float, default=1.0, help="Poll interval in seconds (default: 1)")
    ap.add_argument("--snapshot-every", type=float, default=30.0,
                     help="Seconds between periodic full-state 'snapshot' checkpoint events while a run is active (default: 30)")
    ap.add_argument("--sync-every", type=float, default=10.0,
                     help="Minimum seconds between upload attempts to the Step 3 server while a run is "
                          "active (default: 10). Only takes effect if GUILDRUN_API_URL is set in the "
                          "environment -- uploading is off by default.")
    ap.add_argument("--keep-runs", type=int, default=50,
                     help="Local run history to retain in run_data/runs/ (default: 50), oldest first. "
                          "Only ever deletes runs already fully synced to the server -- an unsynced "
                          "run's local file is its only copy, so it's kept regardless of age. Pass 0 "
                          "to disable cleanup entirely.")
    args = ap.parse_args()

    os.makedirs(RUNS_DIR, exist_ok=True)

    if args.keep_runs > 0:
        result = cleanup_old_runs(keep_count=args.keep_runs)
        if result["deleted"]:
            print(f"Cleaned up {result['deleted']} old run(s) locally (kept {args.keep_runs} most recent, out of {result['total']}).")
        if result["skipped_unsynced"]:
            print(f"Kept {result['skipped_unsynced']} old run(s) that haven't fully synced yet -- will retry next launch.")

    watcher = Watcher(snapshot_every=args.snapshot_every, sync_every=args.sync_every)
    print(f"Game version: {watcher.game_version}")
    print(f"Parser version: {gc.PARSER_VERSION}")
    print(f"Polling every {args.interval}s. Data -> {DATA_DIR}")
    if uploader.enabled():
        print(f"Uploading to {uploader.API_URL} at most every {args.sync_every}s.")
    else:
        print("Uploading disabled (set GUILDRUN_API_URL to enable).")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            watcher.poll_once()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        watcher.shutdown()
        print("\nStopped.")


if __name__ == "__main__":
    main()
