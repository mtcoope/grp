#!/usr/bin/env python3
"""
Shared decode helpers for the Guildrun local data pipeline.

Used by guildrun_state_watcher.py (Step 2: per-run event/state parser) and, later,
the lookup-table builder. Deliberately has NO localization/name-resolution logic --
raw numeric-id data only. See GUILDRUN_DATA_NOTES.md for the full writeup of file
locations, formats, and open questions this is based on.
"""

import glob
import os
import re

try:
    import msgpack
except ImportError:
    raise SystemExit("Missing dependency. Run: pip install msgpack --break-system-packages")

PARSER_VERSION = "0.1.0"

PROFILE_GLOB = os.path.expanduser(
    "~/Library/Application Support/Leyline/Guildrun/Saves/steam-*/Profile"
)
RUN_GLOB = os.path.expanduser(
    "~/Library/Application Support/Leyline/Guildrun/Saves/steam-*/Run"
)
PLAYER_LOG_PATH = os.path.expanduser(
    "~/Library/Logs/Leyline/Guildrun/Player.log"
)

STEAM_ID_RE = re.compile(r"steam-(\d+)")


def find_run_path():
    matches = glob.glob(RUN_GLOB)
    return matches[0] if matches else None


def find_profile_path():
    matches = glob.glob(PROFILE_GLOB)
    return matches[0] if matches else None


def extract_steam_id(path):
    """Pull the SteamID64 out of a .../steam-<id>/... save path."""
    if not path:
        return None
    m = STEAM_ID_RE.search(path)
    return m.group(1) if m else None


def load_msgpack(path):
    with open(path, "rb") as f:
        data = f.read()
    return msgpack.unpackb(data, raw=False, strict_map_key=False)


def load_run_raw(path):
    """
    Decode a Run save file. Run files wrap the real data in a second layer of
    MessagePack inside the 'Payload' field of the outer envelope -- see
    GUILDRUN_DATA_NOTES.md section 2. Returns the outer dict with 'Payload'
    replaced by its fully-decoded inner dict (RunSessionDto, PlayerDataDto,
    GameRegistryDto, DifficultyDto, ChallengeDto, MasteryDto, EffectsDto,
    EventDto, ShopDto, CrossroadsDto).
    """
    outer = load_msgpack(path)
    inner = msgpack.unpackb(outer["Payload"], raw=False, strict_map_key=False)
    outer["Payload"] = inner
    return outer


def load_profile_raw(path):
    """Decode a Profile save file (single layer of MessagePack, no wrapping)."""
    return load_msgpack(path)


def get_game_version(player_log_path=None):
    """
    Parse the game's version/build/branch/commit out of Player.log. Printed once
    per launch as:

        Parsing versions file:
        0.5.3
        757
        releases/0.5.3
        09248caa

    Returns a dict with those 4 fields, or all-None values if not found (e.g. log
    missing, or the game hasn't been launched since a fresh Player.log rotation).
    Uses the LAST occurrence in the file defensively, in case of multiple launches
    logged in one file.
    """
    result = {"version": None, "build_number": None, "branch": None, "commit": None}
    path = player_log_path or PLAYER_LOG_PATH
    if not os.path.isfile(path):
        return result

    try:
        with open(path, "r", errors="replace") as f:
            lines = f.readlines()
    except OSError:
        return result

    marker = "Parsing versions file:"
    last_idx = None
    for i, line in enumerate(lines):
        if line.strip() == marker:
            last_idx = i
    if last_idx is None:
        return result

    values = []
    i = last_idx + 1
    while i < len(lines) and len(values) < 4:
        stripped = lines[i].strip()
        if stripped:
            values.append(stripped)
        i += 1

    keys = ["version", "build_number", "branch", "commit"]
    for k, v in zip(keys, values):
        result[k] = v
    return result
