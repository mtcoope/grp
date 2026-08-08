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
import sys

try:
    import msgpack
except ImportError:
    raise SystemExit("Missing dependency. Run: pip install msgpack --break-system-packages")

PARSER_VERSION = "0.1.0"

CONFIG_TEMPLATE = """\
# Guildrun Run Parser (GRP) configuration.
#
# Fill in GUILDRUN_API_URL and GUILDRUN_API_KEY to upload your runs to the
# web site, then restart GRP. Leave GUILDRUN_API_URL blank to run locally
# only (no uploading) -- GRP still tracks your runs on this machine either
# way, in the run_data/ folder next to this file.
#
# Environment variables of the same name, if set, always override this file.

GUILDRUN_API_URL=
GUILDRUN_API_KEY=
"""


def get_app_dir():
    """Directory to read config.env / write run_data from -- next to the
    executable when packaged (PyInstaller sets sys.frozen), next to this
    script otherwise. Not just os.getcwd(), so a packaged .exe behaves the
    same double-clicked from anywhere as it does run from a terminal."""
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


CONFIG_PATH = os.path.join(get_app_dir(), "config.env")


def ensure_config_template():
    """Create config.env with placeholder values if it doesn't exist yet.
    Returns True if it just created one (caller should tell the user to fill
    it in), False if one already existed."""
    if os.path.isfile(CONFIG_PATH):
        return False
    with open(CONFIG_PATH, "w") as f:
        f.write(CONFIG_TEMPLATE)
    return True


def load_config():
    """Parse config.env (simple KEY=value lines, '#' comments) into a dict.
    Missing file just means an empty config -- not an error, since running
    fully local with no uploading is a valid setup."""
    config = {}
    if not os.path.isfile(CONFIG_PATH):
        return config
    with open(CONFIG_PATH) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            config[key.strip()] = value.strip()
    return config


def get_setting(key, config):
    """Environment variable wins if set; otherwise falls back to config.env."""
    return os.environ.get(key) or config.get(key, "")

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
