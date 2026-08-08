#!/usr/bin/env python3
"""
Shared decode helpers for the Guildrun local data pipeline.

Used by guildrun_state_watcher.py (Step 2: per-run event/state parser) and, later,
the lookup-table builder. Deliberately has NO localization/name-resolution logic --
raw numeric-id data only. See GUILDRUN_DATA_NOTES.md for the full writeup of file
locations, formats, and open questions this is based on.
"""

import glob
import json
import os
import re
import shutil
import ssl
import sys
import urllib.error
import urllib.request

try:
    import msgpack
except ImportError:
    raise SystemExit("Missing dependency. Run: pip install msgpack --break-system-packages")

try:
    import certifi
except ImportError:
    raise SystemExit("Missing dependency. Run: pip install certifi --break-system-packages")

PARSER_VERSION = "0.1.2"

# Explicit CA bundle for every HTTPS request GRP makes, added 2026-08-08 after
# a real report: Python's ssl module falls back to an OS-provided trust store
# by default, and python.org's macOS installer doesn't wire one up at all
# (no post-install step run == no CA file on disk whatsoever, not just a
# stale one) -- every request fails with CERTIFICATE_VERIFY_FAILED /
# "unable to get local issuer certificate", not just guildrunlogs.app's.
# certifi ships its own maintained bundle instead of relying on the OS having
# one, which also matters for the packaged --onefile binary (guildrun_common
# has no access to python.org's installer or the user's browser's trust
# store either way). PyInstaller has a built-in hook for certifi that bundles
# its cacert.pem automatically -- no extra --add-data needed in build.yml.
_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())


def urlopen(req, timeout):
    """The one place every HTTPS request in GRP should go through, so the
    certifi-backed context above is never accidentally skipped."""
    return urllib.request.urlopen(req, timeout=timeout, context=_SSL_CONTEXT)


def _parse_version(v):
    """'v0.1.1' or '0.1.1' -> (0, 1, 1) for numeric comparison. Non-numeric
    segments (a stray suffix like '0.1.1-beta') become 0 rather than raising --
    this only needs to be good enough to detect "newer", not a full semver parser."""
    parts = []
    for p in v.lstrip("vV").split("."):
        try:
            parts.append(int(p))
        except ValueError:
            parts.append(0)
    return tuple(parts)


def check_for_update(api_url, timeout=3.0):
    """Best-effort update check, added 2026-08-08. grp (this tool's source repo)
    is private, so end-user machines can't hit GitHub's releases API without a
    credential embedded in the distributed binary -- instead this checks
    <api_url>/downloads/latest_version.json, a small static file on the same
    site GUILDRUN_API_URL already points to (kept in step with the release
    binaries in Client/public/downloads/ -- update both by hand when cutting a
    release, see PROJECT_SUMMARY.md).

    Notify-only by design (see PROJECT_SUMMARY.md's 2026-08-08 discussion):
    prints a message and returns True if a newer version is available, but
    never downloads or replaces anything itself. Never raises -- no api_url
    configured, the site being unreachable, or a malformed response are all
    just "no update available" to a courtesy check that shouldn't block
    startup."""
    if not api_url:
        return False
    try:
        url = f"{api_url.rstrip('/')}/downloads/latest_version.json"
        req = urllib.request.Request(url, headers={"user-agent": "guildrun-run-parser"})
        with urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        latest = str(data.get("version") or "")
        if latest and _parse_version(latest) > _parse_version(PARSER_VERSION):
            print(f"\nA new version of GRP is available: v{latest} (you have v{PARSER_VERSION}).")
            print(f"Download it from {api_url.rstrip('/')}/\n")
            return True
        return False
    except Exception:
        return False


DEFAULT_API_URL = "https://guildrunlogs.app"

CONFIG_TEMPLATE = f"""\
# Guildrun Run Parser (GRP) configuration.
#
# GUILDRUN_API_URL is pre-set to the official site below -- you shouldn't
# normally need to touch it (only change it if you're pointing GRP at a
# different server, e.g. for local development). GRP prompts you for
# GUILDRUN_API_KEY the first time it runs without one and saves your answer
# here, so you shouldn't need to touch that by hand either. If you ever
# need to change either later (new key, different site), edit them
# directly here and restart GRP.
#
# GRP looks for your Guildrun save data in its usual OS-specific location
# automatically -- you should NOT need to touch GUILDRUN_DATA_DIR below. If
# GRP prints "Could not find a Guildrun save folder" when you launch it,
# that guess was wrong for your system. Find your actual save folder
# yourself (it should contain a "Saves" folder with a "steam-<numbers>"
# folder inside it, and a Player.log) and paste that path here, e.g.
#   GUILDRUN_DATA_DIR=C:\\Users\\you\\AppData\\LocalLow\\Leyline\\Guildrun
# then restart GRP. Please also report this to the developer -- it means
# GRP's default guess for your OS needs fixing for everyone else too.
#
# Environment variables of the same name, if set, always override this file
# (and are never prompted for or written here).

GUILDRUN_API_URL={DEFAULT_API_URL}
GUILDRUN_API_KEY=
GUILDRUN_DATA_DIR=
"""


def _legacy_app_dir():
    """Where config.env/run_data lived before 2026-08-08 -- next to the
    executable when packaged, next to this script otherwise. Kept only so
    get_app_dir() can migrate an existing install's data over once."""
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def _migrate_legacy_app_dir(app_dir):
    """One-time move of an existing install's config.env/run_data from the
    old next-to-the-executable location into the new fixed one, so nobody's
    saved upload key or local run history gets orphaned by this change.
    No-ops if the new location already has a config.env (already migrated,
    or a genuinely fresh install writes its own) or there's nothing at the
    old location to migrate (also a fresh install)."""
    new_config = os.path.join(app_dir, "config.env")
    if os.path.isfile(new_config):
        return
    legacy_dir = _legacy_app_dir()
    legacy_config = os.path.join(legacy_dir, "config.env")
    if not os.path.isfile(legacy_config):
        return

    print(f"Moving existing config.env (and run_data/, if present) from {legacy_dir} to {app_dir}.\n")
    shutil.move(legacy_config, new_config)
    legacy_run_data = os.path.join(legacy_dir, "run_data")
    if os.path.isdir(legacy_run_data):
        shutil.move(legacy_run_data, os.path.join(app_dir, "run_data"))


def get_app_dir():
    """Fixed, OS-standard per-user directory for config.env and run_data/ --
    changed 2026-08-08 from "next to the executable." Two reasons: (1) a
    bare exe left loose in someone's Downloads folder would otherwise
    scatter config.env/run_data right there; (2) GRP now ships as a folder
    (see Watcher/.github/workflows/build.yml), and "updating" means
    extracting a new one, which would orphan the old config.env/run_data if
    they lived inside the old folder instead of somewhere stable.
    Migrates an existing pre-2026-08-08 install's data automatically --
    see _migrate_legacy_app_dir."""
    if sys.platform == "win32":
        base = os.environ.get("APPDATA") or os.path.expanduser("~")
    elif sys.platform == "darwin":
        base = os.path.expanduser("~/Library/Application Support")
    else:
        base = os.environ.get("XDG_CONFIG_HOME") or os.path.expanduser("~/.config")
    app_dir = os.path.join(base, "GRP")

    os.makedirs(app_dir, exist_ok=True)
    _migrate_legacy_app_dir(app_dir)
    return app_dir


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
    A missing file just means an empty config (ensure_config_template
    creates one before this is ever called for real, so this mainly matters
    for callers that haven't done that yet)."""
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


def get_api_url(config):
    """Resolves GUILDRUN_API_URL: environment variable, then config.env,
    then DEFAULT_API_URL. There's no interactive prompt for this (unlike
    GUILDRUN_API_KEY) since guildrunlogs.app is real and hosted now -- there's
    one official site, not a per-player value to ask for. Still overridable
    via env var or config.env, e.g. for local development against
    http://localhost:3000."""
    return get_setting("GUILDRUN_API_URL", config) or DEFAULT_API_URL


def _write_config_values(updates):
    """Rewrites specific KEY=value lines in config.env in place, leaving
    everything else (comments, GUILDRUN_DATA_DIR, formatting) untouched --
    doesn't just regenerate the whole file from CONFIG_TEMPLATE, since the
    user may have already hand-edited it (e.g. set GUILDRUN_DATA_DIR)."""
    if not os.path.isfile(CONFIG_PATH):
        with open(CONFIG_PATH, "w") as f:
            f.write(CONFIG_TEMPLATE)

    with open(CONFIG_PATH) as f:
        lines = f.readlines()

    remaining = dict(updates)
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key = stripped.split("=", 1)[0].strip()
        if key in remaining:
            lines[i] = f"{key}={remaining.pop(key)}\n"

    # Shouldn't normally happen (CONFIG_TEMPLATE always has both keys
    # already), but don't silently drop an update if the file was edited
    # into some other shape.
    for key, value in remaining.items():
        lines.append(f"{key}={value}\n")

    with open(CONFIG_PATH, "w") as f:
        f.writelines(lines)


def ensure_credentials(config):
    """GRP requires GUILDRUN_API_KEY to run at all (added 2026-08-08 --
    there's no more local-only mode). GUILDRUN_API_URL is no longer part of
    this prompt (removed 2026-08-08 now that guildrunlogs.app is real and
    hosted -- see get_api_url) -- only the per-player key is ever missing
    in a way that needs asking. If the key is missing from both the
    environment and config.env, prompts for it in the terminal and saves
    the answer to config.env so it isn't asked again. An env-var-sourced
    key is never prompted for or written back (it already wins over the
    file per get_setting, and overwriting the file with an env-sourced
    value would be surprising if the env var were later unset). Returns
    the config dict with the key present."""
    key = get_setting("GUILDRUN_API_KEY", config)
    if key:
        return config

    print("GRP needs your upload key to run -- it no longer runs in a local-only mode.")
    while not key:
        key = input("  GUILDRUN_API_KEY (generate one from your profile page on the site): ").strip()
    config["GUILDRUN_API_KEY"] = key

    _write_config_values({"GUILDRUN_API_KEY": key})
    print(f"Saved to {CONFIG_PATH} -- edit that file any time to change this.\n")
    return config


# Confirmed on macOS (2026-08-07/08, direct inspection of a real install):
# ~/Library/Application Support/Leyline/Guildrun/... -- exactly Unity's
# documented Application.persistentDataPath convention for macOS
# (~/Library/Application Support/<CompanyName>/<ProductName>).
#
# The Windows paths below are NOT independently verified against a real
# Windows install -- there's no Windows machine in this dev loop to test
# against, and a web search turned up no public documentation of Guildrun's
# specific save location. They're inferred from Unity's own documented,
# consistently-enforced cross-platform convention (persistentDataPath ->
# %USERPROFILE%\AppData\LocalLow\<CompanyName>\<ProductName> on Windows;
# Player.log lives in that same folder on Windows, unlike macOS's separate
# ~/Library/Logs/ location) -- high confidence in the general Unity
# convention, but this specific game could still deviate. **Needs real
# verification on an actual Windows machine before trusting the Windows
# build finds anything.**
#
# GUILDRUN_DATA_DIR (config.env or env var) overrides the guess entirely --
# added 2026-08-08 as a safety net for exactly that uncertainty, so a wrong
# guess is a one-line config edit for the affected player, not a rebuild.
# Assumes Saves/ and Player.log share that one root, true on Windows
# (LocalLow) and true if this guess is right -- not applicable on macOS,
# which doesn't need it since that path is already confirmed.
_config = load_config()
_data_dir_override = get_setting("GUILDRUN_DATA_DIR", _config)

if _data_dir_override:
    PROFILE_GLOB = os.path.join(_data_dir_override, "Saves", "steam-*", "Profile")
    RUN_GLOB = os.path.join(_data_dir_override, "Saves", "steam-*", "Run")
    PLAYER_LOG_PATH = os.path.join(_data_dir_override, "Player.log")
elif sys.platform == "win32":
    _APPDATA_LOCALLOW = os.path.join(os.environ.get("USERPROFILE", "~"), "AppData", "LocalLow")
    PROFILE_GLOB = os.path.join(_APPDATA_LOCALLOW, "Leyline", "Guildrun", "Saves", "steam-*", "Profile")
    RUN_GLOB = os.path.join(_APPDATA_LOCALLOW, "Leyline", "Guildrun", "Saves", "steam-*", "Run")
    PLAYER_LOG_PATH = os.path.join(_APPDATA_LOCALLOW, "Leyline", "Guildrun", "Player.log")
else:
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


def diagnose_data_paths():
    """One-time, human-readable check of whether the platform-specific paths
    above actually point at something -- added 2026-08-08 because those paths
    are guessed (not verified) on Windows, see the comment above PROFILE_GLOB.
    Distinguishes "no save folder at all" (probably a wrong-path bug -- please
    report the real path) from "found it, just no active run right now"
    (totally normal). Meant to be printed once at startup, not polled."""
    saves_root = os.path.dirname(os.path.dirname(RUN_GLOB))  # .../Guildrun/Saves
    if not os.path.isdir(saves_root):
        return (
            f"Could not find a Guildrun save folder at {saves_root}\n"
            f"  If Guildrun is installed and you've played at least once, this path is "
            f"probably wrong for your system. Find your real save folder (look for one "
            f"containing a 'Saves' folder with a 'steam-<numbers>' folder inside it, and "
            f"a Player.log) and set GUILDRUN_DATA_DIR to it in {CONFIG_PATH}, then "
            f"restart. Please also report this to the developer -- it means the default "
            f"guess needs fixing for everyone else on your OS too."
        )
    steam_dirs = glob.glob(os.path.join(saves_root, "steam-*"))
    if not steam_dirs:
        return f"Found {saves_root}, but no steam-<id> folder inside it yet -- try launching Guildrun at least once first."
    return f"Found Guildrun save data at {steam_dirs[0]}"


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
