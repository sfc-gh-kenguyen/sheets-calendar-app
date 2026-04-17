import hashlib
import json
import os
import re
import shutil
import subprocess
import time
from datetime import datetime, date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_calendar import calendar

from github_sync import push_file_to_github, delete_file_from_github

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
APP_DIR = Path(__file__).parent
CONFIG_PATH = APP_DIR / "config.json"
DATA_DIR = APP_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

def _git_short_hash() -> str:
    """Return the short git commit hash of the current checkout, or '' on failure."""
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=APP_DIR,
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return ""

GIT_HASH = _git_short_hash()

REQUIRED_FIELDS = ["title", "start"]
OPTIONAL_FIELDS = ["end", "title_prefix", "description", "location", "color"]
ALL_FIELDS = REQUIRED_FIELDS + OPTIONAL_FIELDS

FIELD_DESCRIPTIONS = {
    "title": "Event title (required)",
    "start": "Start date / datetime (required)",
    "end": "End date / datetime",
    "title_prefix": "Prefix column — prepended as [Value] before the title",
    "description": "Event description",
    "location": "Event location",
    "color": "Event colour (hex code or CSS colour name)",
}

_TIME_PATTERN = re.compile(
    r'(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM|am|pm)?', re.IGNORECASE
)

DEFAULT_COLORS = [
    "#3788d8",  # blue
    "#e5383b",  # red
    "#2d6a4f",  # green
    "#7209b7",  # purple
    "#e76f51",  # orange
    "#219ebc",  # teal
    "#f4a261",  # amber
    "#6d6875",  # grey-purple
]

SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".tsv"}

# Set to True when deploying to Streamlit Community Cloud (or any remote host).
# Hides local-only features: "Link to file on disk", "Watch a folder",
# refresh folder sync, and auto-refresh.
IS_CLOUD = (
    os.environ.get("STREAMLIT_CLOUD", "").lower() in ("1", "true", "yes")
    or os.environ.get("STREAMLIT_SHARING_MODE") is not None
    or str(Path.home()) == "/home/appuser"
    or Path("/mount/src").is_dir()
)


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_config() -> dict:
    """Load sheet configurations from JSON file (cached by mtime within a rerun)."""
    mtime = CONFIG_PATH.stat().st_mtime if CONFIG_PATH.exists() else 0
    return _load_config_cached(mtime)


@st.cache_data(show_spinner=False, ttl=60)
def _load_config_cached(_mtime: float) -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    return {"sheets": [], "watch_folders": []}


def save_config(config: dict):
    """Persist sheet configurations to JSON file (and sync to GitHub on cloud)."""
    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)
    _load_config_cached.clear()
    if IS_CLOUD:
        push_file_to_github("config.json", "Update config via app")


def _init_saved_views_state():
    """Ensure session-state mirror of saved views exists (seed from config)."""
    if "_saved_views" not in st.session_state:
        st.session_state["_saved_views"] = load_config().get("saved_views", [])


def get_saved_views() -> list[dict]:
    """Return the list of saved views (from session state, seeded from config)."""
    _init_saved_views_state()
    return list(st.session_state["_saved_views"])


def _persist_saved_views(views: list[dict]):
    """Write saved views to session state and try to persist to config.json."""
    st.session_state["_saved_views"] = views
    try:
        config = load_config()
        config["saved_views"] = views
        save_config(config)
    except OSError:
        # On Streamlit Cloud the repo filesystem may be read-only;
        # views will still work for the current session via session state.
        pass


def save_view(name: str, view_data: dict):
    """Add or update a saved view."""
    _init_saved_views_state()
    views = st.session_state["_saved_views"]
    views = [v for v in views if v["name"] != name]
    views.append({"name": name, **view_data})
    _persist_saved_views(views)


def delete_view(name: str):
    """Remove a saved view by name."""
    _init_saved_views_state()
    views = st.session_state["_saved_views"]
    views = [v for v in views if v["name"] != name]
    _persist_saved_views(views)


# ---------------------------------------------------------------------------
# File helpers
# ---------------------------------------------------------------------------

def save_uploaded_file(uploaded_file) -> Path:
    """Save an uploaded file to the data/ directory (and sync to GitHub on cloud)."""
    dest = DATA_DIR / uploaded_file.name
    counter = 1
    while dest.exists():
        stem = Path(uploaded_file.name).stem
        suffix = Path(uploaded_file.name).suffix
        dest = DATA_DIR / f"{stem}_{counter}{suffix}"
        counter += 1
    with open(dest, "wb") as f:
        f.write(uploaded_file.getbuffer())
    if IS_CLOUD:
        push_file_to_github(
            dest.relative_to(APP_DIR),
            f"Add data file: {dest.name}",
        )
    return dest


def read_file_to_df(file_path: str | Path, header_row: int = 1) -> pd.DataFrame:
    """Read a CSV, TSV, or Excel file into a DataFrame (all strings).

    ``header_row`` is 1-based (row 1 = first row, the default).  Rows above
    the header row are skipped automatically.
    """
    path = Path(file_path)
    # Resolve relative paths against the app directory so "data/foo.csv" works
    if not path.is_absolute():
        path = APP_DIR / path
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    pandas_header = max(header_row - 1, 0)  # pandas uses 0-based index
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, header=pandas_header, dtype=str, keep_default_na=False)
    elif suffix == ".tsv":
        return pd.read_csv(path, sep="\t", header=pandas_header, dtype=str, keep_default_na=False)
    elif suffix in (".xlsx", ".xls"):
        return pd.read_excel(path, header=pandas_header, dtype=str, keep_default_na=False)
    else:
        raise ValueError(f"Unsupported file type: {suffix}")


def get_file_headers(file_path: str | Path, header_row: int = 1) -> list[str]:
    """Return column headers from a data file."""
    df = read_file_to_df(file_path, header_row)
    return list(df.columns)


def resolve_path(raw: str) -> Path:
    """Expand ~ and resolve a path string."""
    return Path(raw).expanduser().resolve()


def discover_files_in_folder(folder: str | Path) -> list[Path]:
    """Return all supported data files in a folder (non-recursive, cached)."""
    folder = resolve_path(str(folder))
    if not folder.is_dir():
        return []
    mtime = folder.stat().st_mtime
    return _discover_files_cached(str(folder), mtime)


@st.cache_data(show_spinner=False, ttl=120)
def _discover_files_cached(folder_str: str, _mtime: float) -> list[Path]:
    folder = Path(folder_str)
    return sorted(
        p for p in folder.iterdir()
        if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS
    )


def file_mod_time(path: str | Path) -> str:
    """Return the last-modified time for a file.

    Prefers the git commit timestamp (accurate on Streamlit Cloud where
    filesystem mtime is just the clone time).  Falls back to filesystem mtime.
    """
    p = Path(path)
    if not p.is_absolute():
        p = APP_DIR / p
    if not p.exists():
        return "file missing"

    # Try git log for the most recent commit that touched this file
    try:
        rel = p.relative_to(APP_DIR)
        out = subprocess.check_output(
            ["git", "log", "-1", "--format=%ai", "--", str(rel)],
            cwd=APP_DIR,
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        if out:
            # out looks like "2026-02-12 08:31:00 -0800"
            return out.rsplit(" ", 1)[0]  # drop timezone offset
    except Exception:
        pass

    # Fallback: filesystem mtime
    ts = p.stat().st_mtime
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


@st.cache_data(show_spinner=False, ttl=300)
def latest_data_refresh(file_paths: tuple[str, ...]) -> str:
    """Return the most recent modification time across all data source files."""
    latest = ""
    for fp in file_paths:
        if not fp:
            continue
        ts = file_mod_time(fp)
        if ts != "file missing" and ts > latest:
            latest = ts
    return latest or "unknown"


def _match_file_to_source(filename: str, config: dict) -> int | None:
    """Return the config['sheets'] index that matches *filename*, or None."""
    stem = Path(filename).stem
    for idx, s in enumerate(config.get("sheets", [])):
        if s.get("source_type") != "upload":
            continue
        stored_name = Path(s.get("file_path", "")).name
        stored_stem = Path(stored_name).stem
        # Exact filename match
        if filename == stored_name:
            return idx
        # Match ignoring _N suffix that save_uploaded_file may have added
        base = re.sub(r"_\d+$", "", stored_stem)
        if stem == base or stem == stored_stem:
            return idx
    return None


def sync_refresh_folder(config: dict) -> int:
    """Scan the configured refresh folder and overwrite stale source files.

    Returns the number of sources that were refreshed.
    """
    folder_raw = config.get("refresh_folder", "").strip()
    if not folder_raw:
        return 0
    folder = resolve_path(folder_raw)
    if not folder.is_dir():
        return 0

    refreshed = 0
    for fp in folder.iterdir():
        if not fp.is_file() or fp.suffix.lower() not in SUPPORTED_EXTENSIONS:
            continue
        idx = _match_file_to_source(fp.name, config)
        if idx is None:
            continue
        stored_path = Path(config["sheets"][idx]["file_path"])
        # Only copy if the file in the refresh folder is newer
        if stored_path.exists():
            if fp.stat().st_mtime <= stored_path.stat().st_mtime:
                continue
        # Copy the newer file over the stored one
        shutil.copy2(fp, stored_path)
        refreshed += 1
    return refreshed


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

def _has_time(fmt: str) -> bool:
    """Return True if a strptime format string includes a time component."""
    return any(code in fmt for code in ("%H", "%I", "%M", "%S", "%p"))


def _is_likely_date_only(dt: datetime) -> bool:
    """Return True if the time component is likely an artifact, not intentional.

    Google Sheets stores dates as floating-point serial numbers.  Cells
    that display as date-only (e.g. ``7/16/2025``) can have hidden time
    components from timezone offsets, copy-paste, or internal storage.
    When exported via Apps Script ``getValues()``, these surface as times
    like ``05:30:00``, ``16:00:00``, ``23:00:00``, etc.

    Since this calendar app uses separate columns for date and time,
    any time on a "date" column is treated as an artifact and stripped.
    Only truly unusual fractional-second times are preserved.
    """
    # If seconds have a fractional/non-zero value, it was likely set
    # intentionally by some system — preserve it.
    if dt.second != 0:
        return False
    # Midnight is always date-only
    if dt.hour == 0 and dt.minute == 0:
        return True
    # On-the-hour times are almost always timezone offsets or artifacts
    if dt.minute == 0:
        return True
    # Half-hour / quarter-hour times before 8 AM are storage artifacts
    if dt.hour < 8:
        return True
    # Afternoon/evening on-the-half-hour — likely a hidden time from
    # Google Sheets cell storage rather than an intentional event time
    # in a "Date" column.  Real event times belong in a "Time" column.
    if dt.minute in (0, 15, 30, 45):
        return True
    return False


def _to_date_or_datetime(parsed: datetime, fmt_has_time: bool) -> str:
    """Return date-only ISO string or full datetime ISO string.

    Returns date-only when there is no meaningful time component, so
    FullCalendar treats the event as all-day.
    """
    if not fmt_has_time:
        return parsed.date().isoformat()
    if _is_likely_date_only(parsed):
        return parsed.date().isoformat()
    return parsed.isoformat()


def parse_date(value) -> str | None:
    """Best-effort parse of a date/datetime string into ISO format.

    Returns a date-only string (``2025-03-15``) when there is no time
    component, so FullCalendar treats the event as all-day.  Returns a
    full ISO datetime (``2025-03-15T14:30:00``) when time info is present.
    """
    if pd.isna(value) or value == "":
        return None
    if isinstance(value, (datetime, date)):
        if isinstance(value, datetime) and not _is_likely_date_only(value):
            if value.hour or value.minute or value.second:
                return value.isoformat()
        return value.date().isoformat() if isinstance(value, datetime) else value.isoformat()
    value = str(value).strip()

    # --- Formats that include a year ---
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%Y/%m/%d",
    ):
        try:
            parsed = datetime.strptime(value, fmt)
            return _to_date_or_datetime(parsed, _has_time(fmt))
        except ValueError:
            continue

    # --- Formats without a year (assume current year) ---
    # Prepend the current year to the value so strptime always sees a year
    # and avoids the Python 3.13+ DeprecationWarning about year-less parsing.
    current_year = str(datetime.now().year)
    for fmt in (
        "%B %d",      # March 3
        "%b %d",      # Mar 4, Feb 3
        "%B %dst",    # March 1st
        "%B %dnd",    # March 2nd
        "%B %drd",    # March 3rd
        "%B %dth",    # March 4th
        "%b %dst",
        "%b %dnd",
        "%b %drd",
        "%b %dth",
        "%d %B",      # 3 March
        "%d %b",      # 3 Mar
    ):
        try:
            parsed = datetime.strptime(f"{current_year} {value}", f"%Y {fmt}")
            return parsed.date().isoformat()
        except ValueError:
            continue

    # --- Fallback: pandas parser (handles many other formats) ---
    try:
        parsed = pd.to_datetime(value)
        return _to_date_or_datetime(parsed.to_pydatetime(), True)
    except Exception:
        return None


def parse_time(value) -> str | None:
    """Extract a time string (HH:MM) from a cell value.

    Handles common formats:
      - ``10:00 AM``, ``2:30 PM``, ``14:30``
      - ``1899-12-30 10:00:00`` (Google Sheets time-only epoch)
      - ``10:00 - 10:30 AM PST`` (takes the first time)
    Returns ``HH:MM`` in 24-hour format, or None if nothing usable found.
    """
    if pd.isna(value) or value == "":
        return None
    value = str(value).strip()
    if not value:
        return None

    # If it looks like a Google Sheets epoch time (1899-12-30 ...), extract the time
    if value.startswith("1899-12-30") or value.startswith("1899-12-31"):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                parsed = datetime.strptime(value, fmt)
                return parsed.strftime("%H:%M")
            except ValueError:
                continue

    m = _TIME_PATTERN.search(value)
    if m:
        hour, minute = int(m.group(1)), int(m.group(2))
        ampm = m.group(4)
        if ampm:
            ampm = ampm.upper()
            if ampm == "PM" and hour != 12:
                hour += 12
            elif ampm == "AM" and hour == 12:
                hour = 0
        return f"{hour:02d}:{minute:02d}"

    return None


# ---------------------------------------------------------------------------
# Build calendar events
# ---------------------------------------------------------------------------

def rows_to_events(
    df: pd.DataFrame,
    mapping: dict,
    sheet_name: str,
    sheet_color: str,
    source_url: str = "",
) -> list[dict]:
    """Convert DataFrame rows into FullCalendar event dicts."""
    events: list[dict] = []
    row_filter = mapping.get("row_filter")
    # Pre-compute allowed values (comma-separated, case-insensitive)
    # Strips surrounding quotes so users can enter: 'ValueA', 'ValueB'
    filter_values: set[str] | None = None
    if row_filter and row_filter.get("column") and row_filter.get("value"):
        filter_values = set()
        for v in row_filter["value"].split(","):
            v = v.strip()
            # Strip matched surrounding quotes: 'val' or "val"
            if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
                v = v[1:-1]
            if v:
                filter_values.add(v.lower())
    for row in df.to_dict(orient="records"):
        # Per-source row filter: skip rows that don't match any allowed value
        if filter_values is not None:
            cell_val = str(row.get(row_filter["column"], "")).strip().lower()
            if cell_val not in filter_values:
                continue

        title = str(row.get(mapping["title"], "")).strip()
        start = parse_date(row.get(mapping["start"]))
        if not title or not start:
            continue

        # Prepend [prefix] to title if a prefix column is mapped
        if "title_prefix" in mapping and mapping["title_prefix"]:
            prefix = str(row.get(mapping["title_prefix"], "")).strip()
            if prefix and prefix.lower() != "nan":
                title = f"[{prefix}] {title}"

        event: dict = {
            "title": title,
            "start": start,
            "allDay": "T" not in start,
        }

        if "end" in mapping and mapping["end"]:
            end = parse_date(row.get(mapping["end"]))
            if end:
                event["end"] = end
                event["allDay"] = "T" not in start and "T" not in end

        if "color" in mapping and mapping["color"]:
            row_color = str(row.get(mapping["color"], "")).strip()
            event["color"] = row_color if row_color else sheet_color
        else:
            event["color"] = sheet_color

        ext: dict = {"source": sheet_name}
        if source_url:
            ext["source_url"] = source_url
        if "description" in mapping and mapping["description"]:
            ext["description"] = str(row.get(mapping["description"], ""))
        if "location" in mapping and mapping["location"]:
            ext["location"] = str(row.get(mapping["location"], ""))

        # Custom fields
        custom_data: dict[str, str] = {}
        for cf in mapping.get("custom_fields", []):
            if cf.get("static"):
                val = cf.get("static_value", "").strip()
            else:
                val = str(row.get(cf.get("column", ""), "")).strip()
            # Clean up Google Sheets time-only epoch values (1899-12-30 ...)
            # After updating the Apps Script, these export as HH:mm directly.
            if val and val.startswith("1899-12-3"):
                cleaned_time = parse_time(val)
                val = cleaned_time if cleaned_time else ""
            if val:
                custom_data[cf["label"]] = val
        if custom_data:
            ext["custom"] = custom_data

        event["extendedProps"] = ext

        events.append(event)
    return events


def _config_fingerprint(config: dict, config_json: str | None = None) -> str:
    """Return a stable hash of config + data file modification times.

    Accepts optional pre-serialized ``config_json`` to avoid redundant
    ``json.dumps`` calls.
    """
    if config_json is None:
        config_json = json.dumps(config, sort_keys=True)
    parts = [config_json]
    for s in config.get("sheets", []):
        fp = s.get("file_path", "")
        if fp:
            p = Path(fp) if Path(fp).is_absolute() else APP_DIR / fp
            if p.exists():
                parts.append(f"{fp}:{p.stat().st_mtime}")
    for wf in config.get("watch_folders", []):
        folder_path = wf.get("folder_path", "")
        for fp in discover_files_in_folder(folder_path):
            parts.append(f"{fp}:{fp.stat().st_mtime}")
    return hashlib.md5("|".join(parts).encode()).hexdigest()


@st.cache_data(show_spinner=False, ttl=300)
def _build_events_cached(_fingerprint: str, config_json: str) -> tuple[list[dict], list[str]]:
    """Cached event builder. Returns (events, warnings)."""
    config = json.loads(config_json)
    events: list[dict] = []
    warnings: list[str] = []

    for idx, sheet_cfg in enumerate(config.get("sheets", [])):
        file_path = sheet_cfg.get("file_path", "")
        mapping = sheet_cfg.get("mapping", {})
        sheet_color = sheet_cfg.get("default_color", DEFAULT_COLORS[idx % len(DEFAULT_COLORS)])
        sheet_name = sheet_cfg.get("name", f"Source {idx + 1}")

        if "title" not in mapping or "start" not in mapping:
            continue
        header_row = sheet_cfg.get("header_row", 1)
        try:
            df = read_file_to_df(file_path, header_row)
        except Exception as e:
            warnings.append(f"Could not load **{sheet_name}**: {e}")
            continue

        source_url = sheet_cfg.get("source_url", "")
        events.extend(rows_to_events(df, mapping, sheet_name, sheet_color, source_url))

    for wf in config.get("watch_folders", []):
        folder_path = wf.get("folder_path", "")
        mapping = wf.get("mapping", {})
        wf_color = wf.get("default_color", "#3788d8")
        wf_name = wf.get("name", folder_path)
        wf_source_url = wf.get("source_url", "")
        wf_header_row = wf.get("header_row", 1)

        if "title" not in mapping or "start" not in mapping:
            continue

        files = discover_files_in_folder(folder_path)
        for fp in files:
            try:
                df = read_file_to_df(fp, wf_header_row)
            except Exception as e:
                warnings.append(f"Could not load **{fp.name}** from {wf_name}: {e}")
                continue
            source_label = f"{wf_name} / {fp.name}"
            events.extend(rows_to_events(df, mapping, source_label, wf_color, wf_source_url))

    return events, warnings


def build_events(config: dict) -> tuple[list[dict], str]:
    """Read data from every configured source and return (events, fingerprint)."""
    config_json = json.dumps(config, sort_keys=True)
    fingerprint = _config_fingerprint(config, config_json)
    events, warnings = _build_events_cached(fingerprint, config_json)
    for w in warnings:
        st.warning(w)
    return events, fingerprint


# ---------------------------------------------------------------------------
# Column mapping form (reusable)
# ---------------------------------------------------------------------------

def _init_custom_fields(form_key: str, existing: list[dict] | None = None):
    """Initialise session-state list for custom field rows in a mapping form."""
    key = f"_custom_fields_{form_key}"
    if key not in st.session_state:
        st.session_state[key] = list(existing) if existing else []
    return key


def render_column_mapping_form(
    headers: list[str],
    form_key: str,
    existing_mapping: dict | None = None,
) -> dict | None:
    """Show a column-mapping form and return the mapping if saved, else None.

    The returned dict always contains:
      - Standard field keys (title, start, end, ...)
      - A ``custom_fields`` key with a list of {label, column} dicts
    """
    existing_mapping = existing_mapping or {}
    existing_custom: list[dict] = existing_mapping.get("custom_fields", [])
    cf_key = _init_custom_fields(form_key, existing_custom)

    # --- Button to add another custom row (outside the form) ---
    add_col1, add_col2 = st.columns([3, 1])
    with add_col2:
        if st.button("+ Add custom field", key=f"{form_key}_add_cf"):
            st.session_state[cf_key].append({"label": "", "column": ""})
            st.rerun()

    with st.form(form_key):
        st.markdown("**Standard fields** — map each calendar field to a column:")
        mapping: dict = {}
        none_option = ["-- none --"]
        for field in ALL_FIELDS:
            label = FIELD_DESCRIPTIONS.get(field, field)
            default_idx = 0
            existing_col = existing_mapping.get(field, "")
            if existing_col in headers:
                default_idx = headers.index(existing_col) + 1
            else:
                for i, h in enumerate(headers):
                    if h.lower().replace(" ", "_") == field or field in h.lower():
                        default_idx = i + 1
                        break
            choice = st.selectbox(
                label,
                options=none_option + headers,
                index=default_idx,
                key=f"{form_key}_{field}",
            )
            if choice != "-- none --":
                mapping[field] = choice

        # --- Custom fields ---
        custom_entries: list[dict] = st.session_state[cf_key]
        if custom_entries:
            st.markdown("**Custom fields** — these will show up in the event detail panel:")
        saved_custom: list[dict] = []
        for ci, cf in enumerate(custom_entries):
            c1, c2, c3, c4, c5 = st.columns([2, 1, 2, 2, 1])
            with c1:
                cf_label = st.text_input(
                    "Display label",
                    value=cf.get("label", ""),
                    key=f"{form_key}_cf_label_{ci}",
                    placeholder="e.g. Organizer",
                )
            with c2:
                use_static = st.checkbox(
                    "Static text",
                    value=cf.get("static", False),
                    key=f"{form_key}_cf_static_{ci}",
                    help="Check this to use a fixed text value instead of a column.",
                )
            with c3:
                default_col_idx = 0
                if cf.get("column", "") in headers:
                    default_col_idx = headers.index(cf["column"]) + 1
                cf_column = st.selectbox(
                    "Column",
                    options=none_option + headers,
                    index=default_col_idx,
                    key=f"{form_key}_cf_col_{ci}",
                )
            with c4:
                cf_value = st.text_input(
                    "Static value",
                    value=cf.get("static_value", ""),
                    key=f"{form_key}_cf_val_{ci}",
                    placeholder="e.g. Marketing Team",
                )
            with c5:
                remove = st.form_submit_button(
                    f"Remove #{ci + 1}",
                    type="secondary",
                )
                if remove:
                    st.session_state[cf_key].pop(ci)
                    st.rerun()

            if cf_label.strip():
                if use_static and cf_value.strip():
                    saved_custom.append({
                        "label": cf_label.strip(),
                        "static": True,
                        "static_value": cf_value.strip(),
                    })
                elif not use_static and cf_column != "-- none --":
                    saved_custom.append({
                        "label": cf_label.strip(),
                        "column": cf_column,
                    })

        # --- Row filter (optional) ---
        st.markdown("**Row filter** — only include rows where a column matches a value:")
        existing_filter = existing_mapping.get("row_filter", {})
        filter_col_default = 0
        if existing_filter.get("column", "") in headers:
            filter_col_default = headers.index(existing_filter["column"]) + 1
        filter_column = st.selectbox(
            "Filter column",
            options=none_option + headers,
            index=filter_col_default,
            key=f"{form_key}_row_filter_col",
            help="Only rows where this column matches the value below will appear on the calendar.",
        )
        filter_value = st.text_input(
            "Filter value(s) — comma-separated, case-insensitive",
            value=existing_filter.get("value", ""),
            key=f"{form_key}_row_filter_val",
            placeholder='e.g. Scheduled, Confirmed, Active',
            help="Separate multiple allowed values with commas.",
        )

        if st.form_submit_button("Save mapping"):
            if "title" not in mapping or "start" not in mapping:
                st.error("You must map at least **title** and **start** columns.")
                return None
            mapping["custom_fields"] = saved_custom
            # Save row filter if configured
            if filter_column != "-- none --" and filter_value.strip():
                mapping["row_filter"] = {
                    "column": filter_column,
                    "value": filter_value.strip(),
                }
            else:
                mapping.pop("row_filter", None)
            # Clean up session state
            if cf_key in st.session_state:
                del st.session_state[cf_key]
            return mapping
    return None


# ---------------------------------------------------------------------------
# UI – Manage Sources page
# ---------------------------------------------------------------------------

def render_manage_sheets():
    """Page for adding, editing, and removing data sources."""
    config = load_config()
    if "watch_folders" not in config:
        config["watch_folders"] = []

    st.header("Manage Data Sources")

    if IS_CLOUD:
        add_options = ["Upload a file"]
    else:
        add_options = ["Upload a file", "Link to a file on disk", "Watch a folder"]

    add_method = st.radio(
        "How would you like to add data?",
        options=add_options,
        horizontal=True,
    )

    # ==================================================================
    # TAB 1 – Upload a file
    # ==================================================================
    if add_method == "Upload a file":
        st.subheader("Upload a file")
        st.caption("Export from Google Sheets: **File → Download → CSV (.csv)**")

        with st.form("add_upload_form", clear_on_submit=True):
            new_name = st.text_input("Friendly name", placeholder="e.g. Team Birthdays")
            uploaded = st.file_uploader(
                "Upload a file",
                type=["csv", "xlsx", "xls", "tsv"],
            )
            new_source_url = st.text_input(
                "Original Google Sheet link (optional)",
                placeholder="https://docs.google.com/spreadsheets/d/...",
                help="Shown when someone clicks an event so they can open the original sheet.",
            )
            new_header_row = st.number_input(
                "Header row",
                min_value=1,
                value=1,
                step=1,
                help="Which row contains the column headers? (1 = first row)",
            )
            new_color = st.color_picker(
                "Default event colour",
                value=DEFAULT_COLORS[len(config["sheets"]) % len(DEFAULT_COLORS)],
            )
            submitted = st.form_submit_button("Upload & read columns")

        if submitted and uploaded:
            try:
                saved_path = save_uploaded_file(uploaded)
                headers = get_file_headers(saved_path, int(new_header_row))
                st.session_state["_new_sheet_headers"] = headers
                st.session_state["_new_sheet_meta"] = {
                    "name": new_name or uploaded.name,
                    "file_path": str(saved_path.relative_to(APP_DIR)),
                    "default_color": new_color,
                    "source_type": "upload",
                    "source_url": new_source_url.strip(),
                    "header_row": int(new_header_row),
                }
            except Exception as e:
                st.error(f"Failed to read file: {e}")

    # ==================================================================
    # TAB 2 – Link to a file on disk
    # ==================================================================
    elif add_method == "Link to a file on disk":
        st.subheader("Link to a file on disk")
        st.caption(
            "Point to a CSV/Excel file on your computer. "
            "When you re-export to the **same file path**, the calendar updates automatically."
        )

        with st.form("add_linked_form"):
            new_name = st.text_input("Friendly name", placeholder="e.g. Marketing Events")
            file_path_input = st.text_input(
                "Full file path",
                placeholder="~/Downloads/events.csv",
                help="Supports ~ for your home directory.",
            )
            new_source_url = st.text_input(
                "Original Google Sheet link (optional)",
                placeholder="https://docs.google.com/spreadsheets/d/...",
                help="Shown when someone clicks an event so they can open the original sheet.",
            )
            new_header_row = st.number_input(
                "Header row",
                min_value=1,
                value=1,
                step=1,
                help="Which row contains the column headers? (1 = first row)",
            )
            new_color = st.color_picker(
                "Default event colour",
                value=DEFAULT_COLORS[len(config["sheets"]) % len(DEFAULT_COLORS)],
            )
            submitted = st.form_submit_button("Read columns")

        if submitted and file_path_input:
            resolved = resolve_path(file_path_input)
            if not resolved.exists():
                st.error(f"File not found: `{resolved}`")
            elif resolved.suffix.lower() not in SUPPORTED_EXTENSIONS:
                st.error(f"Unsupported file type: `{resolved.suffix}`. Use CSV, TSV, XLSX, or XLS.")
            else:
                try:
                    headers = get_file_headers(resolved, int(new_header_row))
                    st.session_state["_new_sheet_headers"] = headers
                    st.session_state["_new_sheet_meta"] = {
                        "name": new_name or resolved.stem,
                        "file_path": str(resolved),
                        "default_color": new_color,
                        "source_type": "linked",
                        "source_url": new_source_url.strip(),
                        "header_row": int(new_header_row),
                    }
                except Exception as e:
                    st.error(f"Failed to read file: {e}")

    # ==================================================================
    # TAB 3 – Watch a folder
    # ==================================================================
    elif add_method == "Watch a folder":
        st.subheader("Watch a folder")
        st.caption(
            "Point to a folder on your computer. Every CSV/Excel file inside it "
            "will be loaded using the **same column mapping**. Drop new files in "
            "to add more events — no app changes needed."
        )

        with st.form("add_folder_form"):
            wf_name = st.text_input("Friendly name", placeholder="e.g. All Event Exports")
            folder_input = st.text_input(
                "Folder path",
                placeholder="~/Documents/event-exports",
            )
            wf_source_url = st.text_input(
                "Original Google Sheet link (optional)",
                placeholder="https://docs.google.com/spreadsheets/d/...",
                help="Shown when someone clicks an event so they can open the original sheet.",
            )
            wf_header_row = st.number_input(
                "Header row",
                min_value=1,
                value=1,
                step=1,
                help="Which row contains the column headers? (1 = first row)",
            )
            wf_color = st.color_picker(
                "Default event colour",
                value=DEFAULT_COLORS[
                    (len(config["sheets"]) + len(config["watch_folders"])) % len(DEFAULT_COLORS)
                ],
            )
            submitted = st.form_submit_button("Scan folder")

        if submitted and folder_input:
            resolved_folder = resolve_path(folder_input)
            if not resolved_folder.is_dir():
                st.error(f"Not a valid directory: `{resolved_folder}`")
            else:
                files = discover_files_in_folder(resolved_folder)
                if not files:
                    st.warning("No CSV / Excel / TSV files found in that folder.")
                else:
                    st.success(f"Found **{len(files)}** file(s): {', '.join(f.name for f in files)}")
                    try:
                        headers = get_file_headers(files[0], int(wf_header_row))
                        st.session_state["_new_wf_headers"] = headers
                        st.session_state["_new_wf_meta"] = {
                            "name": wf_name or resolved_folder.name,
                            "folder_path": str(resolved_folder),
                            "default_color": wf_color,
                            "source_url": wf_source_url.strip(),
                            "header_row": int(wf_header_row),
                        }
                    except Exception as e:
                        st.error(f"Failed to read {files[0].name}: {e}")

    # ------------------------------------------------------------------
    # Column mapping for new individual source (upload or linked)
    # ------------------------------------------------------------------
    if "_new_sheet_headers" in st.session_state:
        headers = st.session_state["_new_sheet_headers"]
        meta = st.session_state["_new_sheet_meta"]
        st.divider()
        st.markdown(f"**Columns found in *{meta['name']}*:** `{'`, `'.join(headers)}`")

        mapping = render_column_mapping_form(headers, "map_new_source")
        if mapping is not None:
            sheet_entry = {**meta, "mapping": mapping}
            config["sheets"].append(sheet_entry)
            save_config(config)
            del st.session_state["_new_sheet_headers"]
            del st.session_state["_new_sheet_meta"]
            st.success(f"**{meta['name']}** added!")
            st.rerun()

    # ------------------------------------------------------------------
    # Column mapping for new watch folder
    # ------------------------------------------------------------------
    if "_new_wf_headers" in st.session_state:
        headers = st.session_state["_new_wf_headers"]
        meta = st.session_state["_new_wf_meta"]
        st.divider()
        st.markdown(
            f"**Columns from first file in *{meta['name']}*:** `{'`, `'.join(headers)}`  \n"
            f"This mapping will apply to **all** files in the folder."
        )

        mapping = render_column_mapping_form(headers, "map_new_wf")
        if mapping is not None:
            wf_entry = {**meta, "mapping": mapping}
            config["watch_folders"].append(wf_entry)
            save_config(config)
            del st.session_state["_new_wf_headers"]
            del st.session_state["_new_wf_meta"]
            st.success(f"Watch folder **{meta['name']}** added!")
            st.rerun()

    # ==================================================================
    # Refresh Folder (local mode only)
    # ==================================================================
    upload_sources = [
        s for s in config["sheets"] if s.get("source_type") == "upload"
    ]
    if upload_sources and not IS_CLOUD:
        st.divider()
        st.subheader("Refresh Folder")
        st.caption(
            "Point to a folder on your computer where you save your CSV/Excel exports. "
            "Whenever you replace a file in that folder, the app will automatically "
            "pick up the newer version by matching filenames to existing sources."
        )

        current_folder = config.get("refresh_folder", "")
        new_folder = st.text_input(
            "Refresh folder path",
            value=current_folder,
            placeholder="~/Downloads/calendar-exports",
            help="The app scans this folder on every page load and copies any file "
                 "that is newer than the stored version.",
            key="refresh_folder_input",
        )

        folder_col1, folder_col2 = st.columns([1, 1])
        with folder_col1:
            if new_folder.strip() != current_folder:
                if st.button("Save refresh folder", key="save_refresh_folder"):
                    resolved = resolve_path(new_folder.strip())
                    if new_folder.strip() and not resolved.is_dir():
                        st.error(f"Not a valid directory: `{resolved}`")
                    else:
                        config["refresh_folder"] = new_folder.strip()
                        save_config(config)
                        st.success("Refresh folder saved!")
                        st.rerun()
        with folder_col2:
            if current_folder:
                if st.button("Remove refresh folder", key="remove_refresh_folder"):
                    config["refresh_folder"] = ""
                    save_config(config)
                    st.success("Refresh folder removed.")
                    st.rerun()

        # Show current sync status
        if current_folder:
            resolved_folder = resolve_path(current_folder)
            if resolved_folder.is_dir():
                folder_files = [
                    fp for fp in resolved_folder.iterdir()
                    if fp.is_file() and fp.suffix.lower() in SUPPORTED_EXTENSIONS
                ]
                matched_names = []
                unmatched_names = []
                for fp in folder_files:
                    idx = _match_file_to_source(fp.name, config)
                    if idx is not None:
                        src_name = config["sheets"][idx].get("name", "Unnamed")
                        stored_path = Path(config["sheets"][idx]["file_path"])
                        is_newer = (
                            not stored_path.exists()
                            or fp.stat().st_mtime > stored_path.stat().st_mtime
                        )
                        status = "**newer** — will sync" if is_newer else "up to date"
                        matched_names.append(f"- `{fp.name}` → **{src_name}** ({status})")
                    else:
                        unmatched_names.append(fp.name)

                with st.expander(
                    f"Folder status — {len(folder_files)} file(s), {len(matched_names)} matched",
                    expanded=False,
                ):
                    if matched_names:
                        st.markdown("\n".join(matched_names))
                    if unmatched_names:
                        st.markdown(
                            "**Unmatched files** (no matching source): "
                            + ", ".join(f"`{n}`" for n in unmatched_names)
                        )
                    if not folder_files:
                        st.info("No CSV/Excel/TSV files found in this folder.")
            else:
                st.warning(f"Folder not found: `{resolved_folder}`")

    # ==================================================================
    # Existing individual sources
    # ==================================================================
    if config["sheets"]:
        st.divider()
        st.subheader("Configured sources")

        for idx, sheet_cfg in enumerate(config["sheets"]):
            fpath = sheet_cfg.get("file_path", "")
            p = Path(fpath) if Path(fpath).is_absolute() else APP_DIR / fpath
            file_exists = p.exists()
            source_type = sheet_cfg.get("source_type", "upload")
            type_label = "linked" if source_type == "linked" else "uploaded"
            status_icon = "🟢" if (sheet_cfg.get("mapping") and file_exists) else "🔴"

            with st.expander(f"{status_icon} {sheet_cfg.get('name', 'Unnamed')}"):
                src_url = sheet_cfg.get("source_url", "")
                st.markdown(
                    f"**File:** `{p.name}` ({type_label})  \n"
                    f"**Path:** `{fpath}`  \n"
                    f"**Last modified:** {file_mod_time(fpath)}  \n"
                    f"**Status:** {'found' if file_exists else 'MISSING'}  \n"
                    f"**Default colour:** {sheet_cfg.get('default_color', '#3788d8')}"
                    + (f"  \n**Source link:** [Open original sheet]({src_url})" if src_url else "")
                )
                if sheet_cfg.get("mapping"):
                    st.markdown("**Column mapping:**")
                    for field, col in sheet_cfg["mapping"].items():
                        if field == "custom_fields":
                            continue
                        st.markdown(f"- {field} → `{col}`")
                    custom_fields = sheet_cfg["mapping"].get("custom_fields", [])
                    if custom_fields:
                        st.markdown("**Custom fields:**")
                        for cf in custom_fields:
                            if cf.get("static"):
                                st.markdown(f"- {cf['label']} = `{cf['static_value']}`")
                            else:
                                st.markdown(f"- {cf['label']} → `{cf.get('column', '')}`")

                # Rename, source URL & header row
                edit_col1, edit_col2, edit_col3 = st.columns([2, 2, 1])
                with edit_col1:
                    new_name = st.text_input(
                        "Name",
                        value=sheet_cfg.get("name", "Unnamed"),
                        key=f"rename_{idx}",
                    )
                with edit_col2:
                    new_src_url = st.text_input(
                        "Source link",
                        value=sheet_cfg.get("source_url", ""),
                        key=f"srcurl_{idx}",
                        placeholder="https://docs.google.com/spreadsheets/d/...",
                    )
                with edit_col3:
                    new_hdr_row = st.number_input(
                        "Header row",
                        min_value=1,
                        value=sheet_cfg.get("header_row", 1),
                        step=1,
                        key=f"hdr_row_{idx}",
                        help="Which row contains the column headers? (1 = first row)",
                    )
                name_changed = new_name != sheet_cfg.get("name", "Unnamed")
                url_changed = new_src_url.strip() != sheet_cfg.get("source_url", "")
                hdr_changed = int(new_hdr_row) != sheet_cfg.get("header_row", 1)
                if name_changed or url_changed or hdr_changed:
                    if st.button("Save changes", key=f"save_edit_{idx}"):
                        config["sheets"][idx]["name"] = new_name
                        config["sheets"][idx]["source_url"] = new_src_url.strip()
                        config["sheets"][idx]["header_row"] = int(new_hdr_row)
                        save_config(config)
                        st.rerun()

                col1, col2, col3 = st.columns(3)

                with col1:
                    if source_type == "upload":
                        replacement = st.file_uploader(
                            "Replace file",
                            type=["csv", "xlsx", "xls", "tsv"],
                            key=f"replace_{idx}",
                            label_visibility="collapsed",
                        )
                        if replacement is not None:
                            try:
                                old_fp = Path(sheet_cfg["file_path"])
                                old = old_fp if old_fp.is_absolute() else APP_DIR / old_fp
                                old_rel = sheet_cfg["file_path"]
                                if old.exists():
                                    old.unlink()
                                if IS_CLOUD:
                                    delete_file_from_github(old_rel, f"Remove old data file: {old.name}")
                                new_path = save_uploaded_file(replacement)
                                config["sheets"][idx]["file_path"] = str(new_path.relative_to(APP_DIR))
                                save_config(config)
                                st.success("File replaced!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Failed: {e}")
                    else:
                        st.caption("Linked file — re-export to the same path to update.")

                with col2:
                    if st.button("Re-map columns", key=f"remap_{idx}"):
                        try:
                            hdr_row = sheet_cfg.get("header_row", 1)
                            headers = get_file_headers(fpath, hdr_row)
                            st.session_state["_remap_idx"] = idx
                            st.session_state["_remap_headers"] = headers
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed: {e}")

                with col3:
                    if st.button("Remove", key=f"remove_{idx}", type="primary"):
                        if source_type == "upload":
                            old_fp = Path(sheet_cfg.get("file_path", ""))
                            old = old_fp if old_fp.is_absolute() else APP_DIR / old_fp
                            if old.exists():
                                old.unlink()
                            if IS_CLOUD:
                                delete_file_from_github(
                                    sheet_cfg.get("file_path", ""),
                                    f"Remove data file: {old.name}",
                                )
                        config["sheets"].pop(idx)
                        save_config(config)
                        st.rerun()

        # Re-mapping flow for individual sources
        if "_remap_idx" in st.session_state:
            remap_idx = st.session_state["_remap_idx"]
            headers = st.session_state["_remap_headers"]
            sheet_cfg = config["sheets"][remap_idx]
            st.divider()
            st.subheader(f"Re-map columns for: {sheet_cfg.get('name', 'Unnamed')}")
            st.markdown(f"**Columns:** `{'`, `'.join(headers)}`")

            mapping = render_column_mapping_form(
                headers, "remap_source", existing_mapping=sheet_cfg.get("mapping")
            )
            if mapping is not None:
                config["sheets"][remap_idx]["mapping"] = mapping
                save_config(config)
                del st.session_state["_remap_idx"]
                del st.session_state["_remap_headers"]
                st.success("Mapping updated!")
                st.rerun()

    # ==================================================================
    # Existing watch folders (local mode only)
    # ==================================================================
    if config.get("watch_folders") and not IS_CLOUD:
        st.divider()
        st.subheader("Watched folders")

        for idx, wf in enumerate(config["watch_folders"]):
            folder_path = wf.get("folder_path", "")
            resolved = resolve_path(folder_path)
            folder_exists = resolved.is_dir()
            files = discover_files_in_folder(folder_path) if folder_exists else []
            status_icon = "🟢" if (wf.get("mapping") and folder_exists) else "🔴"

            with st.expander(f"{status_icon} {wf.get('name', 'Unnamed folder')}"):
                wf_src_url = wf.get("source_url", "")
                st.markdown(
                    f"**Folder:** `{folder_path}`  \n"
                    f"**Status:** {'found' if folder_exists else 'MISSING'}  \n"
                    f"**Files found:** {len(files)}  \n"
                    f"**Default colour:** {wf.get('default_color', '#3788d8')}"
                    + (f"  \n**Source link:** [Open original sheet]({wf_src_url})" if wf_src_url else "")
                )
                if files:
                    for fp in files:
                        st.markdown(f"- `{fp.name}` (modified {file_mod_time(fp)})")
                if wf.get("mapping"):
                    st.markdown("**Column mapping:**")
                    for field, col in wf["mapping"].items():
                        if field == "custom_fields":
                            continue
                        st.markdown(f"- {field} → `{col}`")
                    wf_custom_fields = wf["mapping"].get("custom_fields", [])
                    if wf_custom_fields:
                        st.markdown("**Custom fields:**")
                        for cf in wf_custom_fields:
                            if cf.get("static"):
                                st.markdown(f"- {cf['label']} = `{cf['static_value']}`")
                            else:
                                st.markdown(f"- {cf['label']} → `{cf.get('column', '')}`")

                # Rename, source URL & header row
                wf_edit_col1, wf_edit_col2, wf_edit_col3 = st.columns([2, 2, 1])
                with wf_edit_col1:
                    new_wf_name = st.text_input(
                        "Name",
                        value=wf.get("name", "Unnamed folder"),
                        key=f"rename_wf_{idx}",
                    )
                with wf_edit_col2:
                    new_wf_src_url = st.text_input(
                        "Source link",
                        value=wf.get("source_url", ""),
                        key=f"srcurl_wf_{idx}",
                        placeholder="https://docs.google.com/spreadsheets/d/...",
                    )
                with wf_edit_col3:
                    new_wf_hdr_row = st.number_input(
                        "Header row",
                        min_value=1,
                        value=wf.get("header_row", 1),
                        step=1,
                        key=f"hdr_row_wf_{idx}",
                        help="Which row contains the column headers? (1 = first row)",
                    )
                wf_name_changed = new_wf_name != wf.get("name", "Unnamed folder")
                wf_url_changed = new_wf_src_url.strip() != wf.get("source_url", "")
                wf_hdr_changed = int(new_wf_hdr_row) != wf.get("header_row", 1)
                if wf_name_changed or wf_url_changed or wf_hdr_changed:
                    if st.button("Save changes", key=f"save_edit_wf_{idx}"):
                        config["watch_folders"][idx]["name"] = new_wf_name
                        config["watch_folders"][idx]["source_url"] = new_wf_src_url.strip()
                        config["watch_folders"][idx]["header_row"] = int(new_wf_hdr_row)
                        save_config(config)
                        st.rerun()

                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Re-map columns", key=f"remap_wf_{idx}"):
                        if files:
                            try:
                                wf_hdr_row = wf.get("header_row", 1)
                                headers = get_file_headers(files[0], wf_hdr_row)
                                st.session_state["_remap_wf_idx"] = idx
                                st.session_state["_remap_wf_headers"] = headers
                                st.rerun()
                            except Exception as e:
                                st.error(f"Failed: {e}")
                        else:
                            st.warning("No files in folder to read headers from.")
                with col2:
                    if st.button("Remove", key=f"remove_wf_{idx}", type="primary"):
                        config["watch_folders"].pop(idx)
                        save_config(config)
                        st.rerun()

        # Re-mapping flow for watch folders
        if "_remap_wf_idx" in st.session_state:
            remap_idx = st.session_state["_remap_wf_idx"]
            headers = st.session_state["_remap_wf_headers"]
            wf = config["watch_folders"][remap_idx]
            st.divider()
            st.subheader(f"Re-map columns for: {wf.get('name', 'Unnamed folder')}")
            st.markdown(f"**Columns:** `{'`, `'.join(headers)}`")

            mapping = render_column_mapping_form(
                headers, "remap_wf_source", existing_mapping=wf.get("mapping")
            )
            if mapping is not None:
                config["watch_folders"][remap_idx]["mapping"] = mapping
                save_config(config)
                del st.session_state["_remap_wf_idx"]
                del st.session_state["_remap_wf_headers"]
                st.success("Mapping updated!")
                st.rerun()


# ---------------------------------------------------------------------------
# UI – Apps Script Helper page
# ---------------------------------------------------------------------------

def _parse_sheet_url(url: str) -> tuple[str, int] | None:
    """Extract (spreadsheet_id, gid) from a Google Sheets URL."""
    if not url or "spreadsheets/d/" not in url:
        return None
    try:
        spreadsheet_id = url.split("spreadsheets/d/")[1].split("/")[0]
        gid = 0
        if "gid=" in url:
            gid_str = url.split("gid=")[-1].split("&")[0].split("#")[0]
            gid = int(gid_str)
        return spreadsheet_id, gid
    except (IndexError, ValueError):
        return None


def render_apps_script():
    """Show the Google Apps Script snippet for automated GitHub push."""
    st.header("Automate with Google Apps Script")

    st.markdown(
        "Sync **all** your Google Sheets to the calendar automatically with "
        "a single script. It runs on a schedule, exports each sheet as CSV, "
        "and pushes to GitHub — Streamlit Cloud redeploys and the calendar "
        "updates.\n\n"
        "**This uses Google Apps Script, which is part of Google Workspace — "
        "not Google Cloud.** Works with private sheets because the script "
        "runs as your account."
    )

    # --- Check sources missing URLs ---
    config = load_config()
    sources = config.get("sheets", [])
    sources_missing_url: list[str] = []
    for s in sources:
        parsed = _parse_sheet_url(s.get("source_url", ""))
        if not parsed:
            sources_missing_url.append(s.get("name", "Unnamed"))

    if sources_missing_url:
        st.warning(
            f"These sources are missing a Google Sheet link and won't be "
            f"synced: **{', '.join(sources_missing_url)}**. "
            f"Add a source link on the Manage Sources page to include them."
        )

    st.subheader("Step 1: Create a standalone Apps Script project")
    st.markdown(
        "1. Go to [script.google.com](https://script.google.com) and click "
        "**New project**.\n"
        "2. Name it something like *Calendar Sync*.\n"
        "3. In the left sidebar, click the **gear icon** (Project Settings).\n"
        "4. Scroll to **Script Properties** and add two properties:\n"
        "   - `GITHUB_TOKEN` — your GitHub PAT "
        "([create one here](https://github.com/settings/tokens?type=beta) "
        "scoped to your repo with **Contents** read & write)\n"
        "   - `GITHUB_REPO` — e.g. `Kevinsnowflake/sheets-calendar-app`"
    )

    st.subheader("Step 2: Paste the script")
    st.markdown(
        "1. Go back to **Editor** (the `< >` icon).\n"
        "2. Delete any existing code and paste the script below.\n"
        "3. Click **Save**, then **Run** to test.\n"
        "4. On first run, authorize access when prompted — the script "
        "needs permission to read your Google Sheets and call the "
        "GitHub API."
    )

    script = f'''\
// GitHub credentials are read from Script Properties (Project Settings).
// Required properties:  GITHUB_TOKEN, GITHUB_REPO

/**
 * Sync all sources to GitHub in a SINGLE commit.
 * Sources are read dynamically from config.json in the repo,
 * so you never need to update this script when adding new sources.
 * Set your triggers on this function.
 */
function syncAll() {{
  var props = PropertiesService.getScriptProperties();
  var token = props.getProperty("GITHUB_TOKEN");
  var repo  = props.getProperty("GITHUB_REPO");
  if (!token || !repo) {{
    throw new Error(
      "Missing Script Properties. Go to Project Settings and add "
      + "GITHUB_TOKEN and GITHUB_REPO."
    );
  }}

  // Fetch config.json from the repo to get the current list of sources
  var sources = loadSources_(token, repo);
  Logger.log("Found " + sources.length + " source(s) in config.json");

  var files = [];
  var results = [];
  for (var i = 0; i < sources.length; i++) {{
    var src = sources[i];
    try {{
      var ss = SpreadsheetApp.openById(src.spreadsheetId);
      var sheet = getSheetByGid_(ss, src.gid);
      var csv = sheetToCsv_(ss, sheet);
      files.push({{ path: src.filePath, content: csv }});
      results.push("OK  " + src.name);
    }} catch (e) {{
      results.push("ERR " + src.name + ": " + e.message);
    }}
  }}

  if (files.length > 0) {{
    pushAllFiles_(token, repo, files, "Auto-sync: update all sources");
  }}
  Logger.log("Sync results:\\n" + results.join("\\n"));
}}

// --- Helper: fetch config.json and parse sources ---
function loadSources_(token, repo) {{
  var url = "https://api.github.com/repos/" + repo + "/contents/config.json";
  var resp = UrlFetchApp.fetch(url, {{
    method: "get",
    headers: {{
      "Authorization": "Bearer " + token,
      "Accept": "application/vnd.github.raw+json"
    }},
    muteHttpExceptions: true
  }});
  if (resp.getResponseCode() !== 200) {{
    throw new Error("Could not fetch config.json: HTTP " + resp.getResponseCode());
  }}
  var config = JSON.parse(resp.getContentText());
  var sheets = config.sheets || [];
  var sources = [];
  for (var i = 0; i < sheets.length; i++) {{
    var s = sheets[i];
    var sourceUrl = s.source_url || "";
    var parsed = parseSheetUrl_(sourceUrl);
    if (parsed) {{
      sources.push({{
        name: s.name || "Unnamed",
        spreadsheetId: parsed.spreadsheetId,
        gid: parsed.gid,
        filePath: s.file_path || ""
      }});
    }}
  }}
  return sources;
}}

// --- Helper: extract spreadsheetId and gid from a Google Sheets URL ---
function parseSheetUrl_(url) {{
  if (!url || url.indexOf("spreadsheets/d/") === -1) return null;
  try {{
    var spreadsheetId = url.split("spreadsheets/d/")[1].split("/")[0];
    var gid = 0;
    if (url.indexOf("gid=") > -1) {{
      gid = parseInt(url.split("gid=").pop().split("&")[0].split("#")[0], 10);
    }}
    return {{ spreadsheetId: spreadsheetId, gid: gid }};
  }} catch (e) {{
    return null;
  }}
}}

// --- Helper: find a sheet tab by its gid ---
function getSheetByGid_(ss, gid) {{
  var sheets = ss.getSheets();
  for (var i = 0; i < sheets.length; i++) {{
    if (sheets[i].getSheetId() === gid) return sheets[i];
  }}
  return sheets[0]; // fallback to first tab
}}

// --- Helper: export a sheet to CSV string ---
function sheetToCsv_(ss, sheet) {{
  var tz = ss.getSpreadsheetTimeZone();
  var range = sheet.getDataRange();
  var data = range.getValues();
  var richTexts = range.getRichTextValues();
  return data.map(function(row, r) {{
    return row.map(function(cell, c) {{
      var val;
      if (cell instanceof Date) {{
        var y = cell.getFullYear();
        if (y === 1899) {{
          val = Utilities.formatDate(cell, tz, "HH:mm");
        }} else {{
          var h = cell.getHours(), m = cell.getMinutes(), s = cell.getSeconds();
          if (h === 0 && m === 0 && s === 0) {{
            val = Utilities.formatDate(cell, tz, "yyyy-MM-dd");
          }} else {{
            val = Utilities.formatDate(cell, tz, "yyyy-MM-dd HH:mm");
          }}
        }}
      }} else {{
        val = String(cell);
      }}
      try {{
        var rt = richTexts[r][c];
        if (rt) {{
          var url = rt.getLinkUrl();
          if (url && val.indexOf(url) === -1) {{
            val = val + " (" + url + ")";
          }}
        }}
      }} catch (e) {{}}
      if (val.indexOf(",") > -1 || val.indexOf("\\n") > -1
          || val.indexOf('"') > -1) {{
        val = '"' + val.replace(/"/g, '""') + '"';
      }}
      return val;
    }}).join(",");
  }}).join("\\n");
}}

// --- Helper: push files via the Contents API (one commit per file) ---
function pushAllFiles_(token, repo, files, message) {{
  var apiBase = "https://api.github.com/repos/" + repo + "/contents/";
  var headers = {{
    "Authorization": "Bearer " + token,
    "Accept": "application/vnd.github+json"
  }};

  for (var i = 0; i < files.length; i++) {{
    var filePath = files[i].path;
    var content = Utilities.base64Encode(files[i].content, Utilities.Charset.UTF_8);

    // Get the current file SHA (needed for updates)
    var sha = null;
    var getResp = UrlFetchApp.fetch(apiBase + filePath, {{
      method: "get",
      headers: headers,
      muteHttpExceptions: true
    }});
    if (getResp.getResponseCode() === 200) {{
      sha = JSON.parse(getResp.getContentText()).sha;
    }}

    // Create or update the file
    var body = {{
      message: message + " (" + filePath + ")",
      content: content
    }};
    if (sha) body.sha = sha;

    var putResp = UrlFetchApp.fetch(apiBase + filePath, {{
      method: "put",
      headers: headers,
      contentType: "application/json",
      payload: JSON.stringify(body),
      muteHttpExceptions: true
    }});
    var code = putResp.getResponseCode();
    if (code < 200 || code >= 300) {{
      throw new Error("GitHub API " + code + " for " + filePath + ": "
        + putResp.getContentText().substring(0, 300));
    }}
  }}
}}'''

    st.code(script, language="javascript")

    st.subheader("Step 3: Set up scheduled triggers")
    st.markdown(
        "Create **3 triggers** so the calendar syncs at 7 AM, 12 PM, and 5 PM:\n\n"
        "1. In the Apps Script editor, click the **clock icon** (Triggers) "
        "in the left sidebar.\n"
        "2. **Delete any existing triggers** (e.g. hourly) to avoid excessive commits.\n"
        "3. Click **+ Add Trigger** and configure:\n"
        "   - **Function:** `syncAll`\n"
        "   - **Event source:** Time-driven\n"
        "   - **Type:** Day timer\n"
        "   - **Time:** 7am to 8am\n"
        "4. Repeat to add two more triggers:\n"
        "   - One set to **12pm to 1pm**\n"
        "   - One set to **5pm to 6pm**\n"
        "5. Click **Save** and authorize when prompted.\n\n"
        "Each sync now creates a **single commit** (instead of one per source), "
        "which keeps the repo clean and ensures Streamlit Cloud redeploys reliably."
    )

    st.subheader("Adding new sources")
    st.markdown(
        "The script **automatically reads your sources** from `config.json` "
        "in the repo every time it runs. When you add a new source on the "
        "**Manage Sources** page (with a Google Sheet link), it will be "
        "included in the next sync automatically — no script updates needed."
    )


# ---------------------------------------------------------------------------
# UI – Filter events
# ---------------------------------------------------------------------------

def _event_searchable_text(e: dict) -> str:
    """Build a single lowercase string from all text fields of an event."""
    parts = [e.get("title", "")]
    ext = e.get("extendedProps", {})
    parts.append(ext.get("description", ""))
    parts.append(ext.get("location", ""))
    for val in ext.get("custom", {}).values():
        parts.append(str(val))
    return " ".join(parts).lower()


def filter_events(events: list[dict], config: dict | None = None) -> list[dict]:
    """Show filter controls and return only the events that match."""

    # Build the source list from config so every configured source appears,
    # even if it currently has zero events (e.g. file missing / no valid rows).
    config_sources: set[str] = set()
    if config:
        for idx, s in enumerate(config.get("sheets", [])):
            config_sources.add(s.get("name", f"Source {idx + 1}"))
        for wf in config.get("watch_folders", []):
            wf_name = wf.get("name", wf.get("folder_path", ""))
            # Watch folders generate one source per file: "folder / file.csv"
            folder_path = wf.get("folder_path", "")
            files = discover_files_in_folder(folder_path) if folder_path else []
            if files:
                for fp in files:
                    config_sources.add(f"{wf_name} / {fp.name}")
            else:
                config_sources.add(wf_name)
    # Also include any source names that appear in events (safety net)
    event_sources = {e.get("extendedProps", {}).get("source", "") for e in events} - {""}
    all_sources: list[str] = sorted(config_sources | event_sources)
    all_locations: list[str] = sorted(
        {e.get("extendedProps", {}).get("location", "").strip() for e in events} - {""}
    )

    # --- Sidebar: Source filter (form-based to batch selections) ---
    if "source_filter" not in st.session_state:
        st.session_state["source_filter"] = []
    else:
        valid = [s for s in st.session_state["source_filter"] if s in all_sources]
        if valid != st.session_state["source_filter"]:
            st.session_state["source_filter"] = valid

    with st.sidebar.form("source_filter_form"):
        st.markdown("**Sources**")
        sel_all = st.checkbox(
            "Select all",
            value=set(st.session_state["source_filter"]) == set(all_sources) and len(all_sources) > 0,
        )
        if sel_all:
            default_sources = list(all_sources)
        else:
            default_sources = st.session_state["source_filter"]
        chosen = st.multiselect(
            "Sources",
            options=all_sources,
            default=default_sources,
            placeholder="Select sources to display...",
            help="Choose sources, then click Apply.",
            label_visibility="collapsed",
        )
        applied = st.form_submit_button("Apply", use_container_width=True)
        if applied:
            st.session_state["source_filter"] = chosen

    selected_sources = st.session_state["source_filter"]

    # --- Sidebar: Saved views ---
    saved_views = get_saved_views()
    view_names = [v["name"] for v in saved_views]
    if view_names:
        with st.sidebar.expander("Saved Views", expanded=False):
            sv_options = ["(none)"] + view_names
            if "saved_view_picker" in st.session_state:
                if st.session_state["saved_view_picker"] not in sv_options:
                    del st.session_state["saved_view_picker"]
            selected_view_name = st.selectbox(
                "Saved Views",
                options=sv_options,
                key="saved_view_picker",
                help="Load a previously saved combination of filters.",
                label_visibility="collapsed",
            )
            if selected_view_name != "(none)":
                col_load, col_del = st.columns(2)
                with col_load:
                    if st.button("Load", key="load_saved_view", use_container_width=True):
                        match = next((v for v in saved_views if v["name"] == selected_view_name), None)
                        if match:
                            _apply_saved_view(match)
                with col_del:
                    if st.button("Delete", key="delete_saved_view", type="secondary", use_container_width=True):
                        delete_view(selected_view_name)
                        st.rerun()

    with st.expander("Filters", expanded=False):
        filter_cols = st.columns([2, 2, 1])

        # --- Keyword search (searches all text) ---
        with filter_cols[0]:
            keyword = st.text_input(
                "Search",
                placeholder="Search titles, descriptions, custom fields...",
                help="Case-insensitive search across all event text.",
            ).strip().lower()

        # --- Location filter ---
        with filter_cols[1]:
            if all_locations:
                location_options = ["All locations"] + all_locations
                if "location_filter" not in st.session_state:
                    st.session_state["location_filter"] = ["All locations"]
                valid_locs = [l for l in st.session_state["location_filter"] if l in location_options]
                if valid_locs != st.session_state["location_filter"]:
                    st.session_state["location_filter"] = valid_locs if valid_locs else ["All locations"]
                selected_locations = st.multiselect(
                    "Locations",
                    options=location_options,
                    key="location_filter",
                    help="Filter by event location.",
                )
                filter_by_location = "All locations" not in selected_locations
            else:
                selected_locations = []
                filter_by_location = False
                st.text_input("Locations", value="No location data", disabled=True)

        # --- All-day vs timed toggle ---
        with filter_cols[2]:
            time_filter = st.selectbox(
                "Type",
                options=["All", "All-day", "Timed"],
                key="time_filter_select",
                help="Show all events, only all-day, or only timed events.",
            )

    # Apply filters
    filtered: list[dict] = []
    for e in events:
        ext = e.get("extendedProps", {})

        # Source filter
        source = ext.get("source", "")
        if source and source not in selected_sources:
            continue

        # Global keyword search (across all event text)
        if keyword:
            searchable = _event_searchable_text(e)
            if keyword not in searchable:
                continue

        # Location filter
        if filter_by_location:
            loc = ext.get("location", "").strip()
            if loc not in selected_locations:
                continue

        # All-day / timed filter
        if time_filter == "All-day" and not e.get("allDay", False):
            continue
        if time_filter == "Timed" and e.get("allDay", False):
            continue

        filtered.append(e)

    return filtered


# ---------------------------------------------------------------------------
# UI – Calendar page
# ---------------------------------------------------------------------------

def _apply_saved_view(view_data: dict):
    """Populate session state from a saved view and rerun."""
    if "sources" in view_data:
        st.session_state["source_filter"] = view_data["sources"]
        # Update select-all checkbox to match
        st.session_state["select_all_sources"] = False
    if "locations" in view_data:
        st.session_state["location_filter"] = view_data["locations"]
    if "time_filter" in view_data:
        st.session_state["time_filter_select"] = view_data["time_filter"]
    if "calendar_view" in view_data:
        st.session_state["calendar_view_select"] = view_data["calendar_view"]
    st.rerun()


def render_calendar():
    """Main calendar view."""
    config = load_config()

    n_sources = len(config.get("sheets", [])) + len(config.get("watch_folders", []))
    if n_sources == 0:
        st.info(
            "No data sources configured yet.  \n"
            "Go to **Manage Sources** to add your first file."
        )
        return

    # View selector stays above the calendar for quick access
    view = st.selectbox(
        "View",
        options=[
            "dayGridFourWeek",
            "dayGridMonth",
            "dayGridWeek",
            "dayGridDay",
            "listMonth",
            "listWeek",
        ],
        key="calendar_view_select",
        format_func=lambda v: {
            "dayGridMonth": "Month",
            "dayGridFourWeek": "4 Weeks",
            "dayGridWeek": "Week",
            "dayGridDay": "Day",
            "listMonth": "List (Month)",
            "listWeek": "List (Week)",
        }.get(v, v),
    )

    # Load all events (cached; also returns fingerprint for downstream caches)
    all_events, fingerprint = build_events(config)

    # Apply filters (includes Saved Views inside the expander)
    events = filter_events(all_events, config)

    # Trim events to a reasonable time window to reduce rendering load
    cutoff_past = (date.today() - timedelta(days=90)).isoformat()
    cutoff_future = (date.today() + timedelta(days=365)).isoformat()
    windowed_events = [
        e for e in events
        if cutoff_past <= e.get("start", "")[:10] <= cutoff_future
    ]

    data_file_paths = tuple(s.get("file_path", "") for s in config.get("sheets", []))
    last_refresh = latest_data_refresh(data_file_paths)
    version_tag = f"  ·  deploy {GIT_HASH}" if GIT_HASH else ""
    st.caption(
        f"Showing {len(windowed_events)} of {len(all_events)} events "
        f"from {n_sources} source(s)  ·  data refreshed {last_refresh}{version_tag}"
    )

    if len(windowed_events) < len(events):
        st.caption(
            f"_{len(events) - len(windowed_events)} older/future events outside "
            f"the 3-month to 12-month window are hidden._"
        )

    # --- Sidebar: Save current view ---
    with st.sidebar.expander("Save current view", expanded=False):
        new_view_name = st.text_input(
            "View name",
            placeholder="e.g. Conferences Only",
            key="new_view_name_input",
        )
        if st.button("Save", key="save_view_btn", use_container_width=True):
            if new_view_name.strip():
                view_data = {
                    "sources": st.session_state.get("source_filter", []),
                    "locations": st.session_state.get("location_filter", ["All locations"]),
                    "time_filter": st.session_state.get("time_filter_select", "All"),
                    "calendar_view": st.session_state.get("calendar_view_select", "dayGridMonth"),
                }
                save_view(new_view_name.strip(), view_data)
                st.success(f"Saved view \"{new_view_name.strip()}\"")
                st.rerun()
            else:
                st.warning("Please enter a name for the view.")

    # --- Sidebar: Legend ---
    legend_items: list[tuple[str, str]] = []
    for idx, s in enumerate(config.get("sheets", [])):
        legend_items.append((
            s.get("name", f"Source {idx+1}"),
            s.get("default_color", DEFAULT_COLORS[idx % len(DEFAULT_COLORS)]),
        ))
    for wf in config.get("watch_folders", []):
        legend_items.append((wf.get("name", "Folder"), wf.get("default_color", "#3788d8")))

    with st.sidebar.expander("Legend", expanded=False):
        for name, color in legend_items:
            st.markdown(
                f'<span style="display:inline-block;width:14px;height:14px;'
                f'background:{color};border-radius:3px;margin-right:6px;vertical-align:middle;">'
                f"</span> {name}",
                unsafe_allow_html=True,
            )

    # Build a lookup dict for full event details (keyed by title+start);
    # send only slim events to the calendar to reduce serialization overhead.
    event_detail_lookup: dict[str, dict] = {}
    slim_events: list[dict] = []
    for e in windowed_events:
        key = f"{e.get('title', '')}|{e.get('start', '')}"
        event_detail_lookup[key] = e.get("extendedProps", {})
        slim = {
            "title": e.get("title", ""),
            "start": e.get("start", ""),
            "allDay": e.get("allDay", True),
            "color": e.get("color", ""),
        }
        if e.get("end"):
            slim["end"] = e["end"]
        # Keep only source name for identification on click
        slim["extendedProps"] = {"source": e.get("extendedProps", {}).get("source", "")}
        slim_events.append(slim)

    st.session_state["_event_detail_lookup"] = event_detail_lookup

    @st.fragment
    def _calendar_fragment():
        four_week_start = None
        if view == "dayGridFourWeek":
            _today = date.today()
            days_since_monday = _today.weekday()
            last_monday = _today - timedelta(days=days_since_monday + 7)
            four_week_start = last_monday.isoformat()

        calendar_options = {
            "initialView": view,
            "headerToolbar": {
                "left": "prev,next today",
                "center": "title",
                "right": "",
            },
            "editable": False,
            "selectable": False,
            "navLinks": True,
            "height": 650,
            "displayEventTime": False,
            "views": {
                "dayGridFourWeek": {
                    "type": "dayGrid",
                    "duration": {"weeks": 4},
                },
            },
        }
        if four_week_start:
            calendar_options["initialDate"] = four_week_start

        custom_css = """
            .fc-event-past { opacity: 0.7; }
            .fc-event { cursor: pointer; padding: 2px 4px; border-radius: 3px; }
            .fc-toolbar-title { font-size: 1.3em !important; }
        """

        state = calendar(
            events=slim_events,
            options=calendar_options,
            custom_css=custom_css,
            key=f"main_cal_{view}",
        )

        if state and state.get("eventClick"):
            evt = state["eventClick"]["event"]
            # Look up full details from the stored lookup
            lookup_key = f"{evt.get('title', '')}|{evt.get('start', '')}"
            lookup = st.session_state.get("_event_detail_lookup", {})
            ext = lookup.get(lookup_key, evt.get("extendedProps", {}))

            st.markdown('<div id="event-detail"></div>', unsafe_allow_html=True)
            st.divider()
            st.subheader(evt.get("title", "Event Details"))
            detail_cols = st.columns(2)
            with detail_cols[0]:
                start_str = evt.get("start", "")
                st.markdown(f"**Start:** {start_str}")
                if evt.get("end"):
                    st.markdown(f"**End:** {evt['end']}")
            with detail_cols[1]:
                if ext.get("location"):
                    st.markdown(f"**Location:** {ext['location']}")
                if ext.get("source"):
                    source_text = ext["source"]
                    if ext.get("source_url"):
                        source_text = f"[{ext['source']}]({ext['source_url']})"
                    st.markdown(f"**Source:** {source_text}")
                elif ext.get("source_url"):
                    st.markdown(f"**Source:** [Open original sheet]({ext['source_url']})")
            if ext.get("description"):
                st.markdown(f"**Description:** {ext['description']}")

            if ext.get("custom"):
                for cf_label, cf_value in ext["custom"].items():
                    st.markdown(f"**{cf_label}:** {cf_value}")

            import streamlit.components.v1 as components
            _scroll_ts = int(time.time() * 1000)
            components.html(
                f"""<script>
                /* {_scroll_ts} */
                const el = window.parent.document.getElementById('event-detail');
                if (el) el.scrollIntoView({{behavior: 'smooth', block: 'start'}});
                </script>""",
                height=0,
            )

    _calendar_fragment()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _check_password() -> bool:
    """Show a password prompt and return True if the user is authenticated."""
    if st.session_state.get("authenticated"):
        return True

    st.title("Sheets Calendar")
    st.markdown("---")
    with st.form("app_password_form"):
        pwd = st.text_input("Enter password to access this app", type="password", key="_app_pwd")
        submitted = st.form_submit_button("Submit")
    if submitted:
        if pwd == st.secrets.get("app_password", ""):
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    st.stop()
    return False  # unreachable, but keeps the type checker happy


def main():
    st.set_page_config(
        page_title="Sheets Calendar",
        page_icon="📅",
        layout="wide",
    )

    # --- Password gate (blocks everything until authenticated) ---
    app_password = st.secrets.get("app_password", "")
    if app_password:
        _check_password()

    # Auto-recovery watchdog: if the app stalls (grey overlay) for more than
    # 30 seconds, automatically reload the page to reconnect.
    import streamlit.components.v1 as components
    components.html("""<script>
    (function() {
        if (window._stWatchdog) return;
        window._stWatchdog = true;
        var staleTimer = null;
        var observer = new MutationObserver(function() {
            var running = window.parent.document.querySelector(
                '[data-testid="stStatusWidget"]'
            );
            if (running && running.offsetParent !== null) {
                if (!staleTimer) {
                    staleTimer = setTimeout(function() {
                        window.parent.location.reload();
                    }, 45000);
                }
            } else {
                if (staleTimer) { clearTimeout(staleTimer); staleTimer = null; }
            }
        });
        observer.observe(window.parent.document.body, {
            childList: true, subtree: true, attributes: true
        });
    })();
    </script>""", height=0)

    # Admin unlock via secret password (stored in Streamlit secrets, not in code)
    if "admin_unlocked" not in st.session_state:
        st.session_state["admin_unlocked"] = False

    # Navigation — admin pages only visible when unlocked
    if st.session_state["admin_unlocked"]:
        nav_options = ["Calendar", "Manage Sources", "Automation"]
    else:
        nav_options = ["Calendar"]

    page = st.sidebar.radio(
        "Navigation",
        options=nav_options,
        index=0,
    )

    # Admin login / logout in sidebar
    st.sidebar.divider()
    if st.session_state["admin_unlocked"]:
        if st.sidebar.button("Lock admin", key="admin_lock"):
            st.session_state["admin_unlocked"] = False
            st.rerun()
    else:
        admin_password = st.secrets.get("ADMIN_PASSWORD", None)
        if admin_password:
            with st.sidebar.expander("Admin login"):
                with st.form("admin_login_form"):
                    pwd = st.text_input("Password", type="password", key="admin_pwd_input")
                    submitted = st.form_submit_button("Unlock")
                if submitted:
                    if pwd == admin_password:
                        st.session_state["admin_unlocked"] = True
                        st.rerun()
                    else:
                        st.error("Incorrect password.")

    st.sidebar.divider()

    # Quick stats
    config = load_config()
    n_files = len(config.get("sheets", []))
    st.sidebar.metric("File sources", n_files)
    if not IS_CLOUD:
        n_folders = len(config.get("watch_folders", []))
        st.sidebar.metric("Watched folders", n_folders)

    # Sync from refresh folder on every page load (local mode only)
    if not IS_CLOUD and config.get("refresh_folder", "").strip():
        n_synced = sync_refresh_folder(config)
        if n_synced:
            st.sidebar.success(f"Synced {n_synced} source(s) from refresh folder")

    # Auto-refresh (local mode only — cloud redeploys on git push)
    if not IS_CLOUD:
        st.sidebar.divider()
        auto_refresh = st.sidebar.toggle("Auto-refresh", value=False)
        if auto_refresh:
            interval = st.sidebar.slider(
                "Refresh interval (minutes)", min_value=1, max_value=60, value=5
            )
            st.sidebar.caption(f"Page will reload every {interval} min to pick up file changes.")
            if "_last_refresh" not in st.session_state:
                st.session_state["_last_refresh"] = time.time()
            elapsed = time.time() - st.session_state["_last_refresh"]
            if elapsed >= interval * 60:
                st.session_state["_last_refresh"] = time.time()
                st.rerun()
            else:
                remaining = int(interval * 60 - elapsed)
                st.sidebar.caption(f"Next refresh in ~{remaining}s")
                time.sleep(0)
                st.empty()

    if page == "Calendar":
        render_calendar()
    elif page == "Manage Sources":
        render_manage_sheets()
    else:
        render_apps_script()


if __name__ == "__main__":
    main()
