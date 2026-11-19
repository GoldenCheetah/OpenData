"""GoldenCheetah OpenData (GCOD) as released on OSF: reading athlete zips and turning them into
the tables declared in dataset.toml.

Pure functions over one athlete zip; no I/O besides reading the zip. migrate.py runs them over all
athletes. Needs polars and pyarrow (declared by migrate.py).

Downloaded zips are untrusted input: they are never extracted, and anything unexpected raises
GcodError instead of being guessed at. Every quirk handled here was found in the release
(research/reports/03, 04, 13) and has a test in tests/.
"""

from __future__ import annotations

import io
import json
import re
import unicodedata
import zipfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

# Bump when the output for the same input changes; migrate.py then rebuilds affected files.
CONVERTER_VERSION = "0.4.0"

ATHLETE_ID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
# Activity CSV member names: the local start time YYYY_MM_DD_HH_MM_SS, usually + `.csv`. GoldenCheetah
# sometimes kept the original file's extension: `.fit`, `.fit.csv`, `.pwx`, `.csv.bak.csv` (a backup
# version next to the `.csv`), `.sync-conflict-20180127-150031-75VG4DH.csv` (a file sync tool's
# copy of a conflicting version), ... These are CSVs all the same (the header is checked) and are
# kept as separate activities; their activity_id is the name without a final `.csv`.
ACTIVITY_MEMBER_RE = re.compile(r"^(\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2})((?:\.[A-Za-z0-9-]+)*)$")
START_RE = re.compile(r"^\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2}")

# CSV column -> output column; km -> m. Order is the order in timeseries.
CSV_HEADER = ["secs", "km", "power", "hr", "cad", "alt"]
TS_COLUMNS = {"secs": "time_s", "km": "distance_m", "power": "power_w", "hr": "hr_bpm",
              "cad": "cadence_rpm", "alt": "altitude_m"}
TS_SCALE = {"km": 1000.0}
F32_MIN_NORMAL = 1.1754943508222875e-38
F32_MAX = 3.4028234663852886e38

# Metrics that are variable-length arrays even when one ride happens to have 2 elements.
LIST_METRICS = {
    "power_mmp", "speed_dist", "speed_dist_bins", "heart.rate_dist", "heart.rate_dist_bins",
    "cadence_dist", "cadence_dist_bins", "power_dist", "power_dist_bins", "SDNN", "SDANN",
}

# Linking JSON rides (UTC `date`) to CSVs (local start time in the name): same mm:ss, offset a
# whole quarter-hour within +-14 h; greedy by smallest |offset|. Links 99.9% in the sample.
MAX_OFFSET_S = 14 * 3600
# A ride "fits" a CSV when its duration and distance metrics agree with the CSV's last `secs` and
# `km` within these tolerances (absolute, or relative, whichever is larger).
FIT_TIME_S, FIT_DISTANCE_KM, FIT_RELATIVE = 2.0, 0.01, 0.001


class GcodError(Exception):
    """The zip contains something this converter doesn't know how to handle."""


# --- reading a zip -------------------------------------------------------------------
@dataclass(frozen=True)
class ActivityMember:
    """One activity CSV in a zip. Members are identified by their ZipInfo, not their name: a zip
    can hold two different files under one name."""
    activity_id: str
    info: zipfile.ZipInfo


@dataclass
class AthleteZip:
    athlete_id: str
    json_text: str
    meta: dict
    json_repairs: tuple[str, ...]          # the repairs parse_gc_json needed (usually none)
    activity_members: list[ActivityMember]  # sorted by activity_id
    _zf: zipfile.ZipFile

    def read(self, member: ActivityMember) -> bytes:
        with self._zf.open(member.info) as f:
            return f.read()

    def close(self) -> None:
        self._zf.close()

    def __enter__(self) -> AthleteZip:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


def open_athlete_zip(path: Path) -> AthleteZip:
    """Open an athlete zip without extracting it; raise GcodError on anything unexpected."""
    athlete_id = path.stem
    if not ATHLETE_ID_RE.match(athlete_id):
        raise GcodError(f"zip name {path.name!r} is not an athlete UUID")
    zf = zipfile.ZipFile(path)
    jsons, activities, other = [], [], []
    seen: dict[str, list[zipfile.ZipInfo]] = {}
    for info in zf.infolist():
        if info.is_dir():
            continue
        # Some zips hold a member twice. Byte-identical copies are one member. Copies that differ
        # are two versions of the activity (two files with the same start time in the athlete's
        # library, exported under one name) and are both kept, in zip order.
        same_name = seen.setdefault(info.filename, [])
        if any(_same_content(zf, info, earlier) for earlier in same_name):
            continue
        same_name.append(info)
        base = info.filename.rsplit("/", 1)[-1]
        if base.endswith(".json"):
            jsons.append(info.filename)
        elif ACTIVITY_MEMBER_RE.match(base):
            activities.append(info)
        else:
            other.append(info.filename)
    if other:
        zf.close()
        raise GcodError(f"unexpected members {other[:5]}")
    if len(jsons) > 1:
        zf.close()
        raise GcodError(f"more than one JSON: {jsons[:5]}")
    if not jsons:
        zf.close()
        raise GcodError("no JSON in zip")
    try:
        json_text = zf.read(jsons[0]).decode("utf-8")
    except UnicodeDecodeError as e:
        zf.close()
        raise GcodError(f"JSON {jsons[0]} is not UTF-8: {e}") from e
    try:
        meta, repairs = parse_gc_json(json_text)
    except GcodError as e:
        zf.close()
        raise GcodError(f"invalid JSON {jsons[0]}: {e}") from e
    ids = unique_ids([activity_id(info.filename) for info in activities])
    members = sorted((ActivityMember(i, info) for i, info in zip(ids, activities, strict=True)),
                     key=lambda m: m.activity_id)
    return AthleteZip(athlete_id, json_text, meta, repairs, members, zf)


def _same_content(zf: zipfile.ZipFile, a: zipfile.ZipInfo, b: zipfile.ZipInfo) -> bool:
    if (a.CRC, a.file_size) != (b.CRC, b.file_size):
        return False
    with zf.open(a) as fa, zf.open(b) as fb:
        return fa.read() == fb.read()


def unique_ids(ids: list[str]) -> list[str]:
    """Ids made unique in order: the n-th occurrence (n >= 2) of an id gets `~n`."""
    count: dict[str, int] = {}
    out = []
    for i in ids:
        count[i] = count.get(i, 0) + 1
        out.append(i if count[i] == 1 else f"{i}~{count[i]}")
    return out


# GoldenCheetah writes its JSON by hand, one `"key":value` per line, and has known bugs. A JSON that
# doesn't parse is repaired by these, each applied only where its pattern occurs; the original text
# is kept unchanged (raw_json), and athletes.json_repairs records which repairs were needed.
#   unescaped_quotes     double quotes inside free text not escaped (`"sport":"... "Spring!""`),
#                        also where the text runs over several lines
#   empty_first_metric   a metric left out at the start of METRICS, leaving `{` followed by `,`
#   control_characters   tabs and line breaks inside free text not escaped (parsed as they are)
_STRING_START = re.compile(r'^(\s*"[^"\\]+"\s*:\s*")(.*)$', re.S)
_STRING_LINE = re.compile(r'^(\s*"[^"\\]+"\s*:\s*")(.*)("\s*,?\s*)$', re.S)
_EMPTY_FIRST_METRIC = re.compile(r'("METRICS"\s*:\s*\{[ \t]*\r?\n)[ \t]*,[ \t]*\r?\n')
_KEY_OR_BRACKET = re.compile(r'^\s*("[^"\\]+"\s*:|[\[\]{}],?\s*$)')  # JSON structure, never free text
_MAX_STRING_LINES = 50


def _repair_unescaped_quotes(text: str) -> str:
    lines, out, i = text.split("\n"), [], 0
    while i < len(lines):
        line, j = lines[i], i + 1
        if _STRING_START.match(line) and not _STRING_LINE.match(line):
            # A string value that doesn't end on its line continues on the next lines of free text,
            # up to the line where it ends; it never takes in a line that is JSON structure.
            for k in range(i + 1, min(len(lines), i + _MAX_STRING_LINES)):
                if _KEY_OR_BRACKET.match(lines[k]):
                    break
                if _STRING_LINE.match(joined := "\n".join(lines[i:k + 1])):
                    line, j = joined, k + 1
                    break
        if (m := _STRING_LINE.match(line)) and re.search(r'(?<!\\)"', m.group(2)):
            line = m.group(1) + re.sub(r'(?<!\\)"', r'\\"', m.group(2)) + m.group(3)
        out.append(line)
        i = j
    return "\n".join(out)


def _repair_empty_first_metric(text: str) -> str:
    return _EMPTY_FIRST_METRIC.sub(r"\1", text)


JSON_REPAIRS: dict[str, Callable[[str], str]] = {
    "unescaped_quotes": _repair_unescaped_quotes,
    "empty_first_metric": _repair_empty_first_metric,
}


def parse_gc_json(text: str) -> tuple[dict, tuple[str, ...]]:
    """(parsed JSON, the repairs it needed: names from JSON_REPAIRS and `control_characters`)."""
    try:
        return json.loads(text), ()
    except json.JSONDecodeError as first:
        error = first
    applied = []
    for name, repair in JSON_REPAIRS.items():
        repaired = repair(text)
        if repaired != text:
            applied.append(name)
            text = repaired
    try:
        return json.loads(text), tuple(applied)
    except json.JSONDecodeError:
        pass
    try:
        return json.loads(text, strict=False), (*applied, "control_characters")
    except json.JSONDecodeError:
        raise GcodError(str(error)) from error


def activity_id(member: str) -> str:
    """activity_id of a CSV member: its base name without a trailing `.csv` (keeps `.fit`)."""
    return member.rsplit("/", 1)[-1].removesuffix(".csv")


def utc_activity_id(t: datetime) -> str:
    """activity_id of an activity without a CSV: its UTC start time followed by `Z`."""
    return t.strftime("%Y_%m_%d_%H_%M_%S") + "Z"


def ascii_digits(s: str) -> str:
    """Digits of any script as ASCII digits (some dates are written in Thai digits: `๒๐๑๘/๐๓/๐๗`)."""
    return "".join(str(unicodedata.decimal(c)) if c.isdecimal() else c for c in s)


def parse_utc(s: str) -> datetime | None:
    try:
        return datetime.strptime(ascii_digits(s), "%Y/%m/%d %H:%M:%S UTC").replace(tzinfo=UTC)
    except (TypeError, ValueError):
        return None


def parse_local(activity_id: str) -> datetime | None:
    """The local start time an activity_id begins with (None for anything else)."""
    m = START_RE.match(activity_id)
    if not m:
        return None
    return datetime.strptime(m.group(0), "%Y_%m_%d_%H_%M_%S").replace(tzinfo=UTC)


def match_rides(rides_utc: list[datetime | None], csv_ids: list[str],
                fits: Callable[[int, str], bool] | None = None) -> list[tuple[str | None, int | None]]:
    """For each JSON ride: (activity_id of its CSV, local-minus-UTC offset in seconds), or (None, None).

    One CSV per ride and one ride per CSV, smallest |offset| first. Where that ties (two rides with
    the same start time, or two CSVs with the same name), a ride that `fits(ride, csv)` comes
    first, then JSON order and activity_id order."""
    local = {s: parse_local(s) for s in csv_ids}
    cands = []
    for i, u in enumerate(rides_utc):
        if u is None:
            continue
        for s, lt in local.items():
            if lt is None:
                continue
            off = int((lt - u).total_seconds())
            if abs(off) <= MAX_OFFSET_S and off % 900 == 0:
                cands.append((abs(off), not (fits and fits(i, s)), i, s, off))
    cands.sort()
    used_r, used_c, out = set(), set(), {}
    for _, _, i, s, off in cands:
        if i in used_r or s in used_c:
            continue
        used_r.add(i)
        used_c.add(s)
        out[i] = (s, off)
    return [out.get(i, (None, None)) for i in range(len(rides_utc))]


# --- time series ---------------------------------------------------------------------
SOURCE_SCHEMA = {"athlete_id": pl.String, "activity_id": pl.String, "row_in_activity": pl.Int32,
                 **{c: pl.Float64 for c in CSV_HEADER}}


def iter_source_activities(az: AthleteZip) -> Iterator[tuple[str, pl.DataFrame]]:
    """(activity_id, rows) for each CSV of one athlete, as Float64 (lossless w.r.t. the text), in
    activity_id order.

    One activity at a time, so memory is bounded by the largest activity (~228k rows in the
    sample), not by the athlete (up to ~30M rows). Columns: SOURCE_SCHEMA.
    """
    for m in az.activity_members:
        aid, name = m.activity_id, m.info.filename
        raw = az.read(m)
        header = raw.split(b"\n", 1)[0].strip().decode("utf-8", "replace").split(",")
        if header != CSV_HEADER:
            raise GcodError(f"{name}: header {header}, expected {CSV_HEADER}")
        try:
            df = pl.read_csv(io.BytesIO(raw), schema={c: pl.Float64 for c in CSV_HEADER})
        except Exception as e:  # noqa: BLE001 - polars raises several types; report them all the same way
            raise GcodError(f"{name}: unreadable CSV ({type(e).__name__}: {e})") from e
        del raw
        yield aid, df.with_columns(
            athlete_id=pl.lit(az.athlete_id), activity_id=pl.lit(aid),
            row_in_activity=pl.int_range(pl.len(), dtype=pl.Int32),
        ).select(list(SOURCE_SCHEMA))


EXCEPTIONS_SCHEMA = {
    "athlete_id": pl.String, "activity_id": pl.String, "row_in_activity": pl.Int32,
    "column": pl.String, "source_column": pl.String, "source_value": pl.Float64,
}


def to_output_timeseries(src: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Float64 source -> (Float32 timeseries, exceptions).

    Float32 holds every value GoldenCheetah wrote (<= 6 significant digits) exactly. Values
    outside Float32's normal range (sub-normal junk such as 4.94066e-324) can't be held: they are
    written as 0 and listed, with their exact value, in the exceptions table.
    """
    exc = []
    for c, out in TS_COLUMNS.items():
        a = pl.col(c).abs()
        bad = pl.col(c).is_not_null() & (a != 0) & ((a < F32_MIN_NORMAL) | (a > F32_MAX))
        e = src.filter(bad)
        if e.height:
            exc.append(e.select("athlete_id", "activity_id", "row_in_activity", column=pl.lit(out),
                                source_column=pl.lit(c), source_value=pl.col(c)))
    exceptions = pl.concat(exc) if exc else pl.DataFrame(schema=EXCEPTIONS_SCHEMA)

    # Casting turns sub-normal values into (signed) 0; values beyond Float32's range would become inf.
    out = src.select("athlete_id", "activity_id",
                     *[(pl.col(c) * TS_SCALE.get(c, 1.0)).cast(pl.Float32).alias(o) for c, o in TS_COLUMNS.items()])
    return out, exceptions.sort("activity_id", "row_in_activity", "column")


def cast_mismatches(src: pl.DataFrame, out: pl.DataFrame) -> int:
    """Values in `out` that are not the Float32 of the source value (should be 0). Vectorised.

    Together with GoldenCheetah's <= 6 significant digits (research/reports/09) this means the
    text is preserved; `same_as_source` checks the text itself, but is much slower.
    """
    n = 0
    for c, o in TS_COLUMNS.items():
        want = (src[c] * TS_SCALE.get(c, 1.0)).cast(pl.Float32)
        got = out[o]
        same = want.eq_missing(got) | (want.is_nan() & got.is_nan()).fill_null(False)
        n += int((~same).sum())
    return n


def same_as_source(src: pl.DataFrame, out: pl.DataFrame, exceptions: pl.DataFrame) -> int:
    """Number of values in `out` that differ from the source CSV text (should be 0).

    Equal means: printing the Float32 value with 6 significant digits gives the source number
    (distance scaled to metres); exception values are 0 in `out` and equal in `exceptions`.
    """
    bad = 0
    exc_rows = set(exceptions.select("activity_id", "row_in_activity", "column").iter_rows())
    for c, o in TS_COLUMNS.items():
        scale = TS_SCALE.get(c, 1.0)
        pairs = pl.DataFrame({"activity_id": src["activity_id"], "row": src["row_in_activity"],
                              "a": src[c], "b": out[o]})
        nulls = pairs.filter(pl.col("a").is_null() != pl.col("b").is_null())
        bad += nulls.height
        vals = pairs.drop_nulls(["a", "b"])
        for a, b in vals.select("a", "b").unique().iter_rows():
            if float(f"{a * scale:.6g}") != float(f"{b:.6g}"):
                # an exception row: out is 0 and the exact value is in `exceptions`
                hits = vals.filter((pl.col("a") == a) & (pl.col("b") == b))
                bad += sum(1 for r in hits.select("activity_id", "row").iter_rows() if (*r, o) not in exc_rows)
    return bad


# --- metadata ------------------------------------------------------------------------
def metric_forms(meta: dict) -> dict[str, set[str]]:
    """Which forms each metric takes in one athlete's JSON: str, pair or list."""
    forms: dict[str, set[str]] = {}
    for r in meta.get("RIDES", []):
        for k, v in (r.get("METRICS") or {}).items():
            if isinstance(v, str):
                f = "str"
            elif isinstance(v, list) and len(v) == 2 and k not in LIST_METRICS:
                f = "pair"
            elif isinstance(v, list):
                f = "list"
            else:
                raise GcodError(f"metric {k!r} has unexpected value {v!r}")
            forms.setdefault(k, set()).add(f)
    return forms


def metric_kinds(forms: dict[str, set[str]]) -> dict[str, str]:
    """Column kind per metric over all observed forms: list > pair > scalar."""
    return {k: "list" if ("list" in fs or k in LIST_METRICS) else "pair" if "pair" in fs else "scalar"
            for k, fs in sorted(forms.items())}


ACTIVITY_FIXED_SCHEMA = {
    "athlete_id": pl.String, "activity_id": pl.String, "start_time_utc": pl.Datetime("us", "UTC"),
    "utc_offset_minutes": pl.Int16, "sport": pl.String, "data_flags": pl.String, "xdata": pl.String,
    "has_timeseries": pl.Boolean,
}


def activities_schema(kinds: dict[str, str]) -> dict[str, pl.DataType]:
    schema = dict(ACTIVITY_FIXED_SCHEMA)
    for k, kind in kinds.items():
        if kind == "list":
            schema[k] = pl.List(pl.Float64)
        else:
            schema[k] = pl.Float64
            if kind == "pair":
                schema[f"{k}__weight"] = pl.Float64
    return schema


def _float(v: Any) -> float | None:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def csv_summary(src: pl.DataFrame) -> dict:
    """What the metadata pass needs to know about one activity's CSV (stored in the manifest):
    its number of rows, and its last `secs` and `km` (to tell which ride it belongs to)."""
    return {"rows": src.height, "secs": src["secs"].max(), "km": src["km"].max()}


def _metric(ride: dict, name: str) -> float | None:
    v = (ride.get("METRICS") or {}).get(name)
    return _float(v[0] if isinstance(v, list) and v else v)


def ride_fits_csv(ride: dict, csv: dict | None) -> bool:
    """Whether a ride's duration and distance metrics describe this CSV."""
    if not csv or csv["secs"] is None:
        return False
    duration = _metric(ride, "workout_time")
    if duration is None or abs(duration - csv["secs"]) > max(FIT_TIME_S, FIT_RELATIVE * csv["secs"]):
        return False
    distance, km = _metric(ride, "total_distance"), csv["km"] or 0.0
    return abs((distance or 0.0) - km) <= max(FIT_DISTANCE_KM, FIT_RELATIVE * km)


def build_activities(az: AthleteZip, kinds: dict[str, str], csvs: dict[str, dict]) -> pl.DataFrame:
    """One row per activity: every JSON ride, plus CSVs that no ride links to.

    `csvs` has the csv_summary of each activity_id with a CSV (from the time series pass);
    has_timeseries means at least one row. A ride links to the CSV with its start time (see
    match_rides). Two rides with the same start time are both kept: one links to the CSV, the other
    has no CSV in the release and gets a `Z` id.
    """
    meta = az.meta or {}
    rides = unique_rides(meta.get("RIDES", []))
    csv_ids = [m.activity_id for m in az.activity_members]
    utc = [parse_utc(r.get("date", "")) for r in rides]
    links = match_rides(utc, csv_ids, fits=lambda i, s: ride_fits_csv(rides[i], csvs.get(s)))
    for r, u, (aid, _) in zip(rides, utc, links, strict=True):
        if aid is None and u is None:
            raise GcodError(f"ride without CSV and without a valid date: {r.get('date')!r}")
    z_ids = iter(unique_ids([utc_activity_id(u) for u, (aid, _) in zip(utc, links, strict=True) if aid is None]))
    rows = []
    for r, u, (aid, off) in zip(rides, utc, links, strict=True):
        row = {
            "athlete_id": az.athlete_id, "activity_id": aid or next(z_ids), "start_time_utc": u,
            "utc_offset_minutes": None if off is None else off // 60,
            "sport": r.get("sport"), "data_flags": r.get("data"),
            "xdata": json.dumps(r["XDATA"], sort_keys=True) if "XDATA" in r else None,
            "has_timeseries": csvs.get(aid, {}).get("rows", 0) > 0,
        }
        m = r.get("METRICS") or {}
        unknown = set(m) - set(kinds)
        if unknown:
            raise GcodError(f"metrics missing from the global schema: {sorted(unknown)[:5]}")
        for k, kind in kinds.items():
            v = m.get(k)
            if kind == "list":
                row[k] = None if v is None else [_float(x) for x in (v if isinstance(v, list) else [v])]
            elif kind == "pair":
                if isinstance(v, list):
                    row[k], row[f"{k}__weight"] = _float(v[0]), (_float(v[1]) if len(v) > 1 else None)
                else:
                    row[k], row[f"{k}__weight"] = _float(v), None
            else:
                row[k] = _float(v)
        rows.append(row)
    linked = {aid for aid, _ in links if aid}
    for aid in csv_ids:  # CSVs that no JSON ride links to
        if aid not in linked:
            rows.append({"athlete_id": az.athlete_id, "activity_id": aid,
                         "has_timeseries": csvs.get(aid, {}).get("rows", 0) > 0})
    df = pl.DataFrame(rows, schema=activities_schema(kinds), strict=False)
    dup = df.filter(pl.col("activity_id").is_duplicated())
    if dup.height:  # can't happen by construction; a guard against regressions
        raise GcodError(f"duplicate activity_id {dup['activity_id'].unique().to_list()[:5]}")
    return df.sort("start_time_utc", "activity_id", nulls_last=True)


def unique_rides(rides: list[dict]) -> list[dict]:
    """Rides without byte-for-byte duplicates (some JSONs list a ride twice), in JSON order."""
    seen: set[str] = set()
    out = []
    for r in rides:
        key = json.dumps(r, sort_keys=True)
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out


def athlete_row(az: AthleteZip, n_activities: int, n_timeseries: int) -> dict:
    ath = (az.meta or {}).get("ATHLETE", {})
    yob = ascii_digits(str(ath.get("yob", "")))
    return {
        "athlete_id": az.athlete_id,
        "gender": ath.get("gender") or None,
        "yob": int(yob) if yob.isascii() and yob.isdigit() else None,
        "json_athlete_id": (ath.get("id") or "").strip("{}") or None,
        "gc_json_version": (az.meta or {}).get("VERSION"),
        "json_repairs": list(az.json_repairs),
        "n_activities": n_activities,
        "n_timeseries": n_timeseries,
    }
