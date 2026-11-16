"""GoldenCheetah OpenData (GCOD) as released on OSF: reading athlete zips and turning them into
the tables declared in dataset.toml.

Pure functions over one athlete zip; no I/O besides reading the zip. migrate.py runs them over all
athletes. Needs polars and pyarrow (declared by migrate.py).

Downloaded zips are untrusted input: they are never extracted, members are read by name, and
anything unexpected raises GcodError instead of being guessed at. Every quirk handled here was
found in the sample (research/reports/03, 04) and has a test in tests/.
"""

from __future__ import annotations

import io
import json
import re
import zipfile
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import polars as pl

# Bump when the output for the same input changes; migrate.py then rebuilds affected files.
CONVERTER_VERSION = "0.3.0"

ATHLETE_ID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
# Activity CSV member names: the local start time YYYY_MM_DD_HH_MM_SS, usually + `.csv`. GoldenCheetah
# sometimes kept the original file's extension: `.fit`, `.fit.csv`, `.pwx`, `.csv.bak.csv` (a backup
# version next to the `.csv`), ... These are CSVs all the same (the header is checked) and are kept
# as separate activities; their activity_id is the name without a final `.csv`.
ACTIVITY_MEMBER_RE = re.compile(r"^(\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2})((?:\.[A-Za-z0-9]+)*)$")

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


class GcodError(Exception):
    """The zip contains something this converter doesn't know how to handle."""


# --- reading a zip -------------------------------------------------------------------
@dataclass
class AthleteZip:
    athlete_id: str
    json_text: str
    meta: dict
    json_repaired: bool            # the JSON needed parse_gc_json's repair
    activity_members: list[str]   # CSV members, sorted by activity_id
    _zf: zipfile.ZipFile

    def read(self, member: str) -> bytes:
        return self._zf.read(member)

    def close(self) -> None:
        self._zf.close()

    def __enter__(self) -> "AthleteZip":
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
    seen: dict[str, zipfile.ZipInfo] = {}
    for info in zf.infolist():
        if info.is_dir():
            continue
        # Some zips hold the same member twice. Byte-identical copies are one member; copies that
        # differ can't be told apart by name, so that fails loudly.
        if (first := seen.get(info.filename)) is not None:
            if (first.CRC, first.file_size) == (info.CRC, info.file_size) and \
                    zf.open(first).read() == zf.open(info).read():
                continue
            zf.close()
            raise GcodError(f"member {info.filename} occurs twice with different content")
        seen[info.filename] = info
        base = info.filename.rsplit("/", 1)[-1]
        if base.endswith(".json"):
            jsons.append(info.filename)
        elif ACTIVITY_MEMBER_RE.match(base):
            activities.append(info.filename)
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
        meta, repaired = parse_gc_json(json_text)
    except GcodError as e:
        zf.close()
        raise GcodError(f"invalid JSON {jsons[0]}: {e}") from e
    return AthleteZip(athlete_id, json_text, meta, repaired, sorted(activities, key=activity_id), zf)


# GoldenCheetah writes one `"key":"value"` per line and did not escape double quotes inside free-text
# values (e.g. `"sport":"... "Spring is coming!""`). Only this known bug is repaired; the original
# text is kept unchanged (raw_json), and athletes.json_repaired records that it happened.
_STRING_LINE = re.compile(r'^(\s*"[^"\\]+"\s*:\s*")(.*)("\s*,?\s*)$')


def parse_gc_json(text: str) -> tuple[dict, bool]:
    """(parsed JSON, whether it needed the unescaped-quotes repair)."""
    try:
        return json.loads(text), False
    except json.JSONDecodeError as first:
        lines = []
        for line in text.split("\n"):
            m = _STRING_LINE.match(line)
            if m and re.search(r'(?<!\\)"', m.group(2)):
                line = m.group(1) + re.sub(r'(?<!\\)"', r'\\"', m.group(2)) + m.group(3)
            lines.append(line)
        try:
            return json.loads("\n".join(lines)), True
        except json.JSONDecodeError:
            raise GcodError(str(first)) from first


def activity_id(member: str) -> str:
    """activity_id of a CSV member: its base name without a trailing `.csv` (keeps `.fit`)."""
    return member.rsplit("/", 1)[-1].removesuffix(".csv")


def utc_activity_id(t: datetime) -> str:
    """activity_id of an activity without a CSV: its UTC start time followed by `Z`."""
    return t.strftime("%Y_%m_%d_%H_%M_%S") + "Z"


def parse_utc(s: str) -> datetime | None:
    try:
        return datetime.strptime(s, "%Y/%m/%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def parse_local(stem: str) -> datetime | None:
    m = ACTIVITY_MEMBER_RE.match(stem)
    if not m:
        return None
    return datetime.strptime(m.group(1), "%Y_%m_%d_%H_%M_%S").replace(tzinfo=timezone.utc)


def match_rides(rides_utc: list[datetime | None], csv_ids: list[str]) -> list[tuple[str | None, int | None]]:
    """For each JSON ride: (activity_id of its CSV, local-minus-UTC offset in seconds), or (None, None)."""
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
                cands.append((abs(off), i, s, off))
    cands.sort()
    used_r, used_c, out = set(), set(), {}
    for _, i, s, off in cands:
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
    seen: dict[str, str] = {}
    for m in az.activity_members:
        aid = activity_id(m)
        if aid in seen:
            raise GcodError(f"two CSVs map to activity_id {aid}: {seen[aid]}, {m}")
        seen[aid] = m
        raw = az.read(m)
        header = raw.split(b"\n", 1)[0].strip().decode("utf-8", "replace").split(",")
        if header != CSV_HEADER:
            raise GcodError(f"{m}: header {header}, expected {CSV_HEADER}")
        try:
            df = pl.read_csv(io.BytesIO(raw), schema={c: pl.Float64 for c in CSV_HEADER})
        except Exception as e:  # noqa: BLE001 - polars raises several types; report them all the same way
            raise GcodError(f"{m}: unreadable CSV ({type(e).__name__}: {e})") from e
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


def build_activities(az: AthleteZip, kinds: dict[str, str], samples: dict[str, int]) -> pl.DataFrame:
    """One row per activity: every JSON ride, plus CSVs that no ride links to.

    `samples` is the number of time series rows per activity_id (from the time series pass);
    has_timeseries means at least one row.
    """
    meta = az.meta or {}
    rides = unique_rides(meta.get("RIDES", []))
    csv_ids = [activity_id(m) for m in az.activity_members]
    utc = [parse_utc(r.get("date", "")) for r in rides]
    links = match_rides(utc, csv_ids)
    rows, linked = [], set()
    for r, u, (aid, off) in zip(rides, utc, links):
        if aid is None:
            if u is None:
                raise GcodError(f"ride without CSV and without a valid date: {r.get('date')!r}")
            aid = utc_activity_id(u)
        else:
            linked.add(aid)
        row = {
            "athlete_id": az.athlete_id, "activity_id": aid, "start_time_utc": u,
            "utc_offset_minutes": None if off is None else off // 60,
            "sport": r.get("sport"), "data_flags": r.get("data"),
            "xdata": json.dumps(r["XDATA"], sort_keys=True) if "XDATA" in r else None,
            "has_timeseries": samples.get(aid, 0) > 0,
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
    for aid in csv_ids:  # CSVs that no JSON ride links to
        if aid not in linked:
            rows.append({"athlete_id": az.athlete_id, "activity_id": aid, "has_timeseries": samples.get(aid, 0) > 0})
    df = pl.DataFrame(rows, schema=activities_schema(kinds), strict=False)
    dup = df.filter(pl.col("activity_id").is_duplicated())
    if dup.height:
        raise GcodError(f"duplicate activity_id {dup['activity_id'].unique().to_list()[:5]}")
    return df.sort("start_time_utc", "activity_id", nulls_last=True)


def unique_rides(rides: list[dict]) -> list[dict]:
    """Rides without byte-for-byte duplicates (some JSONs list a ride twice). Two different rides
    with the same start time can't both be kept under one key: that fails loudly."""
    out, by_date = [], {}
    for r in rides:
        key = json.dumps(r, sort_keys=True)
        date = r.get("date")
        if date in by_date:
            if by_date[date] == key:
                continue
            raise GcodError(f"two different rides start at {date}")
        by_date[date] = key
        out.append(r)
    return out


def athlete_row(az: AthleteZip, n_activities: int, n_timeseries: int) -> dict:
    ath = (az.meta or {}).get("ATHLETE", {})
    yob = str(ath.get("yob", ""))
    return {
        "athlete_id": az.athlete_id,
        "gender": ath.get("gender") or None,
        "yob": int(yob) if yob.isdigit() else None,
        "json_athlete_id": (ath.get("id") or "").strip("{}") or None,
        "gc_json_version": (az.meta or {}).get("VERSION"),
        "json_repaired": az.json_repaired,
        "n_activities": n_activities,
        "n_timeseries": n_timeseries,
    }


def form_counter(forms: dict[str, set[str]]) -> Counter:
    return Counter(f for fs in forms.values() for f in fs)
