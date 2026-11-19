"""Shared helpers for working with GoldenCheetah OpenData (GCOD) as published on OSF.

Imported by the scripts in ``research/explore`` (frozen; production code is in datasets/opendata/) via::

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))
    import gcod

Only depends on the standard library + polars, which every script declares in its
inline (PEP 723) dependency block.

Downloaded zips are treated as untrusted data: they are never extracted to disk,
members are read into memory by name.
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
from typing import Iterator

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
OSF_ZIPS_DIR = DATA_DIR / "osf" / "athletes"     # downloaded athlete zips (untrusted)
OSF_LISTING = DATA_DIR / "osf" / "listing.parquet"
SAMPLE_FILE = DATA_DIR / "sample_athletes.parquet"
INTERIM_DIR = DATA_DIR / "interim"
REPORTS_DIR = ROOT / "research" / "reports"

OSF_NODE = "6hfpz"
OSF_API = "https://api.osf.io/v2"

# Activity CSV member names. Normally YYYY_MM_DD_HH_MM_SS.csv, but some zips contain
# YYYY_..._SS.fit (a CSV despite the name) and YYYY_..._SS.fit.csv, sometimes next to a
# plain .csv of the same start time with different content (reports/03). All are kept.
ACTIVITY_RE = re.compile(r"^(\d{4})_(\d{2})_(\d{2})_(\d{2})_(\d{2})_(\d{2})(\.fit)?(\.csv)?$")
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


@dataclass
class AthleteZip:
    athlete_id: str
    path: Path
    meta: dict | None                 # parsed {uuid}.json (None if missing/broken)
    activity_names: list[str]         # CSV member names matching ACTIVITY_RE
    other_names: list[str]            # anything else in the zip
    _zf: zipfile.ZipFile

    def read(self, name: str) -> bytes:
        return self._zf.read(name)

    def iter_activities(self) -> Iterator[tuple[str, bytes]]:
        for name in self.activity_names:
            yield name, self._zf.read(name)

    def close(self) -> None:
        self._zf.close()

    def __enter__(self) -> "AthleteZip":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def open_athlete_zip(path: Path) -> AthleteZip:
    """Open an OSF athlete zip without extracting it."""
    athlete_id = path.stem
    zf = zipfile.ZipFile(path)
    names = [n for n in zf.namelist() if not n.endswith("/")]
    meta = None
    activity_names: list[str] = []
    other: list[str] = []
    for n in names:
        base = n.rsplit("/", 1)[-1]
        if base.endswith(".json") and meta is None:
            try:
                meta = json.loads(zf.read(n))
            except Exception:
                other.append(n)
        elif ACTIVITY_RE.match(base):
            activity_names.append(n)
        else:
            other.append(n)
    return AthleteZip(athlete_id, path, meta, sorted(activity_names), other, zf)


def activity_stem(name: str) -> str:
    """activity_id: member basename without a trailing '.csv' (keeps '.fit' to stay unique)."""
    return name.rsplit("/", 1)[-1].removesuffix(".csv")


# Canonical in-memory schema of the raw time series (what a CSV reader would produce,
# with explicit types). Float64 is lossless w.r.t. the CSV text (see 07_verify_lossless).
TS_COLUMNS = ["secs", "km", "power", "hr", "cad", "alt"]
TS_CSV_SCHEMA = {
    "secs": "Float64",
    "km": "Float64",
    "power": "Float64",
    "hr": "Float64",
    "cad": "Float64",
    "alt": "Float64",
}


def load_athlete_timeseries(path: Path):
    """Read all activity CSVs of one athlete zip into a single polars DataFrame.

    Columns: athlete_id, activity_id (= CSV stem, local start time), secs, km, power, hr, cad, alt.
    Rows keep their original order (activity_id ascending, then file order).
    """
    import polars as pl

    schema = {k: getattr(pl, v) for k, v in TS_CSV_SCHEMA.items()}
    frames = []
    with open_athlete_zip(path) as az:
        for name, raw in az.iter_activities():
            df = pl.read_csv(raw, schema_overrides=schema, infer_schema=False)
            missing = [c for c in TS_COLUMNS if c not in df.columns]
            extra = [c for c in df.columns if c not in TS_COLUMNS]
            if missing or extra:
                raise ValueError(f"{path.name}:{name} unexpected header {df.columns}")
            frames.append(df.with_columns(activity_id=pl.lit(activity_stem(name))))
        aid = az.athlete_id
    if not frames:
        return None
    return (
        pl.concat(frames)
        .with_columns(athlete_id=pl.lit(aid))
        .select("athlete_id", "activity_id", *TS_COLUMNS)
    )


# --- linking JSON rides to activity CSVs -------------------------------------------
# The JSON ride `date` is UTC; the CSV filename is the *local* start time. We match a ride
# to the CSV with identical mm:ss whose offset is a whole quarter-hour within +-14 h,
# greedily by smallest |offset|. The offset is kept as utc_offset_minutes.
MAX_OFFSET_S = 14 * 3600


def parse_utc(s: str) -> datetime | None:
    try:
        return datetime.strptime(s, "%Y/%m/%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def parse_local(stem: str) -> datetime | None:
    try:
        return datetime.strptime(stem[:19], "%Y_%m_%d_%H_%M_%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def match_rides(rides_utc: list[datetime | None], csv_stems: list[str]) -> list[tuple[str | None, int | None, int]]:
    """For each ride return (csv_stem, offset_seconds, n_candidates). Greedy, closest offset first."""
    csv_local = {s: parse_local(s) for s in csv_stems}
    cands = []
    for i, u in enumerate(rides_utc):
        if u is None:
            continue
        for s, l in csv_local.items():
            if l is None:
                continue
            off = int((l - u).total_seconds())
            if abs(off) <= MAX_OFFSET_S and off % 900 == 0:
                cands.append((abs(off), i, s, off))
    n_cand = Counter(i for _, i, _, _ in cands)
    cands.sort()
    used_r, used_c, out = set(), set(), {}
    for _, i, s, off in cands:
        if i in used_r or s in used_c:
            continue
        used_r.add(i)
        used_c.add(s)
        out[i] = (s, off)
    return [(*out.get(i, (None, None)), n_cand.get(i, 0)) for i in range(len(rides_utc))]



def list_local_zips() -> list[Path]:
    return sorted(OSF_ZIPS_DIR.glob("*.zip"))


def write_report(name: str, markdown: str) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    p = REPORTS_DIR / f"{name}.md"
    p.write_text(markdown)
    return p


def md_table(rows: list[dict], cols: list[str] | None = None) -> str:
    """Tiny markdown table renderer (avoids a tabulate dependency)."""
    if not rows:
        return "_(empty)_\n"
    cols = cols or list(rows[0].keys())
    out = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for r in rows:
        out.append("| " + " | ".join(_fmt(r.get(c)) for c in cols) + " |")
    return "\n".join(out) + "\n"


def _fmt(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        if v != v:
            return "NaN"
        return f"{v:,.3f}" if abs(v) < 1000 else f"{v:,.0f}"
    if isinstance(v, int):
        return f"{v:,}"
    return str(v)


def human_bytes(n: float) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(n) < 1000:
            return f"{n:.1f} {unit}"
        n /= 1000
    return f"{n:.1f} PB"
