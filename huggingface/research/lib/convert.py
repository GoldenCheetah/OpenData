"""Conversion of GCOD athlete zips into Hugging Face-ready Parquet tables.

Shared by research/explore/07_*, 08_* and (later) the production converter. Needs polars
and pyarrow>=21 (declared by the calling script).

Tables
  timeseries   athlete_id, activity_id, time_s, distance_m, power_w, hr_bpm, cadence_rpm,
               altitude_m (Float32); one file per athlete: timeseries/athlete_id=<uuid>/part.parquet
  timeseries_exceptions  the few source values Float32 cannot represent (see below)
  activities   one row per activity: identifiers, start time (UTC + local offset), sport,
               flags, and every GoldenCheetah METRICS key as a typed column
  athletes     one row per athlete: gender, yob, counts, OSF provenance (file id, sha256)
  raw_json     athlete_id + the original {uuid}.json text, verbatim (optional)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Iterator

import polars as pl
import pyarrow as pa
import pyarrow.compute  # noqa: F401  (pa.compute)
import pyarrow.parquet as pq

import gcod

# Time series representation (reports/09, reports/10):
# GoldenCheetah writes CSV numbers with 6 significant digits (QString::number, %g). Every
# such decimal survives decimal -> float32 -> "%.6g" unchanged (FLT_DIG = 6), so Float32
# stores the source exactly at the text level, at ~half the size of Float64. Distance is
# stored in metres (km x 1000 = same 6 digits, decimal point shifted), which compresses
# ~25% better than km in Float32. Values outside the float32 normal range (sub-normal
# garbage like 4.94066e-324, seen in 2 of 169 sample athletes) are written to
# `timeseries_exceptions` with their exact Float64 value, so nothing is lost.
# Output names carry their unit and are shared by the GoldenCheetah datasets on Hugging Face.
TS_SOURCE_TO_OUTPUT = {
    "secs": "time_s", "km": "distance_m", "power": "power_w", "hr": "hr_bpm", "cad": "cadence_rpm", "alt": "altitude_m",
}
TS_SCALE = {"km": 1000.0}
TS_FLOATS = list(TS_SOURCE_TO_OUTPUT.values())
F32_MIN_NORMAL = 1.1754943508222875e-38
F32_MAX = 3.4028234663852886e38

# --- writer settings -----------------------------------------------------------------
# "Optimized Parquet" per HF docs: content-defined chunking (Xet dedup across versions)
# and a page index (fast filtered reads / streaming / dataset viewer).
_HF_OPTS = dict(use_content_defined_chunking=True, write_page_index=True)

TIMESERIES_WRITE_OPTS = dict(
    compression="zstd",
    compression_level=15,  # -8% vs 9 at ~2.7x write time; 19+ adds <1% (measured on a 210 MB shard, see PLAN.md)
    use_dictionary=["athlete_id", "activity_id"],
    column_encoding={c: "BYTE_STREAM_SPLIT" for c in TS_FLOATS},
    **_HF_OPTS,
)
TABLE_WRITE_OPTS = dict(compression="zstd", compression_level=9, **_HF_OPTS)


# --- activities / athletes ------------------------------------------------------------
# Metrics that are variable-length arrays even when a particular ride has 2 elements.
LIST_METRICS = {
    "power_mmp", "speed_dist", "speed_dist_bins", "heart.rate_dist", "heart.rate_dist_bins",
    "cadence_dist", "cadence_dist_bins", "power_dist", "power_dist_bins", "SDNN", "SDANN",
}


def _float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def athlete_metadata(path: Path) -> tuple[list[dict], dict, dict[str, set], str | None]:
    """Parse one zip's JSON. Returns (activity rows with raw `_metrics`, athlete row,
    metric-form observations, raw JSON text)."""
    forms: dict[str, set] = {}
    with gcod.open_athlete_zip(path) as az:
        meta = az.meta or {}
        json_name = next((n for n in az._zf.namelist() if n.endswith(".json")), None)
        raw_json = az.read(json_name).decode("utf-8", "replace") if json_name else None
        stems = [gcod.activity_stem(n) for n in az.activity_names]
        rides = meta.get("RIDES", [])
        utc = [gcod.parse_utc(r.get("date", "")) for r in rides]
        matches = gcod.match_rides(utc, stems)
        rows, matched = [], set()
        for r, u, (stem, off, _) in zip(rides, utc, matches):
            m = r.get("METRICS", {})
            for k, v in m.items():
                if isinstance(v, str):
                    f = "str"
                elif isinstance(v, list) and len(v) == 2 and k not in LIST_METRICS:
                    f = "pair"
                else:
                    f = "list"
                forms.setdefault(k, set()).add(f)
            if stem:
                matched.add(stem)
            rows.append(
                {
                    "athlete_id": az.athlete_id,
                    "activity_id": stem,
                    "start_time_utc": u,
                    "utc_offset_minutes": None if off is None else off // 60,
                    "sport": r.get("sport"),
                    "data_flags": r.get("data"),
                    "xdata": json.dumps(r["XDATA"], sort_keys=True) if "XDATA" in r else None,
                    "has_timeseries": stem is not None,
                    "_metrics": m,
                }
            )
        for s in sorted(set(stems) - matched):  # CSV without a JSON ride
            rows.append({"athlete_id": az.athlete_id, "activity_id": s, "has_timeseries": True, "_metrics": {}})
        ath = meta.get("ATHLETE", {})
        yob = str(ath.get("yob", ""))
        arow = {
            "athlete_id": az.athlete_id,
            "gender": ath.get("gender") or None,
            "yob": int(yob) if yob.isdigit() else None,
            "json_athlete_id": (ath.get("id") or "").strip("{}") or None,  # differs from zip name in rare re-uploads
            "gc_json_version": meta.get("VERSION"),
            "n_activities": len(rows),
            "n_timeseries": len(stems),
        }
    return rows, arow, forms, raw_json


def metric_kinds(forms: dict[str, set]) -> dict[str, str]:
    """Decide the column kind per metric key over all observed forms."""
    kinds = {}
    for k, fs in forms.items():
        if "list" in fs or k in LIST_METRICS:
            kinds[k] = "list"
        elif "pair" in fs:
            kinds[k] = "pair"
        else:
            kinds[k] = "scalar"
    return kinds


def activities_schema(kinds: dict[str, str]) -> dict[str, pl.DataType]:
    schema: dict[str, pl.DataType] = {
        "athlete_id": pl.String, "activity_id": pl.String, "start_time_utc": pl.Datetime("us", "UTC"),
        "utc_offset_minutes": pl.Int16, "sport": pl.String, "data_flags": pl.String, "xdata": pl.String,
        "has_timeseries": pl.Boolean,
    }
    for k in sorted(kinds):
        if kinds[k] == "list":
            schema[k] = pl.List(pl.Float64)
        else:
            schema[k] = pl.Float64
            if kinds[k] == "pair":
                schema[f"{k}__weight"] = pl.Float64
    return schema


def build_activities(rows: list[dict], kinds: dict[str, str]) -> pl.DataFrame:
    for row in rows:
        m = row.pop("_metrics")
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
    return pl.DataFrame(rows, schema=activities_schema(kinds)).sort(
        "athlete_id", "start_time_utc", "activity_id", nulls_last=True
    )


def build_athletes(arows: list[dict], listing: pl.DataFrame) -> pl.DataFrame:
    prov = listing.filter("is_athlete").select(
        "athlete_id",
        osf_file_id="osf_file_id",
        osf_sha256="sha256",
        osf_size_bytes="size",
        osf_date_created="date_created",
    )
    return pl.DataFrame(arows).join(prov, on="athlete_id", how="left").sort("athlete_id")


# --- time series layout ---------------------------------------------------------------
# One file per athlete in a Hive-style folder, the layout shared by the GoldenCheetah datasets:
#   timeseries/athlete_id=<uuid>/part.parquet
# polars, DuckDB and `datasets` read it with a plain glob; `athlete_id` is also stored in the
# file so a single file is self-contained. Read amplification for one athlete is 1.0
# (reports/06). This replaces the earlier ~1 GB shards.
def athlete_file(out_dir: Path, athlete_id: str) -> Path:
    return out_dir / f"athlete_id={athlete_id}" / "part.parquet"


def write_athlete_files(
    tables: Iterable[pa.Table],
    out_dir: Path,
    row_group_size: int = 256_000,  # ~2 MB row groups (reports/06)
) -> list[Path]:
    """Write each athlete's table (one athlete per table) to its own Hive partition file."""
    written: list[Path] = []
    for t in tables:
        ids = pa.compute.unique(t.column("athlete_id")).to_pylist()
        if len(ids) != 1:
            raise ValueError(f"expected one athlete per table, got {len(ids)}")
        p = athlete_file(out_dir, ids[0])
        p.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(t, p, row_group_size=row_group_size, **TIMESERIES_WRITE_OPTS)
        written.append(p)
    return written


def to_output_timeseries(src: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Canonical Float64 source frame -> (Float32 output frame, exceptions frame)."""
    src = src.with_columns(row_in_activity=pl.int_range(pl.len(), dtype=pl.Int32).over("activity_id"))
    exc = []
    for c in TS_SOURCE_TO_OUTPUT:
        a = pl.col(c).abs()
        bad = pl.col(c).is_not_null() & (a != 0) & ((a < F32_MIN_NORMAL) | (a > F32_MAX))
        e = src.filter(bad)
        if e.height:
            exc.append(e.select("athlete_id", "activity_id", "row_in_activity",
                                column=pl.lit(TS_SOURCE_TO_OUTPUT[c]), source_column=pl.lit(c),
                                source_value=pl.col(c)))
    out = src.select(
        "athlete_id", "activity_id",
        *[(pl.col(c) * TS_SCALE.get(c, 1.0)).cast(pl.Float32).alias(o) for c, o in TS_SOURCE_TO_OUTPUT.items()],
    )
    exceptions = pl.concat(exc) if exc else pl.DataFrame(schema=EXCEPTIONS_SCHEMA)
    return out, exceptions


EXCEPTIONS_SCHEMA = {
    "athlete_id": pl.String, "activity_id": pl.String, "row_in_activity": pl.Int32,
    "column": pl.String, "source_column": pl.String, "source_value": pl.Float64,
}


def iter_timeseries(zips: list[Path], exceptions_sink: list | None = None) -> Iterator[pa.Table]:
    """Yield one Float32 output table per athlete (sorted by athlete_id); exceptions are
    appended to `exceptions_sink` when given."""
    for p in sorted(zips, key=lambda p: p.stem):
        df = gcod.load_athlete_timeseries(p)
        if df is None:
            continue
        out, exc = to_output_timeseries(df)
        if exceptions_sink is not None and exc.height:
            exceptions_sink.append(exc)
        yield out.to_arrow()
