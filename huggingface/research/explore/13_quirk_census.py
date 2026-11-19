# /// script
# requires-python = ">=3.11"
# dependencies = ["polars>=1.20", "pyarrow>=21"]
# ///
"""Which quirks occur in the full release, and which of them would stop the converter?

Runs over every mirrored athlete zip (data/osf/athletes/, see migrate.py mirror), reading only
the zip directory, the JSON and the first line of each CSV. Unlike the other scripts here it uses
the production converter (datasets/opendata/gcod.py), so `converter_error` is exactly what
`migrate.py convert` would report for that athlete (time series contents aside: the census does
not parse CSV bodies).

Per athlete it records the converter's verdict plus the details that decisions need:
  - zip members: duplicates (identical, or different content under one name), activity file
    extensions, CSV headers
  - JSON: repairs needed, rides, identical duplicate rides, rides that share a start time but
    differ (and how: metrics only, data flags, sport, other fields), dates not in ASCII digits,
    rides without a valid date
  - linking: rides with a CSV, rides without one (`Z` ids), CSVs without a ride, ids that needed
    `~n` to be unique

Output: data/interim/13_census.parquet (one row per athlete), data/interim/13_same_start.parquet
(one row per same-start group), reports/13_quirk_census.md
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import zipfile
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

HF = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(HF / "datasets" / "opendata"))

import polars as pl  # noqa: E402

import gcod  # noqa: E402

ZIPS = HF / "data" / "osf" / "athletes"
INTERIM = HF / "data" / "interim"
REPORT = HF / "research" / "reports" / "13_quirk_census.md"
TOP_LEVEL_IGNORED = {"METRICS"}


def classify_difference(rides: list[dict]) -> str:
    """How rides that share a start time differ: the fields that differ, METRICS last."""
    keys = sorted(set().union(*rides) - TOP_LEVEL_IGNORED)
    fields = [k for k in keys if len({json.dumps(r.get(k), sort_keys=True) for r in rides}) > 1]
    metrics = len({json.dumps(r.get("METRICS"), sort_keys=True) for r in rides}) > 1
    return "+".join(fields + (["METRICS"] if metrics else [])) or "none"


def census(path_str: str) -> tuple[dict, list[dict]]:
    os.nice(10)
    path = Path(path_str)
    aid = path.stem
    row: dict = {"athlete_id": aid, "converter_error": None}
    groups: list[dict] = []

    # zip directory, independent of the converter's checks
    with zipfile.ZipFile(path) as zf:
        infos = [i for i in zf.infolist() if not i.is_dir()]
        names = Counter(i.filename for i in infos)
        row["dup_members"] = sum(n - 1 for n in names.values() if n > 1)
        contents: dict[str, set] = {}
        for i in infos:
            contents.setdefault(i.filename, set()).add((i.CRC, i.file_size))
        row["dup_members_differing"] = sum(len(c) - 1 for c in contents.values())
        csvs = sorted({i.filename for i in infos if gcod.ACTIVITY_MEMBER_RE.match(i.filename.rsplit("/", 1)[-1])})
        row["n_csv_members"] = len(csvs)
        exts = Counter(gcod.ACTIVITY_MEMBER_RE.match(m.rsplit("/", 1)[-1]).group(2) for m in csvs)
        row["extensions"] = json.dumps(dict(sorted(exts.items())))
        headers = Counter()
        for m in csvs:
            with zf.open(m) as f:
                headers[f.readline().strip().decode("utf-8", "replace")] += 1
        row["bad_headers"] = sum(n for h, n in headers.items() if h.split(",") != gcod.CSV_HEADER)
        row["bad_header_examples"] = json.dumps([h for h in headers if h.split(",") != gcod.CSV_HEADER][:3])

    try:
        az = gcod.open_athlete_zip(path)
    except gcod.GcodError as e:
        row["converter_error"] = f"zip: {e}"
        return row, groups
    with az:
        row["json_repairs"] = ",".join(az.json_repairs)
        rides = (az.meta or {}).get("RIDES", [])
        row["n_rides"] = len(rides)
        row["non_ascii_dates"] = sum(not str(r.get("date", "")).isascii() for r in rides)
        by_date: dict = {}
        for r in rides:
            by_date.setdefault(r.get("date"), []).append(r)
        row["identical_dup_rides"] = 0
        row["same_start_groups"] = 0
        row["same_start_rides"] = 0
        for date, rs in by_date.items():
            distinct = list({json.dumps(r, sort_keys=True): r for r in rs}.values())
            row["identical_dup_rides"] += len(rs) - len(distinct)
            if len(distinct) > 1:
                row["same_start_groups"] += 1
                row["same_start_rides"] += len(distinct)
                groups.append({"athlete_id": aid, "date": date, "n": len(distinct),
                               "differs_in": classify_difference(distinct),
                               "data_flags": json.dumps(sorted({str(r.get("data")) for r in distinct})),
                               "sports": json.dumps(sorted({str(r.get("sport")) for r in distinct}))})
        csv_ids = [m.activity_id for m in az.activity_members]
        row["numbered_csv_ids"] = sum("~" in i for i in csv_ids)
        uniq = list({json.dumps(r, sort_keys=True): r for r in rides}.values())
        utc = [gcod.parse_utc(r.get("date", "")) for r in uniq]
        links = gcod.match_rides(utc, csv_ids)
        row["rides_bad_date"] = sum(u is None for u in utc)
        row["rides_with_csv"] = sum(a is not None for a, _ in links)
        row["rides_without_csv"] = sum(a is None for a, _ in links)
        row["csvs_without_ride"] = len(set(csv_ids) - {a for a, _ in links if a})
        # for a same-start group: does one of its rides link to a CSV?
        linked_dates = {r.get("date") for r, (a, _) in zip(uniq, links) if a}
        for g in groups:
            g["has_csv"] = g["date"] in linked_dates

        # The converter's verdict: its metadata dry run, plus the check iter_source_activities makes
        # before parsing a CSV body (the header).
        try:
            kinds = gcod.metric_kinds(gcod.metric_forms(az.meta or {}))
            acts = gcod.build_activities(az, kinds, {})
            row["numbered_z_ids"] = acts.filter(pl.col("activity_id").str.contains("Z~"))["activity_id"].len()
            if row["bad_headers"]:
                raise gcod.GcodError(f"CSV header {row['bad_header_examples']}")
        except gcod.GcodError as e:
            row["converter_error"] = f"metadata: {e}"
    return row, groups


def main() -> None:
    INTERIM.mkdir(parents=True, exist_ok=True)
    paths = sorted(str(p) for p in ZIPS.glob("*.zip"))
    jobs = max(1, (os.cpu_count() or 2) // 2)
    print(f"census of {len(paths):,} athletes with {jobs} processes ...", flush=True)
    t0 = time.monotonic()
    rows, groups = [], []
    with ProcessPoolExecutor(jobs) as ex:
        for i, (r, g) in enumerate(ex.map(census, paths, chunksize=4), 1):
            rows.append(r)
            groups += g
            if i % 500 == 0:
                print(f"  {i:,}/{len(paths):,} ({time.monotonic() - t0:.0f} s)", flush=True)
    df = (pl.DataFrame(rows, infer_schema_length=None)  # all-null columns would be typed Null
          .with_columns(pl.col("converter_error", "json_repairs").cast(pl.String)).sort("athlete_id"))
    gdf = pl.DataFrame(groups, infer_schema_length=None) if groups else pl.DataFrame()
    df.write_parquet(INTERIM / "13_census.parquet")
    gdf.write_parquet(INTERIM / "13_same_start.parquet")
    write_report(df, gdf, time.monotonic() - t0)
    print(f"done in {time.monotonic() - t0:.0f} s -> {REPORT}")


def write_report(df: pl.DataFrame, g: pl.DataFrame, seconds: float) -> None:
    n = df.height

    def athletes(expr: pl.Expr) -> str:
        k = df.filter(expr).height
        return f"{k:,} ({k / n:.1%})"

    err = df.filter(pl.col("converter_error").is_not_null())
    err_kind = err.with_columns(kind=pl.col("converter_error").str.replace(r"(\d{4}/\d{2}/\d{2} [\d:]+ UTC|\[.*\]|\{.*\}|'.*')", "…"))
    exts = Counter()
    for e in df["extensions"].drop_nulls():
        for suffix, count in json.loads(e).items():  # one row for all sync-conflict copies
            exts[re.sub(r"\.sync-conflict-[^.]+", ".sync-conflict-…", suffix)] += count
    lines = [
        "# 13 · Quirk census of the full release",
        "",
        f"All {n:,} athlete zips, read with the production converter (`datasets/opendata/gcod.py`, "
        f"converter {gcod.CONVERTER_VERSION}). Metadata and CSV headers only; {seconds:.0f} s.",
        "",
        "## Converter verdict",
        "",
        f"- Athletes the converter accepts: {athletes(pl.col('converter_error').is_null())}",
        f"- Athletes it stops on: {athletes(pl.col('converter_error').is_not_null())}",
        "",
        "| error | athletes |",
        "|---|---:|",
        *[f"| {k} | {c:,} |" for k, c in err_kind.group_by("kind").len().sort("len", descending=True).iter_rows()],
        "",
        "## Quirks (handled or not)",
        "",
        "| quirk | athletes | instances |",
        "|---|---:|---:|",
        f"| duplicate zip members | {athletes(pl.col('dup_members') > 0)} | {df['dup_members'].sum():,} |",
        f"| ... with different content under one name | {athletes(pl.col('dup_members_differing') > 0)} | "
        f"{df['dup_members_differing'].sum():,} |",
        *[f"| JSON repair `{name}` | {athletes(pl.col('json_repairs').fill_null('').str.contains(name))} | |"
          for name in [*gcod.JSON_REPAIRS, "control_characters"]],
        f"| JSON not readable (even repaired) | {athletes(pl.col('converter_error').str.contains('invalid JSON'))} | |",
        f"| identical duplicate rides | {athletes(pl.col('identical_dup_rides') > 0)} | {df['identical_dup_rides'].sum():,} |",
        f"| rides sharing a start time but differing | {athletes(pl.col('same_start_groups') > 0)} | {df['same_start_groups'].sum():,} groups, {df['same_start_rides'].sum():,} rides |",
        f"| dates not in ASCII digits | {athletes(pl.col('non_ascii_dates') > 0)} | {df['non_ascii_dates'].sum():,} |",
        f"| rides without a valid date | {athletes(pl.col('rides_bad_date') > 0)} | {df['rides_bad_date'].sum():,} |",
        f"| CSV ids numbered `~n` | {athletes(pl.col('numbered_csv_ids') > 0)} | {df['numbered_csv_ids'].sum():,} |",
        f"| `Z` ids numbered `~n` | {athletes(pl.col('numbered_z_ids') > 0)} | {df['numbered_z_ids'].sum():,} |",
        f"| CSVs with an unexpected header | {athletes(pl.col('bad_headers') > 0)} | {df['bad_headers'].sum():,} |",
        f"| rides without a CSV (`Z` ids) | {athletes(pl.col('rides_without_csv') > 0)} | {df['rides_without_csv'].sum():,} |",
        f"| CSVs without a ride | {athletes(pl.col('csvs_without_ride') > 0)} | {df['csvs_without_ride'].sum():,} |",
        "",
        f"Totals: {df['n_rides'].sum():,} JSON rides, {df['n_csv_members'].sum():,} CSV members, "
        f"{df['rides_with_csv'].sum():,} rides linked to a CSV.",
        "",
        "Activity file name suffixes (after the start time):",
        "",
        "| suffix | files |",
        "|---|---:|",
        *[f"| `{k or '(none)'}` | {c:,} |" for k, c in exts.most_common()],
    ]
    if g.height:
        lines += [
            "",
            "## Rides that share a start time but differ",
            "",
            "Fields that differ within a group (`METRICS` = metric values):",
            "",
            "| differs in | groups | with a CSV |",
            "|---|---:|---:|",
            *[f"| {k} | {c:,} | {s:,} |" for k, c, s in
              g.group_by("differs_in").agg(pl.len(), pl.col("has_csv").sum()).sort("len", descending=True).iter_rows()],
            "",
            f"Group sizes: {dict(sorted(Counter(g['n'].to_list()).items()))}",
        ]
    REPORT.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
