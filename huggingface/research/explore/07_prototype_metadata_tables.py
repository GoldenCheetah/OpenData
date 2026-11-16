# /// script
# requires-python = ">=3.11"
# dependencies = ["polars", "pyarrow>=21"]
# ///
"""Prototype the `activities` and `athletes` tables built from the per-athlete JSON
(logic lives in research/lib/convert.py; this script reports on the result).

activities: athlete_id, activity_id (CSV stem = local start time, null if no CSV),
  start_time_utc, utc_offset_minutes, sport (as recorded), data_flags, xdata (JSON string),
  has_timeseries, then every METRICS key as a typed column:
    "123.4"            -> Float64 `<metric>`
    ["123.4", "7.0"]   -> Float64 `<metric>` + Float64 `<metric>__weight`
    [n0, n1, ...]      -> List(Float64) `<metric>` (power_mmp, *_dist, *_dist_bins)
athletes: athlete_id, gender, yob, gc_json_version, counts, OSF provenance.

Also measures what keeping the verbatim JSON (`raw_json` subset) would cost.

Output: data/interim/07_*.parquet, reports/07_prototype_metadata_tables.md
"""

import re
import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))

import polars as pl
import pyarrow.parquet as pq

import convert
import gcod


def main() -> None:
    zips = gcod.list_local_zips()
    zip_bytes = sum(p.stat().st_size for p in zips)
    rows, arows, forms, raws = [], [], {}, []
    with ProcessPoolExecutor() as ex:
        for r, a, f, raw in ex.map(convert.athlete_metadata, zips, chunksize=4):
            rows += r
            arows.append(a)
            raws.append({"athlete_id": a["athlete_id"], "json": raw})
            for k, v in f.items():
                forms.setdefault(k, set()).update(v)
    kinds = convert.metric_kinds(forms)
    acts = convert.build_activities(rows, kinds)
    aths = convert.build_athletes(arows, pl.read_parquet(gcod.OSF_LISTING))
    raw_df = pl.DataFrame(raws)

    paths = {
        "activities": gcod.INTERIM_DIR / "07_activities.parquet",
        "athletes": gcod.INTERIM_DIR / "07_athletes.parquet",
        "raw_json": gcod.INTERIM_DIR / "07_raw_json.parquet",
    }
    pq.write_table(acts.to_arrow(), paths["activities"], row_group_size=50_000, **convert.TABLE_WRITE_OPTS)
    pq.write_table(aths.to_arrow(), paths["athletes"], **convert.TABLE_WRITE_OPTS)
    pq.write_table(raw_df.to_arrow(), paths["raw_json"], row_group_size=100, **convert.TABLE_WRITE_OPTS)
    sizes = {k: p.stat().st_size for k, p in paths.items()}

    metric_cols = sorted(kinds)
    n_kind = {k: sum(1 for v in kinds.values() if v == k) for k in ("scalar", "pair", "list")}
    sql_unsafe = [c for c in metric_cols if not re.fullmatch(r"[a-z_][a-z0-9_]*", c)]
    fill = acts.select(pl.col(metric_cols).is_not_null().mean()).transpose(include_header=True, column_names=["fill"])
    sports = acts.group_by("sport").agg(n=pl.len()).sort("n", descending=True)
    yob = aths.select(
        yob_min=pl.col("yob").min(), yob_max=pl.col("yob").max(),
        yob_implausible=((pl.col("yob") < 1920) | (pl.col("yob") > 2010)).sum(), yob_null=pl.col("yob").is_null().sum(),
    )
    md = [
        "# 07 · Prototype `activities` / `athletes` tables\n",
        f"Sample: {len(zips)} athletes ({gcod.human_bytes(zip_bytes)} zips).\n",
        f"- Activities: **{acts.height:,}** rows ({acts['has_timeseries'].sum():,} with time series, "
        f"{acts.filter(pl.col('start_time_utc').is_null()).height} CSV-only without JSON ride, "
        f"{acts.filter(~pl.col('has_timeseries')).height} JSON-only without CSV)",
        f"- Columns: **{acts.width}** (metrics: {n_kind['scalar']} scalar, {n_kind['pair']} value+weight pairs, {n_kind['list']} arrays)",
        f"- Metric names that need quoting in SQL (digit first, '.', capitals, ...): {len(sql_unsafe)} "
        f"(e.g. {', '.join(sql_unsafe[:6])})",
        f"- Distinct `sport` strings: {sports.height}",
        "\n## Sizes and projection to the full dataset (115.6 GB zips)\n",
        gcod.md_table([
            {"table": k, "sample": gcod.human_bytes(v), "projected_full": gcod.human_bytes(v * 115.6e9 / zip_bytes)}
            for k, v in sizes.items()
        ]),
        "\n## Athletes\n",
        gcod.md_table(yob.to_dicts()),
        gcod.md_table(aths.group_by("gender").agg(n=pl.len()).to_dicts()),
        "\n## Metric fill rate (share of activities with a value): top 20 / bottom 10\n",
        gcod.md_table(fill.sort("fill", descending=True).head(20).to_dicts()),
        gcod.md_table(fill.sort("fill").head(10).to_dicts()),
        "\n## Array metrics\n",
        ", ".join(c for c in metric_cols if kinds[c] == "list"),
        "\n\n## Top sports strings\n",
        gcod.md_table(sports.head(20).to_dicts()),
    ]
    p = gcod.write_report("07_prototype_metadata_tables", "\n".join(md))
    print(f"wrote {p}")


if __name__ == "__main__":
    main()
