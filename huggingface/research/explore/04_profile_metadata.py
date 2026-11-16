# /// script
# requires-python = ">=3.11"
# dependencies = ["polars"]
# ///
"""Profile the per-athlete JSON metadata ({uuid}.json) inside the downloaded zips.

Answers:
  * Top-level / ATHLETE / ride keys and value forms; VERSION values.
  * Which METRICS exist, how often, and in which form ("123.4" vs ["123.4", "count"]).
  * How do JSON rides link to CSV files? The JSON `date` is UTC, the CSV filename is
    *local* time. We match on identical mm:ss with a whole-quarter-hour offset of at most
    14h, and report match quality and the offset distribution.
  * XDATA: only series names are present (no data) - confirm.

Output: data/interim/04_rides.parquet     one row per JSON ride (+ matched CSV, offset)
        data/interim/04_metrics.parquet   one row per (metric key, value form) with counts
        reports/04_profile_metadata.md
"""

import json
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))

import polars as pl

import gcod

def profile_zip(path: Path) -> tuple[list[dict], Counter, dict]:
    rides: list[dict] = []
    metric_forms: Counter = Counter()
    with gcod.open_athlete_zip(path) as az:
        meta = az.meta or {}
        stems = [gcod.activity_stem(n) for n in az.activity_names]
        ath = meta.get("ATHLETE", {})
        info = {
            "athlete_id": az.athlete_id,
            "has_json": az.meta is not None,
            "version": meta.get("VERSION"),
            "top_keys": ",".join(sorted(meta.keys())),
            "athlete_keys": ",".join(sorted(ath.keys())),
            "gender": ath.get("gender"),
            "yob": ath.get("yob"),
            "json_id_matches": (ath.get("id", "").strip("{}") == az.athlete_id),
            "n_csv": len(stems),
            "n_rides": len(meta.get("RIDES", [])),
        }
        rlist = meta.get("RIDES", [])
        utc = [gcod.parse_utc(r.get("date", "")) for r in rlist]
        matches = gcod.match_rides(utc, stems)
        matched_csvs = {m[0] for m in matches if m[0]}
        info["csv_unmatched"] = len(set(stems) - matched_csvs)
        for r, u, (stem, off, ncand) in zip(rlist, utc, matches):
            m = r.get("METRICS", {})
            for k, v in m.items():
                if isinstance(v, str):
                    form = "str"
                elif isinstance(v, list):
                    form = f"list{len(v)}" if len(v) <= 2 else "listN"  # >2 = variable-length array
                else:
                    form = type(v).__name__
                metric_forms[(k, form)] += 1
            rides.append(
                {
                    "athlete_id": az.athlete_id,
                    "date_raw": r.get("date"),
                    "date_utc": u,
                    "sport": r.get("sport"),
                    "data_flags": r.get("data"),
                    "ride_keys": ",".join(sorted(r.keys())),
                    "n_metrics": len(m),
                    "xdata": json.dumps(r["XDATA"], sort_keys=True) if "XDATA" in r else None,
                    "csv": stem,
                    "utc_offset_s": off,
                    "n_candidates": ncand,
                }
            )
    return rides, metric_forms, info


def main() -> None:
    zips = gcod.list_local_zips()
    print(f"profiling metadata of {len(zips)} athlete zips")
    rides, infos, forms = [], [], Counter()
    with ProcessPoolExecutor() as ex:
        for r, f, i in ex.map(profile_zip, zips, chunksize=4):
            rides += r
            forms.update(f)
            infos.append(i)
    rides_df = pl.DataFrame(rides, infer_schema_length=None)
    ath_df = pl.DataFrame(infos)
    metrics_df = pl.DataFrame(
        [{"metric": k, "form": f, "count": c} for (k, f), c in forms.items()]
    ).sort("count", descending=True)
    gcod.INTERIM_DIR.mkdir(parents=True, exist_ok=True)
    rides_df.write_parquet(gcod.INTERIM_DIR / "04_rides.parquet")
    ath_df.write_parquet(gcod.INTERIM_DIR / "04_athletes.parquet")
    metrics_df.write_parquet(gcod.INTERIM_DIR / "04_metrics.parquet")
    report(rides_df, ath_df, metrics_df)


def report(rides: pl.DataFrame, ath: pl.DataFrame, metrics: pl.DataFrame) -> None:
    n_rides = rides.height
    per_metric = metrics.group_by("metric").agg(
        rides=pl.col("count").sum(), forms=pl.col("form").unique().sort().str.join("/")
    ).sort("rides", descending=True)
    forms_summary = per_metric.group_by("forms").agg(n_metrics=pl.len()).sort("n_metrics", descending=True)
    match = rides.select(
        rides=pl.len(),
        matched=pl.col("csv").is_not_null().sum(),
        ambiguous=(pl.col("n_candidates") > 1).sum(),
        unparseable_date=pl.col("date_utc").is_null().sum(),
    ).to_dicts()[0]
    unmatched_by_sport = (
        rides.filter(pl.col("csv").is_null()).group_by("sport").agg(n=pl.len()).sort("n", descending=True).head(10)
    )
    offsets = (
        rides.filter(pl.col("utc_offset_s").is_not_null())
        .group_by((pl.col("utc_offset_s") / 3600).alias("utc_offset_h"))
        .agg(rides=pl.len()).sort("rides", descending=True).head(15)
    )
    sports = rides.group_by("sport").agg(n=pl.len()).sort("n", descending=True).head(15)
    flags_len = rides.group_by(pl.col("data_flags").str.len_chars().alias("flag_len")).agg(n=pl.len())
    # character-position frequency of the data flag string
    flag_pos = []
    for pos in range(15):
        c = rides.select(pl.col("data_flags").str.slice(pos, 1).alias("c")).group_by("c").agg(n=pl.len())
        chars = {r["c"]: r["n"] for r in c.to_dicts()}
        letter = next((k for k in chars if k and k != "-"), "")
        flag_pos.append({"pos": pos, "letter": letter, "pct_set": round(100 * chars.get(letter, 0) / max(n_rides, 1), 1)})
    xdata = rides.group_by("xdata").agg(n=pl.len()).sort("n", descending=True).head(8)

    md = [
        "# 04 · JSON metadata profile\n",
        f"Athletes: **{ath.height}**, JSON rides: **{n_rides:,}**, CSVs: **{ath['n_csv'].sum():,}**, "
        f"CSVs without a matching ride: **{ath['csv_unmatched'].sum():,}**\n",
        "\n## Athlete-level\n",
        gcod.md_table(
            ath.select(
                has_json=pl.col("has_json").all(),
                id_matches_filename=pl.col("json_id_matches").all(),
                versions=pl.col("version").unique().sort().str.join(", "),
                top_keys=pl.col("top_keys").unique().str.join(" | "),
                athlete_keys=pl.col("athlete_keys").unique().str.join(" | "),
                genders=pl.col("gender").value_counts().struct.json_encode().str.join(" "),
                yob_null=pl.col("yob").is_null().sum(),
                yob_range=pl.format("{}-{}", pl.col("yob").min(), pl.col("yob").max()),
            ).to_dicts()
        ),
        "\n## Ride ↔ CSV matching (UTC date vs local-time filename)\n",
        gcod.md_table([match]),
        "\nUnmatched rides by sport:\n",
        gcod.md_table(unmatched_by_sport.to_dicts()),
        "\nUTC offset (local − UTC) of matched rides:\n",
        gcod.md_table(offsets.to_dicts()),
        "\n## Sports\n",
        gcod.md_table(sports.to_dicts()),
        "\n## `data` flag string\n",
        gcod.md_table(flags_len.to_dicts()),
        gcod.md_table(flag_pos),
        "\n## Ride keys\n",
        gcod.md_table(rides.group_by("ride_keys").agg(n=pl.len()).sort("n", descending=True).to_dicts()),
        "\n## XDATA (names only)\n",
        gcod.md_table(xdata.to_dicts()),
        f"\n## METRICS\n\nDistinct metric keys: **{per_metric.height}**; metrics per ride: "
        f"median {rides['n_metrics'].median()}, max {rides['n_metrics'].max()}\n",
        "\nValue forms (`list2` = [value, count/weight]):\n",
        gcod.md_table(forms_summary.to_dicts()),
        "\nTop 40 metrics by coverage:\n",
        gcod.md_table(
            per_metric.head(40).with_columns(pct=(pl.col("rides") / n_rides * 100).round(1)).to_dicts()
        ),
        "\nLeast common 15 metrics:\n",
        gcod.md_table(per_metric.tail(15).to_dicts()),
    ]
    p = gcod.write_report("04_profile_metadata", "\n".join(md))
    print(f"wrote {p}")


if __name__ == "__main__":
    main()
