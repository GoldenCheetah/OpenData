# /// script
# requires-python = ">=3.11"
# dependencies = ["polars"]
# ///
"""Profile the activity CSVs inside the downloaded athlete zips.

Answers the questions that decide the Parquet schema:
  * Which headers/columns occur? Is it always secs,km,power,hr,cad,alt?
  * Per column: null share, non-numeric values, negative values, scientific notation,
    min/max, and the maximum number of *significant* decimals (=> can we store it as a
    scaled integer losslessly?).
  * secs: integer? strictly increasing? sampling interval distribution, gaps, duplicates.
  * Empty / header-only files, filenames that don't match YYYY_MM_DD_HH_MM_SS.csv.

Output: data/interim/03_files.parquet     one row per activity CSV
        data/interim/03_columns.parquet   one row per (activity CSV, column)
        reports/03_profile_timeseries.md
"""

import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))

import polars as pl

import gcod


def column_stats(df: pl.DataFrame, col: str) -> pl.DataFrame:
    s = pl.col(col)
    num = s.cast(pl.Float64, strict=False)
    frac = s.str.split_exact(".", 1).struct.field("field_1")
    sig_dec = frac.str.strip_chars_end("0").str.len_chars().fill_null(0)
    return df.group_by("file").agg(
        column=pl.lit(col),
        n=pl.len(),
        n_null=s.is_null().sum(),
        n_nonnumeric=(s.is_not_null() & num.is_null()).sum(),
        n_exp=s.str.contains("[eE]").sum(),
        n_negative=(num < 0).sum(),
        n_zero=(num == 0).sum(),
        min=num.min(),
        max=num.max(),
        max_sig_decimals=sig_dec.max(),
        max_raw_decimals=frac.str.len_chars().max(),
        max_abs_int_digits=num.abs().floor().cast(pl.Int64, strict=False).cast(pl.String).str.len_chars().max(),
    )


def profile_zip(path: Path) -> tuple[list[dict], list[pl.DataFrame]]:
    files: list[dict] = []
    cols: list[pl.DataFrame] = []
    with gcod.open_athlete_zip(path) as az:
        for name in az.other_names:
            files.append({"athlete_id": az.athlete_id, "file": name, "kind": "other"})
        for name, raw in az.iter_activities():
            stem = gcod.activity_stem(name)
            rec = {"athlete_id": az.athlete_id, "file": stem, "kind": "activity", "bytes": len(raw)}
            header = raw.split(b"\n", 1)[0].decode("utf-8", "replace").strip()
            rec["header"] = header
            try:
                df = pl.read_csv(raw, infer_schema=False)
            except Exception as e:  # malformed file
                rec["error"] = str(e)[:200]
                files.append(rec)
                continue
            rec["rows"] = df.height
            if df.height and "secs" in df.columns:
                secs = df["secs"].cast(pl.Float64, strict=False)
                d = secs.diff().drop_nulls()
                rec |= {
                    "secs_nonnumeric": int((secs.is_null() & df["secs"].is_not_null()).sum()),
                    "secs_fractional": int((secs != secs.floor()).sum()),
                    "secs_first": secs[0],
                    "secs_last": secs[-1],
                    "dt_eq1": int((d == 1).sum()),
                    "dt_lt1_pos": int(((d > 0) & (d < 1)).sum()),
                    "dt_gt1": int((d > 1).sum()),
                    "dt_zero": int((d == 0).sum()),
                    "dt_negative": int((d < 0).sum()),
                    "dt_max": d.max() if d.len() else None,
                    "dt_median": d.median() if d.len() else None,
                    "gap_seconds": float((d.filter(d > 1) - 1).sum()) if d.len() else 0.0,
                }
            files.append(rec)
            if df.height:
                df = df.with_columns(file=pl.lit(stem))
                for c in df.columns:
                    if c != "file":
                        cols.append(column_stats(df, c).with_columns(athlete_id=pl.lit(az.athlete_id)))
    return files, cols


def main() -> None:
    zips = gcod.list_local_zips()
    print(f"profiling {len(zips)} athlete zips")
    all_files: list[dict] = []
    all_cols: list[pl.DataFrame] = []
    with ProcessPoolExecutor() as ex:
        for i, (f, c) in enumerate(ex.map(profile_zip, zips, chunksize=1), 1):
            all_files += f
            all_cols += c
            if i % 10 == 0:
                print(f"  {i}/{len(zips)}")

    files = pl.DataFrame(all_files, infer_schema_length=None)
    cols = pl.concat(all_cols, how="diagonal_relaxed")
    gcod.INTERIM_DIR.mkdir(parents=True, exist_ok=True)
    files.write_parquet(gcod.INTERIM_DIR / "03_files.parquet")
    cols.write_parquet(gcod.INTERIM_DIR / "03_columns.parquet")
    report(files, cols, len(zips))


def report(files: pl.DataFrame, cols: pl.DataFrame, n_zips: int) -> None:
    act = files.filter(pl.col("kind") == "activity")
    other = files.filter(pl.col("kind") == "other")
    total_rows = act["rows"].sum()
    headers = (
        act.group_by("header").agg(files=pl.len(), athletes=pl.col("athlete_id").n_unique())
        .sort("files", descending=True)
    )
    col_summary = (
        cols.group_by("column")
        .agg(
            files=pl.len(),
            rows=pl.col("n").sum(),
            null_pct=(pl.col("n_null").sum() / pl.col("n").sum() * 100).round(2),
            nonnumeric=pl.col("n_nonnumeric").sum(),
            exp=pl.col("n_exp").sum(),
            negative=pl.col("n_negative").sum(),
            min=pl.col("min").min(),
            max=pl.col("max").max(),
            max_sig_dec=pl.col("max_sig_decimals").max(),
            p99_sig_dec=pl.col("max_sig_decimals").quantile(0.99),
            max_raw_dec=pl.col("max_raw_decimals").max(),
            max_int_digits=pl.col("max_abs_int_digits").max(),
        )
        .sort("files", descending=True)
    )
    dec_dist = (
        cols.group_by("column", "max_sig_decimals").agg(files=pl.len())
        .sort("column", "max_sig_decimals")
        .filter(pl.col("column").is_in(["secs", "km", "power", "hr", "cad", "alt"]))
    )
    # all-null columns per file: candidates for "sensor absent"
    all_null = (
        cols.filter(pl.col("n_null") == pl.col("n")).group_by("column").agg(files_all_null=pl.len())
    )
    s = act.select(
        files=pl.len(),
        empty_or_header_only=(pl.col("rows").fill_null(0) == 0).sum(),
        parse_errors=pl.col("error").is_not_null().sum() if "error" in act.columns else pl.lit(0),
        secs_fractional_files=(pl.col("secs_fractional") > 0).sum(),
        files_with_gaps=(pl.col("dt_gt1") > 0).sum(),
        files_with_subsecond=(pl.col("dt_lt1_pos") > 0).sum(),
        files_with_dup_secs=(pl.col("dt_zero") > 0).sum(),
        files_non_monotonic=(pl.col("dt_negative") > 0).sum(),
        files_secs_not_start_0=(pl.col("secs_first") != 0).sum(),
        median_rows=pl.col("rows").median(),
        max_rows=pl.col("rows").max(),
    ).to_dicts()[0]
    rows_dt = act.select(
        dt_eq1=pl.col("dt_eq1").sum(), dt_gt1=pl.col("dt_gt1").sum(),
        dt_lt1=pl.col("dt_lt1_pos").sum(), dt_zero=pl.col("dt_zero").sum(), dt_neg=pl.col("dt_negative").sum(),
        gap_seconds=pl.col("gap_seconds").sum(),
    ).to_dicts()[0]
    median_dt = act.group_by("dt_median").agg(files=pl.len()).sort("files", descending=True).head(8)

    md = [
        "# 03 · Activity CSV profile\n",
        f"Sample: **{n_zips}** athlete zips, **{act.height:,}** activity CSVs, **{total_rows:,}** rows, "
        f"{gcod.human_bytes(act['bytes'].sum())} uncompressed CSV.\n",
        f"Non-activity members in zips: {other.height} "
        + (f"(e.g. {', '.join(other['file'].head(5).to_list())})" if other.height else ""),
        "\n## File-level checks\n",
        gcod.md_table([{"check": k, "value": v} for k, v in s.items()]),
        "\n## Sampling interval (row-to-row Δsecs, summed over all files)\n",
        gcod.md_table([{"metric": k, "value": v} for k, v in rows_dt.items()]),
        "\nMedian Δsecs per file:\n",
        gcod.md_table(median_dt.to_dicts()),
        "\n## Headers\n",
        gcod.md_table(headers.head(15).to_dicts()),
        "\n## Columns\n",
        gcod.md_table(col_summary.to_dicts()),
        "\n## Files where a column is entirely empty\n",
        gcod.md_table(all_null.sort("column").to_dicts()),
        "\n## Max significant decimals per file (distribution)\n",
        gcod.md_table(dec_dist.to_dicts()),
    ]
    p = gcod.write_report("03_profile_timeseries", "\n".join(md))
    print(f"wrote {p}")


if __name__ == "__main__":
    main()
