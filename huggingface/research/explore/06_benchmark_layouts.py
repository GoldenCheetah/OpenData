# /// script
# requires-python = ">=3.11"
# dependencies = ["polars", "pyarrow>=21"]
# ///
"""Benchmark file layouts for the raw time series on the Hub.

Layouts (all use the same encoding: Float64 + BYTE_STREAM_SPLIT + zstd 9 + CDC + page index):
  per_athlete          one file per athlete                 (6,613 files for the full set)
  shard_256MB_rg1M     athletes packed into ~256 MB shards, 1M-row row groups
  shard_1GB_rg1M       ~1 GB shards, 1M-row row groups
  shard_1GB_rg256k     ~1 GB shards, 256k-row row groups
Shards keep athletes contiguous and sorted by athlete_id, so min/max statistics let a
reader skip row groups.

Metrics
  files (sample / projected full dataset)
  read amplification for "give me athlete X": bytes in the row groups whose athlete_id
    min/max range contains X, divided by X's own compressed bytes. Over HTTP (hf://) this
    is roughly what a reader downloads (plus footers). Page index can reduce it further.
  local timings: one-athlete filter, full-scan aggregation.

Usage: uv run research/explore/06_benchmark_layouts.py
Output: reports/06_benchmark_layouts.md, data/interim/06_layouts.parquet
"""

import shutil
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

import gcod

FLOATS = ["secs", "km", "power", "hr", "cad", "alt"]
WRITE_OPTS = dict(
    compression="zstd",
    compression_level=9,
    use_dictionary=["athlete_id", "activity_id"],
    column_encoding={c: "BYTE_STREAM_SPLIT" for c in FLOATS},
    use_content_defined_chunking=True,
    write_page_index=True,
)
FULL_DATASET_ZIP_BYTES = 115.6e9


def write_per_athlete(tables: list[pa.Table], out: Path) -> None:
    for t in tables:
        aid = t.column("athlete_id")[0].as_py()
        pq.write_table(t, out / f"{aid}.parquet", row_group_size=1_000_000, **WRITE_OPTS)


def write_shards(tables: list[pa.Table], out: Path, target_bytes: float, rg_rows: int, bytes_per_row: float) -> None:
    rows_per_shard = int(target_bytes / bytes_per_row)
    shard, rows, idx = [], 0, 0

    def flush():
        nonlocal shard, rows, idx
        if shard:
            pq.write_table(pa.concat_tables(shard), out / f"part-{idx:05d}.parquet", row_group_size=rg_rows, **WRITE_OPTS)
            idx += 1
            shard, rows = [], 0

    for t in tables:  # never split an athlete across shards
        if rows and rows + t.num_rows > rows_per_shard:
            flush()
        shard.append(t)
        rows += t.num_rows
    flush()


def rowgroup_index(files: list[Path]) -> pl.DataFrame:
    recs = []
    for f in files:
        md = pq.ParquetFile(f).metadata
        ai = md.schema.to_arrow_schema().get_field_index("athlete_id")
        for rg in range(md.num_row_groups):
            r = md.row_group(rg)
            st = r.column(ai).statistics
            recs.append(
                {"file": f.name, "rg": rg, "min": st.min, "max": st.max, "rows": r.num_rows, "bytes": r.total_byte_size,
                 "cbytes": sum(r.column(i).total_compressed_size for i in range(r.num_columns))}
            )
    return pl.DataFrame(recs)


def main() -> None:
    zips = sorted(gcod.list_local_zips())
    tables = []
    for p in zips:
        df = gcod.load_athlete_timeseries(p)
        if df is not None:
            tables.append(df.to_arrow())
    tables.sort(key=lambda t: t.column("athlete_id")[0].as_py())
    total_rows = sum(t.num_rows for t in tables)
    zip_bytes = sum(p.stat().st_size for p in zips)
    print(f"{len(tables)} athletes, {total_rows:,} rows")

    root = gcod.DATA_DIR / "interim" / "06_layouts"
    shutil.rmtree(root, ignore_errors=True)

    # per-athlete first: also gives each athlete's own compressed size and bytes/row
    layouts = {
        "per_athlete": lambda out: write_per_athlete(tables, out),
    }
    t0 = time.perf_counter()
    (root / "per_athlete").mkdir(parents=True)
    layouts["per_athlete"](root / "per_athlete")
    write_s = {"per_athlete": time.perf_counter() - t0}
    own = {f.stem: f.stat().st_size for f in (root / "per_athlete").glob("*.parquet")}
    total_pq = sum(own.values())
    bpr = total_pq / total_rows
    print(f"parquet total {gcod.human_bytes(total_pq)} ({bpr:.2f} B/row)")

    for name, target, rg in [
        ("shard_256MB_rg1M", 256e6, 1_000_000),
        ("shard_1GB_rg1M", 1e9, 1_000_000),
        ("shard_1GB_rg256k", 1e9, 256_000),
    ]:
        out = root / name
        out.mkdir(parents=True)
        t0 = time.perf_counter()
        write_shards(tables, out, target, rg, bpr)
        write_s[name] = time.perf_counter() - t0

    rng_ids = sorted(own)[:: max(1, len(own) // 25)]  # ~25 probe athletes
    results = []
    for name in ["per_athlete", "shard_256MB_rg1M", "shard_1GB_rg1M", "shard_1GB_rg256k"]:
        d = root / name
        files = sorted(d.glob("*.parquet"))
        idx = rowgroup_index(files)
        size = sum(f.stat().st_size for f in files)
        amps, t_one = [], []
        for aid in rng_ids:
            if name == "per_athlete":
                fetched = own[aid]
            else:
                fetched = idx.filter((pl.col("min") <= aid) & (pl.col("max") >= aid))["cbytes"].sum()
            amps.append(fetched / own[aid])
            t0 = time.perf_counter()
            pl.scan_parquet(d / "*.parquet").filter(pl.col("athlete_id") == aid).collect()
            t_one.append(time.perf_counter() - t0)
        t0 = time.perf_counter()
        pl.scan_parquet(d / "*.parquet").group_by("athlete_id").agg(pl.col("power").max(), pl.len()).collect()
        t_scan = time.perf_counter() - t0
        results.append(
            {
                "layout": name,
                "files": len(files),
                "projected_full_files": 6613 if name == "per_athlete" else round(len(files) / (size / (FULL_DATASET_ZIP_BYTES * size / zip_bytes))),
                "MB": size / 1e6,
                "row_groups": idx.height,
                "median_rg_MB": float(idx["cbytes"].median()) / 1e6,
                "read_amp_median": statistics.median(amps),
                "read_amp_max": max(amps),
                "one_athlete_ms": 1000 * statistics.median(t_one),
                "full_scan_s": t_scan,
                "write_s": write_s[name],
            }
        )
        print(results[-1])

    res = pl.DataFrame(results)
    res.write_parquet(gcod.INTERIM_DIR / "06_layouts.parquet")
    sizes = sorted(own.values())
    md = [
        "# 06 · Layout benchmark (raw time series)\n",
        f"Sample: **{len(tables)}** athletes, **{total_rows:,}** rows, {gcod.human_bytes(total_pq)} Parquet "
        f"(Float64 + BSS + zstd9 + CDC + page index).\n",
        f"Per-athlete Parquet size: median {gcod.human_bytes(statistics.median(sizes))}, "
        f"max {gcod.human_bytes(sizes[-1])}, min {gcod.human_bytes(sizes[0])}.\n",
        "`read_amp` = bytes in row groups that may contain the athlete / the athlete's own bytes "
        "(≈ HTTP download amplification without page-index pruning). Shard counts are projected to the full dataset by size.\n",
        gcod.md_table(res.to_dicts()),
    ]
    p = gcod.write_report("06_benchmark_layouts", "\n".join(md))
    print(f"wrote {p}")


if __name__ == "__main__":
    main()
