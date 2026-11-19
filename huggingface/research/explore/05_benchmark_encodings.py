# /// script
# requires-python = ">=3.11"
# dependencies = ["polars", "pyarrow>=21", "numpy"]
# ///
"""Benchmark Parquet encodings for the raw time series.

All variants are written from the same canonical table (sample athletes concatenated,
rows in original order). For each variant we record file size relative to the original
OSF zips, write time, full read time, a column-subset read time, and exactness against
the canonical Float64 parse of the CSV text.

Variant families
  f64_*        Float64 columns (what a CSV reader produces). Lossless.
  f32_*        Float32. Lossy (~7 significant digits).
  int_e9_*     n = round(x * 1e9) as Int64.  Lossless when values have <= 9 significant
               decimals (verified per run) - decoding n / 1e9 gives the identical Float64.
  dec_e9_*     same integers, typed as Parquet DECIMAL(18,9) on INT64 (self-describing).
  *_phys_*     rounded to physical precision first (secs 10 ms, km 1 cm, alt 1 cm,
               power/hr/cad 0.1). Lossy by design; max abs error reported.

Encodings: BSS = BYTE_STREAM_SPLIT (floats), DELTA = DELTA_BINARY_PACKED (ints),
CDC = content-defined chunking (stable pages => Xet dedup across dataset versions).

Usage: uv run research/explore/05_benchmark_encodings.py [--max-athletes 60]
Output: data/interim/05_encodings.parquet, reports/05_benchmark_encodings.md
"""

import argparse
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

import gcod

FLOATS = ["secs", "km", "power", "hr", "cad", "alt"]
LOSSLESS_DEC = {c: 9 for c in FLOATS}
PHYSICAL_DEC = {"secs": 2, "km": 5, "power": 1, "hr": 1, "cad": 1, "alt": 2}
DICT_COLS = ["athlete_id", "activity_id"]
Z9 = {"compression": "zstd", "compression_level": 9}


def int64_to_decimal(arr: pa.ChunkedArray, scale: int) -> pa.Array:
    """Reinterpret Int64 values n as DECIMAL(18, scale) with unscaled value n (exact)."""
    a = arr.combine_chunks()
    vals = a.to_numpy(zero_copy_only=False)
    vals = np.where(np.isnan(vals), 0, vals).astype(np.int64) if vals.dtype != np.int64 else vals
    lo = vals.astype("<i8")
    hi = np.where(lo < 0, -1, 0).astype("<i8")
    buf = np.empty(len(lo) * 2, dtype="<i8")
    buf[0::2], buf[1::2] = lo, hi
    validity = a.buffers()[0]
    return pa.Array.from_buffers(pa.decimal128(18, scale), len(a), [validity, pa.py_buffer(buf.tobytes())], null_count=a.null_count)


def scaled(df: pl.DataFrame, decimals: dict[str, int], as_decimal: bool) -> pa.Table:
    t = df.with_columns(
        (pl.col(c) * 10**d).round().cast(pl.Int64).alias(c) for c, d in decimals.items()
    ).to_arrow()
    if as_decimal:
        for c, d in decimals.items():
            t = t.set_column(t.schema.get_field_index(c), c, int64_to_decimal(t.column(c), d))
    return t


def decode(path: Path, decimals: dict[str, int] | None) -> pl.DataFrame:
    """Read a variant back as Float64 columns.

    NB: polars computes `col / 1e9` as `col * 1e-9`, which is off by 1 ulp for many values.
    We decode with numpy true division (IEEE correctly rounded) and pyarrow's
    decimal->float64 cast, which are exact for values with <= 15 significant digits.
    """
    t = pq.read_table(path)
    cols = {}
    for c in FLOATS:
        a = t.column(c).combine_chunks()
        if pa.types.is_decimal(a.type):
            a = a.cast(pa.float64())
        elif decimals and pa.types.is_integer(a.type):
            vals = a.to_numpy(zero_copy_only=False).astype(np.float64) / 10 ** decimals[c]
            a = pa.array(vals, mask=a.is_null().to_numpy(zero_copy_only=False))
        else:
            a = a.cast(pa.float64())
        cols[c] = a
    return pl.DataFrame({c: pl.from_arrow(cols[c]) for c in FLOATS})


def inexact(ref: pl.DataFrame, got: pl.DataFrame) -> dict[str, tuple[int, float]]:
    """Per column: (# values not bit-identical, max abs error)."""
    out = {}
    for c in FLOATS:
        a, b = ref[c], got[c]
        diff = (a != b).fill_null(a.is_null() != b.is_null())
        out[c] = (int(diff.sum()), float((a - b).abs().max() or 0.0))
    return out


def rounded(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(pl.col(c).round(d) for c, d in PHYSICAL_DEC.items())


def variants(df: pl.DataFrame):
    """name -> (table, writer options, decimals-or-None, as_decimal)"""
    base = df.to_arrow()
    phys = rounded(df)
    bss = {c: "BYTE_STREAM_SPLIT" for c in FLOATS}
    delta = {c: "DELTA_BINARY_PACKED" for c in FLOATS}
    v = {
        "f64_snappy": (base, {}, None, False),
        "f64_zstd3": (base, {"compression": "zstd", "compression_level": 3}, None, False),
        "f64_dict_zstd9": (base, Z9, None, False),
        "f64_bss_zstd3": (base, {"compression": "zstd", "compression_level": 3, "use_dictionary": DICT_COLS, "column_encoding": bss}, None, False),
        "f64_bss_zstd9": (base, {**Z9, "use_dictionary": DICT_COLS, "column_encoding": bss}, None, False),
        "f64_bss_zstd19": (base, {"compression": "zstd", "compression_level": 19, "use_dictionary": DICT_COLS, "column_encoding": bss}, None, False),
        "f32_bss_zstd9": (df.with_columns(pl.col(FLOATS).cast(pl.Float32)).to_arrow(), {**Z9, "use_dictionary": DICT_COLS, "column_encoding": bss}, None, False),
        "int_e9_plain_zstd9": (scaled(df, LOSSLESS_DEC, False), {**Z9, "use_dictionary": DICT_COLS}, LOSSLESS_DEC, False),
        "int_e9_delta_zstd9": (scaled(df, LOSSLESS_DEC, False), {**Z9, "use_dictionary": DICT_COLS, "column_encoding": delta}, LOSSLESS_DEC, False),
        "dec_e9_delta_zstd9": (scaled(df, LOSSLESS_DEC, True), {**Z9, "use_dictionary": DICT_COLS, "column_encoding": delta, "store_decimal_as_integer": True}, LOSSLESS_DEC, True),
        "dec_e9_delta_zstd9_cdc": (scaled(df, LOSSLESS_DEC, True), {**Z9, "use_dictionary": DICT_COLS, "column_encoding": delta, "store_decimal_as_integer": True, "use_content_defined_chunking": True}, LOSSLESS_DEC, True),
        "f64_phys_bss_zstd9": (phys.to_arrow(), {**Z9, "use_dictionary": DICT_COLS, "column_encoding": bss}, None, False),
        "int_phys_delta_zstd9": (scaled(df, PHYSICAL_DEC, False), {**Z9, "use_dictionary": DICT_COLS, "column_encoding": delta}, PHYSICAL_DEC, False),
        "int_phys_dict_zstd9": (scaled(df, PHYSICAL_DEC, False), Z9, PHYSICAL_DEC, False),
        "dec_phys_delta_zstd9": (scaled(df, PHYSICAL_DEC, True), {**Z9, "use_dictionary": DICT_COLS, "column_encoding": delta, "store_decimal_as_integer": True}, PHYSICAL_DEC, True),
    }
    return v


def column_sizes(path: Path) -> dict[str, int]:
    md = pq.ParquetFile(path).metadata
    sizes: dict[str, int] = {}
    for rg in range(md.num_row_groups):
        for i in range(md.num_columns):
            c = md.row_group(rg).column(i)
            sizes[c.path_in_schema] = sizes.get(c.path_in_schema, 0) + c.total_compressed_size
    return sizes


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-athletes", type=int, default=60)
    args = ap.parse_args()

    zips = sorted(gcod.list_local_zips(), key=lambda p: p.stat().st_size)
    step = max(1, len(zips) // args.max_athletes)
    zips = zips[::step][: args.max_athletes]
    zip_bytes = sum(p.stat().st_size for p in zips)
    df = pl.concat([f for p in zips if (f := gcod.load_athlete_timeseries(p)) is not None])
    print(f"{len(zips)} athletes, {df.height:,} rows, zips {gcod.human_bytes(zip_bytes)}")

    results, per_col = [], {}
    with tempfile.TemporaryDirectory(dir=gcod.DATA_DIR) as tmp:
        for name, (table, opts, decs, as_dec) in variants(df).items():
            path = Path(tmp) / f"{name}.parquet"
            t0 = time.perf_counter()
            pq.write_table(table, path, row_group_size=1_000_000, **opts)
            t_write = time.perf_counter() - t0
            t0 = time.perf_counter()
            pl.read_parquet(path)
            t_read = time.perf_counter() - t0
            t0 = time.perf_counter()
            pl.scan_parquet(path).select("athlete_id", "secs", "power").collect()
            t_cols = time.perf_counter() - t0
            err = inexact(df.select(FLOATS), decode(path, decs))
            n_bad = sum(v[0] for v in err.values())
            lossless = n_bad == 0
            size = path.stat().st_size
            results.append(
                {
                    "variant": name,
                    "MB": size / 1e6,
                    "vs_zip": size / zip_bytes,
                    "write_s": t_write,
                    "read_s": t_read,
                    "read_3cols_s": t_cols,
                    "lossless": lossless,
                    "inexact_values": n_bad,
                    "inexact_pct": 100 * n_bad / (df.height * len(FLOATS)),
                    "max_abs_err": ", ".join(f"{k}={v[1]:.2g}" for k, v in err.items() if v[0]) or "",
                }
            )
            per_col[name] = column_sizes(path)
            print(f"  {name:26s} {size/1e6:8.1f} MB  x{size/zip_bytes:.2f} of zip  lossless={lossless}")

    res = pl.DataFrame(results)
    gcod.INTERIM_DIR.mkdir(parents=True, exist_ok=True)
    res.write_parquet(gcod.INTERIM_DIR / "05_encodings.parquet")
    cols = ["athlete_id", "activity_id", *FLOATS]
    col_rows = [
        {"variant": k, **{c: round(v.get(c, 0) / 1e6, 1) for c in cols}}
        for k, v in per_col.items()
        if k in ("f64_bss_zstd9", "int_e9_delta_zstd9", "dec_e9_delta_zstd9", "int_phys_delta_zstd9")
    ]
    proj = [
        {"variant": r["variant"], "projected_full_GB": round(115.6 * r["vs_zip"], 1), "lossless": r["lossless"]}
        for r in results
    ]
    md = [
        "# 05 · Parquet encoding benchmark (raw time series)\n",
        f"Input: **{len(zips)}** athletes spread over the size range, **{df.height:,}** rows; "
        f"original OSF zips **{gcod.human_bytes(zip_bytes)}**. One file, 1M-row row groups.\n",
        "`vs_zip` = Parquet size / zip size. `lossless` = every value bit-identical to the Float64 CSV parse after "
        "decoding (scaled ints decoded with numpy true division; see `decode()`); otherwise the count of inexact values.\n",
        gcod.md_table(res.to_dicts()),
        "\n## Per-column compressed size (MB)\n",
        gcod.md_table(col_rows),
        "\n## Projection to the full 115.6 GB of zips\n",
        gcod.md_table(proj),
    ]
    p = gcod.write_report("05_benchmark_encodings", "\n".join(md))
    print(f"wrote {p}")


if __name__ == "__main__":
    main()
