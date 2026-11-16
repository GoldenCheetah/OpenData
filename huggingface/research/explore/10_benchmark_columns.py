# /// script
# requires-python = ">=3.11"
# dependencies = ["polars", "pyarrow>=21", "numpy"]
# ///
"""Per-column representation benchmark, informed by 09 (all values have <= 6 significant
digits, so float32 is exact at the source-text level except for sub-normal garbage).

Each column is written on its own (compression is per column, so sizes add up), with the
production layout (256k-row row groups, CDC, page index), for several representations:

  generic      f64_bss, f64_dict, f32_bss, f32_dict
  distance     km_f32_bss, m_f32_bss / m_f32_dict           (metres as float32)
               m_int32_delta  (integer metres, lossy < 100 km)
               dm_int32_delta (decimetres), cm_int32_delta (centimetres),
               mm_int32_delta (millimetres), um_int64_delta (micrometres)
  others       int-scaled + DELTA / dictionary where the column is mostly integers
               (power, hr, cad as Int16/Int32 ×1 or ×10; secs ×100 Int32; alt ×10/×100 Int32)

Exactness is judged at the level of the source text (the decimal number in the CSV):
  * float representations: float(f"{v:.6g}") == decimal value  (exhaustive on distinct values)
  * integer representations: n == decimal value × scale exactly
We report the number of values that are *not* exact and the max absolute error.

Usage: uv run research/explore/10_benchmark_columns.py [--max-athletes 60] [--level 9]
Output: data/interim/10_columns.parquet, reports/10_benchmark_columns.md
"""

import argparse
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

import gcod

RG = 256_000


def write_col(arr: pa.Array, path: Path, encoding: str | None, dictionary: bool, level: int) -> int:
    t = pa.table({"x": arr})
    opts = dict(
        compression="zstd", compression_level=level, use_dictionary=dictionary,
        use_content_defined_chunking=True, write_page_index=True,
    )
    if encoding:
        opts["column_encoding"] = {"x": encoding}
        opts["use_dictionary"] = False
    pq.write_table(t, path, row_group_size=RG, **opts)
    return path.stat().st_size


def exact_float_repr(x: np.ndarray, y: np.ndarray, factor: int) -> tuple[int, float]:
    """x: float64 source values (decimal-exact parse), y: candidate float array representing
    x*factor. Exact if the 6-significant-digit decimal of y equals decimal(x)*factor.
    Checked once per distinct x value."""
    ux, idx, counts = np.unique(x, return_index=True, return_counts=True)
    bad = 0
    err = 0.0
    yy = y[idx]
    for a, b, c in zip(ux.tolist(), yy.tolist(), counts.tolist()):
        da = Decimal(f"{a:.6g}") * factor
        db = Decimal(f"{float(b):.6g}")
        if da != db:
            bad += c
            err = max(err, abs(float(da - db)) / factor)
    return bad, err


def exact_int_repr(x: np.ndarray, n: np.ndarray, scale: int) -> tuple[int, float]:
    """Exact if n == decimal(x) * scale. Uses the float64 test n / scale == x, which is exact
    for |n| < 2**53 (numpy true division is correctly rounded)."""
    back = n.astype(np.float64) / scale
    neq = back != x
    bad = int(neq.sum())
    err = float(np.abs(back - x)[neq].max()) if bad else 0.0
    return bad, err


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-athletes", type=int, default=60)
    ap.add_argument("--level", type=int, default=9)
    args = ap.parse_args()

    zips = sorted(gcod.list_local_zips(), key=lambda p: p.stat().st_size)
    step = max(1, len(zips) // args.max_athletes)
    zips = zips[::step][: args.max_athletes]
    zip_bytes = sum(p.stat().st_size for p in zips)
    df = pl.concat([f for p in zips if (f := gcod.load_athlete_timeseries(p)) is not None])
    n_rows = df.height
    print(f"{len(zips)} athletes, {n_rows:,} rows, zips {gcod.human_bytes(zip_bytes)}")

    cands: list[tuple[str, str, pa.Array, str | None, bool, str, int]] = []
    # (column, representation, array, encoding, dictionary, kind, scale/factor)

    def add_float(col, name, values: np.ndarray, mask, factor=1):
        f64 = pa.array(values, mask=mask)
        f32 = pa.array(values.astype(np.float32), mask=mask)
        if name.startswith("f64") or name == "all":
            cands.append((col, f"{name}_f64_bss", f64, "BYTE_STREAM_SPLIT", False, "f64", factor))
            cands.append((col, f"{name}_f64_dict", f64, None, True, "f64", factor))
        cands.append((col, f"{name}_f32_bss", f32, "BYTE_STREAM_SPLIT", False, "f32", factor))
        cands.append((col, f"{name}_f32_dict", f32, None, True, "f32", factor))

    def add_int(col, name, values: np.ndarray, mask, scale, dtype):
        n = np.round(np.nan_to_num(values) * scale)
        if np.abs(n).max() >= np.iinfo(dtype).max:
            return
        arr = pa.array(n.astype(dtype), mask=mask)
        cands.append((col, f"{name}_delta", arr, "DELTA_BINARY_PACKED", False, "int", scale))
        cands.append((col, f"{name}_dict", arr, None, True, "int", scale))

    for col in gcod.TS_COLUMNS:
        s = df[col]
        mask = s.is_null().to_numpy()
        x = s.fill_null(0.0).to_numpy()
        if col == "km":
            add_float(col, "all", x, mask)                       # km as f64 / f32
            add_float(col, "m", x * 1000, mask, factor=1000)       # metres as float32
            for unit, scale, dt in [("m_int", 1_000, np.int32), ("dm_int", 10_000, np.int32),
                                    ("cm_int", 100_000, np.int32), ("mm_int", 1_000_000, np.int64),
                                    ("um_int", 1_000_000_000, np.int64)]:
                add_int(col, unit, x, mask, scale, dt)
        else:
            add_float(col, "all", x, mask)
            ints = {
                "secs": [("cs_int", 100, np.int32), ("s_int", 1, np.int32)],
                "power": [("int", 1, np.int32), ("x10_int", 10, np.int32)],
                "hr": [("int", 1, np.int16), ("x10_int", 10, np.int16)],
                "cad": [("int", 1, np.int32), ("x10_int", 10, np.int32)],
                "alt": [("dm_int", 10, np.int32), ("cm_int", 100, np.int32)],
            }[col]
            for name, scale, dt in ints:
                add_int(col, name, x, mask, scale, dt)

    rows = []
    with tempfile.TemporaryDirectory(dir=gcod.DATA_DIR) as tmp:
        for col, rep, arr, enc, dic, kind, scale in cands:
            path = Path(tmp) / f"{col}_{rep}.parquet"
            size = write_col(arr, path, enc, dic, args.level)
            src = df[col]
            m = ~src.is_null().to_numpy()
            x = src.fill_null(0.0).to_numpy()[m]
            if kind == "f64":
                bad, err = 0, 0.0
            elif kind == "f32":
                y = arr.to_numpy(zero_copy_only=False)[m]
                bad, err = exact_float_repr(x, y, scale)
            else:
                n = arr.fill_null(0).to_numpy()[m]
                bad, err = exact_int_repr(x, n, scale)
            rows.append({"column": col, "representation": rep, "MB": size / 1e6, "inexact": bad,
                         "inexact_pct": 100 * bad / max(len(x), 1), "max_abs_err_source_units": err})
            print(f"  {col:5s} {rep:22s} {size/1e6:8.1f} MB  inexact={bad:,}")

    res = pl.DataFrame(rows)
    res.write_parquet(gcod.INTERIM_DIR / "10_columns.parquet")

    # best exact (text-level, allowing <=0.001% exceptions stored in a side table) per column
    tol = 0.001
    best_exact = (
        res.filter(pl.col("inexact_pct") <= tol).sort("MB").group_by("column", maintain_order=True).first()
    )
    baseline = res.filter(pl.col("representation") == "all_f64_bss").select("column", base_MB="MB")
    best_exact = best_exact.join(baseline, on="column").with_columns(saving_pct=(1 - pl.col("MB") / pl.col("base_MB")) * 100)
    ids_mb = 0.7 * n_rows / 55_685_628  # athlete_id + activity_id (measured in 05)
    total_base = baseline["base_MB"].sum() + ids_mb
    total_best = best_exact["MB"].sum() + ids_mb
    f32_all = res.filter(pl.col("representation") == "all_f32_bss")["MB"].sum() + ids_mb

    md = [
        "# 10 · Per-column representation benchmark\n",
        f"{len(zips)} athletes, {n_rows:,} rows, {gcod.human_bytes(zip_bytes)} of zips. zstd {args.level}, "
        f"{RG:,}-row row groups, CDC + page index. Exactness judged against the decimal value in the CSV text.\n",
        "## Best exact representation per column (≤ 0.001 % exceptions, kept in a side table)\n",
        gcod.md_table(best_exact.to_dicts()),
        f"\n| layout | MB | vs zip | projected full |\n|---|---|---|---|\n"
        f"| all Float64 + BSS (current plan) | {total_base:,.0f} | {total_base*1e6/zip_bytes:.2f} | {115.6*total_base*1e6/zip_bytes:.0f} GB |\n"
        f"| all Float32 + BSS | {f32_all:,.0f} | {f32_all*1e6/zip_bytes:.2f} | {115.6*f32_all*1e6/zip_bytes:.0f} GB |\n"
        f"| best exact per column | {total_best:,.0f} | {total_best*1e6/zip_bytes:.2f} | {115.6*total_best*1e6/zip_bytes:.0f} GB |\n",
        "\n## All candidates\n",
    ]
    for col in gcod.TS_COLUMNS:
        md += [f"\n### {col}\n", gcod.md_table(res.filter(pl.col("column") == col).sort("MB").drop("column").to_dicts())]
    p = gcod.write_report("10_benchmark_columns", "\n".join(md))
    print(f"wrote {p}")


if __name__ == "__main__":
    main()
