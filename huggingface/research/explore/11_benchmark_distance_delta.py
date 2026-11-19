# /// script
# requires-python = ">=3.11"
# dependencies = ["polars", "pyarrow>=21", "numpy"]
# ///
"""Should distance be stored as per-sample increments ("metres in this sample") instead of
the cumulative value?

Compares, on the same rows (Float32 representation family, zstd 9, 256k row groups):
  cum_m_f32          cumulative metres, Float32 + BYTE_STREAM_SPLIT   (current choice)
  delta_m_f32_bss    per-sample increment in metres, Float32 + BSS
  delta_m_f32_dict   same, dictionary encoding
  delta_cm_int_delta increment in integer cm (+ Parquet DELTA)          (lossy < 1 km, like cm_int)
And checks whether the cumulative values can be reconstructed exactly from the increments:
  cumsum (float64, per activity, starting at the first value) then rounding to 6 significant
  digits must give back the original 6-digit decimal.

Output: reports/11_benchmark_distance_delta.md
"""

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


def size(arr: pa.Array, encoding: str | None, dictionary: bool, tmp: Path, name: str) -> float:
    opts = dict(compression="zstd", compression_level=9, use_dictionary=dictionary,
                use_content_defined_chunking=True, write_page_index=True)
    if encoding:
        opts |= {"column_encoding": {"x": encoding}, "use_dictionary": False}
    p = tmp / f"{name}.parquet"
    pq.write_table(pa.table({"x": arr}), p, row_group_size=256_000, **opts)
    return p.stat().st_size / 1e6


def main() -> None:
    zips = sorted(gcod.list_local_zips(), key=lambda p: p.stat().st_size)[::2][:60]
    df = pl.concat([f for p in zips if (f := gcod.load_athlete_timeseries(p)) is not None])
    df = df.with_columns(m=pl.col("km") * 1000)
    df = df.with_columns(dm=pl.col("m").diff().over("athlete_id", "activity_id").fill_null(pl.col("m")))
    n = df.height
    with tempfile.TemporaryDirectory(dir=gcod.DATA_DIR) as t:
        tmp = Path(t)
        cum = pa.array(df["m"].cast(pl.Float32).to_numpy())
        d32 = df["dm"].cast(pl.Float32)
        dcm = (df["dm"] * 100).round().cast(pl.Int32)
        rows = [
            {"variant": "cum_m_f32 (current)", "MB": size(cum, "BYTE_STREAM_SPLIT", False, tmp, "a")},
            {"variant": "delta_m_f32_bss", "MB": size(pa.array(d32.to_numpy()), "BYTE_STREAM_SPLIT", False, tmp, "b")},
            {"variant": "delta_m_f32_dict", "MB": size(pa.array(d32.to_numpy()), None, True, tmp, "c")},
            {"variant": "delta_cm_int_delta", "MB": size(pa.array(dcm.to_numpy()), "DELTA_BINARY_PACKED", False, tmp, "d")},
        ]

    # reconstruction exactness from Float32 increments
    rec = df.with_columns(d32=d32).with_columns(
        rec=pl.col("d32").cast(pl.Float64).cum_sum().over("athlete_id", "activity_id")
    )
    x = rec["m"].to_numpy()
    r = rec["rec"].to_numpy()
    u = pl.DataFrame({"x": x, "r": r}).unique()
    bad = sum(Decimal(f"{a:.6g}") != Decimal(f"{b:.6g}") for a, b in zip(u["x"].to_list(), u["r"].to_list()))
    drift = np.abs(r - x)
    # how many increments are negative or non-monotonic (GPS resets etc.)
    neg = int((df["dm"] < 0).sum())
    md = [
        "# 11 · Cumulative distance vs per-sample increments\n",
        f"{len(zips)} athletes, {n:,} rows.\n",
        gcod.md_table(rows),
        "\n## Rebuilding cumulative distance from Float32 increments\n",
        f"- distinct (original, rebuilt) pairs that differ after rounding to 6 significant digits: **{bad:,}** of {u.height:,}",
        f"- max absolute drift before rounding: {drift.max():.3g} m; median {np.median(drift):.3g} m",
        f"- negative increments (distance going backwards): {neg:,}",
    ]
    p = gcod.write_report("11_benchmark_distance_delta", "\n".join(md))
    print("\n".join(md))


if __name__ == "__main__":
    main()
