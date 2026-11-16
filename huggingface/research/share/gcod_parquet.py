# /// script
# requires-python = ">=3.11"
# dependencies = ["polars>=1.20", "pyarrow>=21"]
# ///
"""How GoldenCheetah OpenData (GCOD) time series are stored on Hugging Face, in one file.

Converts one GCOD athlete zip (from OSF) to the opendata time-series layout and checks that
every value is unchanged:

    uv run gcod_parquet.py <athlete>.zip <out_dir>

Conventions shared by the GoldenCheetah datasets on Hugging Face
  * one file per athlete:          timeseries/athlete_id=<uuid>/part.parquet
  * activity key:                  (athlete_id, activity_id); activity_id is the CSV file name
                                   in the GCOD zip without ".csv", i.e. the local start time
  * columns with units:            time_s, distance_m, power_w, hr_bpm, cadence_rpm, altitude_m
  * Float32 for the measurements:  GoldenCheetah wrote every CSV number with 6 significant digits,
                                   and Float32 holds any 6-digit decimal exactly (f"{x:.6g}" gives
                                   back the CSV text), so this is lossless at half the size of Float64
  * distance in metres:            the CSV `km` x 1000; same digits, compresses ~25% better than km
  * writer settings:               WRITE_OPTS below (byte-stream-split floats + zstd 15, CDC, page index)

Dataset-specific columns come after these.
Raw GCOD: 115.6 GB of zips on OSF -> ~45 GB of time series.
"""

import argparse
import io
import re
import zipfile
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

# CSV column -> output column. Scale: km -> m.
COLUMNS = {"secs": "time_s", "km": "distance_m", "power": "power_w", "hr": "hr_bpm", "cad": "cadence_rpm", "alt": "altitude_m"}
SCALE = {"km": 1000.0}
MEASUREMENTS = list(COLUMNS.values())

WRITE_OPTS = dict(
    compression="zstd",
    compression_level=15,
    use_dictionary=["athlete_id", "activity_id"],  # dictionary only for the repeated ids
    column_encoding={c: "BYTE_STREAM_SPLIT" for c in MEASUREMENTS},  # best for float time series
    use_content_defined_chunking=True,  # Hugging Face (Xet) de-duplicates unchanged chunks across versions
    write_page_index=True,  # fast filtered reads, dataset viewer
)
ROW_GROUP_SIZE = 256_000

# CSV names are the local start time; a few have .fit or .fit.csv suffixes and are real activities.
ACTIVITY_RE = re.compile(r"^\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2}(\.fit)?(\.csv)?$")
F32_MIN_NORMAL = 1.1754943508222875e-38


def write(df: pl.DataFrame, out_dir: Path, athlete_id: str) -> Path:
    path = out_dir / "timeseries" / f"athlete_id={athlete_id}" / "part.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(df.to_arrow(), path, row_group_size=ROW_GROUP_SIZE, **WRITE_OPTS)
    return path


# --- raw GCOD ----------------------------------------------------------------------------
def convert_raw(zip_path: Path, out_dir: Path) -> None:
    athlete_id = zip_path.stem
    frames = []
    with zipfile.ZipFile(zip_path) as zf:  # read members directly, never extract
        for name in sorted(zf.namelist()):
            base = name.rsplit("/", 1)[-1]
            if not ACTIVITY_RE.match(base):
                continue
            src = pl.read_csv(io.BytesIO(zf.read(name)), schema={c: pl.Float64 for c in COLUMNS})
            frames.append(src.with_columns(activity_id=pl.lit(base.removesuffix(".csv"))))
    if not frames:
        print(f"{athlete_id}: no activities")
        return
    src = pl.concat(frames)

    # Values Float32 cannot hold: sub-normal junk such as 4.94066e-324 (rare). Opendata keeps
    # them in a separate exceptions table; here we only count them.
    junk = sum(int(((src[c].abs() < F32_MIN_NORMAL) & (src[c] != 0)).sum()) for c in COLUMNS)

    df = src.select(
        pl.lit(athlete_id).alias("athlete_id"),
        "activity_id",
        *[(pl.col(c) * SCALE.get(c, 1.0)).cast(pl.Float32).alias(o) for c, o in COLUMNS.items()],
    )
    path = write(df, out_dir, athlete_id)

    # check: every value prints back as the decimal in the CSV
    bad = 0
    for c, o in COLUMNS.items():
        is_junk = (src[c].abs() < F32_MIN_NORMAL) & (src[c] != 0)
        pairs = pl.DataFrame({"src": src[c], "out": df[o]}).filter(~is_junk.fill_null(False)).drop_nulls().unique()
        for a, b in pairs.iter_rows():
            if float(f"{a * SCALE.get(c, 1.0):.6g}") != float(f"{b:.6g}"):
                bad += 1
    print(f"{athlete_id}: {src.height:,} rows, {len(frames)} activities, zip {zip_path.stat().st_size/1e6:.1f} MB "
          f"-> {path.stat().st_size/1e6:.1f} MB; values differing from the CSV: {bad} (+{junk} sub-normal junk values)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("zip", type=Path, help="one GCOD athlete zip from OSF")
    ap.add_argument("out_dir", type=Path)
    a = ap.parse_args()
    convert_raw(a.zip, a.out_dir)


if __name__ == "__main__":
    main()
