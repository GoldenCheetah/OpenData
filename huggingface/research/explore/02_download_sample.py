# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx", "polars"]
# ///
"""Select a stratified sample of athletes and download their zips from OSF.

Stratification: zip-size deciles x upload year, so small/large athletes and all three
GoldenCheetah upload periods (2018/2019/2020) are represented. The N largest zips are
always added to exercise worst-case files.

Usage:
    uv run research/explore/02_download_sample.py [--per-stratum 5] [--largest 3] [--seed 42]

Output: data/osf/athletes/{uuid}.zip  (sha256-verified)
        data/sample_athletes.parquet
"""

import argparse
import hashlib
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))

import httpx
import polars as pl

import gcod


def select_sample(listing: pl.DataFrame, per_stratum: int, largest: int, seed: int) -> pl.DataFrame:
    ath = listing.filter("is_athlete").with_columns(
        year=pl.col("date_created").dt.year(),
        size_decile=pl.col("size").rank("ordinal").mul(10).floordiv(pl.len() + 1),
    )
    sample = (
        ath.with_columns(r=pl.int_range(pl.len()).shuffle(seed=seed).over("year", "size_decile"))
        .filter(pl.col("r") < per_stratum)
        .drop("r")
    )
    biggest = ath.sort("size", descending=True).head(largest)
    return pl.concat([sample, biggest]).unique("athlete_id").sort("size")


def download(client: httpx.Client, row: dict, attempts: int = 5) -> tuple[str, str]:
    """Download with retries: OSF intermittently answers 500/403 under load."""
    for i in range(attempts):
        try:
            return _download_once(client, row)
        except (httpx.HTTPStatusError, httpx.TransportError):
            if i == attempts - 1:
                raise
            time.sleep(5 * 2**i)
    raise AssertionError


def _download_once(client: httpx.Client, row: dict) -> tuple[str, str]:
    out = gcod.OSF_ZIPS_DIR / f"{row['athlete_id']}.zip"
    if out.exists() and hashlib.sha256(out.read_bytes()).hexdigest() == row["sha256"]:
        return row["athlete_id"], "cached"
    tmp = out.with_suffix(".part")
    h = hashlib.sha256()
    with client.stream("GET", row["download_url"]) as r:
        r.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in r.iter_bytes(1 << 20):
                f.write(chunk)
                h.update(chunk)
    if h.hexdigest() != row["sha256"]:
        tmp.unlink()
        raise ValueError(f"sha256 mismatch for {row['athlete_id']}")
    tmp.rename(out)
    return row["athlete_id"], "downloaded"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--per-stratum", type=int, default=5)
    ap.add_argument("--largest", type=int, default=3)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--workers", type=int, default=3)
    args = ap.parse_args()

    listing = pl.read_parquet(gcod.OSF_LISTING)
    sample = select_sample(listing, args.per_stratum, args.largest, args.seed)
    sample.write_parquet(gcod.SAMPLE_FILE)
    print(f"sample: {sample.height} athletes, {gcod.human_bytes(sample['size'].sum())}")

    gcod.OSF_ZIPS_DIR.mkdir(parents=True, exist_ok=True)
    done = 0
    with httpx.Client(timeout=300, follow_redirects=True) as client, ThreadPoolExecutor(args.workers) as ex:
        futs = [ex.submit(download, client, r) for r in sample.to_dicts()]
        for f in as_completed(futs):
            try:
                aid, status = f.result()
            except Exception as e:  # keep going; report at the end
                print(f"  FAILED: {e}")
                continue
            done += 1
            print(f"  [{done}/{sample.height}] {aid} {status}")


if __name__ == "__main__":
    main()
