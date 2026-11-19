# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx", "polars"]
# ///
"""Fetch the complete OSF file listing for GCOD (node 6hfpz) and store it as Parquet.

Output: data/osf/listing.parquet  (one row per file: id, name, size, hashes, download url, ...)
        reports/01_osf_listing.md

The listing doubles as the source manifest for the conversion: it has md5/sha256 per
zip, so downloads can be verified and the HF dataset can record which OSF file version
each athlete came from.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))

import httpx
import polars as pl

import gcod


def fetch_folder(client: httpx.Client, url: str, prefix: str = "") -> list[dict]:
    rows = []
    while url:
        r = client.get(url)
        r.raise_for_status()
        d = r.json()
        for x in d["data"]:
            a = x["attributes"]
            if a["kind"] == "folder":
                sub = x["relationships"]["files"]["links"]["related"]["href"]
                rows += fetch_folder(client, sub + "?page[size]=100", prefix + a["name"] + "/")
                continue
            h = a.get("extra", {}).get("hashes", {})
            rows.append(
                {
                    "osf_file_id": x["id"],
                    "guid": a.get("guid"),
                    "path": prefix + a["name"],
                    "name": a["name"],
                    "size": a["size"],
                    "md5": h.get("md5"),
                    "sha256": h.get("sha256"),
                    "date_created": a["date_created"],
                    "date_modified": a["date_modified"],
                    "version": a.get("current_version"),
                    "osf_downloads": a.get("extra", {}).get("downloads"),
                    "download_url": x["links"]["download"],
                }
            )
        url = d["links"].get("next")
        print(f"  {len(rows)} files ...", end="\r", file=sys.stderr)
    return rows


def main() -> None:
    url = f"{gcod.OSF_API}/nodes/{gcod.OSF_NODE}/files/osfstorage/?page[size]=100"
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        rows = fetch_folder(client, url)

    df = pl.DataFrame(rows).with_columns(
        pl.col("date_created").str.to_datetime(time_zone="UTC", strict=False),
        pl.col("date_modified").str.to_datetime(strict=False),
        is_athlete=pl.col("name").str.contains(r"^[0-9a-f-]{36}\.zip$"),
    ).with_columns(
        athlete_id=pl.when(pl.col("is_athlete")).then(pl.col("name").str.replace(r"\.zip$", "")),
    )
    gcod.OSF_LISTING.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(gcod.OSF_LISTING)

    ath = df.filter("is_athlete")
    sizes = ath["size"]
    by_year = (
        ath.group_by(pl.col("date_created").dt.year().alias("year"))
        .agg(n=pl.len(), gb=(pl.col("size").sum() / 1e9).round(1))
        .with_columns(pl.col("year").cast(pl.String))
        .sort("year")
    )
    q = [0.1, 0.25, 0.5, 0.75, 0.9, 0.99]
    md = [
        "# 01 · OSF listing\n",
        f"- Files: **{df.height:,}** ({ath.height:,} athlete zips, {df.height - ath.height} other)",
        f"- Athlete zips total: **{gcod.human_bytes(sizes.sum())}**",
        f"- Zip size quantiles: "
        + ", ".join(f"p{int(x*100)}={gcod.human_bytes(sizes.quantile(x))}" for x in q)
        + f", max={gcod.human_bytes(sizes.max())}, min={sizes.min()} B",
        f"- Duplicate athlete ids: {ath.height - ath['athlete_id'].n_unique()}",
        f"- Zips with sha256: {ath['sha256'].is_not_null().sum():,}",
        "\n## Upload year\n",
        gcod.md_table(by_year.to_dicts()),
        "\n## Non-athlete files\n",
        gcod.md_table(df.filter(~pl.col("is_athlete")).select("path", "size", "date_modified").to_dicts()),
    ]
    p = gcod.write_report("01_osf_listing", "\n".join(md))
    print(f"\nwrote {gcod.OSF_LISTING} and {p}")


if __name__ == "__main__":
    main()
