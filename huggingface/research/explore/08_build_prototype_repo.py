# /// script
# requires-python = ">=3.11"
# dependencies = ["polars", "pyarrow>=21", "datasets>=3", "duckdb"]
# ///
"""End-to-end dry run: build the proposed Hugging Face repo folder from the local sample,
then verify it the way users would consume it.

Builds  data/hf/opendata/
          README.md                      dataset card (datasets/opendata/README.md.tmpl)
          data/athletes/athletes.parquet
          data/activities/part-*.parquet
          data/timeseries/athlete_id=<uuid>/part.parquet   Float32, one file per athlete
          data/timeseries_exceptions/*.parquet   source values Float32 cannot hold
          data/raw_json/part-*.parquet

Checks
  * datasets.load_dataset(<folder>, <subset>) works for every subset (YAML configs valid)
  * join integrity: every activity with has_timeseries has samples and vice versa
  * exactness at the source-text level for every sample athlete: each Float32 value prints
    ("%.6g") as exactly the decimal in the CSV (x1000 for distance), and every exception row
    restores the exact Float64 source value
  * a DuckDB query over the folder with the same glob as on the Hub

Nothing is uploaded.

Usage: uv run research/explore/08_build_prototype_repo.py
"""

import argparse
import datetime as dt
import shutil
import sys
import time
from decimal import Decimal
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))

import duckdb
import polars as pl
import pyarrow.parquet as pq

import convert
import gcod

TEMPLATE = gcod.ROOT / "datasets" / "opendata" / "README.md.tmpl"


def human_count(n: int) -> str:
    for unit, div in (("B", 1e9), ("M", 1e6), ("k", 1e3)):
        if n >= div:
            return f"{n / div:.1f}{unit}"
    return str(n)


def build(out: Path) -> dict:
    zips = gcod.list_local_zips()
    listing = pl.read_parquet(gcod.OSF_LISTING)
    shutil.rmtree(out, ignore_errors=True)
    t0 = time.perf_counter()

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
    aths = convert.build_athletes(arows, listing)

    for sub in ("athletes", "activities", "raw_json"):
        (out / "data" / sub).mkdir(parents=True)
    pq.write_table(aths.to_arrow(), out / "data/athletes/athletes.parquet", **convert.TABLE_WRITE_OPTS)
    pq.write_table(acts.to_arrow(), out / "data/activities/part-00000.parquet", row_group_size=50_000, **convert.TABLE_WRITE_OPTS)
    pq.write_table(
        pl.DataFrame(raws).sort("athlete_id").to_arrow(), out / "data/raw_json/part-00000.parquet",
        row_group_size=100, **convert.TABLE_WRITE_OPTS,
    )

    exc_sink: list[pl.DataFrame] = []
    ts_files = convert.write_athlete_files(convert.iter_timeseries(zips, exc_sink), out / "data/timeseries")
    (out / "data/timeseries_exceptions").mkdir(parents=True)
    exc = pl.concat(exc_sink) if exc_sink else pl.DataFrame(schema=convert.EXCEPTIONS_SCHEMA)
    pq.write_table(exc.to_arrow(), out / "data/timeseries_exceptions/exceptions.parquet", **convert.TABLE_WRITE_OPTS)
    n_rows = sum(pq.ParquetFile(f).metadata.num_rows for f in ts_files)
    elapsed = time.perf_counter() - t0

    card = TEMPLATE.read_text()
    for k, v in {
        "n_athletes": f"{aths.height:,}",
        "n_activities": f"{acts.height:,}",
        "n_rows": human_count(n_rows),
        "repo_id": "goldencheetah/opendata",
        "code_url": "https://github.com/goldencheetah/opendata",
        "date": dt.date.today().isoformat(),
        "doi_sentence": "",
    }.items():
        card = card.replace("{{" + k + "}}", v)
    (out / "README.md").write_text(card)
    return {"zips": zips, "acts": acts, "aths": aths, "ts_files": ts_files, "n_rows": n_rows, "build_s": elapsed, "exc": exc}


def verify(out: Path, b: dict) -> list[dict]:
    from datasets import load_dataset

    checks = []

    def check(name, ok, detail=""):
        checks.append({"check": name, "ok": "✅" if ok else "❌", "detail": detail})
        print(("OK  " if ok else "FAIL"), name, detail)

    for cfg in ("athletes", "activities", "raw_json", "timeseries_exceptions"):
        ds = load_dataset(str(out), cfg, split="train")
        check(f"load_dataset('{cfg}')", ds.num_rows > 0, f"{ds.num_rows:,} rows, {len(ds.column_names)} columns")
    ds = load_dataset(str(out), "timeseries", split="train", streaming=True)
    first = next(iter(ds))
    check("load_dataset('timeseries', streaming=True)", set(first) >= {"athlete_id", "time_s", "power_w"}, str(first)[:120])

    ts = pl.scan_parquet(out / "data/timeseries/*/*.parquet")
    ts_keys = ts.select("athlete_id", "activity_id").unique().collect()
    act_keys = b["acts"].filter("has_timeseries").select("athlete_id", "activity_id")
    missing = act_keys.join(ts_keys, on=["athlete_id", "activity_id"], how="anti").height
    orphan = ts_keys.join(act_keys, on=["athlete_id", "activity_id"], how="anti").height
    check("join: activities.has_timeseries ⇔ timeseries", missing == 0 and orphan == 0, f"missing={missing}, orphan={orphan}")
    check("athletes unique", b["aths"]["athlete_id"].is_unique().all(), "")
    check("activities key unique", b["acts"].filter(pl.col("activity_id").is_not_null()).select("athlete_id", "activity_id").is_unique().all(), "")

    # exactness at the source-text level vs a fresh CSV parse
    n_vals = n_bad = n_exc_bad = 0
    exc_all = b["exc"]
    for z in b["zips"]:
        ref = gcod.load_athlete_timeseries(z)
        if ref is None:
            continue
        got = ts.filter(pl.col("athlete_id") == z.stem).collect()
        if got.height != ref.height or not got["activity_id"].equals(ref["activity_id"]):
            n_bad += 1
            continue
        ref = ref.with_columns(row_in_activity=pl.int_range(pl.len(), dtype=pl.Int32).over("activity_id"))
        exc = exc_all.filter(pl.col("athlete_id") == z.stem)
        for src_col, out_col in convert.TS_SOURCE_TO_OUTPUT.items():
            factor = int(convert.TS_SCALE.get(src_col, 1))
            pairs = pl.DataFrame({"src": ref[src_col], "got": got[out_col],
                                  "activity_id": ref["activity_id"], "row": ref["row_in_activity"]})
            e = exc.filter(pl.col("source_column") == src_col)
            if e.height:  # exception rows must restore the exact source value
                chk = pairs.join(e, left_on=["activity_id", "row"], right_on=["activity_id", "row_in_activity"])
                n_exc_bad += chk.filter(pl.col("src") != pl.col("source_value")).height + (e.height - chk.height)
                pairs = pairs.join(e, left_on=["activity_id", "row"], right_on=["activity_id", "row_in_activity"], how="anti")
            nulls_ok = pairs.filter(pl.col("src").is_null() != pl.col("got").is_null()).height
            n_bad += nulls_ok
            u = pairs.drop_nulls().select("src", "got").unique()
            n_vals += pairs.height
            for a, g in zip(u["src"].to_list(), u["got"].to_list()):
                if Decimal(f"{a:.6g}") * factor != Decimal(f"{g:.6g}"):
                    n_bad += 1
    check("timeseries exact at source-text level (all athletes, all values)", n_bad == 0,
          f"{len(b['zips'])} athletes, {n_vals:,} values, {n_bad} mismatches")
    check("timeseries_exceptions restore exact Float64 source values", n_exc_bad == 0,
          f"{exc_all.height} exception rows, {n_exc_bad} mismatches")

    con = duckdb.connect()
    r = con.execute(
        f"SELECT count(*) AS n, count(DISTINCT athlete_id) AS a, max(power_w) AS pmax "
        f"FROM '{out}/data/timeseries/*/*.parquet'"
    ).fetchone()
    check("duckdb glob query", r[0] == b["n_rows"], f"rows={r[0]:,}, athletes={r[1]}, max power={r[2]}")
    return checks


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=gcod.DATA_DIR / "hf" / "opendata")
    args = ap.parse_args()

    b = build(args.out)
    checks = verify(args.out, b)

    files = sorted(p for p in args.out.rglob("*") if p.is_file())
    tree = [{"path": str(p.relative_to(args.out)), "size": gcod.human_bytes(p.stat().st_size)} for p in files]
    zip_bytes = sum(z.stat().st_size for z in b["zips"])
    pq_bytes = sum(p.stat().st_size for p in files)
    md = [
        "# 08 · Prototype Hugging Face repo (dry run)\n",
        f"Built from **{len(b['zips'])}** sample athletes ({gcod.human_bytes(zip_bytes)} of OSF zips) in "
        f"{b['build_s']:.0f} s → **{gcod.human_bytes(pq_bytes)}** total "
        f"({pq_bytes / zip_bytes:.2f}× the zips), {b['n_rows']:,} time-series rows.\n",
        "## Checks\n",
        gcod.md_table(checks),
        "\n## Repo tree\n",
        gcod.md_table(tree),
        f"\nDataset card: `{args.out.relative_to(gcod.ROOT)}/README.md`\n",
    ]
    p = gcod.write_report("08_build_prototype_repo", "\n".join(md))
    print(f"wrote {p}")


if __name__ == "__main__":
    main()
