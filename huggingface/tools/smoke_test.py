# /// script
# requires-python = ">=3.11"
# dependencies = ["datasets>=4", "duckdb>=1.1", "huggingface_hub>=2.4", "httpx>=0.27", "polars[rt64]>=1.33",
#                 "pyarrow>=21"]
# ///
"""Read a dataset on the Hugging Face Hub the way its users will, and check what comes back.

    uv run tools/smoke_test.py <dataset.toml> <repo_id> [--local <dataset folder>]

For every table in dataset.toml, through each of the three ways the cards show:

  polars     pl.scan_parquet("hf://datasets/<repo>/<files>"): columns, a few rows, the row count
  duckdb     SELECT ... FROM 'hf://datasets/<repo>/<files>': columns, the row count
  datasets   load_dataset(<repo>, <subset>, streaming=True): columns of the first row

For clustered tables (the time series), polars and DuckDB also read the rows of one athlete,
the way the cards show: filtered on athlete_id, over all files.

Columns must be the declared ones, in order (extra columns follow them), and the row counts of
polars and DuckDB must agree, with each other and with --local (the folder that was uploaded).
Counting a large table's rows reads only file footers, so it stays within the Hub's rate limits.
Also reports the Dataset Viewer status (informational: private repos may not have one).
Uses the token of `hf auth login`, so private repos work. Exit code 0 when everything passes.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import conventions as C  # noqa: E402


def expected_columns(t: C.TableSpec) -> list[str]:
    return [c.name for c in t.columns]


def columns_problem(got: list[str], t: C.TableSpec) -> str | None:
    want = expected_columns(t)
    if got[: len(want)] != want or (len(got) > len(want) and t.extra is None):
        return f"columns {got[:len(want) + 2]}{' ...' if len(got) > len(want) + 2 else ''}, expected {want}"
    return None


def local_rows(folder: Path, t: C.TableSpec, value: str | None = None) -> int:
    """Rows of a table in the local folder; with `value`, only those of that cluster value."""
    import polars as pl

    lf = pl.scan_parquet(folder / t.glob)
    if value is not None:
        lf = lf.filter(pl.col(t.cluster_by) == value)
    return lf.select(pl.len()).collect().item()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("spec", type=Path)
    ap.add_argument("repo_id")
    ap.add_argument("--local", type=Path, help="the dataset folder that was uploaded, to compare row counts")
    a = ap.parse_args()

    import duckdb
    import polars as pl
    from huggingface_hub import get_token

    from datasets import load_dataset

    spec = C.load_dataset_spec(a.spec)
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("CREATE SECRET hf (TYPE huggingface, PROVIDER credential_chain)")
    failures: list[str] = []

    def fail(table: str, how: str, msg: str) -> None:
        failures.append(f"{table} [{how}]: {msg}")
        print(f"    FAIL {how}: {msg}")

    for name, t in spec.tables.items():
        url = f"hf://datasets/{a.repo_id}/{t.glob}"
        print(f"{name}  ({url})")
        counts: dict[str, int] = {}
        one: dict[str, int] = {}  # rows of one cluster value (clustered tables)
        value = None

        t0 = time.monotonic()
        try:
            lf = pl.scan_parquet(url)
            cols = lf.collect_schema().names()
            if p := columns_problem(cols, t):
                fail(name, "polars", p)
            head = lf.head(3).collect()
            counts["polars"] = lf.select(pl.len()).collect().item()
            print(f"    polars    {counts['polars']:>15,} rows, {len(cols)} columns, head {head.shape} "
                  f"({time.monotonic() - t0:.0f} s)")
            if t.cluster_by and head.height:
                value = head[t.cluster_by][0]
                t0 = time.monotonic()
                one["polars"] = lf.filter(pl.col(t.cluster_by) == value).select(pl.len()).collect().item()
                print(f"    polars    {one['polars']:>15,} rows of {t.cluster_by} {value} "
                      f"({time.monotonic() - t0:.0f} s)")
        except Exception as e:  # noqa: BLE001 - report and go on with the other readers
            fail(name, "polars", f"{type(e).__name__}: {e}")

        t0 = time.monotonic()
        try:
            cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{url}'").fetchall()]
            if p := columns_problem(cols, t):
                fail(name, "duckdb", p)
            counts["duckdb"] = con.execute(f"SELECT count(*) FROM '{url}'").fetchone()[0]
            print(f"    duckdb    {counts['duckdb']:>15,} rows, {len(cols)} columns ({time.monotonic() - t0:.0f} s)")
            if value is not None:
                t0 = time.monotonic()
                one["duckdb"] = con.execute(f"SELECT count(*) FROM '{url}' WHERE {t.cluster_by} = ?",
                                            [value]).fetchone()[0]
                print(f"    duckdb    {one['duckdb']:>15,} rows of {t.cluster_by} {value} "
                      f"({time.monotonic() - t0:.0f} s)")
        except Exception as e:  # noqa: BLE001
            fail(name, "duckdb", f"{type(e).__name__}: {e}")

        t0 = time.monotonic()
        try:
            ds = load_dataset(a.repo_id, name, split="train", streaming=True)
            row = next(iter(ds))
            if p := columns_problem(list(row), t):
                fail(name, "datasets", p)
            print(f"    datasets  first row has {len(row)} columns ({time.monotonic() - t0:.0f} s)")
        except Exception as e:  # noqa: BLE001
            fail(name, "datasets", f"{type(e).__name__}: {e}")

        if a.local:
            counts["local"] = local_rows(a.local, t)
            if value is not None:
                one["local"] = local_rows(a.local, t, value)
        if len(set(counts.values())) > 1:
            fail(name, "rows", f"row counts differ: {counts}")
        if len(set(one.values())) > 1:
            fail(name, "rows", f"row counts of {t.cluster_by} {value} differ: {one}")

    try:
        import httpx

        r = httpx.get("https://datasets-server.huggingface.co/is-valid", params={"dataset": a.repo_id},
                      headers={"Authorization": f"Bearer {get_token()}"}, timeout=30)
        print(f"viewer: {r.status_code} {r.text[:300]}")
    except Exception as e:  # noqa: BLE001
        print(f"viewer: could not ask ({type(e).__name__}: {e})")

    print(f"\n{'FAIL' if failures else 'PASS'}: {len(spec.tables)} tables, {len(failures)} problems")
    for f in failures:
        print(f"  {f}")
    return 1 if failures else 0


if __name__ == "__main__":
    code = main()
    sys.stdout.flush()
    # A `datasets` stream stopped early can hang at interpreter exit (its prefetch threads):
    # everything is done and reported here, so exit without waiting for them.
    os._exit(code)
