# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "datasets>=4",
#     "duckdb>=1.1",
#     "fsspec",
#     "huggingface_hub>=2.4",
#     "httpx>=0.27",
#     "pandas>=2.2",
#     "polars[rt64]>=2",
#     "pyarrow>=21",
# ]
# ///
"""Which time series layout serves people who read the dataset from the Hugging Face Hub?

The first full upload (one file per athlete, `athlete_id=<id>/part.parquet`, 6,509 files) could
not be read as a whole: DuckDB lists every athlete folder with its own API call and is cut off by
the Hub's rate limit (HTTP 429), and polars spends minutes on file metadata even to read one
athlete. Report 06 compared layouts on local disk only, where none of this shows.

This script builds the same rows in several layouts, uploads them side by side to one Hub repo
(one config per layout), and times what users do, in a fresh process each time and anonymously
(as most users read), recording the Hub's rate-limit budget each run used:

    count      the number of rows (Parquet metadata only)
    athlete    every sample of one athlete (median size)
    activity   one activity
    scattered  20 activities of 20 different athletes, as found through `activities`
    scan       mean heart rate over the whole table (one column of every file: 4.2 of 47 GB)
    stream     the first 100,000 rows with `datasets` streaming

through polars (`scan_parquet("hf://...")`), DuckDB (`FROM 'hf://...'`), pandas
(`read_parquet("hf://...", filters=...)`) and `datasets`.

Every layout holds exactly the rows of the build in data/build/opendata, written with the
conventions' time series writer settings; only the files and row groups differ. Athletes are
never split across files, and files hold athletes in athlete_id order.

    uv run research/explore/14_hub_layouts.py build              # -> data/lab/14_layouts/ (~50 GB per layout)
    uv run research/explore/14_hub_layouts.py upload --repo GoldenCheetah/opendata-layouts
    uv run research/explore/14_hub_layouts.py bench --repo GoldenCheetah/opendata-layouts
    uv run research/explore/14_hub_layouts.py report

Output: data/interim/14_bench.jsonl (one line per run), reports/14_hub_layouts.md
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import random
import re
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

HF = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(HF))

import conventions as C  # noqa: E402

BUILD = HF / "data" / "build" / "opendata"
SOURCE = BUILD / "data" / "timeseries"
LAB = HF / "data" / "lab" / "14_layouts"
INTERIM = HF / "data" / "interim"
RESULTS = INTERIM / "14_bench.jsonl"
REPORT = HF / "research" / "reports" / "14_hub_layouts.md"
HUB = "https://huggingface.co"


@dataclasses.dataclass(frozen=True)
class Layout:
    name: str
    files: int | None          # None: one file per athlete
    row_group: int = 256_000   # rows; the production profile's size unless stated
    hive: bool = False         # athlete_id=<id>/part.parquet folders

    def folder(self, repo: str) -> str:
        """The layout's folder: on the Hub, or in a local folder (to check the queries)."""
        return f"{repo}/{self.name}" if repo.startswith("/") else f"hf://datasets/{repo}/{self.name}"

    def glob(self, repo: str) -> str:
        return f"{self.folder(repo)}/{'athlete_id=*/*.parquet' if self.hive else '*.parquet'}"


LAYOUTS = [
    Layout("per_athlete_hive", None, hive=True),   # what opendata-test has
    Layout("per_athlete_flat", None),
    Layout("files_400_rg1m", 400, 1_000_000),
    Layout("files_100_rg256k", 100, 256_000),
    Layout("files_100_rg1m", 100, 1_000_000),
    Layout("files_100_rg4m", 100, 4_000_000),
]
BY_NAME = {lay.name: lay for lay in LAYOUTS}


# --- build ---------------------------------------------------------------------------------------

def source_files() -> list[tuple[str, Path, int]]:
    """(athlete_id, file, bytes) for every athlete with a time series, in athlete_id order."""
    out = []
    for d in sorted(SOURCE.iterdir()):
        f = d / "part.parquet"
        out.append((d.name.removeprefix("athlete_id="), f, f.stat().st_size))
    return out


def pack(sources: list[tuple[str, Path, int]], n_files: int) -> list[list[tuple[str, Path, int]]]:
    """Contiguous runs of athletes, about equal in bytes: each athlete goes to the file its middle
    byte falls in, so there are at most n_files and no athlete is split."""
    total = sum(s for *_, s in sources)
    groups: dict[int, list] = {}
    cum = 0
    for src in sources:
        groups.setdefault(int((cum + src[2] / 2) / total * n_files), []).append(src)
        cum += src[2]
    return [groups[k] for k in sorted(groups)]


def profile_for(row_group: int) -> str:
    name = f"timeseries_rg{row_group}"
    C.WRITER_PROFILES.setdefault(name, dataclasses.replace(C.WRITER_PROFILES["timeseries"], row_group_size=row_group))
    return name


def write_packed(out: str, members: list[tuple[str, str]], row_group: int) -> tuple[str, int, int]:
    """Concatenate whole athlete files into one file. Returns (out, rows written, rows read)."""
    import pyarrow.parquet as pq

    os.nice(10)
    schema = pq.read_schema(members[0][1])
    expected = sum(pq.ParquetFile(f).metadata.num_rows for _, f in members)
    with C.TableWriter(Path(out), schema, profile_for(row_group), dataset="opendata", table_name="timeseries") as w:
        for _, f in members:
            for batch in pq.ParquetFile(f).iter_batches(batch_size=row_group):
                w.write(pa_table(batch, schema))
    written = pq.ParquetFile(out).metadata.num_rows
    return out, written, expected


def pa_table(batch, schema):
    import pyarrow as pa

    return pa.Table.from_batches([batch]).cast(schema)


def build(jobs: int) -> None:
    import polars as pl

    sources = source_files()
    print(f"{len(sources):,} athletes, {sum(s for *_, s in sources) / 1e9:.1f} GB", flush=True)
    for lay in LAYOUTS:
        root = LAB / lay.name
        done = root / ".done"
        if done.exists():
            print(f"{lay.name}: done", flush=True)
            continue
        t0 = time.monotonic()
        index = []
        if lay.files is None:   # the build's own files, hard-linked under a new name
            for aid, f, _ in sources:
                dst = root / (f"athlete_id={aid}/part.parquet" if lay.hive else f"{aid}.parquet")
                dst.parent.mkdir(parents=True, exist_ok=True)
                if not dst.exists():
                    os.link(f, dst)
                index.append({"athlete_id": aid, "file": str(dst.relative_to(root))})
        else:
            groups = pack(sources, lay.files)
            root.mkdir(parents=True, exist_ok=True)
            work = {}
            for i, g in enumerate(groups):
                out = root / f"part-{i:05d}.parquet"
                index += [{"athlete_id": aid, "file": out.name} for aid, *_ in g]
                if not out.exists():
                    work[str(out)] = [(aid, str(f)) for aid, f, _ in g]
            with ProcessPoolExecutor(jobs) as ex:
                futures = [ex.submit(write_packed, o, m, lay.row_group) for o, m in work.items()]
                for n, fut in enumerate(futures, 1):
                    out, written, expected = fut.result()
                    if written != expected:
                        raise SystemExit(f"{out}: wrote {written:,} rows, expected {expected:,}")
                    print(f"  {lay.name}: {n}/{len(futures)} files", end="\r", flush=True)
        pl.DataFrame(index).write_parquet(INTERIM / f"14_index_{lay.name}.parquet")
        done.write_text(json.dumps({"seconds": round(time.monotonic() - t0)}))
        print(f"{lay.name}: {len(set(r['file'] for r in index)):,} files ({time.monotonic() - t0:.0f} s)", flush=True)
    verify()


def verify() -> None:
    """Every layout holds exactly the build's rows: per athlete, the same number of rows, and for
    a sample of athletes the same values in the same order."""
    import polars as pl
    import pyarrow.parquet as pq

    sources = source_files()
    rows = {aid: pq.ParquetFile(f).metadata.num_rows for aid, f, _ in sources}
    sample = random.Random(14).sample(sources, 25)
    for lay in LAYOUTS:
        idx = pl.read_parquet(INTERIM / f"14_index_{lay.name}.parquet")
        assert idx["athlete_id"].is_unique().all() and set(idx["athlete_id"]) == set(rows), lay.name
        per_file = (idx.with_columns(rows=pl.col("athlete_id").replace_strict(rows))
                    .group_by("file").agg(pl.col("rows").sum()))
        for file, n in per_file.iter_rows():
            assert pq.ParquetFile(LAB / lay.name / file).metadata.num_rows == n, (lay.name, file)
        files = dict(idx.iter_rows())
        for aid, f, _ in sample:
            want = pl.read_parquet(f)
            got = (pl.read_parquet(LAB / lay.name / files[aid], hive_partitioning=False)
                   .filter(pl.col("athlete_id") == aid))
            assert got.equals(want), (lay.name, aid)
        print(f"verified {lay.name}: {len(rows):,} athletes, {sum(rows.values()):,} rows", flush=True)


# --- upload --------------------------------------------------------------------------------------

CARD = """---
license: cc0-1.0
pretty_name: GoldenCheetah OpenData time series layouts (experiment)
configs:
{configs}
---

# Time series layouts (experiment)

**Not a dataset to use**: this repo compares ways to lay out the time series of
[GoldenCheetah OpenData](https://huggingface.co/datasets/GoldenCheetah/opendata-test) on the Hub,
for `huggingface/research/explore/14_hub_layouts.py` in
[GoldenCheetah/OpenData](https://github.com/GoldenCheetah/OpenData). Every config holds the same
13.3 billion rows; only the files and row groups differ. It will be deleted once the layout is
chosen.
"""


def upload(repo: str) -> None:
    from huggingface_hub import HfApi

    configs = "\n".join(
        f"- config_name: {lay.name}\n  data_files: \"{lay.name}/{'athlete_id=*/' if lay.hive else ''}*.parquet\""
        for lay in LAYOUTS
    )
    (LAB / "README.md").write_text(CARD.format(configs=configs))
    api = HfApi()
    api.create_repo(repo, repo_type="dataset", exist_ok=True, private=False)
    api.upload_folder(repo_id=repo, repo_type="dataset", folder_path=LAB, ignore_patterns=["**/.done"],
                      commit_message="Time series layouts for research/explore/14_hub_layouts.py")


# --- bench ---------------------------------------------------------------------------------------

def targets() -> dict:
    """The ids the queries use, fixed from the build: the median athlete by size, its median
    activity, and 20 activities of 20 random athletes."""
    import polars as pl

    sources = source_files()
    by_size = sorted(sources, key=lambda s: s[2])
    aid = by_size[len(by_size) // 2][0]
    ts = pl.read_parquet(SOURCE / f"athlete_id={aid}" / "part.parquet", columns=["activity_id"])
    lengths = ts.group_by("activity_id").len().sort("len")
    act = lengths["activity_id"][lengths.height // 2]
    rng = random.Random(14)
    scattered = []
    for a, f, _ in rng.sample(sources, 20):
        ids = pl.read_parquet(f, columns=["activity_id"])["activity_id"].unique().sort().to_list()
        scattered.append((a, rng.choice(ids)))
    import pyarrow.parquet as pq

    total = sum(pq.ParquetFile(f).metadata.num_rows for _, f, _ in sources)
    converter = json.loads((BUILD.parent / "check.json").read_text()).get("converter")
    return {"total_rows": total, "converter": converter,
            "athlete": aid, "athlete_rows": ts.height, "activity": [aid, act],
            "activity_rows": int(lengths["len"][lengths.height // 2]), "scattered": scattered}


READERS = {
    "polars": ["count", "athlete", "activity", "scattered", "scan"],
    "duckdb": ["count", "athlete", "activity", "scattered", "scan"],
    "pandas": ["count", "athlete", "activity"],
    "datasets": ["stream"],
}
TIMEOUT = {"scan": 3600, "stream": 900}
DEFAULT_TIMEOUT = 1200


def run_one(repo: str, layout: str, reader: str, query: str, t: dict) -> dict:
    """One query in this process; returns {"rows": ..., "value": ...}."""
    lay = BY_NAME[layout]
    url = lay.glob(repo)
    aid = t["athlete"]
    pairs = [tuple(p) for p in ([t["activity"]] if query == "activity" else t["scattered"])]

    if reader == "polars":
        import polars as pl

        lf = pl.scan_parquet(url, hive_partitioning=False)
        if query == "count":
            return {"rows": lf.select(pl.len()).collect().item()}
        if query == "athlete":
            return {"rows": lf.filter(pl.col("athlete_id") == aid).collect().height}
        if query in ("activity", "scattered"):
            athletes = sorted({a for a, _ in pairs})
            keys = pl.DataFrame(pairs, schema=["athlete_id", "activity_id"], orient="row")
            df = (lf.filter(pl.col("athlete_id").is_in(athletes))
                  .join(keys.lazy(), on=["athlete_id", "activity_id"]).collect())
            return {"rows": df.height}
        if query == "scan":
            r = lf.select(pl.col("hr_bpm").mean().alias("mean"), pl.col("hr_bpm").count().alias("n")).collect()
            return {"rows": int(r[0, 1]), "value": float(r[0, 0])}

    if reader == "duckdb":
        import duckdb

        con = duckdb.connect()
        if os.environ.get("HF_LAB_AUTH"):
            con.execute("CREATE SECRET hf (TYPE huggingface, PROVIDER credential_chain)")
        src = f"read_parquet('{url}', hive_partitioning = false)"
        if query == "count":
            return {"rows": con.execute(f"SELECT count(*) FROM {src}").fetchone()[0]}
        if query == "athlete":
            sql = f"SELECT count(*) FROM (SELECT * FROM {src} WHERE athlete_id = ?)"
            return {"rows": con.execute(sql, [aid]).fetchone()[0]}
        if query in ("activity", "scattered"):
            athletes = sorted({a for a, _ in pairs})
            cond = " OR ".join("(athlete_id = ? AND activity_id = ?)" for _ in pairs)
            sql = (f"SELECT count(*) FROM (SELECT * FROM {src} WHERE athlete_id IN ({', '.join('?' * len(athletes))}) "
                   f"AND ({cond}))")
            return {"rows": con.execute(sql, athletes + [x for p in pairs for x in p]).fetchone()[0]}
        if query == "scan":
            mean, n = con.execute(f"SELECT avg(hr_bpm), count(hr_bpm) FROM {src}").fetchone()
            return {"rows": n, "value": mean}

    if reader == "pandas":
        import pandas as pd

        folder = lay.folder(repo)
        if query == "count":   # what pandas users reach for: pyarrow's dataset over HfFileSystem
            return {"rows": _pa_count(folder)}
        filters = [("athlete_id", "==", aid)]
        if query == "activity":
            filters.append(("activity_id", "==", t["activity"][1]))
        return {"rows": len(pd.read_parquet(folder, filters=filters, partitioning=None))}

    if reader == "datasets":
        from datasets import load_dataset

        ds = load_dataset(repo, layout, split="train", streaming=True)
        n = 0
        for _ in ds:
            n += 1
            if n == 100_000:
                break
        return {"rows": n}

    raise ValueError(f"{reader} {query}")


def _pa_count(folder: str) -> int:
    import pyarrow.dataset as pads
    from huggingface_hub import HfFileSystem

    if not folder.startswith("hf://"):
        return pads.dataset(folder, format="parquet", partitioning=None).count_rows()
    fs = HfFileSystem()
    return pads.dataset(folder.removeprefix("hf://"), filesystem=fs, format="parquet", partitioning=None).count_rows()


def budgets(auth: bool) -> dict:
    """Remaining budget per rate-limit policy, and seconds to its reset, anonymous or as the
    logged-in user."""
    import httpx
    from huggingface_hub import get_token

    headers = {"Authorization": f"Bearer {get_token()}"} if auth else {}
    out = {}
    for policy, url in [("api", f"{HUB}/api/datasets/GoldenCheetah/opendata-test/tree/main?limit=1"),
                        ("resolvers", f"{HUB}/datasets/GoldenCheetah/opendata-test/resolve/main/README.md")]:
        try:
            r = httpx.head(url, headers=headers, follow_redirects=False, timeout=30)
            fields = dict(kv.split("=") for kv in r.headers.get("ratelimit", "").split(";")[1:])
            out[policy] = {"left": int(fields["r"]), "reset_s": int(fields["t"])}
        except Exception as e:  # noqa: BLE001 - informational
            out[policy] = {"error": f"{type(e).__name__}: {e}"}
    return out


QUOTA = {False: {"api": 500, "resolvers": 3000}, True: {"api": 1000, "resolvers": 5000}}   # measured


def wait_for_fresh_budget(auth: bool) -> None:
    """Start every run with at least half the budget: more than any run that succeeds uses (the
    most is ~900 resolves), so a run is never cut short by the runs before it."""
    while True:
        b = budgets(auth)
        short = [p for p, v in b.items() if "left" in v and v["left"] < QUOTA[auth][p] // 2]
        if not short:
            return
        wait = max(b[p]["reset_s"] for p in short) + 2
        print(f"    (waiting {wait} s for a fresh rate-limit window: {short})", flush=True)
        time.sleep(wait)


def summarize_error(stderr: str) -> str:
    """The exception line of a traceback, plus any HTTP status or reason in what follows it."""
    lines = [x.rstrip() for x in stderr.strip().splitlines()]
    for i, x in enumerate(lines):
        if re.match(r"^[A-Za-z_.]+(Error|Exception)\b", x):
            rest = " ".join(y.strip() for y in lines[i:i + 30] if y.strip() and not set(y.strip()) <= set("^~ "))
            return rest[:600]
    return " ".join(lines[-5:])[:600]


def bench(repo: str, layouts: list[str], readers: list[str], queries: list[str] | None, repeat: int,
          auth: bool) -> None:
    t = targets()
    (INTERIM / "14_targets.json").write_text(json.dumps(t, indent=1))
    done, failed = set(), set()
    if RESULTS.exists():
        for line in RESULTS.read_text().splitlines():
            r = json.loads(line)
            done.add((r["layout"], r["reader"], r["query"], r["rep"], r.get("auth", False)))
            if not r["ok"]:
                failed.add((r["layout"], r["reader"], r["query"], r.get("auth", False)))
    env = {k: v for k, v in os.environ.items() if k not in ("HF_TOKEN", "HUGGING_FACE_HUB_TOKEN")}
    env |= {"PYTHONUNBUFFERED": "1"} | ({"HF_LAB_AUTH": "1"} if auth else
                                       {"HF_HUB_DISABLE_IMPLICIT_TOKEN": "1", "HF_TOKEN": ""})
    for rep in range(repeat):
        for layout in layouts:
            for reader in readers:
                for query in READERS[reader]:
                    if queries and query not in queries:
                        continue
                    if query in ("scan", "stream") and rep > 0:
                        continue
                    if (layout, reader, query, rep, auth) in done:
                        continue
                    if rep > 0 and (layout, reader, query, auth) in failed:
                        continue   # a repeat measures time; a failure is reported once
                    wait_for_fresh_budget(auth)
                    before = budgets(auth)
                    t0 = time.monotonic()
                    try:
                        p = subprocess.run(
                            [sys.executable, __file__, "run-one", "--repo", repo, "--layout", layout,
                             "--reader", reader, "--query", query],
                            capture_output=True, text=True, env=env, timeout=TIMEOUT.get(query, DEFAULT_TIMEOUT))
                        out = json.loads(p.stdout.strip().splitlines()[-1]) if p.returncode == 0 else None
                        error = None if out else (summarize_error(p.stderr) or f"exit {p.returncode}")
                    except subprocess.TimeoutExpired:
                        out, error = None, f"timeout after {TIMEOUT.get(query, DEFAULT_TIMEOUT)} s"
                    seconds = time.monotonic() - t0
                    after = budgets(auth)
                    used = {}
                    for pol in QUOTA[auth]:
                        b, a = before.get(pol, {}), after.get(pol, {})
                        # countable only when the run ended inside the window it started in
                        if "left" in b and "left" in a and seconds + 5 < b["reset_s"]:
                            used[pol] = b["left"] - a["left"] - 1   # minus the probe itself
                    rec = {"layout": layout, "reader": reader, "query": query, "rep": rep, "auth": auth,
                           "budget_before": before,
                           "seconds": round(seconds, 1), "ok": error is None, "error": error,
                           "result": out, "budget_used": used, "budget_after": after,
                           "at": time.strftime("%Y-%m-%d %H:%M:%S")}
                    with RESULTS.open("a") as f:
                        f.write(json.dumps(rec) + "\n")
                    if error:
                        failed.add((layout, reader, query, auth))
                    status = f"{out['rows']:,} rows" if out else f"FAIL {error[:120]}"
                    who = "logged in" if auth else "anonymous"
                    print(f"{layout:18} {reader:8} {query:9} rep {rep} {who}: {seconds:7.1f} s  {status}  used {used}",
                          flush=True)


# --- report --------------------------------------------------------------------------------------

def expected_rows(t: dict) -> dict[str, int]:
    """What each query must return, from the local build."""
    import polars as pl

    files = {aid: f for aid, f, _ in source_files()}
    scattered = sum(
        pl.read_parquet(files[a], columns=["activity_id"]).filter(pl.col("activity_id") == act).height
        for a, act in t["scattered"])
    return {"count": t["total_rows"], "athlete": t["athlete_rows"], "activity": t["activity_rows"],
            "scattered": scattered, "stream": 100_000}


FINDINGS = """\
## Findings

- **The Hub's rate limits decide the layout, not file sizes.** Reading a file costs at least one
  `resolvers` request, and each row group read costs another; listing a folder costs an `api` request.
  6,509 per-athlete files exceed the anonymous budget (3,000 resolves per 5 min) before a query starts:
  polars and DuckDB fail on every query, pandas needs 11–13 min for one athlete. Logging in (5,000
  resolves) does not rescue it: polars counts in 5 min but fails on one athlete, DuckDB times out.
- **About 100 files sorted by athlete work for every reader.** Each athlete stays in one file, so
  row-group statistics on `athlete_id` let readers skip the other 99 files' data: one athlete or one
  activity takes 4–11 s with polars and DuckDB, 10–22 s with pandas, using under 10% of the anonymous
  budget. 400 files are 2–3× slower; DuckDB resolves each file twice.
- **1M-row row groups.** 256k doubles the resolves without a gain; 4M is up to 2× faster for pandas but
  the Dataset Viewer drops to preview only (no viewer, search, filter or statistics, still so after
  reprocessing), while 1M keeps every viewer feature.
- **Full-table scans over HTTP are not reliable at any layout**, anonymous or logged in: they need a
  resolve per row group and column (~13,000 for one column at 1M) and run into the limit; the two
  that passed took 4.5 and 18 min. Analyses of the whole table should download it first (47 GB,
  `hf download`), then read it locally.
- **polars needs `polars[rt64]`** for more than 4.3 billion rows (the table has 13.3 billion).
- **`datasets` streaming is layout-independent** (11–13 s to the first 100,000 rows) but a stream
  stopped early can hang at interpreter exit (10 of 15 runs); the benchmark measures time to result.
"""


def report() -> None:
    import polars as pl

    runs = [json.loads(line) for line in RESULTS.read_text().splitlines()]
    t = json.loads((INTERIM / "14_targets.json").read_text())
    expected = expected_rows(t)
    scans = {(r["result"]["rows"], round(r["result"]["value"], 3)) for r in runs if r["ok"] and r["query"] == "scan"}
    if len(scans) > 1:
        raise SystemExit(f"full scans disagree: {scans}")

    def wrong(r: dict) -> str | None:
        """A successful run that returned the wrong rows is a failure too."""
        want = expected.get(r["query"])
        if r["ok"] and want is not None and r["result"]["rows"] != want:
            return f"wrong result: {r['result']['rows']:,} rows, expected {want:,}"
        return None

    df = pl.DataFrame([{k: r[k] for k in ("layout", "reader", "query", "rep", "seconds")} |
                       {"ok": r["ok"] and not wrong(r), "error": r["error"] or wrong(r),
                        "auth": r.get("auth", False), "api": r["budget_used"].get("api"),
                        "resolvers": r["budget_used"].get("resolvers")} for r in runs])
    sizes = {}
    for lay in LAYOUTS:
        files = [p for p in (LAB / lay.name).rglob("*.parquet")]
        sizes[lay.name] = (len(files), sum(p.stat().st_size for p in files))
    lines = [
        "# 14 · Time series layouts on the Hub",
        "",
        f"Every layout holds the same {t['total_rows']:,} rows of the converter {t['converter']} build, "
        "written with the conventions' time series settings; only files and row groups differ. Queries run "
        "anonymously unless marked logged in, each in a fresh process with a fresh rate-limit window. Times are the median of the "
        "repetitions; `api` and `resolvers` are the Hub requests a run used (anonymous budget per 5 min: "
        f"{QUOTA[False]['api']:,} and {QUOTA[False]['resolvers']:,}; logged in: {QUOTA[True]['api']:,} and "
        f"{QUOTA[True]['resolvers']:,}).",
        "",
        f"Targets: athlete `{t['athlete']}` ({t['athlete_rows']:,} rows, the median athlete by size), its "
        f"median activity ({t['activity_rows']:,} rows), and 20 activities of 20 random athletes.",
        "",
        FINDINGS,
        "## Results",
        "",
        "| layout | files | GB | median file MB |",
        "|---|---:|---:|---:|",
    ]
    for lay in LAYOUTS:
        n, b = sizes[lay.name]
        lines.append(f"| `{lay.name}` | {n:,} | {b / 1e9:.1f} | {b / max(n, 1) / 1e6:.1f} |")
    for auth, reader in [(a, r) for a in (False, True) for r in READERS]:
        mine = df.filter((pl.col("auth") == auth) & (pl.col("reader") == reader))
        if mine.is_empty():
            continue
        lines += ["", f"### {reader}, {'logged in' if auth else 'anonymous'}", "",
                  "| layout | " + " | ".join(READERS[reader]) + " |", "|---|" + "---:|" * len(READERS[reader])]
        for lay in LAYOUTS:
            if mine.filter(pl.col("layout") == lay.name).is_empty():
                continue
            cells = []
            for q in READERS[reader]:
                sel = mine.filter((pl.col("layout") == lay.name) & (pl.col("query") == q))
                if sel.is_empty():
                    cells.append("")
                elif not sel["ok"].all():
                    err = sel.filter(~pl.col("ok"))["error"][0] or ""
                    cells.append("fails: " + ("rate limited" if "429" in err else "timeout" if "timeout" in err.lower()
                                              else err[:60].replace("|", "/")))
                else:
                    api, res = sel["api"].max(), sel["resolvers"].max()
                    used = "/".join("?" if v is None else str(v) for v in (api, res))
                    cells.append(f"{sel['seconds'].median():.1f} s ({used})")
            lines.append(f"| `{lay.name}` | " + " | ".join(cells) + " |")
    lines += ["", "Cells: median seconds (api calls / file resolves used)."]
    REPORT.write_text("\n".join(lines) + "\n")
    print(REPORT.read_text())


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--jobs", type=int, default=max(1, (os.cpu_count() or 2) // 2))
    sub.add_parser("verify")
    u = sub.add_parser("upload")
    u.add_argument("--repo", required=True)
    be = sub.add_parser("bench")
    be.add_argument("--repo", required=True)
    be.add_argument("--layouts", nargs="*", default=[lay.name for lay in LAYOUTS])
    be.add_argument("--readers", nargs="*", default=list(READERS))
    be.add_argument("--queries", nargs="*")
    be.add_argument("--repeat", type=int, default=2)
    be.add_argument("--auth", action="store_true", help="as the logged-in user instead of anonymously")
    r1 = sub.add_parser("run-one")
    for a in ("--repo", "--layout", "--reader", "--query"):
        r1.add_argument(a, required=True)
    sub.add_parser("report")
    args = ap.parse_args()
    INTERIM.mkdir(parents=True, exist_ok=True)
    if args.cmd == "build":
        build(args.jobs)
    elif args.cmd == "verify":
        verify()
    elif args.cmd == "upload":
        upload(args.repo)
    elif args.cmd == "bench":
        bench(args.repo, args.layouts, args.readers, args.queries, args.repeat, args.auth)
    elif args.cmd == "run-one":
        t = json.loads((INTERIM / "14_targets.json").read_text())
        print(json.dumps(run_one(args.repo, args.layout, args.reader, args.query, t)), flush=True)
        # Time to the result, not to interpreter teardown: `datasets` streaming stopped early can
        # hang at exit (in 10 of 15 runs; reports/14_hub_layouts.md), which is not the layout's doing.
        os._exit(0)
    elif args.cmd == "report":
        report()


if __name__ == "__main__":
    main()
