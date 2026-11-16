# /// script
# requires-python = ">=3.11"
# dependencies = ["polars>=1.20", "pyarrow>=21", "pyyaml>=6"]
# ///
"""Check a built dataset against its dataset.toml and the shared conventions.

    uv run tools/validate.py <dataset.toml> <dataset folder> [--jobs 8] [--max-files N]

The dataset folder is what gets uploaded to Hugging Face: README.md (the card) and data/.
Exit code 0 when every check passes, 1 otherwise. Checks:

  layout     every declared table has files where the conventions put them; nothing else in data/
  schema     column names, order and types; shared columns as defined in conventions.py; verbatim
             column groups; the same schema in every file of a table
  writer     zstd, page index, byte-stream-split floats and row-group size for the table's profile;
             the `goldencheetah` file metadata (dataset, table, conventions version)
  values     required columns without nulls; id patterns; unique keys; time series partition value,
             activity order and contiguity; declared time_s guarantees (grid, monotonic)
  relations  every key referenced across tables exists (e.g. timeseries -> activities)
  card       README.md configs: one subset per table, the right files, the right default

`--max-files N` checks only the first N files of each table (quick runs); relations are then
checked only for the keys seen.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

import conventions as C  # noqa: E402

MAX_EXAMPLES = 5


# --- findings ------------------------------------------------------------------------
@dataclass
class Findings:
    """Problems grouped by (table, check), with a count and a few examples each."""
    counts: dict[tuple[str, str], int] = field(default_factory=lambda: defaultdict(int))
    examples: dict[tuple[str, str], list[str]] = field(default_factory=lambda: defaultdict(list))

    def add(self, table: str, check: str, example: str, n: int = 1) -> None:
        k = (table, check)
        self.counts[k] += n
        if len(self.examples[k]) < MAX_EXAMPLES:
            self.examples[k].append(example)

    def merge(self, other: "Findings") -> None:
        for k, n in other.counts.items():
            self.counts[k] += n
            for e in other.examples[k]:
                if len(self.examples[k]) < MAX_EXAMPLES:
                    self.examples[k].append(e)

    def __bool__(self) -> bool:
        return bool(self.counts)


# --- per file ------------------------------------------------------------------------
@dataclass
class FileResult:
    path: str
    rows: int
    schema: tuple[tuple[str, str], ...]
    findings: Findings
    keys: dict[tuple[str, ...], set[tuple]]   # distinct values of the column sets relations need
    key_rows: list[tuple] | None              # the table key of every row (tables with a key)


def check_file(path: Path, root: Path, ds: C.DatasetSpec, t: C.TableSpec,
               relation_cols: list[tuple[str, ...]]) -> FileResult:
    _low_priority()
    f = Findings()
    rel = str(path.relative_to(root))
    pf = pq.ParquetFile(path)
    schema = pf.schema_arrow
    md = pf.metadata
    types = {fl.name: C.logical_type(fl.type) for fl in schema}
    names = list(schema.names)

    # schema: declared columns first, in order, then (if declared) a group of extra columns
    declared = [c.name for c in t.columns]
    if names[: len(declared)] != declared:
        i = next((i for i, (a, b) in enumerate(zip(names, declared)) if a != b), min(len(names), len(declared)))
        f.add(t.name, "columns", f"{rel}: expected {declared[i:i + 3]}... at position {i}, found {names[i:i + 3]}...")
    for c in t.columns:
        if c.name in types and types[c.name] != c.type:
            f.add(t.name, "column types", f"{rel}: {c.name} is {types[c.name]}, should be {c.type}")
    extras = [n for n in names if n not in set(declared)]
    if extras and not t.extra:
        f.add(t.name, "undeclared columns", f"{rel}: {extras[:5]}", len(extras))
    elif t.extra:
        for n in extras:
            if n in C.SHARED_COLUMNS:
                f.add(t.name, "shared column name in a column group", f"{rel}: {n}")
            if types[n] not in t.extra.types:
                f.add(t.name, "column group types", f"{rel}: {n} is {types[n]}, allowed {list(t.extra.types)}")
            if not t.extra.verbatim_names and not re.match(C.NAME_RE, n):
                f.add(t.name, "column names", f"{rel}: {n!r} is not lower snake_case")

    # file metadata
    raw = (schema.metadata or {}).get(C.FILE_METADATA_KEY)
    if raw is None:
        f.add(t.name, "file metadata", f"{rel}: no `goldencheetah` metadata (write with conventions.write_table)")
    else:
        meta = json.loads(raw)
        want = {"dataset": ds.name, "table": t.name, "conventions": ds.conventions}
        if meta != want:
            f.add(t.name, "file metadata", f"{rel}: {meta}, expected {want}")

    # writer settings
    profile = C.WRITER_PROFILES[t.profile]
    declared_enc = {c.name: c.encoding for c in t.columns if c.encoding}
    reported: set[tuple[str, str]] = set()  # one finding per file and column, not per row group

    def once(check: str, name: str, msg: str) -> None:
        if (check, name) not in reported:
            reported.add((check, name))
            f.add(t.name, check, msg)

    for rg in range(md.num_row_groups):
        g = md.row_group(rg)
        if profile.row_group_size and g.num_rows > profile.row_group_size:
            f.add(t.name, "row group size", f"{rel}: row group {rg} has {g.num_rows:,} rows (max {profile.row_group_size:,})")
        for ci in range(g.num_columns):
            col = g.column(ci)
            name = col.path_in_schema.split(".")[0]
            if col.compression != "ZSTD":
                once("compression", name, f"{rel}: {name} is {col.compression}")
            # Offset index always; column index wherever there are min/max statistics to put in it
            # (Parquet drops them for very long values such as whole JSON documents).
            has_min_max = col.is_stats_set and col.statistics is not None and col.statistics.has_min_max
            if not col.has_offset_index or (has_min_max and not col.has_column_index):
                once("page index", name, f"{rel}: {name} has no page index")
            # Encoding only affects size: enforce declared encodings, and the default that matters
            # (byte-stream-split floats in time series, which are most of a dataset's bytes).
            default = C.default_encoding(t.profile, types.get(name, ""))
            want = declared_enc.get(name) or (default if default == "byte_stream_split" else None)
            if want == "byte_stream_split" and "BYTE_STREAM_SPLIT" not in col.encodings:
                once("encoding", name, f"{rel}: {name} should be byte-stream-split, is {col.encodings}")
            elif want == "dictionary" and not {"RLE_DICTIONARY", "PLAIN_DICTIONARY"} & set(col.encodings):
                once("encoding", name, f"{rel}: {name} should be dictionary-encoded, is {col.encodings}")

    # values: read only the columns some check needs, one row group at a time (bounded memory)
    by_name = {c.name: c for c in t.columns}
    time_col = by_name.get("time_s")
    time_checks = bool(time_col and (time_col.grid or time_col.monotonic))
    needed = {c.name for c in t.columns if c.required or c.pattern}
    needed |= set(t.key) | {c for cols in relation_cols for c in cols}
    if t.partition_by:
        needed |= {t.partition_by, "activity_id"}
    if time_checks:
        needed |= {"activity_id", "time_s"}
    needed = sorted(needed & set(names))

    nulls: dict[str, int] = defaultdict(int)
    distinct: dict[str, set] = defaultdict(set)       # pattern and partition columns
    runs: list[str] = []                              # activity_id of each run of rows, in order
    grid_off = backwards = 0
    time_example = None
    prev: pl.DataFrame | None = None                  # last row of the previous row group (time checks)
    keys: dict[tuple[str, ...], set[tuple]] = {cols: set() for cols in relation_cols if set(cols) <= set(needed)}
    key_rows: list[tuple] | None = [] if t.key and set(t.key) <= set(needed) else None
    for rg in range(md.num_row_groups if needed else 0):
        df = pl.from_arrow(pf.read_row_group(rg, columns=needed))
        for c in t.columns:
            if c.name in df.columns and c.required:
                nulls[c.name] += df[c.name].null_count()
            if c.name in df.columns and (c.pattern or c.name == t.partition_by):
                distinct[c.name] |= set(df[c.name].drop_nulls().unique().to_list())
        if t.partition_by and "activity_id" in df.columns and df.height:
            r = df.select(pl.col("activity_id").filter(pl.col("activity_id") != pl.col("activity_id").shift(1).fill_null("")))
            seq = r["activity_id"].to_list()
            if runs and seq and seq[0] == runs[-1]:
                seq = seq[1:]                         # the same run continues across row groups
            runs += seq
        if time_checks and df.height:
            ext = pl.concat([prev, df.select("activity_id", "time_s")]) if prev is not None else df.select("activity_id", "time_s")
            d = ext.with_columns(step=pl.col("time_s").diff().over("activity_id")).slice(0 if prev is None else 1)
            if time_col.grid:
                g = time_col.grid
                bad = d.filter(((pl.col("time_s") / g).round(0) * g != pl.col("time_s"))
                               | (pl.col("step").is_not_null() & (pl.col("step") != g)))
                grid_off += bad.height
            else:
                bad = d.filter(pl.col("step") <= 0)
                backwards += bad.height
            if bad.height and time_example is None:
                time_example = bad["activity_id"][0]
            prev = df.select("activity_id", "time_s").tail(1)
        for cols in keys:
            keys[cols] |= set(df.select(cols).unique().iter_rows())
        if key_rows is not None:
            key_rows += list(df.select(t.key).iter_rows())

    for name, n in nulls.items():
        if n:
            f.add(t.name, "nulls in required columns", f"{rel}: {name} has {n:,} nulls", n)
    for c in t.columns:
        if c.pattern and c.name in distinct:
            bad_ids = sorted(v for v in distinct[c.name] if not re.search(c.pattern, v))
            if bad_ids:
                f.add(t.name, "id format", f"{rel}: {c.name} {bad_ids[:3]}", len(bad_ids))
    if t.partition_by and t.partition_by in distinct:
        expected = path.parent.name.split("=", 1)[1]
        if distinct[t.partition_by] != {expected}:
            f.add(t.name, "partition value", f"{rel}: {t.partition_by} values {sorted(distinct[t.partition_by])[:3]} in folder {expected}")
    if runs:
        if len(runs) != len(set(runs)):
            f.add(t.name, "activity rows not contiguous", f"{rel}: {len(runs) - len(set(runs))} activities split")
        elif runs != sorted(runs):
            f.add(t.name, "activity order", f"{rel}: activities not in activity_id order")
    if grid_off:
        f.add(t.name, "time_s grid", f"{rel}: {grid_off:,} samples off the {time_col.grid} s grid, e.g. activity {time_example}", grid_off)
    if backwards:
        f.add(t.name, "time_s monotonic", f"{rel}: {backwards:,} non-increasing steps, e.g. activity {time_example}", backwards)

    return FileResult(rel, md.num_rows, tuple(types.items()), f, keys, key_rows)


def _low_priority() -> None:
    """Validation is heavy on big datasets: run workers at low CPU priority."""
    try:
        if os.getpriority(os.PRIO_PROCESS, 0) < 10:
            os.nice(10)
    except OSError:
        pass


# --- per table / dataset -------------------------------------------------------------
def table_files(root: Path, t: C.TableSpec, f: Findings) -> list[Path]:
    folder = root / "data" / t.name
    if not folder.is_dir():
        f.add(t.name, "layout", f"missing folder data/{t.name}/")
        return []
    files = sorted(root.glob(t.glob))
    stray = sorted(set(p for p in folder.rglob("*") if p.is_file()) - set(files))
    for p in stray:
        f.add(t.name, "layout", f"unexpected file {p.relative_to(root)}")
    if t.partition_by:
        for p in files:
            if p.name != "part.parquet":
                f.add(t.name, "layout", f"{p.relative_to(root)}: partition files are named part.parquet")
    if not files:
        f.add(t.name, "layout", f"no files matching {t.glob}")
    return files


def check_card(root: Path, ds: C.DatasetSpec, f: Findings) -> None:
    import yaml

    card = root / "README.md"
    if not card.exists():
        f.add("card", "card", "no README.md")
        return
    text = card.read_text()
    m = re.match(r"^---\n(.*?)\n---\n", text, re.S)
    if not m:
        f.add("card", "card", "README.md has no YAML header")
        return
    header = yaml.safe_load(m.group(1)) or {}
    if header.get("license") != ds.license:
        f.add("card", "card", f"license {header.get('license')!r}, dataset.toml says {ds.license!r}")
    configs = {c.get("config_name"): c for c in header.get("configs", [])}
    for name, t in ds.tables.items():
        c = configs.pop(name, None)
        if c is None:
            f.add("card", "card configs", f"no subset for table {name}")
            continue
        files = c.get("data_files")
        paths = [files] if isinstance(files, str) else [x["path"] if isinstance(x, dict) else x for x in files or []]
        paths = [p for x in paths for p in ([x] if isinstance(x, str) else x)]
        if paths != [t.glob]:
            f.add("card", "card configs", f"{name}: data_files {paths}, should be [{t.glob!r}]")
        if bool(c.get("default", False)) != t.default:
            f.add("card", "card configs", f"{name}: default is {bool(c.get('default'))}, should be {t.default}")
    for name in configs:
        f.add("card", "card configs", f"subset {name!r} has no table in dataset.toml")


def validate(spec_path: Path, root: Path, jobs: int, max_files: int | None) -> int:
    t0 = time.monotonic()
    ds = C.load_dataset_spec(spec_path)
    findings = Findings()

    declared = set(ds.tables)
    present = {p.name for p in (root / "data").iterdir() if p.is_dir()} if (root / "data").is_dir() else set()
    for extra in sorted(present - declared):
        findings.add(extra, "layout", f"data/{extra}/ is not a table in dataset.toml")
    stray_top = [p.name for p in (root / "data").iterdir() if p.is_file()] if (root / "data").is_dir() else []
    for name in stray_top:
        findings.add("data", "layout", f"unexpected file data/{name} (tables are folders)")

    relation_cols: dict[str, set[tuple[str, ...]]] = defaultdict(set)
    for r in ds.relations:
        relation_cols[r.table].add(r.on)
        relation_cols[r.references].add(r.on)

    keys: dict[str, dict[tuple[str, ...], set[tuple]]] = {}
    summary = []
    with ProcessPoolExecutor(jobs) as ex:
        for name, t in ds.tables.items():
            files = table_files(root, t, findings)
            if max_files:
                files = files[:max_files]
            cols = sorted(relation_cols[name])
            results = list(ex.map(check_file, files, [root] * len(files), [ds] * len(files),
                                  [t] * len(files), [cols] * len(files), chunksize=8))
            schemas = defaultdict(list)
            table_keys: dict[tuple[str, ...], set[tuple]] = defaultdict(set)
            seen_key_rows: set[tuple] = set()
            dup = 0
            for r in results:
                findings.merge(r.findings)
                schemas[r.schema].append(r.path)
                for k, v in r.keys.items():
                    table_keys[k] |= v
                if r.key_rows is not None:
                    for row in r.key_rows:
                        if row in seen_key_rows:
                            dup += 1
                            if dup <= MAX_EXAMPLES:
                                findings.add(name, "duplicate keys", f"{dict(zip(t.key, row))}", 0)
                        seen_key_rows.add(row)
            if dup:
                findings.counts[(name, "duplicate keys")] += dup
            if len(schemas) > 1:
                variants = sorted(schemas.values(), key=len)
                findings.add(name, "schema differs between files", f"{len(schemas)} variants, e.g. {variants[0][0]}", len(schemas) - 1)
            keys[name] = table_keys
            summary.append((name, len(files), sum(r.rows for r in results)))
            print(f"  {name}: {len(files):,} files, {sum(r.rows for r in results):,} rows", file=sys.stderr)

    for r in ds.relations:
        missing = keys.get(r.table, {}).get(r.on, set()) - keys.get(r.references, {}).get(r.on, set())
        if missing and not max_files:
            ex_ = sorted(missing)[:MAX_EXAMPLES]
            findings.add(r.table, f"relation -> {r.references}",
                         f"{len(missing):,} {r.on} values not in {r.references}, e.g. {ex_[0]}", len(missing))

    check_card(root, ds, findings)

    print(f"\n{ds.pretty_name} ({ds.repo_id}) at {root}, conventions {ds.conventions}")
    for name, n_files, n_rows in summary:
        print(f"  {name:24s} {n_files:>7,} files {n_rows:>16,} rows")
    if max_files:
        print(f"  (first {max_files} files per table only; relations not checked)")
    if not findings:
        print(f"\nPASS: all checks passed ({time.monotonic() - t0:.0f} s)")
        return 0
    print(f"\nFAIL: {sum(findings.counts.values()):,} problems in {len(findings.counts)} checks "
          f"({time.monotonic() - t0:.0f} s)\n")
    for (table, check), n in sorted(findings.counts.items()):
        print(f"  [{table}] {check}: {n:,}")
        for e in findings.examples[(table, check)]:
            print(f"      {e}")
    return 1


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("spec", type=Path, help="the dataset's dataset.toml")
    ap.add_argument("root", type=Path, help="the built dataset folder (README.md + data/)")
    ap.add_argument("--jobs", type=int, default=max(1, (os.cpu_count() or 2) // 2),
                    help="parallel processes (default: half the cores)")
    ap.add_argument("--max-files", type=int, help="check only the first N files of each table")
    a = ap.parse_args()
    try:
        sys.exit(validate(a.spec, a.root, a.jobs, a.max_files))
    except C.SpecError as e:
        sys.exit(f"invalid dataset.toml: {e}")


if __name__ == "__main__":
    main()
