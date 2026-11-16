# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Conventions shared by the GoldenCheetah datasets on Hugging Face.

The single source of truth for what makes a GoldenCheetah dataset consistent with the others:
shared column names (each with one type, unit and meaning), keys, table names, file layout and
Parquet writer settings. The point is that code written against one dataset works on another
without changes and without reading a new specification.

    conventions.py          this file: the rules, as data (standard library only)
    CONVENTIONS.md          the same rules for people; its column table is generated from here
    datasets/<name>/dataset.toml   what one dataset contains, in terms of these rules
    tools/validate.py       checks a built dataset against its dataset.toml and these rules

Importing this module needs only the standard library. `write_table` and the type helpers import
pyarrow lazily, so scripts that only read the rules don't need it.

    uv run conventions.py --check-docs     fail if CONVENTIONS.md is out of date
    uv run conventions.py --write-docs     regenerate the generated part of CONVENTIONS.md
"""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Version of these rules. Every dataset.toml names the version it follows; the validator applies
# that version's rules. 0.x while nothing is published; 1.0 with the first public release.
VERSION = "0.1"

ROOT = Path(__file__).resolve().parent


# --- types ---------------------------------------------------------------------------
# Logical types, written the way pyarrow prints them. Physical variants that readers can't tell
# apart (string vs large_string, list vs large_list) count as the same type.
TYPES = {
    "string", "bool", "uint8", "int16", "int32", "int64", "float32", "float64",
    "timestamp[us, tz=UTC]", "list<float64>",
}


def logical_type(arrow_type: Any) -> str:
    """Normalise a pyarrow DataType to one of TYPES (or its own string if it isn't one)."""
    import pyarrow as pa

    t = arrow_type
    if pa.types.is_large_string(t) or pa.types.is_string(t) or pa.types.is_string_view(t):
        return "string"
    if pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_list_view(t):
        return f"list<{logical_type(t.value_type)}>"
    if pa.types.is_float32(t):
        return "float32"
    if pa.types.is_float64(t):
        return "float64"
    if pa.types.is_timestamp(t):
        return f"timestamp[{t.unit}, tz={t.tz}]" if t.tz else f"timestamp[{t.unit}]"
    return str(t)


def arrow_type(logical: str) -> Any:
    import pyarrow as pa

    simple = {
        "string": pa.string(), "bool": pa.bool_(), "uint8": pa.uint8(), "int16": pa.int16(),
        "int32": pa.int32(), "int64": pa.int64(), "float32": pa.float32(), "float64": pa.float64(),
        "timestamp[us, tz=UTC]": pa.timestamp("us", tz="UTC"), "list<float64>": pa.list_(pa.float64()),
    }
    return simple[logical]


# --- shared columns ------------------------------------------------------------------
@dataclass(frozen=True)
class Column:
    name: str
    type: str
    unit: str | None
    description: str
    pattern: str | None = None  # regex every non-null value must match (string columns)


ATHLETE_ID_RE = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
# Local start time from the activity's CSV name (rarely with a `.fit` suffix, which GoldenCheetah
# also wrote), or, for activities that have no CSV, the UTC start time followed by `Z`.
ACTIVITY_ID_RE = r"^\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2}(\.fit|Z)?$"

# A name in this registry means the same thing, with the same type and unit, in every dataset.
# A dataset may leave a shared column out, but if it has a column with this name, it is this one.
SHARED_COLUMNS: dict[str, Column] = {c.name: c for c in [
    # keys
    Column("athlete_id", "string", None,
           "Anonymous athlete id (UUID) from the GoldenCheetah OpenData release; the name of the "
           "athlete's zip on OSF. Identical across datasets.", ATHLETE_ID_RE),
    Column("activity_id", "string", None,
           "Activity id, unique within an athlete: the local start time `YYYY_MM_DD_HH_MM_SS` from "
           "the original CSV name (a few end in `.fit`, as GoldenCheetah wrote them). Activities "
           "without a CSV use their UTC start time followed by `Z`. Identical across datasets.",
           ACTIVITY_ID_RE),
    # time series
    Column("time_s", "float32", "s",
           "Seconds since the start of the activity, as stored in this dataset. Fractional for "
           "devices that record at other intervals (e.g. 0.5 s, 1.26 s). A dataset that trims or "
           "resamples activities says so in its card; its `time_s` then need not match the raw data."),
    Column("distance_m", "float32", "m", "Cumulative distance since the start of the activity."),
    Column("power_w", "float32", "W", "Power."),
    Column("hr_bpm", "float32", "bpm", "Heart rate."),
    Column("cadence_rpm", "float32", "rpm", "Cadence (pedal or step revolutions per minute)."),
    Column("altitude_m", "float32", "m", "Altitude."),
    # activities
    Column("start_time_utc", "timestamp[us, tz=UTC]", None, "Start time of the activity, UTC."),
    Column("utc_offset_minutes", "int16", "min",
           "Local time minus UTC at the start of the activity. Null when unknown."),
    Column("sport", "string", None,
           "Sport as entered in GoldenCheetah: free text (`Bike`, `bike`, `Rennrad`, `Run`, ...)."),
    # athletes
    Column("gender", "string", None, "Gender as entered in GoldenCheetah (`M`, `F`), or null."),
    Column("yob", "int64", None,
           "Year of birth as entered in GoldenCheetah. Not validated; implausible values occur."),
]}

KEY_COLUMNS = ("athlete_id", "activity_id")

# Columns that are not in the registry: lower snake_case, with a unit suffix for physical
# quantities (`_s`, `_min`, `_m`, `_w`, `_wkg`, `_bpm`, `_rpm`, `_kg`, `_bytes`). Columns whose
# names come verbatim from another system (GoldenCheetah metric names) are declared as such in
# dataset.toml and exempt.
NAME_RE = r"^[a-z][a-z0-9_]*$"

# --- tables and layout ---------------------------------------------------------------
# Every table is a folder under data/ and a subset (YAML `configs` entry) in the dataset card.
# Tables with these names mean the same thing in every dataset:
SHARED_TABLES = {
    "athletes": "One row per athlete; key `athlete_id`.",
    "activities": "One row per activity; key `(athlete_id, activity_id)`.",
    "timeseries": "One row per sample. One file per athlete: "
                  "`data/timeseries/athlete_id=<uuid>/part.parquet`. Rows of an activity are "
                  "contiguous and in recorded order, activities in `activity_id` order.",
}


def table_glob(table: str, partition_by: str | None) -> str:
    """Files of a table, relative to the repo root (also the card's `data_files`)."""
    return f"data/{table}/{partition_by}=*/*.parquet" if partition_by else f"data/{table}/*.parquet"


def partition_file(table: str, partition_by: str, value: str) -> str:
    return f"data/{table}/{partition_by}={value}/part.parquet"


# --- Parquet writer settings ---------------------------------------------------------
# Two profiles. Both follow Hugging Face's "optimized Parquet": content-defined chunking (so a new
# release only uploads changed chunks) and a page index (fast filtered and streamed reads).
# research/reports/05, 06, 09, 10 have the measurements behind these choices.
@dataclass(frozen=True)
class WriterProfile:
    compression_level: int
    row_group_size: int | None
    byte_stream_split: tuple[str, ...] = ()   # column types written with BYTE_STREAM_SPLIT
    dictionary: str = "all"                   # "all", or "strings": dictionary only for strings


WRITER_PROFILES = {
    # Long tables of measurements: byte-stream-split floats, dictionary-encoded ids, zstd 15,
    # ~2 MB row groups so one athlete or activity can be read without much else.
    "timeseries": WriterProfile(compression_level=15, row_group_size=256_000,
                                byte_stream_split=("float32", "float64"), dictionary="strings"),
    # Everything else: zstd, 100k-row row groups. Some tables are wide (hundreds of columns), and
    # the writer holds a row group in memory; Hugging Face's dataset viewer also prefers ~100k.
    "table": WriterProfile(compression_level=9, row_group_size=100_000),
}

# Encodings a column may declare in dataset.toml to override its profile's default. Encoding only
# affects file size, never what readers get; some columns compress better with a dictionary
# (few distinct values) than with byte-stream-split.
ENCODINGS = {"byte_stream_split", "dictionary"}


def default_encoding(profile: str, logical: str) -> str | None:
    p = WRITER_PROFILES[profile]
    if logical in p.byte_stream_split:
        return "byte_stream_split"
    if logical == "bool":
        return None  # Parquet never dictionary-encodes booleans
    if p.dictionary == "all" or logical == "string":
        return "dictionary"
    return None


def plain_schema(schema: Any) -> Any:
    """The same schema with large_string/large_list replaced by string/list, so that every file
    uses the same physical types whichever library produced the data (polars uses the large ones)."""
    import pyarrow as pa

    def plain(t: Any) -> Any:
        if pa.types.is_large_string(t) or pa.types.is_string_view(t):
            return pa.string()
        if pa.types.is_large_list(t) or pa.types.is_list(t):
            return pa.list_(plain(t.value_type))
        return t

    return pa.schema([f.with_type(plain(f.type)) for f in schema], metadata=schema.metadata)


# Stored in every file's key-value metadata, so a single file says where it belongs.
FILE_METADATA_KEY = b"goldencheetah"


class TableWriter:
    """Write a Parquet file with the conventions' settings, in pieces, in bounded memory.

    The only supported way to write. Tables passed to `write` are buffered and written in row
    groups of exactly the profile's size (the last one may be smaller), so the file is identical
    to writing everything at once. The file appears under its final name only when `close`
    succeeds; on an error nothing is left behind.

        with TableWriter(path, schema, "timeseries", dataset="opendata", table_name="timeseries") as w:
            for t in pieces:
                w.write(t)

    `encodings` overrides the profile's default per column (see ENCODINGS); pass the encodings a
    table declares in dataset.toml (`TableSpec.encodings`).
    """

    def __init__(self, path: Path, schema: Any, profile: str, *, dataset: str, table_name: str,
                 encodings: dict[str, str] | None = None):
        import json

        import pyarrow.parquet as pq

        p = WRITER_PROFILES[profile]
        self.row_group_size = p.row_group_size
        schema = plain_schema(schema)
        types = {f.name: logical_type(f.type) for f in schema}
        meta = dict(schema.metadata or {})
        meta[FILE_METADATA_KEY] = json.dumps(
            {"dataset": dataset, "table": table_name, "conventions": VERSION}, sort_keys=True
        ).encode()
        self.schema = schema.with_metadata(meta)
        enc = {c: (encodings or {}).get(c) or default_encoding(profile, t) for c, t in types.items()}
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.tmp = self.path.with_name(self.path.name + ".tmp")
        self.writer = pq.ParquetWriter(
            self.tmp, self.schema,
            compression="zstd", compression_level=p.compression_level,
            use_dictionary=[c for c, e in enc.items() if e == "dictionary"],
            column_encoding={c: "BYTE_STREAM_SPLIT" for c, e in enc.items() if e == "byte_stream_split"},
            use_content_defined_chunking=True, write_page_index=True,
        )
        self.buffer: list[Any] = []
        self.buffered = 0
        self.rows = 0

    def write(self, table: Any) -> None:
        if table.num_rows == 0:
            return
        self.buffer.append(table.cast(self.schema))  # same schema everywhere (also drops metadata)
        self.buffered += table.num_rows
        while self.buffered >= self.row_group_size:
            self._flush(self.row_group_size)

    def _flush(self, n: int) -> None:
        import pyarrow as pa

        t = pa.concat_tables(self.buffer)
        self.writer.write_table(t.slice(0, n), row_group_size=n)
        rest = t.slice(n)
        self.buffer = [rest] if rest.num_rows else []
        self.buffered = rest.num_rows
        self.rows += n

    def close(self) -> None:
        if self.buffered:
            self._flush(self.buffered)
        self.writer.close()
        self.tmp.replace(self.path)  # never leave a half-written file under its final name

    def abort(self) -> None:
        self.writer.close()
        self.tmp.unlink(missing_ok=True)

    def __enter__(self) -> "TableWriter":
        return self

    def __exit__(self, exc_type: Any, *exc: Any) -> None:
        if exc_type is None:
            self.close()
        else:
            self.abort()


def write_table(table: Any, path: Path, profile: str, *, dataset: str, table_name: str,
                encodings: dict[str, str] | None = None) -> None:
    """Write a whole pyarrow Table at once (see TableWriter, which this uses)."""
    with TableWriter(path, table.schema, profile, dataset=dataset, table_name=table_name,
                     encodings=encodings) as w:
        w.write(table)


# --- dataset.toml --------------------------------------------------------------------
@dataclass
class ColumnSpec:
    name: str
    type: str
    unit: str | None
    description: str
    required: bool = False      # no nulls
    shared: bool = False        # from SHARED_COLUMNS
    pattern: str | None = None
    grid: float | None = None   # time_s only: values k*grid, step exactly `grid` within an activity
    monotonic: bool = False     # time_s only: strictly increasing within an activity (implied by grid)
    encoding: str | None = None # overrides the profile's default encoding (ENCODINGS)


@dataclass
class ExtraColumns:
    """A declared group of columns not listed one by one (e.g. GoldenCheetah metrics)."""
    types: tuple[str, ...]
    description: str
    verbatim_names: bool        # names kept from the source system; exempt from NAME_RE


@dataclass
class TableSpec:
    name: str
    description: str
    profile: str
    key: tuple[str, ...]
    columns: list[ColumnSpec]
    extra: ExtraColumns | None = None
    partition_by: str | None = None
    default: bool = False

    @property
    def glob(self) -> str:
        return table_glob(self.name, self.partition_by)

    @property
    def encodings(self) -> dict[str, str]:
        """Declared per-column encoding overrides, for write_table."""
        return {c.name: c.encoding for c in self.columns if c.encoding}


@dataclass
class Relation:
    """Every key in `table` exists in `references` (e.g. timeseries -> activities)."""
    table: str
    references: str
    on: tuple[str, ...]


@dataclass
class DatasetSpec:
    name: str
    repo_id: str
    pretty_name: str
    license: str
    conventions: str
    tables: dict[str, TableSpec]
    relations: list[Relation] = field(default_factory=list)


class SpecError(ValueError):
    pass


def load_dataset_spec(path: Path) -> DatasetSpec:
    """Read and check a dataset.toml. Raises SpecError listing every problem found."""
    raw = tomllib.loads(Path(path).read_text())
    errors: list[str] = []
    ds = raw.get("dataset", {})
    for k in ("name", "repo_id", "pretty_name", "license", "conventions"):
        if k not in ds:
            errors.append(f"[dataset] needs `{k}`")
    if ds.get("conventions") and ds["conventions"] != VERSION:
        errors.append(f"[dataset] follows conventions {ds['conventions']}, this is {VERSION}")

    tables: dict[str, TableSpec] = {}
    for tname, t in raw.get("tables", {}).items():
        where = f"[tables.{tname}]"
        if not re.match(NAME_RE, tname):
            errors.append(f"{where}: table names are lower snake_case")
        profile = t.get("profile", "table")
        if profile not in WRITER_PROFILES:
            errors.append(f"{where}: unknown profile {profile!r}")
        cols: list[ColumnSpec] = []
        for c in t.get("columns", []):
            name = c.get("name")
            if not name:
                errors.append(f"{where}: column without a name")
                continue
            if name in SHARED_COLUMNS:
                s = SHARED_COLUMNS[name]
                for k in ("type", "unit", "description", "pattern"):
                    if k in c:
                        errors.append(f"{where}.{name}: shared column; `{k}` comes from conventions.py")
                grid = c.get("grid")
                if grid is not None and (name != "time_s" or isinstance(grid, bool)
                                         or not isinstance(grid, (int, float)) or grid <= 0):
                    errors.append(f"{where}.{name}: `grid` is a positive number of seconds, on time_s only")
                monotonic = c.get("monotonic", False)
                if "monotonic" in c and (name != "time_s" or not isinstance(monotonic, bool)):
                    errors.append(f"{where}.{name}: `monotonic` is true or false, on time_s only")
                cols.append(ColumnSpec(name, s.type, s.unit, s.description, c.get("required", False),
                                       True, s.pattern, grid, bool(monotonic) or grid is not None,
                                       c.get("encoding")))
            else:
                if not re.match(NAME_RE, name):
                    errors.append(f"{where}.{name}: column names are lower snake_case")
                if c.get("type") not in TYPES:
                    errors.append(f"{where}.{name}: type {c.get('type')!r} not one of {sorted(TYPES)}")
                if not c.get("description"):
                    errors.append(f"{where}.{name}: needs a description")
                for k in ("grid", "monotonic"):
                    if k in c:
                        errors.append(f"{where}.{name}: `{k}` applies to time_s only")
                cols.append(ColumnSpec(name, c.get("type", "?"), c.get("unit"), c.get("description", ""),
                                       c.get("required", False), False, c.get("pattern"),
                                       encoding=c.get("encoding")))
            if c.get("encoding") is not None and c["encoding"] not in ENCODINGS:
                errors.append(f"{where}.{name}: encoding {c['encoding']!r} not one of {sorted(ENCODINGS)}")
        names = [c.name for c in cols]
        if len(set(names)) != len(names):
            errors.append(f"{where}: duplicate column names")
        key = tuple(t.get("key", ()))
        for k in key:
            if k not in names:
                errors.append(f"{where}: key column {k!r} is not declared")
        for c in cols:
            if c.name in key:
                c.required = True  # keys never have nulls
        extra = None
        if "extra_columns" in t:
            e = t["extra_columns"]
            bad = [x for x in e.get("types", []) if x not in TYPES]
            if bad or not e.get("types"):
                errors.append(f"{where}.extra_columns: types must be a non-empty subset of {sorted(TYPES)}")
            extra = ExtraColumns(tuple(e.get("types", ())), e.get("description", ""), e.get("verbatim_names", False))
        part = t.get("partition_by")
        if part and part not in names:
            errors.append(f"{where}: partition_by column {part!r} is not declared")
        if tname in SHARED_TABLES and tname == "timeseries" and part != "athlete_id":
            errors.append(f"{where}: the timeseries table is partitioned by athlete_id")
        if not t.get("description"):
            errors.append(f"{where}: needs a description")
        tables[tname] = TableSpec(tname, t.get("description", ""), profile, key, cols, extra, part,
                                  t.get("default", False))
    if sum(t.default for t in tables.values()) != 1:
        errors.append("exactly one table needs `default = true` (the subset the dataset viewer opens)")

    relations = []
    for r in raw.get("relations", []):
        rel = Relation(r.get("table", ""), r.get("references", ""), tuple(r.get("on", ())))
        for side in (rel.table, rel.references):
            if side not in tables:
                errors.append(f"relation {rel.table} -> {rel.references}: unknown table {side!r}")
        relations.append(rel)

    if errors:
        raise SpecError(f"{path}:\n  " + "\n  ".join(errors))
    return DatasetSpec(ds["name"], ds["repo_id"], ds["pretty_name"], ds["license"], ds["conventions"],
                       tables, relations)


# --- dataset card --------------------------------------------------------------------
def card_configs(spec: DatasetSpec) -> str:
    """The card's YAML `configs`: one subset per table, default table first."""
    tables = sorted(spec.tables.values(), key=lambda t: not t.default)
    lines = ["configs:"]
    for t in tables:
        lines.append(f"- config_name: {t.name}")
        if t.default:
            lines.append("  default: true")
        lines.append(f'  data_files: "{t.glob}"')
    return "\n".join(lines)


# --- docs ----------------------------------------------------------------------------
DOCS = ROOT / "CONVENTIONS.md"
BEGIN, END = "<!-- BEGIN GENERATED: conventions.py -->", "<!-- END GENERATED -->"


def docs_section() -> str:
    def cell(s: str | None) -> str:
        return (s or "").replace("|", "\\|")

    lines = [BEGIN, "", f"Conventions version **{VERSION}**.", "",
             "| column | type | unit | meaning |", "|---|---|---|---|"]
    for c in SHARED_COLUMNS.values():
        lines.append(f"| `{c.name}` | `{c.type}` | {cell(c.unit)} | {cell(c.description)} |")
    lines += ["", "| table | meaning |", "|---|---|"]
    lines += [f"| `{t}` | {cell(d)} |" for t, d in SHARED_TABLES.items()]
    lines += ["", "| writer profile | compression | row group | byte stream split | dictionary |",
              "|---|---|---|---|---|"]
    for n, p in WRITER_PROFILES.items():
        lines.append(f"| `{n}` | zstd {p.compression_level} | {p.row_group_size or 'pyarrow default'} | "
                     f"{', '.join(p.byte_stream_split) or '-'} | {p.dictionary} |")
    lines += ["", END]
    return "\n".join(lines)


def _docs_with_section(text: str) -> str:
    if BEGIN not in text or END not in text:
        raise SystemExit(f"{DOCS} has no generated section markers")
    head, rest = text.split(BEGIN, 1)
    _, tail = rest.split(END, 1)
    return head + docs_section() + tail


if __name__ == "__main__":
    import sys

    text = DOCS.read_text()
    new = _docs_with_section(text)
    if "--write-docs" in sys.argv:
        DOCS.write_text(new)
        print(f"updated {DOCS}")
    elif "--check-docs" in sys.argv:
        if new != text:
            raise SystemExit(f"{DOCS} is out of date: run `uv run conventions.py --write-docs`")
        print(f"{DOCS} is up to date")
    else:
        print(__doc__)
