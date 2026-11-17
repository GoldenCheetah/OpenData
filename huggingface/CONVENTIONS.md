# Conventions for GoldenCheetah datasets on Hugging Face

The GoldenCheetah datasets on Hugging Face follow one set of conventions, so that **code written
for one dataset works on another without changes and without reading a new specification**. A
column called `power_w` is power in watts, Float32, in every dataset; `athlete_id` and
`activity_id` identify the same athlete and activity everywhere, so datasets join directly; every
table lives in the same place and is written the same way.

The rules are defined once, in [`conventions.py`](conventions.py). This document explains them;
the tables in the [reference](#reference) section are generated from that file. Each dataset
declares its contents in a `dataset.toml` (see [`datasets/opendata/dataset.toml`](datasets/opendata/dataset.toml)),
and `tools/validate.py` checks a built dataset against both.

## Columns

- **Shared columns** ([reference](#reference)) have one name, type, unit and meaning across all
  datasets. A dataset may leave one out, but never uses its name for something else, and never
  stores it with a different type. A dataset's `dataset.toml` lists shared columns by name only.
- **Same name, same value.** Where a dataset's row is the same row as in the dataset it derives
  from (same key), its shared columns hold the same values: a derived dataset's `sport` for an
  activity is OpenData's `sport` for that activity. A dataset that changes a value (a normalised
  sport, a corrected start time) puts it in a column with its own name. The validator checks
  this (see [Relations](#relations)).
- **Other columns** are declared in the dataset's `dataset.toml` with a type, unit and description.
  Names are lower `snake_case`; names of physical quantities end in their unit (`_s`, `_min`, `_m`,
  `_w`, `_wkg`, `_bpm`, `_rpm`, `_kg`, `_bytes`).
- **Verbatim names.** Columns whose names come from another system, such as GoldenCheetah's metric
  names (`10m_critical_power`, `xPace`), keep those names so they can be looked up. They are
  declared as a group (`extra_columns` with `verbatim_names = true`) rather than one by one.
- **Types** are the logical Arrow types listed in `conventions.TYPES`. `string` and `large_string`
  (and `list`/`large_list`) count as the same type: readers can't tell them apart.
- **Nulls** mean "not available". Key columns and columns declared `required` never contain nulls.

## Keys

- `athlete_id`: the UUID of the athlete in the original OpenData release (the name of their zip on
  OSF). Every dataset derived from it uses the same id.
- `activity_id`: unique within an athlete. It is the local start time from the original CSV name,
  `YYYY_MM_DD_HH_MM_SS`. A few CSVs carry the original file's extension (`.fit`, `.pwx`,
  `.csv.bak`, `.sync-conflict-...`), which is kept: the release has activities whose only
  difference is that suffix, such as a backup version next to the regular one. Activities without
  a CSV (manual entries, days off, ...) use their UTC start time followed by `Z`:
  `2017_01_31_17_36_44Z`. The `Z` keeps these ids apart from CSV-based ids and says that the time
  is UTC, not local.
- Where two activities would still get the same id, the second gets `~2` (the third `~3`, ...).
  This happens where an athlete's library held two versions of one activity, for example a file
  imported twice: the release then has two rides with the same start time, and sometimes two
  different CSVs under one name. Both versions are kept.
- Tables with a key declare it; the validator checks that keys are unique and complete.

## Tables and layout

```
<org>/<dataset>
├── README.md                                   dataset card
└── data/
    ├── <table>/*.parquet                       one folder per table
    └── timeseries/athlete_id=<uuid>/part.parquet   time series: one file per athlete
```

- Every table is a folder under `data/` and a subset (`configs` entry) of the dataset card, so
  `load_dataset(repo, "<table>")` and `hf://datasets/<repo>/data/<table>/...` work the same way in
  every dataset. A new table is a new folder and does not affect existing ones.
- `athletes`, `activities` and `timeseries` have a fixed meaning ([reference](#reference)).
  Datasets name other tables freely.
- **Time series** are Hive-partitioned by athlete, one file per athlete. `athlete_id` is also a
  column inside each file, so a single file is self-contained. Rows of one activity are
  contiguous and in recorded order, and activities appear in `activity_id` order.

## Relations

- Relations between tables (every `activities` row has an `athletes` row, every time series
  belongs to an activity, ...) are declared in `dataset.toml` and checked.
- A dataset derived from another one declares relations to its tables as
  `references = "<dataset>.<table>"`, for example `opendata.activities`: every activity in the
  derived dataset exists in OpenData. When the relation is on the referenced table's whole key,
  rows match one to one and the validator also checks that the shared columns both tables have
  hold the same values. Validate with
  `--against <the other dataset.toml> <its folder or hf://datasets/org/name>`; without it, these
  relations fail rather than go unchecked.

## Time series values

- Measurements are **Float32**. GoldenCheetah wrote every number with at most 6 significant
  digits, and Float32 represents any 6-significant-digit decimal exactly: formatting a value with
  `%.6g` gives back the original text. So Float32 is lossless for this data, at half the size of
  Float64 (`research/reports/09_precision_census.md`). Casting to Float64 shows binary noise in
  the 8th digit (`72.5636` becomes `72.56359863`); round to 6 significant digits to get the
  original decimal back.
- **Distance is in metres** (`distance_m`), cumulative from the start of the activity. Metres have
  the same digits as kilometres and compress about 25% better.
- **`time_s` is Float32 in every dataset**: some devices record at fractional intervals (0.5 s,
  1.26 s, ...), and one name has one type. Float32 holds every whole second up to 194 days exactly.
- **Recorded order.** Rows keep the order in which they were recorded; they are not sorted by
  `time_s`. In raw recordings `time_s` repeats now and then, and in rare broken recordings it
  jumps backwards while distance and altitude show that the rows themselves are in order (2 of
  77,804 files in the OpenData sample). Sorting would move those samples to the wrong place.
- **Guarantees are declared.** A dataset whose `time_s` strictly increases within every activity
  declares `monotonic = true` on `time_s`. A dataset that trims or resamples activities to a
  regular grid says so in its card and declares the step (`grid = 1.0`): every value is a multiple
  of the step and consecutive samples of an activity are exactly one step apart (which implies
  `monotonic`). Its `time_s` then need not match the raw recording. The validator checks declared
  guarantees on every file, so code can rely on them.

## Parquet files

- Written with `conventions.write_table`, which applies the settings below, records the dataset,
  table and conventions version in each file's metadata (key `goldencheetah`), and writes
  atomically.
- All files use zstd, content-defined chunking (a new release uploads only changed data) and a page
  index (fast filtered and streamed reads), as Hugging Face recommends.
- Time series use the `timeseries` profile: byte-stream-split encoding for floats, dictionary
  encoding for ids, zstd level 15, 256k-row (~2 MB) row groups. The measurements behind these
  choices are in `research/reports/05`, `06`, `09` and `10`.
- Encoding only affects size, never what readers get. A column that compresses better another way
  (say, a derived float with few distinct values, which suits a dictionary) declares
  `encoding = "dictionary"` in `dataset.toml`; `write_table` applies it and the validator checks it.
  Otherwise only the time series default (byte-stream-split floats) is enforced.
- Checked by the validator: zstd, page index (offset index always; column index wherever there
  are min/max statistics), row-group size, the time series float encoding, declared encodings and
  the file metadata. Compression level and content-defined chunking can't be read back from a
  file; `write_table` sets them.

## Versioning

- The conventions have a version (`conventions.VERSION`); every `dataset.toml` names the version it
  follows. Until the first public release the version is 0.x and may change; from 1.0 on, changes
  that break existing code (renaming or retyping a shared column, moving tables) need a new major
  version.
- Datasets are git repositories on Hugging Face; releases are tagged (`v1.0`), and users pin a
  release with `revision=`.

## Adding a dataset

[`datasets/_template/`](datasets/_template/) is a small working dataset derived from OpenData (its
activities with a time series, resampled to whole seconds); the tests build and validate it.

1. Copy `datasets/_template/` to `datasets/<name>/` (or into your own repository) and describe the
   dataset in its `dataset.toml`: tables, keys, columns, relations. Take `athlete_id` and
   `activity_id` from OpenData's tables rather than deriving them again from the raw files.
2. Build the tables with `conventions.write_table` (or `TableWriter`, to stream), and the card
   from `README.md.tmpl` with `conventions.render_card`, as `build.py` does.
3. Run `uv run tools/validate.py <dataset.toml> <built folder> --against
   datasets/opendata/dataset.toml hf://datasets/GoldenCheetah/opendata` until it passes.
4. After uploading, `uv run tools/smoke_test.py <dataset.toml> <org/name> --local <built folder>`
   reads every table back through polars, DuckDB and `datasets`.

## Reference

<!-- BEGIN GENERATED: conventions.py -->

Conventions version **0.3**.

| column | type | unit | meaning |
|---|---|---|---|
| `athlete_id` | `string` |  | Anonymous athlete id (UUID) from the GoldenCheetah OpenData release; the name of the athlete's zip on OSF. Identical across datasets. |
| `activity_id` | `string` |  | Activity id, unique within an athlete: the local start time `YYYY_MM_DD_HH_MM_SS` from the original CSV name, in a few cases followed by the original file's extension (`.fit`, `.pwx`, `.csv.bak`, ...). Activities without a CSV use their UTC start time followed by `Z`. Where two activities would get the same id (two versions of one activity in the release), the second gets `~2`. Identical across datasets. |
| `time_s` | `float32` | s | Seconds since the start of the activity, as stored in this dataset. Fractional for devices that record at other intervals (e.g. 0.5 s, 1.26 s). A dataset that trims or resamples activities says so in its card; its `time_s` then need not match the raw data. |
| `distance_m` | `float32` | m | Cumulative distance since the start of the activity. |
| `power_w` | `float32` | W | Power. |
| `hr_bpm` | `float32` | bpm | Heart rate. |
| `cadence_rpm` | `float32` | rpm | Cadence (pedal or step revolutions per minute). |
| `altitude_m` | `float32` | m | Altitude. |
| `start_time_utc` | `timestamp[us, tz=UTC]` |  | Start time of the activity, UTC. |
| `utc_offset_minutes` | `int16` | min | Local time minus UTC at the start of the activity. Null when unknown. |
| `sport` | `string` |  | Sport as entered in GoldenCheetah: free text (`Bike`, `bike`, `Rennrad`, `Run`, ...). |
| `gender` | `string` |  | Gender as entered in GoldenCheetah (`M`, `F`), or null. |
| `yob` | `int64` |  | Year of birth as entered in GoldenCheetah. Not validated; implausible values occur. |

| table | meaning |
|---|---|
| `athletes` | One row per athlete; key `athlete_id`. |
| `activities` | One row per activity; key `(athlete_id, activity_id)`. |
| `timeseries` | One row per sample. One file per athlete: `data/timeseries/athlete_id=<uuid>/part.parquet`. Rows of an activity are contiguous and in recorded order, activities in `activity_id` order. |

| writer profile | compression | row group | byte stream split | dictionary |
|---|---|---|---|---|
| `timeseries` | zstd 15 | 256000 | float32, float64 | strings |
| `table` | zstd 9 | 100000 | - | all |

<!-- END GENERATED -->
