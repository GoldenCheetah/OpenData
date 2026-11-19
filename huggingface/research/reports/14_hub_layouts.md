# 14 · Time series layouts on the Hub

Every layout holds the same 13,295,542,039 rows of the converter 0.4.0 build, written with the conventions' time series settings; only files and row groups differ. Queries run anonymously unless marked logged in, each in a fresh process with a fresh rate-limit window. Times are the median of the repetitions; `api` and `resolvers` are the Hub requests a run used (anonymous budget per 5 min: 500 and 3,000; logged in: 1,000 and 5,000).

Targets: athlete `b2b4ec2a-5acd-43dc-8ae0-f90c28959833` (1,012,869 rows, the median athlete by size), its median activity (3,372 rows), and 20 activities of 20 random athletes.

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

## Results

| layout | files | GB | median file MB |
|---|---:|---:|---:|
| `per_athlete_hive` | 6,509 | 47.1 | 7.2 |
| `per_athlete_flat` | 6,509 | 47.1 | 7.2 |
| `files_400_rg1m` | 400 | 46.9 | 117.4 |
| `files_100_rg256k` | 100 | 47.1 | 471.0 |
| `files_100_rg1m` | 100 | 46.9 | 469.5 |
| `files_100_rg4m` | 100 | 46.9 | 469.2 |

### polars, anonymous

| layout | count | athlete | activity | scattered | scan |
|---|---:|---:|---:|---:|---:|
| `per_athlete_hive` | fails: rate limited | fails: rate limited | fails: rate limited | fails: rate limited | fails: rate limited |
| `per_athlete_flat` | fails: rate limited | fails: rate limited | fails: rate limited | fails: rate limited | fails: rate limited |
| `files_400_rg1m` | 30.7 s (2/410) | 29.6 s (2/363) | 20.5 s (2/415) | 21.7 s (2/331) | fails: rate limited |
| `files_100_rg256k` | 17.4 s (2/212) | 15.3 s (2/174) | 9.0 s (2/200) | 25.5 s (2/337) | fails: rate limited |
| `files_100_rg1m` | 10.1 s (2/64) | 10.0 s (2/77) | 7.9 s (2/110) | 26.7 s (2/100) | fails: rate limited |
| `files_100_rg4m` | 9.9 s (2/102) | 10.9 s (2/101) | 6.2 s (2/98) | 37.5 s (2/151) | 275.2 s (?/?) |

### duckdb, anonymous

| layout | count | athlete | activity | scattered | scan |
|---|---:|---:|---:|---:|---:|
| `per_athlete_hive` | fails: rate limited | fails: rate limited | fails: rate limited | fails: rate limited | fails: rate limited |
| `per_athlete_flat` | fails: timeout | fails: timeout | fails: timeout | fails: timeout | fails: timeout |
| `files_400_rg1m` | 14.4 s (1/800) | 15.2 s (1/801) | 14.5 s (1/802) | 13.8 s (1/856) | fails: timeout |
| `files_100_rg256k` | 8.0 s (1/300) | 7.7 s (1/305) | 8.9 s (1/301) | 13.4 s (1/407) | fails: rate limited |
| `files_100_rg1m` | 8.4 s (1/200) | 6.1 s (1/201) | 6.5 s (1/201) | 8.1 s (1/259) | 1058.2 s (?/?) |
| `files_100_rg4m` | 5.5 s (1/200) | 5.3 s (1/201) | 5.2 s (1/201) | 6.1 s (1/229) | fails: rate limited |

### pandas, anonymous

| layout | count | athlete | activity |
|---|---:|---:|---:|
| `per_athlete_hive` | fails: timeout | 772.0 s (?/?) | 642.8 s (?/?) |
| `per_athlete_flat` | fails: timeout | 648.8 s (?/?) | 640.6 s (?/?) |
| `files_400_rg1m` | 124.5 s (4/401) | 32.1 s (3/402) | 34.3 s (3/402) |
| `files_100_rg256k` | 60.6 s (3/202) | 20.6 s (3/203) | 19.9 s (3/203) |
| `files_100_rg1m` | 62.9 s (3/202) | 22.0 s (3/203) | 18.5 s (3/203) |
| `files_100_rg4m` | 29.1 s (3/101) | 11.0 s (3/102) | 9.9 s (3/102) |

### datasets, anonymous

| layout | stream |
|---|---:|
| `per_athlete_hive` | 12.0 s (16/7) |
| `per_athlete_flat` | 13.3 s (23/7) |
| `files_400_rg1m` | 11.9 s (17/8) |
| `files_100_rg256k` | 11.5 s (17/10) |
| `files_100_rg1m` | 11.4 s (17/10) |
| `files_100_rg4m` | 11.2 s (17/9) |

### polars, logged in

| layout | count | athlete | activity | scattered | scan |
|---|---:|---:|---:|---:|---:|
| `per_athlete_hive` | 300.9 s (?/?) | fails: rate limited |  |  | fails: rate limited |
| `files_100_rg1m` | 6.0 s (1/115) | 4.4 s (1/117) |  |  | fails: rate limited |

### duckdb, logged in

| layout | count | athlete | activity | scattered | scan |
|---|---:|---:|---:|---:|---:|
| `per_athlete_hive` | fails: timeout | fails: timeout |  |  | fails: timeout |
| `files_100_rg1m` | 4.3 s (1/200) | 4.1 s (1/202) |  |  | fails: timeout |

Cells: median seconds (api calls / file resolves used).
