# 06 · Layout benchmark (raw time series)

Sample: **167** athletes, **402,137,063** rows, 3.3 GB Parquet (Float64 + BSS + zstd9 + CDC + page index).

Per-athlete Parquet size: median 8.8 MB, max 228.4 MB, min 6.9 KB.

`read_amp` = bytes in row groups that may contain the athlete / the athlete's own bytes (≈ HTTP download amplification without page-index pruning). Shard counts are projected to the full dataset by size.

| layout | files | projected_full_files | MB | row_groups | median_rg_MB | read_amp_median | read_amp_max | one_athlete_ms | full_scan_s | write_s |
|---|---|---|---|---|---|---|---|---|---|---|
| per_athlete | 167 | 6,613 | 3,281 | 504 | 7.140 | 1.000 | 1.000 | 186.496 | 2.021 | 296.690 |
| shard_256MB_rg1M | 16 | 521 | 3,281 | 410 | 7.992 | 1.713 | 492.886 | 168.191 | 2.622 | 313.949 |
| shard_1GB_rg1M | 4 | 130 | 3,281 | 405 | 8.055 | 1.828 | 491.642 | 168.260 | 1.798 | 328.155 |
| shard_1GB_rg256k | 4 | 130 | 3,286 | 1,572 | 2.071 | 1.186 | 122.362 | 135.620 | 1.575 | 328.156 |
