# 05 · Parquet encoding benchmark (raw time series)

Input: **60** athletes spread over the size range, **55,685,628** rows; original OSF zips **498.0 MB**. One file, 1M-row row groups.

`vs_zip` = Parquet size / zip size. `lossless` = every value bit-identical to the Float64 CSV parse after decoding (scaled ints decoded with numpy true division; see `decode()`); otherwise the count of inexact values.

| variant | MB | vs_zip | write_s | read_s | read_3cols_s | lossless | inexact_values | inexact_pct | max_abs_err |
|---|---|---|---|---|---|---|---|---|---|
| f64_snappy | 653.985 | 1.313 | 21.441 | 7.690 | 2.369 | 1 | 0 | 0.000 |  |
| f64_zstd3 | 481.376 | 0.967 | 12.518 | 7.626 | 2.386 | 1 | 0 | 0.000 |  |
| f64_dict_zstd9 | 461.360 | 0.926 | 22.003 | 7.454 | 2.283 | 1 | 0 | 0.000 |  |
| f64_bss_zstd3 | 475.634 | 0.955 | 8.813 | 7.741 | 2.352 | 1 | 0 | 0.000 |  |
| f64_bss_zstd9 | 460.578 | 0.925 | 20.836 | 7.343 | 2.238 | 1 | 0 | 0.000 |  |
| f64_bss_zstd19 | 432.161 | 0.868 | 211.612 | 7.571 | 2.301 | 1 | 0 | 0.000 |  |
| f32_bss_zstd9 | 243.355 | 0.489 | 30.333 | 4.779 | 1.534 | 0 | 99,343,132 | 29.733 | secs=0.00078, km=5.9e-05, power=5.9e-05, hr=7.6e-06, cad=7.6e-06, alt=0.00039 |
| int_e9_plain_zstd9 | 443.946 | 0.891 | 53.987 | 7.296 | 1.968 | 0 | 25,011 | 0.007 | km=5e-10, power=1e-10, cad=7.1e-15, alt=4.9e-10 |
| int_e9_delta_zstd9 | 272.803 | 0.548 | 38.656 | 7.429 | 2.046 | 0 | 25,011 | 0.007 | km=5e-10, power=1e-10, cad=7.1e-15, alt=4.9e-10 |
| dec_e9_delta_zstd9 | 272.803 | 0.548 | 44.608 | 9.755 | 3.004 | 0 | 66,663,886 | 19.952 | secs=5.8e-11, km=5e-10, power=1e-10, hr=2.8e-14, cad=2.8e-14, alt=4.9e-10 |
| dec_e9_delta_zstd9_cdc | 277.487 | 0.557 | 70.545 | 9.656 | 2.465 | 0 | 66,663,886 | 19.952 | secs=5.8e-11, km=5e-10, power=1e-10, hr=2.8e-14, cad=2.8e-14, alt=4.9e-10 |
| f64_phys_bss_zstd9 | 450.328 | 0.904 | 40.073 | 8.172 | 2.781 | 0 | 8,029,025 | 2.403 | secs=0.005, km=5e-06, power=0.05, hr=0.05, cad=0.05, alt=0.005 |
| int_phys_delta_zstd9 | 186.253 | 0.374 | 27.409 | 7.852 | 2.451 | 0 | 8,029,025 | 2.403 | secs=0.005, km=5e-06, power=0.05, hr=0.05, cad=0.05, alt=0.005 |
| int_phys_dict_zstd9 | 359.369 | 0.722 | 38.313 | 7.821 | 2.241 | 0 | 8,029,025 | 2.403 | secs=0.005, km=5e-06, power=0.05, hr=0.05, cad=0.05, alt=0.005 |
| dec_phys_delta_zstd9 | 186.253 | 0.374 | 28.039 | 8.790 | 2.724 | 0 | 45,440,841 | 13.600 | secs=0.005, km=5e-06, power=0.05, hr=0.05, cad=0.05, alt=0.005 |


## Per-column compressed size (MB)

| variant | athlete_id | activity_id | secs | km | power | hr | cad | alt |
|---|---|---|---|---|---|---|---|---|
| f64_bss_zstd9 | 0.300 | 0.400 | 5.200 | 302.400 | 40.600 | 22.800 | 29.600 | 59.300 |
| int_e9_delta_zstd9 | 0.300 | 0.400 | 2.200 | 84.000 | 71.700 | 27.300 | 53.000 | 33.800 |
| dec_e9_delta_zstd9 | 0.300 | 0.400 | 2.200 | 84.000 | 71.700 | 27.300 | 53.000 | 33.800 |
| int_phys_delta_zstd9 | 0.300 | 0.400 | 1.700 | 56.100 | 49.000 | 18.600 | 38.600 | 21.400 |


## Projection to the full 115.6 GB of zips

| variant | projected_full_GB | lossless |
|---|---|---|
| f64_snappy | 151.800 | 1 |
| f64_zstd3 | 111.700 | 1 |
| f64_dict_zstd9 | 107.100 | 1 |
| f64_bss_zstd3 | 110.400 | 1 |
| f64_bss_zstd9 | 106.900 | 1 |
| f64_bss_zstd19 | 100.300 | 1 |
| f32_bss_zstd9 | 56.500 | 0 |
| int_e9_plain_zstd9 | 103.100 | 0 |
| int_e9_delta_zstd9 | 63.300 | 0 |
| dec_e9_delta_zstd9 | 63.300 | 0 |
| dec_e9_delta_zstd9_cdc | 64.400 | 0 |
| f64_phys_bss_zstd9 | 104.500 | 0 |
| int_phys_delta_zstd9 | 43.200 | 0 |
| int_phys_dict_zstd9 | 83.400 | 0 |
| dec_phys_delta_zstd9 | 43.200 | 0 |
