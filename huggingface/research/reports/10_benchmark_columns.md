# 10 · Per-column representation benchmark

60 athletes, 40,742,290 rows, 359.7 MB of zips. zstd 9, 256,000-row row groups, CDC + page index. Exactness judged against the decimal value in the CSV text.

## Best exact representation per column (≤ 0.001 % exceptions, kept in a side table)

| column | representation | MB | inexact | inexact_pct | max_abs_err_source_units | base_MB | saving_pct |
|---|---|---|---|---|---|---|---|
| secs | all_f64_bss | 4.443 | 0 | 0.000 | 0.000 | 4.443 | 0.000 |
| km | um_int_delta | 58.188 | 191 | 0.000 | 0.000 | 216.769 | 73.157 |
| power | all_f32_bss | 29.534 | 0 | 0.000 | 0.000 | 35.620 | 17.086 |
| hr | all_f32_bss | 13.588 | 0 | 0.000 | 0.000 | 16.759 | 18.922 |
| cad | all_f32_bss | 17.743 | 0 | 0.000 | 0.000 | 21.296 | 16.685 |
| alt | all_f32_bss | 30.470 | 0 | 0.000 | 0.000 | 45.562 | 33.124 |


| layout | MB | vs zip | projected full |
|---|---|---|---|
| all Float64 + BSS (current plan) | 341 | 0.95 | 110 GB |
| all Float32 + BSS | 176 | 0.49 | 57 GB |
| best exact per column | 154 | 0.43 | 50 GB |


## All candidates


### secs

| representation | MB | inexact | inexact_pct | max_abs_err_source_units |
|---|---|---|---|---|
| s_int_delta | 0.945 | 1,171,914 | 2.876 | 0.500 |
| all_f64_bss | 4.443 | 0 | 0.000 | 0.000 |
| all_f32_bss | 4.839 | 0 | 0.000 | 0.000 |
| all_f64_dict | 67.526 | 0 | 0.000 | 0.000 |
| s_int_dict | 70.790 | 1,171,914 | 2.876 | 0.500 |
| all_f32_dict | 72.464 | 0 | 0.000 | 0.000 |


### km

| representation | MB | inexact | inexact_pct | max_abs_err_source_units |
|---|---|---|---|---|
| m_int_delta | 12.870 | 33,682,305 | 82.672 | 0.001 |
| dm_int_delta | 26.902 | 6,257,457 | 15.359 | 0.000 |
| cm_int_delta | 37.507 | 536,025 | 1.316 | 0.000 |
| mm_int_delta | 47.279 | 49,669 | 0.122 | 0.000 |
| um_int_delta | 58.188 | 191 | 0.000 | 0.000 |
| m_f32_bss | 60.218 | 0 | 0.000 | 0.000 |
| all_f32_bss | 79.688 | 0 | 0.000 | 0.000 |
| m_int_dict | 104.499 | 33,682,305 | 82.672 | 0.001 |
| mm_int_dict | 124.974 | 49,669 | 0.122 | 0.000 |
| dm_int_dict | 166.042 | 6,257,457 | 15.359 | 0.000 |
| um_int_dict | 167.465 | 191 | 0.000 | 0.000 |
| cm_int_dict | 181.523 | 536,025 | 1.316 | 0.000 |
| all_f64_dict | 181.739 | 0 | 0.000 | 0.000 |
| m_f32_dict | 181.829 | 0 | 0.000 | 0.000 |
| all_f32_dict | 191.514 | 0 | 0.000 | 0.000 |
| all_f64_bss | 216.769 | 0 | 0.000 | 0.000 |


### power

| representation | MB | inexact | inexact_pct | max_abs_err_source_units |
|---|---|---|---|---|
| int_delta | 24.495 | 1,589,209 | 5.831 | 0.500 |
| int_dict | 28.552 | 1,589,209 | 5.831 | 0.500 |
| x10_int_dict | 29.516 | 1,568,900 | 5.757 | 0.050 |
| all_f32_bss | 29.534 | 0 | 0.000 | 0.000 |
| all_f32_dict | 34.252 | 0 | 0.000 | 0.000 |
| x10_int_delta | 34.286 | 1,568,900 | 5.757 | 0.050 |
| all_f64_dict | 34.388 | 0 | 0.000 | 0.000 |
| all_f64_bss | 35.620 | 0 | 0.000 | 0.000 |


### hr

| representation | MB | inexact | inexact_pct | max_abs_err_source_units |
|---|---|---|---|---|
| int_delta | 8.764 | 2,600,395 | 7.825 | 0.500 |
| x10_int_delta | 13.224 | 1,732,738 | 5.214 | 0.050 |
| all_f32_bss | 13.588 | 0 | 0.000 | 0.000 |
| int_dict | 13.835 | 2,600,395 | 7.825 | 0.500 |
| all_f64_bss | 16.759 | 0 | 0.000 | 0.000 |
| x10_int_dict | 17.882 | 1,732,738 | 5.214 | 0.050 |
| all_f32_dict | 19.253 | 0 | 0.000 | 0.000 |
| all_f64_dict | 19.261 | 0 | 0.000 | 0.000 |


### cad

| representation | MB | inexact | inexact_pct | max_abs_err_source_units |
|---|---|---|---|---|
| all_f32_bss | 17.743 | 0 | 0.000 | 0.000 |
| int_dict | 17.940 | 1,399,188 | 4.202 | 0.500 |
| x10_int_dict | 19.832 | 918,452 | 2.758 | 0.050 |
| int_delta | 19.944 | 1,399,188 | 4.202 | 0.500 |
| all_f32_dict | 20.459 | 0 | 0.000 | 0.000 |
| all_f64_dict | 20.476 | 0 | 0.000 | 0.000 |
| all_f64_bss | 21.296 | 0 | 0.000 | 0.000 |
| x10_int_delta | 28.041 | 918,452 | 2.758 | 0.050 |


### alt

| representation | MB | inexact | inexact_pct | max_abs_err_source_units |
|---|---|---|---|---|
| dm_int_delta | 11.674 | 3,989,055 | 11.486 | 0.050 |
| cm_int_delta | 15.557 | 2,932,966 | 8.445 | 0.005 |
| all_f32_bss | 30.470 | 0 | 0.000 | 0.000 |
| dm_int_dict | 33.058 | 3,989,055 | 11.486 | 0.050 |
| cm_int_dict | 39.483 | 2,932,966 | 8.445 | 0.005 |
| all_f64_dict | 42.732 | 0 | 0.000 | 0.000 |
| all_f32_dict | 44.015 | 0 | 0.000 | 0.000 |
| all_f64_bss | 45.562 | 0 | 0.000 | 0.000 |
