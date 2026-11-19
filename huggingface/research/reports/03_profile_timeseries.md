# 03 · Activity CSV profile

Sample: **169** athlete zips, **77,804** activity CSVs, **406,151,365** rows, 10.8 GB uncompressed CSV.

Non-activity members in zips: 0 

## File-level checks

| check | value |
|---|---|
| files | 77,804 |
| empty_or_header_only | 0 |
| parse_errors | 0 |
| secs_fractional_files | 3,853 |
| files_with_gaps | 48,437 |
| files_with_subsecond | 3,195 |
| files_with_dup_secs | 1,301 |
| files_non_monotonic | 3 |
| files_secs_not_start_0 | 0 |
| median_rows | 3,925 |
| max_rows | 227,902 |


## Sampling interval (row-to-row Δsecs, summed over all files)

| metric | value |
|---|---|
| dt_eq1 | 395,614,395 |
| dt_gt1 | 5,361,502 |
| dt_lt1 | 4,943,770 |
| dt_zero | 153,887 |
| dt_neg | 7 |
| gap_seconds | 2,091,652,933 |


Median Δsecs per file:

| dt_median | files |
|---|---|
| 1.000 | 73,952 |
| 0.500 | 624 |
| 5.000 | 589 |
| 2.000 | 431 |
| 1.260 | 410 |
| 4.000 | 266 |
| 3.000 | 263 |
| 6.000 | 207 |


## Headers

| header | files | athletes |
|---|---|---|
| secs,km,power,hr,cad,alt | 77,804 | 167 |


## Columns

| column | files | rows | null_pct | nonnumeric | exp | negative | min | max | max_sig_dec | p99_sig_dec | max_raw_dec | max_int_digits |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| alt | 77,804 | 406,151,365 | 7.070 | 0 | 10,115 | 40,849,916 | -548.951 | 8,850 | 9 | 8.000 | 9 | 4 |
| km | 77,804 | 406,151,365 | 0.000 | 0 | 22,655 | 4,765 | -0.012 | 42,949 | 9 | 9.000 | 9 | 5 |
| hr | 77,804 | 406,151,365 | 14.180 | 0 | 47 | 0 | 0.000 | 255.000 | 9 | 4.000 | 9 | 3 |
| power | 77,804 | 406,151,365 | 32.050 | 0 | 218,210 | 0 | 0.000 | 35,890 | 10 | 6.000 | 10 | 5 |
| cad | 77,804 | 406,151,365 | 18.670 | 0 | 1,458 | 0 | 0.000 | 12,391 | 10 | 6.000 | 10 | 5 |
| secs | 77,804 | 406,151,365 | 0.000 | 0 | 32,441 | 0 | 0.000 | 960,215,000 | 8 | 3.000 | 8 | 9 |


## Files where a column is entirely empty

| column | files_all_null |
|---|---|
| alt | 8,727 |
| cad | 18,730 |
| hr | 14,520 |
| power | 30,729 |


## Max significant decimals per file (distribution)

| column | max_sig_decimals | files |
|---|---|---|
| alt | 0 | 11,526 |
| alt | 1 | 29,929 |
| alt | 2 | 2,792 |
| alt | 3 | 12,047 |
| alt | 4 | 10,958 |
| alt | 5 | 5,704 |
| alt | 6 | 1,787 |
| alt | 7 | 1,841 |
| alt | 8 | 813 |
| alt | 9 | 407 |
| cad | 0 | 56,940 |
| cad | 1 | 4,174 |
| cad | 2 | 948 |
| cad | 3 | 421 |
| cad | 4 | 4,045 |
| cad | 5 | 9,364 |
| cad | 6 | 1,546 |
| cad | 7 | 249 |
| cad | 8 | 2 |
| cad | 9 | 114 |
| cad | 10 | 1 |
| hr | 0 | 45,142 |
| hr | 1 | 3,376 |
| hr | 2 | 935 |
| hr | 3 | 10,954 |
| hr | 4 | 16,911 |
| hr | 5 | 482 |
| hr | 6 | 2 |
| hr | 9 | 2 |
| km | 0 | 4,946 |
| km | 1 | 2 |
| km | 2 | 6 |
| km | 3 | 6,850 |
| km | 4 | 15,287 |
| km | 5 | 24,747 |
| km | 6 | 7,415 |
| km | 7 | 9,893 |
| km | 8 | 5,789 |
| km | 9 | 2,869 |
| power | 0 | 64,820 |
| power | 1 | 4,402 |
| power | 2 | 902 |
| power | 3 | 1,559 |
| power | 4 | 2,763 |
| power | 5 | 2,099 |
| power | 6 | 486 |
| power | 7 | 93 |
| power | 8 | 21 |
| power | 9 | 658 |
| power | 10 | 1 |
| secs | 0 | 73,939 |
| secs | 1 | 1,314 |
| secs | 2 | 1,540 |
| secs | 3 | 563 |
| secs | 4 | 4 |
| secs | 5 | 258 |
| secs | 6 | 174 |
| secs | 8 | 12 |
