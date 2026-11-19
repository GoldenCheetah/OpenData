# 13 · Quirk census of the full release

All 6,613 athlete zips, read with the production converter (`datasets/opendata/gcod.py`, converter 0.4.0). Metadata and CSV headers only; 485 s.

## Converter verdict

- Athletes the converter accepts: 6,613 (100.0%)
- Athletes it stops on: 0 (0.0%)

| error | athletes |
|---|---:|

## Quirks (handled or not)

| quirk | athletes | instances |
|---|---:|---:|
| duplicate zip members | 10 (0.2%) | 47 |
| ... with different content under one name | 7 (0.1%) | 18 |
| JSON repair `unescaped_quotes` | 22 (0.3%) | |
| JSON repair `empty_first_metric` | 4 (0.1%) | |
| JSON repair `control_characters` | 6 (0.1%) | |
| JSON not readable (even repaired) | 0 (0.0%) | |
| identical duplicate rides | 3 (0.0%) | 30 |
| rides sharing a start time but differing | 32 (0.5%) | 532 groups, 1,064 rides |
| dates not in ASCII digits | 3 (0.0%) | 214 |
| rides without a valid date | 0 (0.0%) | 0 |
| CSV ids numbered `~n` | 7 (0.1%) | 18 |
| `Z` ids numbered `~n` | 1 (0.0%) | 3 |
| CSVs with an unexpected header | 0 (0.0%) | 0 |
| rides without a CSV (`Z` ids) | 2,731 (41.3%) | 101,048 |
| CSVs without a ride | 482 (7.3%) | 3,207 |

Totals: 2,609,980 JSON rides, 2,512,091 CSV members, 2,508,902 rides linked to a CSV.

Activity file name suffixes (after the start time):

| suffix | files |
|---|---:|
| `.csv` | 2,510,991 |
| `.fit` | 775 |
| `.sync-conflict-….csv` | 176 |
| `.fitlog` | 107 |
| `.fit.csv` | 35 |
| `.srm` | 3 |
| `.csv.bak.csv` | 2 |
| `.pwx` | 1 |
| `.tcx` | 1 |

## Rides that share a start time but differ

Fields that differ within a group (`METRICS` = metric values):

| differs in | groups | with a CSV |
|---|---:|---:|
| METRICS | 310 | 307 |
| sport+METRICS | 172 | 172 |
| data+METRICS | 35 | 35 |
| data+sport+METRICS | 9 | 9 |
| XDATA+sport+METRICS | 5 | 5 |
| XDATA+data+METRICS | 1 | 1 |

Group sizes: {2: 532}
