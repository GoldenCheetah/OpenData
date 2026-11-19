# 11 · Cumulative distance vs per-sample increments

60 athletes, 40,742,290 rows.

| variant | MB |
|---|---|
| cum_m_f32 (current) | 60.218 |
| delta_m_f32_bss | 57.132 |
| delta_m_f32_dict | 35.816 |
| delta_cm_int_delta | 35.421 |


## Rebuilding cumulative distance from Float32 increments

- distinct (original, rebuilt) pairs that differ after rounding to 6 significant digits: **59** of 35,435,538
- max absolute drift before rounding: 0.0048 m; median 9.91e-06 m
- negative increments (distance going backwards): 420