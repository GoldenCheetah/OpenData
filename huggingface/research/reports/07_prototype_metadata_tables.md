# 07 · Prototype `activities` / `athletes` tables

Sample: 169 athletes (3.5 GB zips).

- Activities: **81,175** rows (77,804 with time series, 51 CSV-only without JSON ride, 3371 JSON-only without CSV)
- Columns: **530** (metrics: 250 scalar, 130 value+weight pairs, 12 arrays)
- Metric names that need quoting in SQL (digit first, '.', capitals, ...): 194 (e.g. 10m_critical_pace, 10m_critical_pace_hr, 10m_critical_pace_swim, 10m_critical_power, 10m_critical_power_hr, 10m_peak_hr)
- Distinct `sport` strings: 101

## Sizes and projection to the full dataset (115.6 GB zips)

| table | sample | projected_full |
|---|---|---|
| activities | 78.0 MB | 2.5 GB |
| athletes | 23.6 KB | 768.0 KB |
| raw_json | 86.3 MB | 2.8 GB |


## Athletes

| yob_min | yob_max | yob_implausible | yob_null |
|---|---|---|---|
| 1,945 | 2,082 | 11 | 0 |

| gender | n |
|---|---|
| M | 164 |
| F | 5 |


## Metric fill rate (share of activities with a value): top 20 / bottom 10

| column | fill |
|---|---|
| activity_crc | 0.991 |
| athlete_weight | 0.991 |
| ride_count | 0.991 |
| cpsolver_best_r | 0.990 |
| average_temp | 0.988 |
| max_temp | 0.986 |
| min_temp | 0.965 |
| workout_time | 0.965 |
| cp_setting | 0.950 |
| time_riding | 0.939 |
| trimp_zonal_points | 0.922 |
| total_distance | 0.896 |
| average_speed | 0.896 |
| distance_swim | 0.896 |
| pace | 0.896 |
| pace_swim | 0.896 |
| triscore | 0.893 |
| max_speed | 0.888 |
| best_50m | 0.885 |
| best_100m | 0.883 |

| column | fill |
|---|---|
| average_run_stance_time_percent | 0.000 |
| average_run_vert_ratio | 0.000 |
| average_step_length | 0.000 |
| min_smo2 | 0.000 |
| Estimated_10K_Time | 0.000 |
| Estimated_21K_Time | 0.000 |
| Estimated_42K_Time | 0.000 |
| Strengthen_Distance | 0.000 |
| average_smo2 | 0.000 |
| average_tHb | 0.000 |


## Array metrics

SDANN, SDNN, cadence_dist, cadence_dist_bins, heart.rate_dist, heart.rate_dist_bins, power_dist, power_dist_bins, power_mmp, power_mmp_secs, speed_dist, speed_dist_bins


## Top sports strings

| sport | n |
|---|---|
| Bike | 58,240 |
| Run | 8,363 |
|  | 6,194 |
| Swim | 2,193 |
| VirtualRide | 1,632 |
| TC | 722 |
| Workout | 703 |
| MTB | 600 |
| Mountain Bike | 373 |
| bike | 240 |
| Other | 214 |
| Hike | 172 |
| WeightTraining | 150 |
| Strength | 149 |
| Yoga | 138 |
| Walk | 129 |
| MTB Tävling | 91 |
| Trainer | 76 |
| XC Ski | 60 |
| VirtualRun | 56 |
