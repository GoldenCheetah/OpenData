# 04 · JSON metadata profile

Athletes: **169**, JSON rides: **81,124**, CSVs: **77,804**, CSVs without a matching ride: **51**


## Athlete-level

| has_json | id_matches_filename | versions | top_keys | athlete_keys | genders | yob_null | yob_range |
|---|---|---|---|---|---|---|---|
| 1 | 0 | 1.8, 1.9, 2.0 | ATHLETE,RIDES,VERSION | gender,id,yob | {"gender":"F","count":5} {"gender":"M","count":164} | 0 | 1945-2082 |


## Ride ↔ CSV matching (UTC date vs local-time filename)

| rides | matched | ambiguous | unparseable_date |
|---|---|---|---|
| 81,124 | 77,753 | 345 | 0 |


Unmatched rides by sport:

| sport | n |
|---|---|
|  | 1,928 |
| Swim | 542 |
| Bike | 398 |
| Workout | 83 |
| Run | 83 |
| WeightTraining | 82 |
| Day Off | 46 |
| Crossfit | 44 |
| Strength | 37 |
| Yoga | 35 |


UTC offset (local − UTC) of matched rides:

| utc_offset_h | rides |
|---|---|
| 2.000 | 20,324 |
| 1.000 | 19,605 |
| -4.000 | 7,530 |
| -5.000 | 5,250 |
| 3.000 | 4,042 |
| 9.000 | 3,646 |
| 0.000 | 3,394 |
| 11.000 | 3,035 |
| 10.000 | 2,631 |
| -6.000 | 2,142 |
| 8.000 | 1,852 |
| 7.000 | 1,578 |
| -7.000 | 1,036 |
| -8.000 | 472 |
| 13.000 | 464 |


## Sports

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


## `data` flag string

| flag_len | n |
|---|---|
| 15 | 80,413 |
| 0 | 711 |

| pos | letter | pct_set |
|---|---|---|
| 0 | T | 94.900 |
| 1 | D | 89.000 |
| 2 | S | 88.800 |
| 3 | P | 57.500 |
| 4 | H | 77.300 |
| 5 | C | 74.900 |
| 6 | N | 2.000 |
| 7 | A | 84.300 |
| 8 | G | 74.600 |
| 9 | L | 80.200 |
| 10 | W | 2.200 |
| 11 | E | 49.400 |
| 12 | V | 13.000 |
| 13 | O | 0.000 |
| 14 | R | 3.100 |


## Ride keys

| ride_keys | n |
|---|---|
| METRICS,data,date,sport | 73,574 |
| METRICS,XDATA,data,date,sport | 7,550 |


## XDATA (names only)

| xdata | n |
|---|---|
|  | 73,574 |
| {"HRV": ["R-R ", "R-R flag "]} | 2,324 |
| {"EXTRA": ["FIELD_33 "]} | 744 |
| {"EXTRA": ["BATTERYSOC "]} | 543 |
| {"EXTRA": ["FIELD_88 "]} | 467 |
| {"SWIM": ["TYPE ", "DURATION ", "STROKES "]} | 430 |
| {"EXTRA": ["FIELD_73 ", "FIELD_42 "]} | 241 |
| {"DEVELOPER": ["wprime_bal "], "HRV": ["R-R ", "R-R flag "]} | 225 |


## METRICS

Distinct metric keys: **392**; metrics per ride: median 160.0, max 270


Value forms (`list2` = [value, count/weight]):

| forms | n_metrics |
|---|---|
| str | 250 |
| list2/str | 72 |
| list2 | 58 |
| list1/list2/listN | 8 |
| listN | 2 |
| list1/listN | 2 |


Top 40 metrics by coverage:

| metric | rides | forms | pct |
|---|---|---|---|
| athlete_weight | 80,413 | str | 99.100 |
| activity_crc | 80,413 | str | 99.100 |
| ride_count | 80,413 | str | 99.100 |
| cpsolver_best_r | 80,403 | str | 99.100 |
| average_temp | 80,200 | list2/str | 98.900 |
| max_temp | 80,047 | str | 98.700 |
| min_temp | 78,314 | str | 96.500 |
| workout_time | 78,310 | str | 96.500 |
| cp_setting | 77,146 | str | 95.100 |
| time_riding | 76,199 | str | 93.900 |
| trimp_zonal_points | 74,811 | str | 92.200 |
| total_distance | 72,760 | str | 89.700 |
| average_speed | 72,757 | str | 89.700 |
| distance_swim | 72,749 | str | 89.700 |
| pace | 72,745 | str | 89.700 |
| pace_swim | 72,745 | str | 89.700 |
| triscore | 72,500 | str | 89.400 |
| max_speed | 72,071 | str | 88.800 |
| best_50m | 71,825 | str | 88.500 |
| best_100m | 71,711 | str | 88.400 |
| best_200m | 71,549 | str | 88.200 |
| best_400m | 71,264 | str | 87.800 |
| best_500m | 71,166 | str | 87.700 |
| speed_dist_bins | 70,901 | list1/list2/listN | 87.400 |
| speed_dist | 70,901 | list1/list2/listN | 87.400 |
| best_800m | 70,807 | str | 87.300 |
| best_1000m | 70,561 | str | 87.000 |
| best_1500m | 69,813 | str | 86.100 |
| best_2000m | 68,986 | str | 85.000 |
| best_3000m | 67,382 | str | 83.100 |
| best_4000m | 66,075 | str | 81.400 |
| best_5000m | 64,599 | str | 79.600 |
| average_hr | 62,785 | list2/str | 77.400 |
| average_ct | 62,718 | list2/str | 77.300 |
| max_ct | 62,718 | str | 77.300 |
| max_heartrate | 62,717 | str | 77.300 |
| heartbeats | 62,716 | str | 77.300 |
| min_heartrate | 62,715 | str | 77.300 |
| ninety_five_percent_hr | 62,535 | str | 77.100 |
| 1m_critical_power_hr | 62,018 | str | 76.400 |


Least common 15 metrics:

| metric | rides | forms |
|---|---|---|
| tss | 11 | str |
| running_effectiveness | 11 | str |
| max_tHb | 11 | str |
| running_stride_length | 11 | str |
| Estimated_42K_Time | 11 | str |
| average_smo2 | 11 | list2 |
| average_tHb | 11 | list2 |
| Strengthen_Distance | 11 | str |
| stryd_leg_spring | 11 | list2 |
| stryd_form_power | 11 | list2 |
| Estimated_21K_Time | 11 | str |
| min_smo2 | 6 | str |
| average_step_length | 5 | list2 |
| average_run_vert_ratio | 5 | list2 |
| average_run_stance_time_percent | 5 | list2 |
