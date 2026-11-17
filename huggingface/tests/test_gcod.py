"""The GCOD-specific conversion logic, one known quirk per test."""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest

import gcod
from conftest import A1, A2, basic_athlete, csv, make_zip, ride


def activities(az):
    """build_activities as migrate.py runs it: with the summaries of the athlete's CSVs."""
    kinds = gcod.metric_kinds(gcod.metric_forms(az.meta))
    csvs = {aid: gcod.csv_summary(df) for aid, df in gcod.iter_source_activities(az)}
    return gcod.build_activities(az, kinds, csvs)


def source(az) -> pl.DataFrame:
    frames = [df for _, df in gcod.iter_source_activities(az)]
    return pl.concat(frames)


# --- ride <-> CSV matching -----------------------------------------------------------
def test_rides_link_to_local_time_csvs_and_give_the_utc_offset(zips):
    with gcod.open_athlete_zip(basic_athlete(zips)) as az:
        a = activities(az).sort("activity_id")
    linked = a.filter(pl.col("activity_id") == "2019_03_02_08_00_00").row(0, named=True)
    assert linked["utc_offset_minutes"] == 60
    assert linked["start_time_utc"] == datetime(2019, 3, 2, 7, 0, tzinfo=UTC)


def test_offsets_are_whole_quarter_hours_within_14_hours():
    utc = [datetime(2019, 1, 1, 12, 0, tzinfo=UTC)]
    assert gcod.match_rides(utc, ["2019_01_01_17_45_00"]) == [("2019_01_01_17_45_00", 5 * 3600 + 45 * 60)]
    assert gcod.match_rides(utc, ["2019_01_01_12_10_00"]) == [(None, None)]   # not a quarter hour
    assert gcod.match_rides(utc, ["2019_01_02_03_00_00"]) == [(None, None)]   # 15 h: too far


def test_closest_offset_wins_when_two_csvs_could_match():
    utc = [datetime(2019, 1, 1, 12, 0, tzinfo=UTC)]
    assert gcod.match_rides(utc, ["2019_01_01_14_00_00", "2019_01_01_13_00_00"])[0][0] == "2019_01_01_13_00_00"


# --- activity ids ----------------------------------------------------------------------
def test_manual_entries_get_their_utc_start_time_with_z(zips):
    with gcod.open_athlete_zip(basic_athlete(zips)) as az:
        a = activities(az)
    manual = a.filter(pl.col("sport") == "Swim").row(0, named=True)
    assert manual["activity_id"] == "2019_03_05_18_00_00Z"
    assert manual["has_timeseries"] is False
    assert manual["utc_offset_minutes"] is None


def test_fit_named_csvs_keep_the_fit_suffix(zips):
    with gcod.open_athlete_zip(basic_athlete(zips)) as az:
        ids = [aid for aid, _ in gcod.iter_source_activities(az)]
    assert "2019_03_03_09_30_15.fit" in ids


def test_a_fit_and_a_fit_csv_of_the_same_start_get_distinct_ids(zips):
    z = make_zip(zips / f"{A1}.zip", [], {"2019_01_01_10_00_00.fit": csv([(0, 0, 1, 1, 1, 1)]),
                                          "2019_01_01_10_00_00.fit.csv": csv([(0, 0, 2, 2, 2, 2)])})
    with gcod.open_athlete_zip(z) as az:
        assert [aid for aid, _ in gcod.iter_source_activities(az)] == ["2019_01_01_10_00_00.fit",
                                                                         "2019_01_01_10_00_00.fit~2"]


def test_csv_without_a_json_ride_is_still_an_activity(zips):
    with gcod.open_athlete_zip(basic_athlete(zips)) as az:
        a = activities(az)
    row = a.filter(pl.col("activity_id") == "2019_03_04_10_00_00").row(0, named=True)
    assert row["has_timeseries"] is True and row["start_time_utc"] is None


def test_every_activity_has_a_unique_id(zips):
    with gcod.open_athlete_zip(basic_athlete(zips)) as az:
        a = activities(az)
    assert a["activity_id"].null_count() == 0
    assert a["activity_id"].n_unique() == a.height == 4


def test_two_identical_manual_entries_are_one_activity(zips):
    z = make_zip(zips / f"{A1}.zip", [ride("2019/01/01 10:00:00 UTC"), ride("2019/01/01 10:00:00 UTC")], {})
    with gcod.open_athlete_zip(z) as az:
        assert activities(az)["activity_id"].to_list() == ["2019_01_01_10_00_00Z"]


def test_a_manual_entry_without_a_valid_date_fails_loudly(zips):
    z = make_zip(zips / f"{A1}.zip", [ride("yesterday")], {})
    with gcod.open_athlete_zip(z) as az, pytest.raises(gcod.GcodError, match="without a valid date"):
        activities(az)


# --- metrics -------------------------------------------------------------------------
def test_metric_forms_become_scalar_pair_and_list_columns(zips):
    with gcod.open_athlete_zip(basic_athlete(zips)) as az:
        a = activities(az)
    first = a.filter(pl.col("activity_id") == "2019_03_02_08_00_00").row(0, named=True)
    assert first["total_work"] == 123.4
    assert (first["average_power"], first["average_power__weight"]) == (210.5, 3600.0)
    assert first["power_mmp"] == [900.0, 800.0]          # a list metric, even with 2 elements
    assert a.schema["power_mmp"] == pl.List(pl.Float64)


def test_a_metric_seen_as_pair_and_list_becomes_a_list():
    assert gcod.metric_kinds({"x": {"pair", "list"}, "y": {"str", "pair"}, "z": {"str"}}) == \
        {"x": "list", "y": "pair", "z": "scalar"}


def test_xdata_is_kept_as_json(zips):
    with gcod.open_athlete_zip(basic_athlete(zips)) as az:
        a = activities(az)
    assert a.filter(pl.col("xdata").is_not_null())["xdata"].to_list() == ['{"ROUTE": {"x": 1}}']


# --- time series -----------------------------------------------------------------------
def test_values_are_float32_exact_and_distance_is_in_metres(zips):
    with gcod.open_athlete_zip(basic_athlete(zips)) as az:
        src = source(az)
    out, exc = gcod.to_output_timeseries(src)
    assert out.schema["power_w"] == pl.Float32
    assert gcod.cast_mismatches(src, out) == 0
    assert gcod.same_as_source(src, out, exc) == 0
    assert f"{out['distance_m'][1]:.6g}" == "5.40637"     # 0.00540637 km
    assert f"{out['time_s'][2]:.6g}" == "2.52"            # fractional seconds kept


def test_subnormal_junk_is_zero_in_timeseries_and_exact_in_exceptions(zips):
    with gcod.open_athlete_zip(basic_athlete(zips)) as az:
        src = source(az)
    out, exc = gcod.to_output_timeseries(src)
    assert out["power_w"][2] == 0
    assert exc.select("activity_id", "row_in_activity", "column", "source_value").rows() == \
        [("2019_03_02_08_00_00", 2, "power_w", 4.94066e-324)]


def test_missing_sensor_values_stay_null(zips):
    with gcod.open_athlete_zip(basic_athlete(zips)) as az:
        out, _ = gcod.to_output_timeseries(source(az))
    assert out["hr_bpm"][2] is None


def test_rows_keep_their_recorded_order_even_when_time_goes_backwards(zips):
    z = make_zip(zips / f"{A1}.zip", [], {"2019_01_01_10_00_00.csv": csv([(0, 0, 1, 1, 1, 1), (66, 0.01, 1, 1, 1, 1),
                                                                         (16, 0.02, 1, 1, 1, 1)])})
    with gcod.open_athlete_zip(z) as az:
        out, _ = gcod.to_output_timeseries(source(az))
    assert out["time_s"].to_list() == [0, 66, 16]


def test_an_unexpected_csv_header_fails_loudly(zips):
    z = make_zip(zips / f"{A1}.zip", [], {"2019_01_01_10_00_00.csv": "secs,km,power,hr,cad,alt,temp\n0,0,1,1,1,1,20\n"})
    with gcod.open_athlete_zip(z) as az, pytest.raises(gcod.GcodError, match="header"):
        list(gcod.iter_source_activities(az))


def test_a_check_notices_a_changed_value(zips):
    with gcod.open_athlete_zip(basic_athlete(zips)) as az:
        src = source(az)
    out, exc = gcod.to_output_timeseries(src)
    tampered = out.with_columns(pl.when(pl.int_range(pl.len()) == 0).then(101.0).otherwise(pl.col("power_w"))
                                .cast(pl.Float32).alias("power_w"))
    assert gcod.cast_mismatches(src, tampered) == 1
    assert gcod.same_as_source(src, tampered, exc) == 1


# --- zip contents --------------------------------------------------------------------
def test_unexpected_members_fail_loudly(zips):
    z = make_zip(zips / f"{A1}.zip", [], {}, extra_members={"notes.txt": "hi"})
    with pytest.raises(gcod.GcodError, match="unexpected members"):
        gcod.open_athlete_zip(z)


def test_invalid_json_fails_loudly(zips):
    z = make_zip(zips / f"{A1}.zip", [], {}, json_text="{not json")
    with pytest.raises(gcod.GcodError, match="invalid JSON"):
        gcod.open_athlete_zip(z)


def test_an_athlete_without_rides_or_csvs_is_fine(zips):
    z = make_zip(zips / f"{A2}.zip", [], {})
    with gcod.open_athlete_zip(z) as az:
        assert activities(az).height == 0
        assert list(gcod.iter_source_activities(az)) == []
        assert gcod.athlete_row(az, 0, 0)["yob"] == 1980


def test_json_athlete_id_shows_a_reupload_under_another_name(zips):
    z = make_zip(zips / f"{A1}.zip", [], {}, athlete={"gender": "F", "yob": "abc", "id": "{" + A2 + "}"})
    with gcod.open_athlete_zip(z) as az:
        row = gcod.athlete_row(az, 0, 0)
    assert row["json_athlete_id"] == A2 and row["yob"] is None and row["gender"] == "F"


# --- quirks found in the first 10% of the full release ----------------------------------
def test_backup_and_other_extensions_are_kept_as_their_own_activities(zips):
    z = make_zip(zips / f"{A1}.zip", [], {
        "2015_01_20_18_50_00.csv": csv([(0, 0, 1, 1, 1, 1)]),
        "2015_01_20_18_50_00.csv.bak.csv": csv([(0, 0, 2, 2, 2, 2)]),   # backup version, different values
        "2019_02_17_08_25_09.pwx": csv([(0, 0, 3, 3, 3, 3)]),           # a CSV named after the original .pwx
    })
    with gcod.open_athlete_zip(z) as az:
        ids = [aid for aid, _ in gcod.iter_source_activities(az)]
        a = activities(az)
    assert ids == ["2015_01_20_18_50_00", "2015_01_20_18_50_00.csv.bak", "2019_02_17_08_25_09.pwx"]
    assert sorted(a["activity_id"]) == ids


def test_activity_ids_match_the_conventions_pattern():
    import re

    import conventions as C

    for ok in ["2019_01_01_10_00_00", "2019_01_01_10_00_00.fit", "2015_01_20_18_50_00.csv.bak",
               "2019_02_17_08_25_09.pwx", "2017_01_31_17_36_44Z",
               "2014_08_10_09_21_17.sync-conflict-20180127-150031-75VG4DH",
               "2019_01_01_10_00_00~2", "2019_01_01_10_00_00.fit~2", "2017_01_31_17_36_44Z~3",
               "2019_01_01_10_00_00~12"]:
        assert re.match(C.ACTIVITY_ID_RE, ok), ok
    for bad in ["2019_01_01_10_00", "2019_01_01_10_00_00Z.fit", "2019_01_01_10_00_00 .fit", "x",
                "2019_01_01_10_00_00~1", "2019_01_01_10_00_00~02", "2019_01_01_10_00_00~", "2019_01_01_10_00_00~2~2"]:
        assert not re.match(C.ACTIVITY_ID_RE, bad), bad


def test_a_member_named_like_an_activity_must_still_be_a_csv(zips):
    z = make_zip(zips / f"{A1}.zip", [], {"2019_02_17_08_25_09.pwx": "<?xml version='1.0'?><pwx/>"})
    with gcod.open_athlete_zip(z) as az, pytest.raises(gcod.GcodError, match="header"):
        list(gcod.iter_source_activities(az))


GC_JSON_WITH_UNESCAPED_QUOTES = """{
\t"VERSION":"1.9",
\t"ATHLETE":{ "gender":"M", "yob":"1980", "id":"{00000000-0000-4000-8000-000000000001}" },
\t"RIDES":[
\t{
\t\t"date":"2019/01/17 17:49:23 UTC",
\t\t"data":"T--PHC---------",
\t\t"sport":"Когда посмотрел на календарь и увидел, что "Spring is coming!"",

\t\t"METRICS":{
\t\t\t"total_work":"10"
\t\t}
\t}
  ]
}"""


def test_unescaped_quotes_in_free_text_are_repaired_and_flagged(zips):
    z = make_zip(zips / f"{A1}.zip", [], {}, json_text=GC_JSON_WITH_UNESCAPED_QUOTES)
    with gcod.open_athlete_zip(z) as az:
        assert az.json_repairs == ("unescaped_quotes",)
        assert az.json_text == GC_JSON_WITH_UNESCAPED_QUOTES          # original text kept unchanged
        assert az.meta["RIDES"][0]["sport"] == 'Когда посмотрел на календарь и увидел, что "Spring is coming!"'
        assert gcod.athlete_row(az, 1, 0)["json_repairs"] == ["unescaped_quotes"]


def test_valid_json_needs_no_repairs(zips):
    with gcod.open_athlete_zip(basic_athlete(zips)) as az:
        assert az.json_repairs == ()
        assert gcod.athlete_row(az, 0, 0)["json_repairs"] == []


def gc_json(sport_line: str, metrics_open: str = '\t\t"METRICS":{\n') -> str:
    """A GoldenCheetah-style JSON with one ride, its sport line and its METRICS opening as given."""
    return ('{\n\t"VERSION":"1.9",\n\t"ATHLETE":{ "gender":"M", "yob":"1980", "id":"{x}" },\n\t"RIDES":[\n\t{\n'
            '\t\t"date":"2019/01/17 17:49:23 UTC",\n\t\t"data":"T--PHC---------",\n'
            f'{sport_line}\n\n{metrics_open}\t\t\t"total_work":"10"\n\t\t}}\n\t}}\n  ]\n}}')


def test_raw_tabs_and_line_breaks_in_free_text_are_read_as_they_are(zips):
    z = make_zip(zips / f"{A1}.zip", [], {}, json_text=gc_json('\t\t"sport":"MTB\t",'))
    with gcod.open_athlete_zip(z) as az:
        assert az.json_repairs == ("control_characters",)
        assert az.meta["RIDES"][0]["sport"] == "MTB\t"
    z = make_zip(zips / f"{A2}.zip", [], {}, json_text=gc_json('\t\t"sport":"\nDunschdig´s Radler",'))
    with gcod.open_athlete_zip(z) as az:
        assert az.meta["RIDES"][0]["sport"] == "\nDunschdig´s Radler"


def test_a_stray_comma_opening_the_metrics_is_removed(zips):
    text = gc_json('\t\t"sport":"Bike",', metrics_open='\t\t"METRICS":{\n,\n')
    z = make_zip(zips / f"{A1}.zip", [], {}, json_text=text)
    with gcod.open_athlete_zip(z) as az:
        assert az.json_repairs == ("empty_first_metric",)
        assert az.meta["RIDES"][0]["METRICS"] == {"total_work": "10"}


def test_unescaped_quotes_in_free_text_over_several_lines_are_repaired(zips):
    sport = '\t\t"sport":"2x5" + 2x5": Ischio\n10 et 20kf ",'
    z = make_zip(zips / f"{A1}.zip", [], {}, json_text=gc_json(sport))
    with gcod.open_athlete_zip(z) as az:
        assert az.json_repairs == ("unescaped_quotes", "control_characters")
        assert az.meta["RIDES"][0]["sport"] == '2x5" + 2x5": Ischio\n10 et 20kf '


def test_the_quote_repair_never_joins_json_structure_into_a_string():
    # `"id":"{x}" },` opens a string that doesn't end its line; the next line is a key, not free text
    text = '{\n\t"ATHLETE":{\n\t\t"id":"{x}" },\n\t"RIDES":[ "a "b" c",\n\t]\n}'
    assert gcod._repair_unescaped_quotes(text) == text


def test_json_broken_in_other_ways_still_fails_loudly(zips):
    z = make_zip(zips / f"{A1}.zip", [], {}, json_text=GC_JSON_WITH_UNESCAPED_QUOTES[:-40])  # truncated
    with pytest.raises(gcod.GcodError, match="invalid JSON"):
        gcod.open_athlete_zip(z)


def _zip_with_duplicate(path, second: str):
    import json
    import warnings
    import zipfile

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # zipfile warns about duplicate names
        with zipfile.ZipFile(path, "w") as zf:
            zf.writestr(f"{path.stem}.json", json.dumps({"VERSION": "1.9", "ATHLETE": {}, "RIDES": []}))
            zf.writestr("2012_07_19_21_50_10.csv", csv([(0, 0, 1, 1, 1, 1)]))
            zf.writestr("2012_07_19_21_50_10.csv", second)
    return path


def test_byte_identical_duplicate_members_count_once(zips):
    z = _zip_with_duplicate(zips / f"{A1}.zip", csv([(0, 0, 1, 1, 1, 1)]))
    with gcod.open_athlete_zip(z) as az:
        assert [aid for aid, _ in gcod.iter_source_activities(az)] == ["2012_07_19_21_50_10"]


def test_two_different_members_with_one_name_are_both_kept_in_zip_order(zips):
    z = _zip_with_duplicate(zips / f"{A1}.zip", csv([(0, 0, 2, 2, 2, 2), (1, 0, 2, 2, 2, 2)]))
    with gcod.open_athlete_zip(z) as az:
        got = {aid: df.height for aid, df in gcod.iter_source_activities(az)}
    assert got == {"2012_07_19_21_50_10": 1, "2012_07_19_21_50_10~2": 2}


def test_sync_conflict_copies_are_kept_as_their_own_activities(zips):
    name = "2014_08_10_09_21_17.sync-conflict-20180127-150031-75VG4DH"
    z = make_zip(zips / f"{A1}.zip", [], {"2014_08_10_09_21_17.csv": csv([(0, 0, 1, 1, 1, 1)]),
                                          f"{name}.csv": csv([(0, 0, 2, 2, 2, 2)])})
    with gcod.open_athlete_zip(z) as az:
        assert [aid for aid, _ in gcod.iter_source_activities(az)] == ["2014_08_10_09_21_17", name]


def test_dates_in_thai_digits_are_read(zips):
    z = make_zip(zips / f"{A1}.zip", [ride("๒๐๑๘/๐๓/๐๗ ๑๒:๑๒:๐๔ UTC")],
                 {"2018_03_07_19_12_04.csv": csv([(0, 0, 1, 1, 1, 1)])})
    with gcod.open_athlete_zip(z) as az:
        row = activities(az).row(0, named=True)
    assert row["activity_id"] == "2018_03_07_19_12_04" and row["utc_offset_minutes"] == 7 * 60
    assert row["start_time_utc"] == datetime(2018, 3, 7, 12, 12, 4, tzinfo=UTC)


def test_identical_duplicate_rides_count_once(zips):
    r = ride("2019/03/02 07:00:00 UTC", metrics={"total_work": "1"})
    z = make_zip(zips / f"{A1}.zip", [r, dict(r), ride("2019/03/09 07:00:00 UTC")],
                 {"2019_03_02_08_00_00.csv": csv([(0, 0, 1, 1, 1, 1)])})
    with gcod.open_athlete_zip(z) as az:
        a = activities(az)
    assert sorted(a["activity_id"]) == ["2019_03_02_08_00_00", "2019_03_09_07_00_00Z"]   # no phantom "...Z" twin


# Two versions of one activity: the athlete's library held two files with the same start time.
ONE_MINUTE = csv([(0, 0, 1, 1, 1, 1), (60, 0.5, 1, 1, 1, 1)])  # a CSV of 60 s and 0.5 km


def version(work: str, minutes: float, km: float) -> dict:
    return ride("2019/03/02 07:00:00 UTC", metrics={"total_work": work, "workout_time": str(minutes * 60),
                                                    "total_distance": str(km)})


def test_two_rides_with_one_csv_link_the_ride_that_fits_it(zips):
    rides = [version("1", 10, 5.0), version("2", 1, 0.5)]  # the CSV is 60 s and 0.5 km: the second ride
    z = make_zip(zips / f"{A1}.zip", rides, {"2019_03_02_08_00_00.csv": ONE_MINUTE})
    with gcod.open_athlete_zip(z) as az:
        a = activities(az).sort("activity_id")
    assert a.select("activity_id", "total_work", "has_timeseries", "utc_offset_minutes").rows() == [
        ("2019_03_02_07_00_00Z", 1.0, False, None),    # no CSV of its own in the release
        ("2019_03_02_08_00_00", 2.0, True, 60)]


def test_two_rides_that_both_fit_the_csv_link_in_json_order(zips):
    rides = [version("1", 1, 0.5), version("2", 1, 0.5)]
    z = make_zip(zips / f"{A1}.zip", rides, {"2019_03_02_08_00_00.csv": ONE_MINUTE})
    with gcod.open_athlete_zip(z) as az:
        a = activities(az)
    assert dict(a.select("activity_id", "total_work").rows()) == {"2019_03_02_08_00_00": 1.0,
                                                                  "2019_03_02_07_00_00Z": 2.0}


def test_two_rides_with_two_csvs_of_one_name_pair_by_fit(zips):
    import json
    import warnings
    import zipfile

    path = zips / f"{A1}.zip"
    rides = [version("1", 10, 5.0), version("2", 1, 0.5)]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with zipfile.ZipFile(path, "w") as zf:
            zf.writestr(f"{A1}.json", json.dumps({"VERSION": "1.9", "ATHLETE": {}, "RIDES": rides}))
            zf.writestr("2019_03_02_08_00_00.csv", ONE_MINUTE)
            zf.writestr("2019_03_02_08_00_00.csv", csv([(0, 0, 1, 1, 1, 1), (600, 5.0, 1, 1, 1, 1)]))  # 10 min
    with gcod.open_athlete_zip(path) as az:
        a = activities(az)
    assert dict(a.select("activity_id", "total_work").rows()) == {"2019_03_02_08_00_00": 2.0,
                                                                  "2019_03_02_08_00_00~2": 1.0}
    assert a["has_timeseries"].all()


def test_two_rides_without_csv_and_with_one_start_get_distinct_z_ids(zips):
    z = make_zip(zips / f"{A1}.zip", [version("1", 1, 0.5), version("2", 1, 0.5)], {})
    with gcod.open_athlete_zip(z) as az:
        a = activities(az)
    assert dict(a.select("activity_id", "total_work").rows()) == {"2019_03_02_07_00_00Z": 1.0,
                                                                  "2019_03_02_07_00_00Z~2": 2.0}


def test_unique_ids_numbers_repeats_in_order():
    assert gcod.unique_ids(["a", "b", "a", "a"]) == ["a", "b", "a~2", "a~3"]
