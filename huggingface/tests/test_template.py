"""The dataset template: it builds, it validates, and the cross-dataset checks catch drift."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import polars as pl
import pytest

import conventions as C
import migrate
import validate
from conftest import A1, A3

TEMPLATE = Path(__file__).resolve().parents[1] / "datasets" / "_template"
SPEC = TEMPLATE / "dataset.toml"


def load_build():
    spec = importlib.util.spec_from_file_location("template_build", TEMPLATE / "build.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def opendata(built: Path) -> Path:
    return built / "build" / "opendata"


@pytest.fixture
def example(opendata: Path, tmp_path: Path) -> Path:
    out = tmp_path / "example"
    load_build().build(opendata, out)
    return out


def against(opendata: Path) -> dict:
    return {"opendata": (C.load_dataset_spec(migrate.SPEC_PATH), str(opendata))}


def test_the_template_builds_a_dataset_that_passes_validation(example: Path, opendata: Path):
    assert validate.validate(SPEC, example, jobs=1, max_files=None, against=against(opendata)) == 0


def test_the_template_resamples_to_whole_seconds(example: Path):
    ts = pl.read_parquet(example / "data" / "timeseries" / f"athlete_id={A1}" / "part.parquet", hive_partitioning=False)
    first = ts.filter(pl.col("activity_id") == "2019_03_02_08_00_00")
    # source: (0, 100 W), (1, 150.5 W), (2.52, sub-normal -> 0 W): one row per second, first sample wins
    assert first["time_s"].to_list() == [0.0, 1.0, 2.0]
    assert first["power_w"].to_list() == [100.0, 150.5, 0.0]
    acts = pl.read_parquet(example / "data" / "activities" / "*.parquet")
    assert not (example / "data" / "timeseries" / f"athlete_id={A3}").exists()
    assert set(acts["athlete_id"]) == set(pl.read_parquet(example / "data" / "athletes" / "*.parquet")["athlete_id"])


def test_the_template_refuses_to_build_into_a_folder_with_files(opendata: Path, tmp_path: Path):
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "x").write_text("keep me")
    with pytest.raises(SystemExit, match="not empty"):
        load_build().build(opendata, tmp_path / "out")
    assert (tmp_path / "out" / "x").read_text() == "keep me"


def test_relations_to_another_dataset_need_it(example: Path, capsys):
    assert validate.validate(SPEC, example, jobs=1, max_files=None) == 1
    assert "pass --against" in capsys.readouterr().out


def rewrite(path: Path, table: str, change) -> None:
    df = change(pl.read_parquet(path, hive_partitioning=False))
    t = C.load_dataset_spec(SPEC).tables[table]
    C.write_table(df.to_arrow(), path, t.profile, dataset="example", table_name=table, encodings=t.encodings)


def test_a_shared_column_with_another_value_than_in_the_other_dataset_fails(example: Path, opendata: Path, capsys):
    part = example / "data" / "activities" / "activities.parquet"
    rewrite(part, "activities", lambda df: df.with_columns(pl.col("sport").str.to_lowercase()))
    assert validate.validate(SPEC, example, jobs=1, max_files=None, against=against(opendata)) == 1
    out = capsys.readouterr().out
    assert "same values as opendata.activities" in out and "sport differs" in out


def test_an_activity_the_other_dataset_does_not_have_fails(example: Path, opendata: Path, capsys):
    part = example / "data" / "activities" / "activities.parquet"
    first_row = pl.int_range(pl.len()) == 0
    rewrite(part, "activities", lambda df: df.with_columns(
        pl.when(first_row).then(pl.lit("2001_01_01_00_00_00")).otherwise("activity_id").alias("activity_id")))
    assert validate.validate(SPEC, example, jobs=1, max_files=None, against=against(opendata)) == 1
    assert "not in opendata.activities" in capsys.readouterr().out


def test_cross_dataset_relations_are_checked_in_the_spec(tmp_path: Path):
    text = SPEC.read_text()
    bad = text.replace('references = "opendata.activities"', 'references = "example.activities"') \
              .replace('references = "opendata.athletes"', 'references = "Open-Data.athletes"')
    (tmp_path / "dataset.toml").write_text(bad)
    with pytest.raises(C.SpecError) as e:
        C.load_dataset_spec(tmp_path / "dataset.toml")
    assert "without the dataset name" in str(e.value) and "`<dataset>.<table>`" in str(e.value)
    assert C.load_dataset_spec(SPEC).other_datasets == ["opendata"]


def test_the_card_has_no_placeholders_left_and_a_data_dictionary(example: Path):
    card = (example / "README.md").read_text()
    assert "{{" not in card
    assert "### `timeseries`" in card and "On a 1 s grid" in card


def test_render_card_refuses_missing_and_unused_values():
    spec = C.load_dataset_spec(SPEC)
    with pytest.raises(ValueError, match="without a value"):
        C.render_card("{{nope}}", spec, {})
    with pytest.raises(ValueError, match="doesn't use"):
        C.render_card("{{repo_id}}", spec, {"extra": "x"})
    assert C.render_card("{{repo_id}}", spec, {}) == "GoldenCheetah/example"
