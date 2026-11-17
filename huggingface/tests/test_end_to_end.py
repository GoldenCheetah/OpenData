"""convert -> check -> validate on made-up zips, plus the guarantees around them."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import polars as pl
import pytest

import conventions as C
import migrate
import validate
from conftest import A1, A2, A3, basic_athlete, make_zip, ride, write_workdir


def test_convert_produces_a_dataset_that_passes_validation(built: Path):
    repo = built / "build" / "opendata"
    assert validate.validate(migrate.SPEC_PATH, repo, jobs=2, max_files=None) == 0


def test_check_passes(built: Path):
    assert migrate.stage_check(built, jobs=2, text_fraction=1.0, seed=0) == 0


def test_tables_have_the_expected_content(built: Path):
    data = built / "build" / "opendata" / "data"
    athletes = pl.read_parquet(data / "athletes" / "*.parquet")
    acts = pl.read_parquet(data / "activities" / "*.parquet")
    ts = pl.read_parquet(data / "timeseries" / "*" / "*.parquet", hive_partitioning=False)
    exc = pl.read_parquet(data / "timeseries_exceptions" / "*.parquet")
    assert athletes.height == 3                                    # including the empty athlete
    assert athletes.filter(pl.col("athlete_id") == A3)["n_activities"].item() == 0
    assert acts.height == 8 and ts.height == 12 and exc.height == 2
    assert not (data / "timeseries" / f"athlete_id={A3}").exists()  # no samples, no file
    assert (built / "build" / "opendata" / "README.md").exists()


def test_converting_again_changes_nothing(built: Path):
    repo = built / "build" / "opendata"
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in repo.rglob("*.parquet")}
    assert migrate.stage_convert(built, jobs=2, athletes=None, athletes_from=None, allow_errors=False) == 0
    after = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in repo.rglob("*.parquet")}
    assert before == after


def test_athletes_converted_by_an_older_converter_are_converted_again(built: Path, capsys):
    mf = built / "build" / "manifest.jsonl"
    old = [{**json.loads(line), "converter": "0.0.1"} for line in mf.read_text().splitlines()]
    mf.write_text("".join(json.dumps(r) + "\n" for r in old))
    assert migrate.stage_convert(built, jobs=1, athletes=None, athletes_from=None, allow_errors=False) == 0
    assert "0 athletes up to date, 3 to convert" in capsys.readouterr().out
    manifest = migrate.load_manifest(mf)
    assert {m["converter"] for m in manifest.values()} == {migrate.gcod.CONVERTER_VERSION}


def test_an_unknown_quirk_stops_before_the_metadata_tables(tmp_path: Path, zips: Path):
    workdir = tmp_path / "work"
    bad = make_zip(zips / f"{A2}.zip", [], {"2019_01_01_10_00_00.csv": "a,b\n1,2\n"})
    write_workdir(workdir, [basic_athlete(zips, A1), bad])
    assert migrate.stage_convert(workdir, jobs=1, athletes=None, athletes_from=None, allow_errors=False) == 1
    assert not (workdir / "build" / "opendata" / "data" / "activities").exists()
    # ... and --allow-errors leaves that athlete out
    assert migrate.stage_convert(workdir, jobs=1, athletes=None, athletes_from=None, allow_errors=True) == 0
    athletes = pl.read_parquet(workdir / "build" / "opendata" / "data" / "athletes" / "*.parquet")
    assert athletes["athlete_id"].to_list() == [A1]


def test_check_notices_a_tampered_time_series_file(built: Path):
    path = built / "build" / "opendata" / "data" / "timeseries" / f"athlete_id={A1}" / "part.parquet"
    df = pl.read_parquet(path, hive_partitioning=False)
    df = df.with_columns(pl.when(pl.int_range(pl.len()) == 0).then(999.0).otherwise(pl.col("power_w"))
                         .cast(pl.Float32).alias("power_w"))
    C.write_table(df.to_arrow(), path, "timeseries", dataset="opendata", table_name="timeseries")
    assert migrate.stage_check(built, jobs=1, text_fraction=0.0, seed=0) == 1


def test_validator_catches_a_null_key(built: Path, tmp_path: Path):
    data = built / "build" / "opendata" / "data" / "activities"
    part = sorted(data.glob("*.parquet"))[0]
    df = pl.read_parquet(part)
    first_row = pl.int_range(pl.len()) == 0
    df = df.with_columns(pl.when(first_row).then(None).otherwise(pl.col("activity_id")).alias("activity_id"))
    C.write_table(df.to_arrow(), part, "table", dataset="opendata", table_name="activities")
    assert validate.validate(migrate.SPEC_PATH, built / "build" / "opendata", jobs=1, max_files=None) == 1


def test_writer_output_is_identical_whether_written_whole_or_in_pieces(tmp_path: Path):
    import pyarrow as pa

    n = 300_000
    t = pa.table({"athlete_id": [A1] * n, "activity_id": [f"2019_01_01_10_{i // 6000:02d}_00" for i in range(n)],
                  "time_s": pa.array(range(n), pa.float32())})
    C.write_table(t, tmp_path / "whole.parquet", "timeseries", dataset="t", table_name="timeseries")
    with C.TableWriter(tmp_path / "pieces.parquet", t.schema, "timeseries", dataset="t", table_name="timeseries") as w:
        for off in range(0, n, 7_000):
            w.write(t.slice(off, 7_000))
    assert (tmp_path / "whole.parquet").read_bytes() == (tmp_path / "pieces.parquet").read_bytes()


def test_a_failed_write_leaves_no_file(tmp_path: Path):
    import pyarrow as pa

    t = pa.table({"x": [1.0]})
    with pytest.raises(RuntimeError):
        with C.TableWriter(tmp_path / "f.parquet", t.schema, "table", dataset="t", table_name="t") as w:
            w.write(t)
            raise RuntimeError
    assert list(tmp_path.iterdir()) == []


def test_a_json_problem_is_reported_before_anything_is_written(tmp_path: Path, zips: Path):
    workdir = tmp_path / "work"
    # a metric value of a kind the converter doesn't know (an object) is only seen in the JSON pass
    dup = make_zip(zips / f"{A2}.zip", [ride("2019/03/02 07:00:00 UTC", metrics={"total_work": {"x": 1}})], {})
    write_workdir(workdir, [basic_athlete(zips, A1), dup])
    assert migrate.stage_convert(workdir, jobs=1, athletes=None, athletes_from=None, allow_errors=False) == 1
    data = workdir / "build" / "opendata" / "data"
    assert not (data / "activities").exists() and not list(data.glob("*.tmp"))
    assert migrate.stage_convert(workdir, jobs=1, athletes=None, athletes_from=None, allow_errors=True) == 0
    assert pl.read_parquet(data / "athletes" / "*.parquet")["athlete_id"].to_list() == [A1]
    assert not list(data.glob("*.tmp"))


# --- upload (everything but the network) ---------------------------------------------
def test_upload_refuses_without_a_passing_check(built: Path):
    with pytest.raises(SystemExit, match="run `check` first"):
        migrate.checked_files(migrate.Build(built))


def test_upload_refuses_when_the_folder_changed_after_the_check(built: Path):
    assert migrate.stage_check(built, jobs=1, text_fraction=0.0, seed=0) == 0
    b = migrate.Build(built)
    assert migrate.checked_files(b)  # unchanged: fine
    (b.repo / "README.md").write_text((b.repo / "README.md").read_text() + "\n")
    with pytest.raises(SystemExit, match="changed since it was checked"):
        migrate.checked_files(b)


def test_a_failed_check_removes_the_record_of_an_earlier_pass(built: Path):
    assert migrate.stage_check(built, jobs=1, text_fraction=0.0, seed=0) == 0
    path = built / "build" / "opendata" / "data" / "timeseries" / f"athlete_id={A1}" / "part.parquet"
    df = pl.read_parquet(path, hive_partitioning=False).with_columns(pl.col("power_w") + 1)
    C.write_table(df.to_arrow(), path, "timeseries", dataset="opendata", table_name="timeseries")
    assert migrate.stage_check(built, jobs=1, text_fraction=0.0, seed=0) == 1
    assert not migrate.Build(built).check_record.exists()


def test_the_upload_folder_is_the_checked_folder_with_the_card_for_the_target_repo(built: Path):
    assert migrate.stage_check(built, jobs=1, text_fraction=0.0, seed=0) == 0
    b = migrate.Build(built)
    files = migrate.checked_files(b)
    folder = migrate.stage_upload_folder(b, files, "someone/opendata-test")
    assert set(migrate.folder_files(folder)) == set(files)
    part = f"data/timeseries/athlete_id={A1}/part.parquet"
    assert (folder / part).stat().st_ino == (b.repo / part).stat().st_ino  # a hard link, not a copy
    card = (folder / "README.md").read_text()
    assert "hf://datasets/someone/opendata-test/data/" in card
    assert f'"{migrate.SPEC.repo_id}"' not in card and f"datasets/{migrate.SPEC.repo_id}/" not in card
    assert card.replace("someone/opendata-test", migrate.SPEC.repo_id) == (b.repo / "README.md").read_text()


def test_remote_differences():
    from types import SimpleNamespace as NS

    readme = b"# card\n"
    expected = {"README.md": {"size": 7, "sha256": hashlib.sha256(readme).hexdigest(),
                              "git_sha1": migrate.git_blob_sha1(readme)},
                "data/a.parquet": {"size": 10, "sha256": "aa"}, "data/b.parquet": {"size": 20, "sha256": "bb"}}
    lfs = lambda sha, size: NS(sha256=sha, size=size)  # noqa: E731
    good = [NS(path=".gitattributes", size=1, blob_id="x", lfs=None),
            NS(path="data", tree_id="t"),  # a folder
            NS(path="README.md", size=7, blob_id=migrate.git_blob_sha1(readme), lfs=None),
            NS(path="data/a.parquet", size=10, blob_id="p", lfs=lfs("aa", 10)),
            NS(path="data/b.parquet", size=20, blob_id="q", lfs=lfs("bb", 20))]
    assert migrate.remote_differences(expected, good) == []
    bad = [good[2], NS(path="data/a.parquet", size=10, blob_id="p", lfs=lfs("ab", 10)),
           NS(path="data/old.parquet", size=5, blob_id="r", lfs=lfs("cc", 5))]
    assert migrate.remote_differences(expected, bad) == [
        "data/a.parquet: sha256 differs", "data/old.parquet: on the Hub but not in the dataset folder",
        "data/b.parquet: missing on the Hub"]
    # git's blob id of a small file, as the Hub reports it
    assert migrate.git_blob_sha1(b"hello\n") == "ce013625030ba8dba906f756967f9e9ca394464a"


def test_every_file_the_card_points_at_exists(built: Path):
    import re

    repo = built / "build" / "opendata"
    card = re.sub(r'"\s*\n\s*"', "", (repo / "README.md").read_text())  # join "..." "..." literals
    paths = re.findall(r"hf://datasets/[^/]+/[^/]+/([^\"']+)", card)
    assert len(paths) >= 4 and any("athlete_id=" in p for p in paths)
    for p in paths:
        assert list(repo.glob(p)), f"the card shows {p}, which matches no file"
