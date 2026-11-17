"""Shared fixtures: small, made-up GCOD athlete zips with the quirks found in the real release.

Run from huggingface/:

    uv run --with-requirements tests/requirements.txt pytest tests -q
"""

from __future__ import annotations

import hashlib
import json
import sys
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "datasets" / "opendata"), str(ROOT / "tools")]

HEADER = "secs,km,power,hr,cad,alt\n"


def csv(rows: list[tuple]) -> str:
    """CSV text in GoldenCheetah's format; None becomes an empty field."""
    return HEADER + "".join(",".join("" if v is None else str(v) for v in r) + "\n" for r in rows)


def ride(date: str, sport: str = "Bike", metrics: dict | None = None, **extra) -> dict:
    return {"date": date, "data": "TDSPHC-A-------", "sport": sport, "METRICS": metrics or {}, **extra}


def make_zip(path: Path, rides: list[dict], csvs: dict[str, str], athlete: dict | None = None,
             json_text: str | None = None, extra_members: dict[str, str] | None = None) -> Path:
    """An athlete zip like those on OSF: {uuid}.json plus one CSV per activity."""
    aid = path.stem
    meta = {"VERSION": "1.9", "ATHLETE": athlete or {"gender": "M", "yob": "1980", "id": "{" + aid + "}"},
            "RIDES": rides}
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{aid}.json", json_text if json_text is not None else json.dumps(meta))
        for name, text in csvs.items():
            zf.writestr(name, text)
        for name, text in (extra_members or {}).items():
            zf.writestr(name, text)
    return path


# Athlete ids used by the fixtures (valid UUIDs)
A1 = "00000000-0000-4000-8000-000000000001"
A2 = "00000000-0000-4000-8000-000000000002"
A3 = "00000000-0000-4000-8000-000000000003"


@pytest.fixture
def zips(tmp_path: Path) -> Path:
    d = tmp_path / "zips"
    d.mkdir()
    return d


def basic_athlete(d: Path, aid: str = A1) -> Path:
    """Two rides with CSVs (local time = UTC+1), one manual entry without CSV, one CSV without ride,
    a sub-normal junk value, fractional seconds, a .fit-named CSV and all three metric forms."""
    rides = [
        ride("2019/03/02 07:00:00 UTC", metrics={"total_work": "123.4", "average_power": ["210.5", "3600"],
                                                  "power_mmp": ["900", "800"]}),
        ride("2019/03/03 08:30:15 UTC", metrics={"total_work": "99"}, XDATA={"ROUTE": {"x": 1}}),
        ride("2019/03/05 18:00:00 UTC", sport="Swim", metrics={"total_work": "5"}),  # manual: no CSV
    ]
    csvs = {
        "2019_03_02_08_00_00.csv": csv([(0, 0, 100, 120, 80, 10.5), (1, 0.00540637, 150.5, 121, 81, 10.6),
                                        (2.52, 0.0123, 4.94066e-324, None, 82, 10.7)]),
        "2019_03_03_09_30_15.fit": csv([(0, 0, 200, 140, 90, 5), (1, 0.008, 210, 141, 91, 5)]),
        "2019_03_04_10_00_00.csv": csv([(0, 0, 50, 100, 60, 1)]),   # no JSON ride links to this one
    }
    return make_zip(d / f"{aid}.zip", rides, csvs)


def write_workdir(workdir: Path, zip_paths: list[Path]) -> None:
    """A migrate.py work directory with these zips mirrored and verified (no network)."""
    import polars as pl

    athletes = workdir / "osf" / "athletes"
    athletes.mkdir(parents=True)
    rows, verified = [], []
    for z in zip_paths:
        target = athletes / z.name
        target.write_bytes(z.read_bytes())
        sha = hashlib.sha256(target.read_bytes()).hexdigest()
        st = target.stat()
        rows.append({"osf_file_id": f"id-{z.stem[-4:]}", "path": z.name, "name": z.name, "size": st.st_size,
                     "md5": None, "sha256": sha, "date_created": datetime(2019, 1, 1, tzinfo=UTC),
                     "date_modified": datetime(2019, 1, 1), "version": 1, "download_url": "https://example.invalid",
                     "is_athlete": True, "athlete_id": z.stem})
        verified.append(json.dumps({"athlete_id": z.stem, "sha256": sha, "size": st.st_size,
                                    "mtime_ns": st.st_mtime_ns}))
    pl.DataFrame(rows).write_parquet(workdir / "osf" / "listing.parquet")
    (workdir / "osf" / "verified.jsonl").write_text("\n".join(verified) + "\n")


@pytest.fixture
def built(tmp_path: Path, zips: Path) -> Path:
    """A migrate.py work directory after `convert` of two basic athletes and an empty one."""
    import migrate

    workdir = tmp_path / "work"
    paths = [basic_athlete(zips, A1), basic_athlete(zips, A2), make_zip(zips / f"{A3}.zip", [], {})]
    write_workdir(workdir, paths)
    assert migrate.stage_convert(workdir, jobs=2, athletes=None, athletes_from=None, allow_errors=False) == 0
    return workdir
