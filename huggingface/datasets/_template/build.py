# /// script
# requires-python = ">=3.11"
# dependencies = ["polars>=1.20", "pyarrow>=21"]
# ///
"""Build the example dataset declared in dataset.toml from a local copy of opendata.

    uv run datasets/_template/build.py <opendata folder> <output folder>
    uv run tools/validate.py datasets/_template/dataset.toml <output folder> \\
        --against datasets/opendata/dataset.toml <opendata folder>

A template: it shows how a dataset is built with the shared tooling. Tables are written with
conventions.write_table / TableWriter (the writer settings, file metadata and atomic writes come
with them), the card is rendered from README.md.tmpl and dataset.toml, and memory stays bounded by
processing one activity at a time. Replace the transformation with your own.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))

import polars as pl  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

import conventions as C  # noqa: E402

SPEC = C.load_dataset_spec(HERE / "dataset.toml")


def iter_activities(path: Path, columns: list[str]) -> Iterator[tuple[str, pl.DataFrame]]:
    """(activity_id, rows) for each activity in a time series file, reading it in batches.

    The conventions keep an activity's rows contiguous, so an activity is complete when the next
    one starts: memory is bounded by the largest activity, not by the athlete."""
    pending: list[pl.DataFrame] = []
    for batch in pq.ParquetFile(path).iter_batches(batch_size=65_536, columns=columns):
        df = pl.from_arrow(batch)
        while df.height:
            aid = df["activity_id"][0]
            n = df.height if (df["activity_id"] == aid).all() else int((df["activity_id"] != aid).arg_true()[0])
            if pending and pending[0]["activity_id"][0] != aid:
                yield pending[0]["activity_id"][0], pl.concat(pending)
                pending = []
            pending.append(df.head(n))
            df = df.slice(n)
    if pending:
        yield pending[0]["activity_id"][0], pl.concat(pending)


def resample(rows: pl.DataFrame) -> pl.DataFrame:
    """One row per whole second from the first to the last recorded second: the first sample
    recorded in that second (in recorded order), null where there was none."""
    firsts = (rows.filter(pl.col("time_s").is_finite() & (pl.col("time_s") >= 0))
              .with_columns(second=pl.col("time_s").floor().cast(pl.Int64))
              .group_by("second", maintain_order=True).first())
    if firsts.height == 0:
        return firsts.clear()
    grid = pl.DataFrame({"second": pl.int_range(firsts["second"].min(), firsts["second"].max() + 1, eager=True)})
    return (grid.join(firsts.drop("time_s"), on="second", how="left")
            .with_columns(athlete_id=pl.lit(rows["athlete_id"][0]), activity_id=pl.lit(rows["activity_id"][0]),
                          time_s=pl.col("second").cast(pl.Float32))
            .select([c.name for c in SPEC.tables["timeseries"].columns]))


def build(opendata: Path, out: Path) -> None:
    if out.exists() and any(out.iterdir()):
        raise SystemExit(f"{out} is not empty; build into a new folder")
    ts = SPEC.tables["timeseries"]
    durations: list[dict] = []
    for path in sorted(opendata.glob(ts.glob)):  # one opendata file per athlete, in athlete order
        athlete_id = path.parent.name.split("=", 1)[1]
        target = out / C.partition_file("timeseries", "athlete_id", athlete_id)
        target.parent.mkdir(parents=True, exist_ok=True)
        with C.TableWriter(target, ts.arrow_schema(), ts.profile, dataset=SPEC.name,
                           table_name="timeseries", encodings=ts.encodings) as w:
            for aid, rows in iter_activities(path, ["athlete_id", "activity_id", "time_s", "power_w", "hr_bpm"]):
                grid = resample(rows)
                if grid.height:
                    w.write(grid.to_arrow().cast(w.schema))
                    durations.append({"athlete_id": athlete_id, "activity_id": aid, "duration_s": float(grid.height)})
        if w.rows == 0:
            target.unlink()
            target.parent.rmdir()

    durations_df = pl.DataFrame(durations, schema={"athlete_id": pl.String, "activity_id": pl.String,
                                                   "duration_s": pl.Float32})
    activities = (pl.read_parquet(opendata / C.table_glob("activities", None),
                                  columns=["athlete_id", "activity_id", "start_time_utc", "sport"])
                  .join(durations_df, on=["athlete_id", "activity_id"], how="inner")
                  .sort("athlete_id", "activity_id"))
    athletes = (pl.read_parquet(opendata / C.table_glob("athletes", None), columns=["athlete_id", "gender", "yob"])
                .filter(pl.col("athlete_id").is_in(activities["athlete_id"].unique().implode()))
                .sort("athlete_id"))
    for name, df in (("activities", activities), ("athletes", athletes)):
        t = SPEC.tables[name]
        C.write_table(df.to_arrow().cast(t.arrow_schema()), out / "data" / name / f"{name}.parquet", t.profile,
                      dataset=SPEC.name, table_name=name, encodings=t.encodings)

    n_rows = int(activities["duration_s"].sum())
    card = C.render_card((HERE / "README.md.tmpl").read_text(), SPEC, {
        "n_athletes": f"{athletes.height:,}", "n_activities": f"{activities.height:,}",
        "size_category": C.size_category(n_rows)})
    (out / "README.md").write_text(card)
    print(f"{athletes.height:,} athletes, {activities.height:,} activities, {n_rows:,} samples -> {out}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit(__doc__)
    build(Path(sys.argv[1]), Path(sys.argv[2]))
