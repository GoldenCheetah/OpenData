# /// script
# requires-python = ">=3.11"
# dependencies = ["polars", "numpy"]
# ///
"""How much information is really in the CSV numbers?

Hypothesis: GoldenCheetah writes CSV values with QString::number(x) (printf "%g",
precision 6), so every value has at most 6 significant digits. If so, Float32 can hold
every value *exactly at the text level*: IEEE binary32 guarantees that any decimal with
<= 6 significant digits survives decimal -> float32 -> "%.6g" unchanged (FLT_DIG = 6),
as long as it lies in the normal range (~1.2e-38 .. 3.4e38).

Checks per column, over ALL sample athletes, on the raw CSV text:
  1. significant-digit census (mantissa digits without sign/point/exponent, leading and
     trailing zeros stripped)
  2. exhaustive float32 round trip on every distinct value:
        float(f"{float32(x):.6g}") == x      (x = float64 parse of the text)
     plus the stricter check for values with 7+ significant digits.
  3. which values (if any) break it, with examples and the files they come from.

Output: data/interim/09_census.parquet, reports/09_precision_census.md
"""

import sys
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "lib"))

import numpy as np
import polars as pl

import gcod

COLS = gcod.TS_COLUMNS


def sig_digits(col: str) -> pl.Expr:
    m = pl.col(col).str.replace(r"[eE].*$", "").str.replace_all(r"[-+.]", "")
    return m.str.strip_chars_start("0").str.strip_chars_end("0").str.len_chars()


def census_zip(path: Path) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Returns (sig-digit histogram per column, distinct values per column with sig digits)."""
    hists, uniq = [], []
    with gcod.open_athlete_zip(path) as az:
        frames = []
        for name, raw in az.iter_activities():
            df = pl.read_csv(raw, infer_schema=False)
            frames.append(df.with_columns(file=pl.lit(gcod.activity_stem(name))))
    if not frames:
        return pl.DataFrame(), pl.DataFrame()
    df = pl.concat(frames, how="diagonal")
    for c in COLS:
        s = df.select(pl.col(c), "file").drop_nulls(c).with_columns(sig=sig_digits(c))
        hists.append(s.group_by("sig").agg(n=pl.len()).with_columns(column=pl.lit(c)))
        u = s.group_by(c).agg(sig=pl.col("sig").first(), file=pl.col("file").first()).rename({c: "text"})
        uniq.append(u.with_columns(column=pl.lit(c), athlete_id=pl.lit(az.athlete_id)))
    return pl.concat(hists), pl.concat(uniq)


def f32_roundtrip_ok(values: np.ndarray) -> np.ndarray:
    """float(f"{float32(x):.6g}") == x, element-wise (exact Python formatting)."""
    f32 = values.astype(np.float32)
    return np.fromiter(
        (float(f"{a:.6g}") == b for a, b in zip(f32.tolist(), values.tolist())), dtype=bool, count=len(values)
    )


def main() -> None:
    zips = gcod.list_local_zips()
    hists, uniqs = [], []
    with ProcessPoolExecutor() as ex:
        for i, (h, u) in enumerate(ex.map(census_zip, zips, chunksize=1), 1):
            if h.height:
                hists.append(h)
                uniqs.append(u)
            if i % 20 == 0:
                print(f"  {i}/{len(zips)}")
    hist = pl.concat(hists).group_by("column", "sig").agg(pl.col("n").sum()).sort("column", "sig")
    uniq = pl.concat(uniqs).with_columns(value=pl.col("text").cast(pl.Float64))
    # distinct (column, value) pairs for the round-trip test
    dv = uniq.group_by("column", "value").agg(
        sig=pl.col("sig").max(), text=pl.col("text").first(), file=pl.col("file").first(),
        athlete_id=pl.col("athlete_id").first(),
    )
    print(f"testing float32 round trip on {dv.height:,} distinct values")
    ok = f32_roundtrip_ok(dv["value"].to_numpy())
    dv = dv.with_columns(f32_ok=pl.Series(ok))
    gcod.INTERIM_DIR.mkdir(parents=True, exist_ok=True)
    dv.write_parquet(gcod.INTERIM_DIR / "09_census.parquet")

    total = hist.group_by("column").agg(total=pl.col("n").sum())
    summary = (
        hist.join(total, on="column")
        .group_by("column")
        .agg(
            values=pl.col("total").first(),
            max_sig=pl.col("sig").max(),
            le6_pct=(pl.col("n").filter(pl.col("sig") <= 6).sum() / pl.col("total").first() * 100),
            gt6=pl.col("n").filter(pl.col("sig") > 6).sum(),
        )
        .sort("column")
    )
    rt = dv.group_by("column").agg(
        distinct=pl.len(), f32_fail=(~pl.col("f32_ok")).sum(),
        f32_fail_le6=((~pl.col("f32_ok")) & (pl.col("sig") <= 6)).sum(),
    ).sort("column")
    bad = dv.filter(~pl.col("f32_ok")).sort("column", "value")
    bad_by_athlete = bad.group_by("athlete_id", "column").agg(n=pl.len(), example=pl.col("text").first()).sort("n", descending=True)

    md = [
        "# 09 · Precision census: how many significant digits does GCOD really have?\n",
        f"All {len(zips)} sample athletes, every value of every CSV.\n",
        "## Significant digits per value\n",
        gcod.md_table(summary.to_dicts()),
        "\nFull histogram:\n",
        gcod.md_table(hist.pivot(on="column", index="sig", values="n").sort("sig").to_dicts()),
        "\n## Float32 round trip on every distinct value: `float(f'{float32(x):.6g}') == x`\n",
        gcod.md_table(rt.to_dicts()),
        "\n## Values that do not survive float32\n",
        gcod.md_table(bad.head(30).select("column", "text", "sig", "athlete_id", "file").to_dicts()),
        "\nBy athlete:\n",
        gcod.md_table(bad_by_athlete.head(20).to_dicts()),
    ]
    p = gcod.write_report("09_precision_census", "\n".join(md))
    print(f"wrote {p}")


if __name__ == "__main__":
    main()
