# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx>=0.27", "huggingface_hub>=2.4", "polars>=1.20", "pyarrow>=21", "pyyaml>=6"]
# ///
"""GoldenCheetah OpenData (GCOD): migrate the OSF release to Hugging Face.

Every stage is resumable and safe to re-run; the files in the work directory are the state.

  listing   fetch the OSF file listing (ids, sizes, sha256)  -> <workdir>/osf/listing.parquet
  mirror    download every athlete zip and verify its sha256 -> <workdir>/osf/athletes/<uuid>.zip
  convert   zips -> the dataset folder to upload              -> <workdir>/build/opendata/
  check     validate it, and compare every time series value with the original CSVs
                                                             -> <workdir>/build/check.json
  upload    upload exactly the checked dataset folder to a Hugging Face dataset repo, and
            verify every file on the Hub against it         -> <workdir>/build/uploads.jsonl

Usage (from huggingface/):

  uv run datasets/opendata/migrate.py listing
  uv run datasets/opendata/migrate.py mirror [--workers 3]
  uv run datasets/opendata/migrate.py convert [--jobs N] [--athletes UUID ... | --athletes-from FILE]
  uv run datasets/opendata/migrate.py check [--jobs N] [--text-fraction 0.05]
  uv run datasets/opendata/migrate.py upload --repo GoldenCheetah/opendata-test

`upload` needs `hf auth login` (write access to the repo), refuses unless `check` passed on the
dataset folder as it is now, and creates the repo as private if it doesn't exist. The repo then
holds exactly the dataset folder: remote files that aren't in it are deleted. Interrupted uploads
resume when run again.

`convert` converts an athlete again only when its zip or the converter version changed. It stops
before writing the metadata tables if any athlete can't be converted (unknown quirks fail loudly);
`--allow-errors` leaves those athletes out instead.

The full mirror is 6,613 zips / 115.6 GB: ~19 h at the ~1.6 MB/s OSF gives 3 workers. For an
unattended run on macOS that keeps the machine awake (caffeinate: no idle/system sleep while on
AC power), restarts after a crash, and is detached from the terminal:

  nohup caffeinate -ims bash -c 'while true; do
      uv run -q datasets/opendata/migrate.py mirror; s=$?
      case $s in 0|1|130) exit $s;; esac       # done, gave up, or stopped on purpose
      echo "migrate exited with $s; restarting in 60 s"; sleep 60
  done' >> data/osf/mirror.log 2>&1 < /dev/null &

Closing the lid still sleeps a MacBook unless it runs in clamshell mode or
`sudo pmset -a disablesleep 1` is set (undo with `... 0` afterwards).

Progress: `tail -f data/osf/mirror.log`, or `data/osf/mirror_status.json` (has the pid).
Stop with Ctrl-C or `kill <pid>`: partial downloads are kept and resumed on the next run.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import shutil
import signal
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import httpx
import polars as pl

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.parents[1])]  # gcod.py next to this file; conventions.py in huggingface/

import conventions as C  # noqa: E402
import gcod  # noqa: E402
DEFAULT_WORKDIR = HERE.parents[1] / "data"  # huggingface/data (git-ignored)

OSF_NODE = "6hfpz"
OSF_API = "https://api.osf.io/v2"
USER_AGENT = "goldencheetah-opendata-migrate (+https://github.com/GoldenCheetah/OpenData)"
ATHLETE_ZIP_RE = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.zip$"

# OSF answers 403/429/5xx intermittently under load; all of these are retried.
RETRY_STATUS = {403, 408, 425, 429, 500, 502, 503, 504}
TIMEOUT = httpx.Timeout(connect=30, read=120, write=30, pool=None)


def log(msg: str) -> None:
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  {msg}", flush=True)


def human_bytes(n: float) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(n) < 1000:
            return f"{n:.1f} {unit}"
        n /= 1000
    return f"{n:.1f} PB"


def human_duration(s: float) -> str:
    s = int(s)
    return f"{s // 3600}h{s % 3600 // 60:02d}m" if s >= 3600 else f"{s // 60}m{s % 60:02d}s"


class Stop(Exception):
    """Raised inside workers once a shutdown was requested."""


STOP = threading.Event()


def backoff_sleep(attempt: int, retry_after: str | None = None, cap: float = 300) -> None:
    """Exponential backoff with jitter; honours Retry-After. Returns early on shutdown."""
    delay = min(cap, 5 * 2**attempt) * random.uniform(0.7, 1.3)
    if retry_after and retry_after.isdigit():
        delay = max(delay, min(float(retry_after), 900))
    if STOP.wait(delay):
        raise Stop


# --- listing ------------------------------------------------------------------------
def fetch_listing(client: httpx.Client) -> pl.DataFrame:
    """All files of OSF node 6hfpz (walks folders, follows pagination, retries)."""
    rows: list[dict] = []
    pending = [(f"{OSF_API}/nodes/{OSF_NODE}/files/osfstorage/?page[size]=100", "")]
    while pending:
        url, prefix = pending.pop()
        while url:
            d = _get_json(client, url)
            for x in d["data"]:
                a = x["attributes"]
                if a["kind"] == "folder":
                    sub = x["relationships"]["files"]["links"]["related"]["href"]
                    pending.append((sub + "?page[size]=100", prefix + a["name"] + "/"))
                    continue
                h = a.get("extra", {}).get("hashes", {})
                rows.append({
                    "osf_file_id": x["id"],
                    "path": prefix + a["name"],
                    "name": a["name"],
                    "size": a["size"],
                    "md5": h.get("md5"),
                    "sha256": h.get("sha256"),
                    "date_created": a["date_created"],
                    "date_modified": a["date_modified"],
                    "version": a.get("current_version"),
                    "download_url": x["links"]["download"],
                })
            url = d["links"].get("next")
            print(f"  listing: {len(rows)} files ...", end="\r", file=sys.stderr, flush=True)
    print(file=sys.stderr)
    return (
        pl.DataFrame(rows)
        .with_columns(
            pl.col("date_created").str.to_datetime(time_zone="UTC", strict=False),
            pl.col("date_modified").str.to_datetime(strict=False),
            is_athlete=pl.col("name").str.contains(ATHLETE_ZIP_RE),
        )
        .with_columns(athlete_id=pl.when("is_athlete").then(pl.col("name").str.replace(r"\.zip$", "")))
        .sort("path")
    )


def _get_json(client: httpx.Client, url: str, attempts: int = 8) -> dict:
    for i in range(attempts):
        try:
            r = client.get(url)
            if r.status_code in RETRY_STATUS:
                raise httpx.HTTPStatusError(f"{r.status_code}", request=r.request, response=r)
            r.raise_for_status()
            return r.json()
        except (httpx.HTTPStatusError, httpx.TransportError) as e:
            if i == attempts - 1:
                raise
            log(f"listing: {e!r}, retrying")
            backoff_sleep(i)
    raise AssertionError


def check_listing(df: pl.DataFrame) -> None:
    """Fail loudly if the listing doesn't look like the GCOD release we designed for."""
    ath = df.filter("is_athlete")
    problems = []
    if ath.height < 6_000:
        problems.append(f"only {ath.height} athlete zips")
    if ath["athlete_id"].n_unique() != ath.height:
        problems.append("duplicate athlete ids")
    if ath["sha256"].null_count():
        problems.append(f"{ath['sha256'].null_count()} zips without sha256")
    if problems:
        raise SystemExit("listing check failed: " + "; ".join(problems))


def stage_listing(workdir: Path) -> pl.DataFrame:
    path = workdir / "osf" / "listing.parquet"
    with httpx.Client(timeout=TIMEOUT, follow_redirects=True, headers={"User-Agent": USER_AGENT}) as client:
        df = fetch_listing(client)
    check_listing(df)
    if path.exists():
        old = pl.read_parquet(path)
        changed = df.join(old.select("path", "sha256"), on="path", how="full", suffix="_old").filter(
            pl.col("sha256").ne_missing(pl.col("sha256_old"))
        )
        if changed.height:
            log(f"listing differs from the previous one in {changed.height} files "
                f"(e.g. {changed['path'].head(3).to_list()} {changed['path_old'].head(3).to_list()})")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    df.write_parquet(tmp)
    os.replace(tmp, path)
    ath = df.filter("is_athlete")
    log(f"listing: {df.height:,} files, {ath.height:,} athlete zips, {human_bytes(ath['size'].sum())} -> {path}")
    return df


# --- mirror -------------------------------------------------------------------------
@dataclass
class Target:
    athlete_id: str
    url: str
    size: int
    sha256: str


class Mirror:
    """Download athlete zips with sha256 verification.

    State: a zip in osf/athletes/ is complete only after its sha256 matched the listing; then
    (size, mtime) are appended to osf/verified.jsonl so later runs don't re-hash 115 GB.
    Interrupted downloads stay as <uuid>.zip.part and are resumed with an HTTP Range request.
    """

    def __init__(self, workdir: Path, workers: int):
        self.dir = workdir / "osf" / "athletes"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.verified_log = workdir / "osf" / "verified.jsonl"
        self.status_path = workdir / "osf" / "mirror_status.json"
        self.workers = workers
        self.lock = threading.Lock()
        self.session_bytes = 0
        self.session_start = time.monotonic()
        self.done_bytes = 0
        self.done_files = 0
        self.total_bytes = 0
        self.total_files = 0
        self.failed: dict[str, str] = {}

    # verified-state log
    def _load_verified(self) -> dict[str, dict]:
        out = {}
        if self.verified_log.exists():
            for line in self.verified_log.read_text().splitlines():
                try:
                    r = json.loads(line)
                    out[r["athlete_id"]] = r
                except (json.JSONDecodeError, KeyError):
                    continue  # a torn last line after a crash
        return out

    def _record_verified(self, t: Target, path: Path) -> None:
        st = path.stat()
        line = json.dumps({"athlete_id": t.athlete_id, "sha256": t.sha256, "size": st.st_size,
                           "mtime_ns": st.st_mtime_ns, "at": datetime.now(timezone.utc).isoformat()})
        with self.lock, self.verified_log.open("a") as f:
            f.write(line + "\n")
            f.flush()
            os.fsync(f.fileno())

    def is_complete(self, t: Target, verified: dict[str, dict]) -> bool:
        p = self.dir / f"{t.athlete_id}.zip"
        if not p.exists():
            return False
        st = p.stat()
        v = verified.get(t.athlete_id)
        if v and v["sha256"] == t.sha256 and v["size"] == st.st_size and v["mtime_ns"] == st.st_mtime_ns:
            return True
        if st.st_size == t.size and sha256_file(p) == t.sha256:  # e.g. from an earlier sample download
            self._record_verified(t, p)
            return True
        log(f"{t.athlete_id}: existing zip does not match the listing, re-downloading")
        p.unlink()
        return False

    # one file
    def download(self, client: httpx.Client, t: Target, attempts: int = 8) -> None:
        for i in range(attempts):
            if STOP.is_set():
                raise Stop
            try:
                self._download_once(client, t)
                return
            except httpx.HTTPStatusError as e:
                if i == attempts - 1 or e.response.status_code not in RETRY_STATUS:
                    raise  # permanent errors (404, 410, ...) are retried in the next pass only
                log(f"{t.athlete_id}: HTTP {e.response.status_code}, retry {i + 1}/{attempts - 1}")
                backoff_sleep(i, e.response.headers.get("Retry-After"))
            except (httpx.TransportError, ChecksumError, OSError) as e:
                if i == attempts - 1:
                    raise
                log(f"{t.athlete_id}: {type(e).__name__}: {e}, retry {i + 1}/{attempts - 1}")
                backoff_sleep(i)

    def _download_once(self, client: httpx.Client, t: Target) -> None:
        out = self.dir / f"{t.athlete_id}.zip"
        part = self.dir / f"{t.athlete_id}.zip.part"
        have = part.stat().st_size if part.exists() else 0
        if have > t.size:
            part.unlink()
            have = 0
        headers = {"Range": f"bytes={have}-"} if 0 < have < t.size else {}
        h = hashlib.sha256()
        with client.stream("GET", t.url, headers=headers) as r:
            if r.status_code >= 400:
                raise httpx.HTTPStatusError(f"HTTP {r.status_code}", request=r.request, response=r)
            if have and r.status_code == 206:
                log(f"{t.athlete_id}: resuming at {human_bytes(have)} of {human_bytes(t.size)}")
                with part.open("rb") as f:  # hash what we already have, then append
                    for chunk in iter(lambda: f.read(1 << 20), b""):
                        h.update(chunk)
                mode = "ab"
            else:
                have, mode = 0, "wb"  # server ignored Range (200): start over
            with part.open(mode) as f:
                for chunk in r.iter_bytes(1 << 20):
                    f.write(chunk)
                    h.update(chunk)
                    with self.lock:
                        self.session_bytes += len(chunk)
                    if STOP.is_set():
                        raise Stop  # the .part file is kept and resumed next time
                f.flush()
                os.fsync(f.fileno())
        size = part.stat().st_size
        if size != t.size or h.hexdigest() != t.sha256:
            part.unlink()
            raise ChecksumError(f"got {size} bytes, sha256 {h.hexdigest()[:12]}…; expected {t.size}, {t.sha256[:12]}…")
        os.replace(part, out)
        self._record_verified(t, out)

    # whole run
    def run(self, targets: list[Target], max_passes: int) -> int:
        verified = self._load_verified()
        log(f"checking {len(targets):,} zips already on disk ...")
        todo = [t for t in targets if not self.is_complete(t, verified)]
        self.total_files, self.total_bytes = len(targets), sum(t.size for t in targets)
        self.done_files = self.total_files - len(todo)
        self.done_bytes = self.total_bytes - sum(t.size for t in todo)
        log(f"{self.done_files:,}/{self.total_files:,} zips complete ({human_bytes(self.done_bytes)} of "
            f"{human_bytes(self.total_bytes)}); {len(todo):,} to download")
        check_disk_space(self.dir, sum(t.size for t in todo))

        for n in range(1, max_passes + 1):
            if not todo or STOP.is_set():
                break
            if n > 1:
                wait = min(3600, 300 * 2 ** (n - 2))
                log(f"pass {n}/{max_passes} in {human_duration(wait)}: retrying {len(todo)} failed zips")
                if STOP.wait(wait):
                    break
            self.failed = {}
            todo = self._pass(todo)
        self._write_status(final=True)
        if STOP.is_set():
            log(f"stopped; {self.total_files - self.done_files:,} zips left. Re-run to resume.")
            return 130
        if todo:
            log(f"FAILED: {len(todo)} zips still missing after {max_passes} passes:")
            for aid, err in sorted(self.failed.items()):
                log(f"  {aid}: {err}")
            return 1
        log(f"DONE: all {self.total_files:,} zips present and verified ({human_bytes(self.total_bytes)})")
        return 0

    def _pass(self, todo: list[Target]) -> list[Target]:
        failed: list[Target] = []
        with httpx.Client(timeout=TIMEOUT, follow_redirects=True, headers={"User-Agent": USER_AGENT},
                          limits=httpx.Limits(max_connections=self.workers)) as client, \
             ThreadPoolExecutor(self.workers) as ex:
            futs = {ex.submit(self.download, client, t): t for t in todo}
            last_status = 0.0
            for f in as_completed(futs):
                t = futs[f]
                try:
                    f.result()
                except Stop:
                    failed.append(t)
                    continue
                except Exception as e:  # noqa: BLE001 - keep going, retried in the next pass
                    self.failed[t.athlete_id] = f"{type(e).__name__}: {e}"
                    log(f"{t.athlete_id}: giving up for this pass ({type(e).__name__}: {e})")
                    failed.append(t)
                    continue
                self.done_files += 1
                self.done_bytes += t.size
                log(f"[{self.done_files:,}/{self.total_files:,}] {t.athlete_id} {human_bytes(t.size)}  {self._progress()}")
                if time.monotonic() - last_status > 30:
                    self._write_status()
                    last_status = time.monotonic()
        return failed

    def _progress(self) -> str:
        elapsed = time.monotonic() - self.session_start
        rate = self.session_bytes / elapsed if elapsed > 0 else 0
        left = self.total_bytes - self.done_bytes
        eta = human_duration(left / rate) if rate > 0 else "?"
        return f"{human_bytes(self.done_bytes)}/{human_bytes(self.total_bytes)}, {human_bytes(rate)}/s, ETA {eta}"

    def _write_status(self, final: bool = False) -> None:
        elapsed = time.monotonic() - self.session_start
        status = {
            "updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "pid": os.getpid(),
            "finished": final,
            "files_done": self.done_files, "files_total": self.total_files,
            "bytes_done": self.done_bytes, "bytes_total": self.total_bytes,
            "session_rate_bytes_per_s": round(self.session_bytes / elapsed) if elapsed > 0 else None,
            "failed": self.failed,
        }
        tmp = self.status_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(status, indent=2))
        os.replace(tmp, self.status_path)


class ChecksumError(Exception):
    pass


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def check_disk_space(path: Path, needed: int, margin: int = 10 * 10**9) -> None:
    free = shutil.disk_usage(path).free
    if free < needed + margin:
        raise SystemExit(f"not enough disk space: {human_bytes(free)} free, need {human_bytes(needed)} + 10 GB margin")
    log(f"disk: {human_bytes(free)} free, {human_bytes(needed)} needed")


def stage_mirror(workdir: Path, workers: int, max_passes: int, refresh_listing: bool,
                 athletes: list[str] | None = None) -> int:
    listing_path = workdir / "osf" / "listing.parquet"
    listing = stage_listing(workdir) if refresh_listing or not listing_path.exists() else pl.read_parquet(listing_path)
    check_listing(listing)
    targets = [
        Target(r["athlete_id"], r["download_url"], r["size"], r["sha256"])
        for r in listing.filter("is_athlete").sort("athlete_id").iter_rows(named=True)
    ]
    if athletes:
        unknown = set(athletes) - {t.athlete_id for t in targets}
        if unknown:
            raise SystemExit(f"not in the OSF listing: {sorted(unknown)}")
        targets = [t for t in targets if t.athlete_id in set(athletes)]
    return Mirror(workdir, workers).run(targets, max_passes)


# --- convert -------------------------------------------------------------------------
# Output: <workdir>/build/opendata/ (the folder that is uploaded: README.md + data/).
# State:  <workdir>/build/manifest.jsonl, one line per converted athlete (source sha256, converter
#         version, samples per activity, errors); <workdir>/build/staging/ for per-athlete
#         exceptions. An athlete is converted again only if its zip or the converter changed.
# Heavy stages use half the cores at low priority by default, so the machine stays usable. Memory
# per process is bounded (streaming per activity): about 1 GB at most, measured on the largest
# athlete in the release.
DEFAULT_JOBS = max(1, (os.cpu_count() or 2) // 2)
SPEC_PATH = HERE / "dataset.toml"
SPEC = C.load_dataset_spec(SPEC_PATH)
CARD_TEMPLATE = HERE / "README.md.tmpl"
ACTIVITIES_PART_ROWS = 300_000       # parts of `activities` (3 row groups, ~300 MB)
RAW_JSON_PART_BYTES = 256 * 10**6     # ~256 MB (uncompressed) parts of `raw_json`


@dataclass
class Build:
    workdir: Path

    @property
    def root(self) -> Path:
        return self.workdir / "build"

    @property
    def repo(self) -> Path:
        return self.root / "opendata"

    @property
    def staging(self) -> Path:
        return self.root / "staging"

    @property
    def manifest(self) -> Path:
        return self.root / "manifest.jsonl"

    @property
    def check_record(self) -> Path:
        return self.root / "check.json"

    @property
    def uploads(self) -> Path:
        return self.root / "uploads.jsonl"

    def zip_path(self, athlete_id: str) -> Path:
        return self.workdir / "osf" / "athletes" / f"{athlete_id}.zip"

    def ts_file(self, athlete_id: str) -> Path:
        return self.repo / C.partition_file("timeseries", "athlete_id", athlete_id)

    def exc_file(self, athlete_id: str) -> Path:
        return self.staging / "exceptions" / f"{athlete_id}.parquet"


def load_manifest(path: Path) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if path.exists():
        for line in path.read_text().splitlines():
            try:
                r = json.loads(line)
                out[r["athlete_id"]] = r  # last line wins
            except (json.JSONDecodeError, KeyError):
                continue
    return out


def ts_arrow_schema():
    import pyarrow as pa

    return pa.schema([(c, pa.string()) for c in ("athlete_id", "activity_id")] +
                     [(o, pa.float32()) for o in gcod.TS_COLUMNS.values()])


def convert_timeseries_worker(athlete_id: str, sha256: str, workdir: str) -> dict:
    """Time series of one athlete -> its partition file + staged exceptions. Runs in a subprocess.

    Streams one activity at a time into the file, so memory stays bounded by the largest activity
    whatever the athlete's size (the writer buffers at most one row group).
    """
    _low_priority()
    b = Build(Path(workdir))
    entry = {"athlete_id": athlete_id, "sha256": sha256, "converter": gcod.CONVERTER_VERSION}
    path = b.ts_file(athlete_id)
    writer = None
    samples: dict[str, int] = {}
    exceptions: list[pl.DataFrame] = []
    try:
        with gcod.open_athlete_zip(b.zip_path(athlete_id)) as az:
            for aid, src in gcod.iter_source_activities(az):
                out, exc = gcod.to_output_timeseries(src)
                samples[aid] = src.height
                if exc.height:
                    exceptions.append(exc)
                if out.height:
                    if writer is None:
                        writer = C.TableWriter(path, ts_arrow_schema(), "timeseries", dataset="opendata",
                                               table_name="timeseries", encodings=SPEC.tables["timeseries"].encodings)
                    writer.write(out.to_arrow())
        if writer is not None:
            writer.close()
        elif path.exists():
            shutil.rmtree(path.parent)
    except gcod.GcodError as e:
        if writer is not None:
            writer.abort()
        return {**entry, "status": "error", "error": str(e)}
    except BaseException:
        if writer is not None:
            writer.abort()
        raise
    exc_all = pl.concat(exceptions) if exceptions else pl.DataFrame(schema=gcod.EXCEPTIONS_SCHEMA)
    if exc_all.height:
        b.exc_file(athlete_id).parent.mkdir(parents=True, exist_ok=True)
        exc_all.write_parquet(b.exc_file(athlete_id))
    else:
        b.exc_file(athlete_id).unlink(missing_ok=True)
    return {**entry, "status": "ok", "rows": sum(samples.values()), "exceptions": exc_all.height, "samples": samples}


def _low_priority() -> None:
    """Run heavy work at low CPU priority so the machine stays usable."""
    try:
        if os.getpriority(os.PRIO_PROCESS, 0) < 10:
            os.nice(10)
    except OSError:
        pass


def metadata_forms_worker(athlete_id: str, workdir: str, samples: dict[str, int]) -> tuple[dict, str | None]:
    """(metric forms, error). Also builds the athlete's activities once with its own metrics, so
    that problems in the JSON surface here, before anything is written."""
    _low_priority()
    try:
        with gcod.open_athlete_zip(Build(Path(workdir)).zip_path(athlete_id)) as az:
            forms = gcod.metric_forms(az.meta or {})
            gcod.build_activities(az, gcod.metric_kinds(forms), samples)
            return {k: sorted(v) for k, v in forms.items()}, None
    except gcod.GcodError as e:
        return {}, str(e)


def metadata_worker(athlete_id: str, workdir: str, kinds: dict[str, str], samples: dict[str, int]):
    """(activities as Arrow, athletes row, original JSON text) for one athlete."""
    with gcod.open_athlete_zip(Build(Path(workdir)).zip_path(athlete_id)) as az:
        acts = gcod.build_activities(az, kinds, samples)
        n_ts = sum(1 for n in samples.values() if n > 0)
        return acts.to_arrow(), gcod.athlete_row(az, acts.height, n_ts), az.json_text


def _replace_dir(tmp: Path, final: Path) -> None:
    """Swap a freshly written table folder into place (old files never mix with new ones)."""
    old = final.with_name(final.name + ".old")
    if old.exists():
        shutil.rmtree(old)
    if final.exists():
        final.rename(old)
    tmp.rename(final)
    if old.exists():
        shutil.rmtree(old)


def select_athletes(workdir: Path, listing: pl.DataFrame, athletes: list[str] | None,
                    athletes_from: Path | None) -> pl.DataFrame:
    ath = listing.filter("is_athlete").sort("athlete_id")
    wanted = set(athletes or [])
    if athletes_from:
        if athletes_from.suffix == ".parquet":
            wanted |= set(pl.read_parquet(athletes_from)["athlete_id"].to_list())
        else:
            wanted |= {s.strip() for s in athletes_from.read_text().split() if s.strip()}
    if wanted:
        unknown = wanted - set(ath["athlete_id"])
        if unknown:
            raise SystemExit(f"not in the OSF listing: {sorted(unknown)[:5]} ({len(unknown)} total)")
        ath = ath.filter(pl.col("athlete_id").is_in(sorted(wanted)))
    verified = {json.loads(line)["athlete_id"] for line in (workdir / "osf" / "verified.jsonl").read_text().splitlines()
                if line.strip()} if (workdir / "osf" / "verified.jsonl").exists() else set()
    missing = [a for a in ath["athlete_id"] if a not in verified or not Build(workdir).zip_path(a).exists()]
    if missing:
        raise SystemExit(f"{len(missing)} selected zips are not mirrored yet (e.g. {missing[:3]}); "
                         f"run `mirror` first or select fewer athletes")
    return ath


def stage_convert(workdir: Path, jobs: int, athletes: list[str] | None, athletes_from: Path | None,
                  allow_errors: bool) -> int:
    b = Build(workdir)
    listing = pl.read_parquet(workdir / "osf" / "listing.parquet")
    sel = select_athletes(workdir, listing, athletes, athletes_from)
    ids = sel["athlete_id"].to_list()
    sha = dict(zip(sel["athlete_id"], sel["sha256"]))
    b.repo.mkdir(parents=True, exist_ok=True)
    log(f"convert: {len(ids):,} athletes -> {b.repo}")

    # 1. time series, per athlete, resumable
    manifest = load_manifest(b.manifest)
    todo = [a for a in ids
            if not (m := manifest.get(a)) or m["sha256"] != sha[a] or m["converter"] != gcod.CONVERTER_VERSION
            or m["status"] != "ok" or (m["rows"] and not b.ts_file(a).exists())]
    log(f"timeseries: {len(ids) - len(todo):,} athletes up to date, {len(todo):,} to convert")
    t0 = time.monotonic()
    with ProcessPoolExecutor(jobs) as ex, b.manifest.open("a") as mf:
        futs = {ex.submit(convert_timeseries_worker, a, sha[a], str(workdir)): a for a in todo}
        for i, f in enumerate(as_completed(futs), 1):
            if STOP.is_set():
                ex.shutdown(cancel_futures=True)
                log("stopped; re-run to resume")
                return 130
            r = f.result()
            mf.write(json.dumps(r) + "\n")
            mf.flush()
            manifest[r["athlete_id"]] = r
            if r["status"] != "ok":
                log(f"ERROR {r['athlete_id']}: {r['error']}")
            if i % 50 == 0 or i == len(todo):
                el = time.monotonic() - t0
                log(f"timeseries: {i:,}/{len(todo):,} athletes, {el / i:.1f} s/athlete, "
                    f"ETA {human_duration(el / i * (len(todo) - i))}")
    errors = {a: manifest[a]["error"] for a in ids if manifest[a]["status"] != "ok"}

    # 2. metric schema over all athletes, and a dry run of their metadata (before anything is written)
    log("metadata: collecting the metric schema and checking every JSON ...")
    forms: dict[str, set[str]] = {}
    candidates = [a for a in ids if a not in errors]
    with ProcessPoolExecutor(jobs) as ex:
        for a, (fm, err) in zip(candidates, ex.map(metadata_forms_worker, candidates, [str(workdir)] * len(candidates),
                                                   [manifest[a].get("samples", {}) for a in candidates], chunksize=8)):
            if err:
                errors[a] = err
            for k, v in fm.items():
                forms.setdefault(k, set()).update(v)
    if errors:
        log(f"{len(errors)} athletes could not be converted:")
        for a, e in sorted(errors.items()):
            log(f"  {a}: {e}")
        if not allow_errors:
            log("not writing the other tables; fix the converter or pass --allow-errors to leave these athletes out")
            return 1
    ok_ids = [a for a in ids if a not in errors]
    if errors:  # the metric schema must come from the athletes that are kept
        forms = {}
        with ProcessPoolExecutor(jobs) as ex:
            for fm, _ in ex.map(metadata_forms_worker, ok_ids, [str(workdir)] * len(ok_ids),
                                [manifest[a].get("samples", {}) for a in ok_ids], chunksize=8):
                for k, v in fm.items():
                    forms.setdefault(k, set()).update(v)
    # time series files of athletes not in this dataset must not linger in the output
    for d in (b.repo / "data" / "timeseries").glob("athlete_id=*"):
        if d.name.split("=", 1)[1] not in set(ok_ids):
            shutil.rmtree(d)
    kinds = gcod.metric_kinds(forms)
    log(f"metadata: {len(kinds)} metrics ({sum(k == 'scalar' for k in kinds.values())} scalar, "
        f"{sum(k == 'pair' for k in kinds.values())} value+weight, {sum(k == 'list' for k in kinds.values())} list)")

    # 3. activities, athletes, raw_json, exceptions (in athlete order, streamed), then the card
    tmp = {t: b.repo / "data" / f"{t}.tmp" for t in ("activities", "athletes", "raw_json", "timeseries_exceptions")}
    for d in tmp.values():
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True)
    try:
        return _write_metadata_tables(b, sel, ok_ids, kinds, manifest, tmp, jobs, errors)
    finally:
        for d in tmp.values():  # left over only if writing failed or was stopped
            if d.exists():
                shutil.rmtree(d)


def _write_metadata_tables(b: Build, sel: pl.DataFrame, ok_ids: list[str], kinds: dict[str, str],
                           manifest: dict[str, dict], tmp: dict[str, Path], jobs: int, errors: dict[str, str]) -> int:
    import pyarrow as pa

    workdir = b.workdir
    json_buf: list[dict] = []
    json_bytes = json_part = 0
    athlete_rows: list[dict] = []
    act_writer: C.TableWriter | None = None
    act_part = 0

    def write_activities(t: pa.Table) -> None:
        """Stream into the current part file (the writer holds at most one row group in memory);
        start a new part every ACTIVITIES_PART_ROWS rows."""
        nonlocal act_writer, act_part
        if act_writer is None:
            act_writer = C.TableWriter(tmp["activities"] / f"part-{act_part:05d}.parquet", t.schema, "table",
                                       dataset="opendata", table_name="activities",
                                       encodings=SPEC.tables["activities"].encodings)
        act_writer.write(t)
        if act_writer.rows + act_writer.buffered >= ACTIVITIES_PART_ROWS:
            act_writer.close()
            act_writer, act_part = None, act_part + 1

    def flush_json() -> None:
        nonlocal json_buf, json_bytes, json_part
        if json_buf:
            t = pa.Table.from_pylist(json_buf, schema=pa.schema([("athlete_id", pa.string()), ("json", pa.string())]))
            C.write_table(t, tmp["raw_json"] / f"part-{json_part:05d}.parquet", "table",
                          dataset="opendata", table_name="raw_json")
            json_buf, json_bytes, json_part = [], 0, json_part + 1

    batch_size = 4 * jobs  # few results in flight, so they can't pile up while parts are written
    next_log = 0
    try:
        with ProcessPoolExecutor(jobs) as ex:
            for start in range(0, len(ok_ids), batch_size):
                batch = ok_ids[start:start + batch_size]
                results = ex.map(metadata_worker, batch, [str(workdir)] * len(batch), [kinds] * len(batch),
                                 [manifest[a].get("samples", {}) for a in batch])
                for a, (acts, arow, text) in zip(batch, results):
                    write_activities(acts)
                    athlete_rows.append(arow)
                    json_buf.append({"athlete_id": a, "json": text})
                    json_bytes += len(text)
                    if json_bytes >= RAW_JSON_PART_BYTES:
                        flush_json()
                done = min(start + batch_size, len(ok_ids))
                if done >= next_log or done == len(ok_ids):
                    log(f"metadata: {done:,}/{len(ok_ids):,} athletes")
                    next_log = done + 250
                if STOP.is_set():
                    log("stopped; re-run to resume")
                    return 130
        if act_writer is not None:
            act_writer.close()
    except BaseException:
        if act_writer is not None:
            act_writer.abort()
        raise
    flush_json()

    prov = sel.select("athlete_id", osf_file_id="osf_file_id", osf_sha256="sha256",
                      osf_size_bytes="size", osf_date_created="date_created")
    athletes_df = pl.DataFrame(athlete_rows, schema_overrides={"yob": pl.Int64}).join(prov, on="athlete_id", how="left")
    order = [c.name for c in SPEC.tables["athletes"].columns]
    C.write_table(athletes_df.select(order).sort("athlete_id").to_arrow(), tmp["athletes"] / "athletes.parquet",
                  "table", dataset="opendata", table_name="athletes")

    exc_files = [b.exc_file(a) for a in ok_ids if b.exc_file(a).exists()]
    exc = (pl.concat([pl.read_parquet(p) for p in exc_files]) if exc_files
           else pl.DataFrame(schema=gcod.EXCEPTIONS_SCHEMA)).sort("athlete_id", "activity_id", "row_in_activity", "column")
    C.write_table(exc.to_arrow(), tmp["timeseries_exceptions"] / "exceptions.parquet", "table",
                  dataset="opendata", table_name="timeseries_exceptions")

    for t, d in tmp.items():
        _replace_dir(d, b.repo / "data" / t)

    # 4. dataset card
    n_rows = sum(manifest[a]["rows"] for a in ok_ids)
    # the card's example athlete: one that is in this build, with the most time series
    example = athletes_df.sort(["n_timeseries", "athlete_id"], descending=[True, False])["athlete_id"][0]
    write_card(b.repo, n_athletes=len(ok_ids), n_activities=int(athletes_df["n_activities"].sum()), n_rows=n_rows,
               example_athlete_id=example)
    log(f"convert: done. {len(ok_ids):,} athletes, {athletes_df['n_activities'].sum():,} activities, "
        f"{n_rows:,} samples, {exc.height:,} exception values -> {b.repo}")
    if errors:
        log(f"left out {len(errors)} athletes that could not be converted (see above)")
    return 0


def write_card(repo: Path, n_athletes: int, n_activities: int, n_rows: int, example_athlete_id: str) -> None:
    text = CARD_TEMPLATE.read_text()
    values = {
        "configs": C.card_configs(SPEC),
        "repo_id": SPEC.repo_id,
        "example_athlete_id": example_athlete_id,
        "n_athletes": f"{n_athletes:,}",
        "n_activities": f"{n_activities:,}",
        "n_rows": f"{n_rows / 1e9:.1f} billion" if n_rows >= 1e9 else f"{n_rows / 1e6:.0f} million",
        "size_category": next(c for lim, c in [(1e6, "100K<n<1M"), (1e7, "1M<n<10M"), (1e8, "10M<n<100M"),
                                                (1e9, "100M<n<1B"), (1e10, "1B<n<10B"), (1e11, "10B<n<100B"),
                                                (float("inf"), "n>100B")] if n_rows < lim),
        "code_url": "https://github.com/GoldenCheetah/OpenData/tree/master/huggingface",
        "converter_version": gcod.CONVERTER_VERSION,
        "conventions_version": C.VERSION,
    }
    for k, v in values.items():
        text = text.replace("{{" + k + "}}", v)
    left = sorted(set(re.findall(r"\{\{(\w+)\}\}", text)))
    if left:
        raise SystemExit(f"card template has unfilled placeholders: {left}")
    (repo / "README.md").write_text(text)


# --- check ---------------------------------------------------------------------------
class _RowStream:
    """Read a Parquet file sequentially, n rows at a time, without loading it whole."""

    def __init__(self, path: Path):
        import pyarrow.parquet as pq

        self.batches = pq.ParquetFile(path).iter_batches(batch_size=65_536) if path.exists() else iter(())
        self.buf: list[pl.DataFrame] = []
        self.n = 0

    def take(self, n: int) -> pl.DataFrame:
        while self.n < n:
            b = next(self.batches, None)
            if b is None:
                break
            df = pl.from_arrow(b)
            self.buf.append(df)
            self.n += df.height
        if not self.buf:
            return pl.DataFrame()
        all_ = pl.concat(self.buf) if len(self.buf) > 1 else self.buf[0]
        head, rest = all_.head(n), all_.slice(n)
        self.buf, self.n = ([rest] if rest.height else []), rest.height
        return head

    def exhausted(self) -> bool:
        return self.n == 0 and next(self.batches, None) is None


def check_worker(athlete_id: str, workdir: str, text_level: bool) -> dict:
    """Compare one athlete's written time series, read back from disk, with the source CSVs.

    Every value: the stored Float32 is the Float32 of the source value. With `text_level`, also
    that it prints back as the source number (slow). Streams activity by activity.
    """
    _low_priority()
    b = Build(Path(workdir))
    exc_path = b.exc_file(athlete_id)
    exc = pl.read_parquet(exc_path) if exc_path.exists() else pl.DataFrame(schema=gcod.EXCEPTIONS_SCHEMA)
    stream = _RowStream(b.ts_file(athlete_id))
    values = differ = 0
    errors = []
    with gcod.open_athlete_zip(b.zip_path(athlete_id)) as az:
        for aid, src in gcod.iter_source_activities(az):
            if src.height == 0:
                continue
            out = stream.take(src.height)
            if out.height != src.height or (out["activity_id"] != aid).any():
                errors.append(f"activity {aid}: rows don't line up with the CSV")
                break
            values += src.height * len(gcod.TS_COLUMNS)
            exc_a = exc.filter(pl.col("activity_id") == aid)
            differ += gcod.cast_mismatches(src, out)
            if text_level:
                differ += gcod.same_as_source(src, out, exc_a)
            if exc_a.height:  # exceptions hold the exact source values
                restored = exc_a.join(
                    src.unpivot(index=["activity_id", "row_in_activity"], on=list(gcod.TS_COLUMNS),
                                variable_name="source_column", value_name="v"),
                    on=["activity_id", "row_in_activity", "source_column"], how="left")
                differ += restored.filter(pl.col("v") != pl.col("source_value")).height + restored["v"].null_count()
    if not errors and not stream.exhausted():
        errors.append("the file has more rows than the CSVs")
    return {"athlete_id": athlete_id, "values": values, "differ": differ,
            "error": "; ".join(errors) or None, "text_level": text_level}


def stage_check(workdir: Path, jobs: int, text_fraction: float, seed: int) -> int:
    b = Build(workdir)
    validate = _load_validator()
    log("check: validating the dataset against dataset.toml and the conventions ...")
    status = validate.validate(SPEC_PATH, b.repo, jobs, None)

    manifest = load_manifest(b.manifest)
    present = sorted(d.name.split("=", 1)[1] for d in (b.repo / "data" / "timeseries").glob("athlete_id=*"))
    ids = [a for a in present if manifest.get(a, {}).get("status") == "ok"]
    text_ids = set(random.Random(seed).sample(ids, round(len(ids) * text_fraction))) if ids else set()
    log(f"check: every time series value of {len(ids):,} athletes against the original CSVs "
        f"(text level for {len(text_ids):,}) ...")
    total = differ = 0
    problems = []
    with ProcessPoolExecutor(jobs) as ex:
        for r in ex.map(check_worker, ids, [str(workdir)] * len(ids), [a in text_ids for a in ids], chunksize=1):
            total += r["values"]
            if r["error"] or r["differ"]:
                problems.append(r)
            differ += r["differ"] or 0
    for r in problems[:20]:
        detail = r["error"] or f"{r['differ']:,} values differ"
        log(f"  {r['athlete_id']}: {detail}")

    # every activity that says it has a time series has samples, and vice versa
    acts = pl.read_parquet(b.repo / "data" / "activities" / "*.parquet", columns=["athlete_id", "activity_id", "has_timeseries"])
    in_dataset = set(acts["athlete_id"].unique())
    counts = pl.DataFrame([{"athlete_id": a, "activity_id": k, "n": n}
                           for a in in_dataset for k, n in manifest[a].get("samples", {}).items()],
                          schema={"athlete_id": pl.String, "activity_id": pl.String, "n": pl.Int64})
    j = acts.join(counts, on=["athlete_id", "activity_id"], how="left").with_columns(pl.col("n").fill_null(0))
    mismatch = j.filter(pl.col("has_timeseries") != (pl.col("n") > 0)).height

    log(f"check: {total:,} values in {len(ids):,} athletes compared, {differ:,} differ; "
        f"{len(problems)} athletes with problems; has_timeseries mismatches: {mismatch}")
    ok = status == 0 and not problems and mismatch == 0
    b.check_record.unlink(missing_ok=True)
    if ok:  # what passed, file by file: `upload` uploads exactly this
        files = folder_files(b.repo, with_sha256=True, jobs=jobs)
        record = {"result": "pass", "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                  "converter": gcod.CONVERTER_VERSION, "conventions": C.VERSION, "values_compared": total,
                  "files": files}
        tmp = b.check_record.with_suffix(".tmp")
        tmp.write_text(json.dumps(record, indent=1) + "\n")
        tmp.replace(b.check_record)
    log("check: PASS" if ok else "check: FAIL")
    return 0 if ok else 1


def folder_files(root: Path, with_sha256: bool = False, jobs: int = 4) -> dict[str, dict]:
    """Every file under root (relative POSIX path) with its size and mtime, and optionally sha256."""
    paths = sorted(p for p in root.rglob("*") if p.is_file())
    out = {p.relative_to(root).as_posix(): {"size": (st := p.stat()).st_size, "mtime_ns": st.st_mtime_ns}
           for p in paths}
    if with_sha256:
        with ThreadPoolExecutor(jobs) as ex:  # hashlib releases the GIL
            for rel, digest in zip(out, ex.map(sha256_file, paths)):
                out[rel]["sha256"] = digest
    return out


# --- upload --------------------------------------------------------------------------
def checked_files(b: Build) -> dict[str, dict]:
    """The files `check` passed, provided the dataset folder still is exactly that; else stop."""
    if not b.check_record.exists():
        raise SystemExit("upload: no passing check of this build; run `check` first")
    rec = json.loads(b.check_record.read_text())
    if rec.get("converter") != gcod.CONVERTER_VERSION:
        raise SystemExit(f"upload: the check was of converter {rec.get('converter')}, this is "
                         f"{gcod.CONVERTER_VERSION}; run `convert` and `check` again")
    now = folder_files(b.repo)
    was = {k: {"size": v["size"], "mtime_ns": v["mtime_ns"]} for k, v in rec["files"].items()}
    if now != was:
        changed = sorted(k for k in now.keys() | was.keys() if now.get(k) != was.get(k))
        raise SystemExit(f"upload: the dataset folder changed since it was checked ({len(changed)} files, "
                         f"e.g. {', '.join(changed[:3])}); run `check` again")
    return rec["files"]


REPO_ID_RE = r"^[A-Za-z0-9][\w.-]*/[A-Za-z0-9][\w.-]*$"


def card_for(card: str, repo_id: str) -> str:
    """The card with its code examples pointing at repo_id instead of the dataset's own repo."""
    return re.sub(rf"(?<![\w.-]){re.escape(SPEC.repo_id)}(?![\w.-])", repo_id, card)


def stage_upload_folder(b: Build, files: dict[str, dict], repo_id: str) -> Path:
    """A copy of the dataset folder (hard links, so free) with the card made for repo_id."""
    folder = b.root / "upload" / repo_id.replace("/", "__")
    if folder.exists():
        shutil.rmtree(folder)
    for rel in files:
        dst = folder / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        if rel == "README.md":
            dst.write_text(card_for((b.repo / rel).read_text(), repo_id))
        else:
            try:
                os.link(b.repo / rel, dst)
            except OSError:
                shutil.copy2(b.repo / rel, dst)
    return folder


def git_blob_sha1(data: bytes) -> str:
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()


def remote_differences(expected: dict[str, dict], remote: list) -> list[str]:
    """Compare files on the Hub (RepoFile-like: path, size, blob_id, lfs) with the expected ones
    (path -> size, sha256 and, for small files kept in git, git_sha1)."""
    out = []
    seen = {}
    for f in remote:
        if f.path == ".gitattributes" or not hasattr(f, "blob_id"):  # skip folders
            continue
        seen[f.path] = f
        e = expected.get(f.path)
        if e is None:
            out.append(f"{f.path}: on the Hub but not in the dataset folder")
        elif f.size != e["size"]:
            out.append(f"{f.path}: {f.size:,} bytes on the Hub, {e['size']:,} here")
        elif f.lfs is not None and f.lfs.sha256 != e["sha256"]:
            out.append(f"{f.path}: sha256 differs")
        elif f.lfs is None and f.blob_id != e.get("git_sha1"):
            out.append(f"{f.path}: git blob differs")
    out += [f"{p}: missing on the Hub" for p in sorted(expected.keys() - seen.keys())]
    return out


def stage_upload(workdir: Path, repo_id: str) -> int:
    from huggingface_hub import HfApi

    if not re.match(REPO_ID_RE, repo_id):
        raise SystemExit(f"upload: {repo_id!r} is not a repo id like `org/name`")
    b = Build(workdir)
    files = checked_files(b)
    api = HfApi()
    user = api.whoami()["name"]  # fails clearly when not logged in
    url = api.create_repo(repo_id, repo_type="dataset", private=True, exist_ok=True)
    private = api.dataset_info(repo_id).private
    log(f"upload: {len(files):,} files, {human_bytes(sum(f['size'] for f in files.values()))} as {user} "
        f"to {url} ({'private' if private else 'PUBLIC'})")

    folder = stage_upload_folder(b, files, repo_id)
    card = (folder / "README.md").read_bytes()
    expected = {**files, "README.md": {"size": len(card), "sha256": hashlib.sha256(card).hexdigest(),
                                       "git_sha1": git_blob_sha1(card)}}
    n_athletes = sum(rel.startswith("data/timeseries/") for rel in files)
    message = (f"OpenData: converter {gcod.CONVERTER_VERSION}, conventions {C.VERSION}, "
               f"{n_athletes:,} athletes with time series")
    commit = api.upload_folder(repo_id=repo_id, repo_type="dataset", folder_path=folder,
                               commit_message=message, delete_patterns=["*"])
    log(f"upload: committed {commit.oid} ; verifying every file on the Hub ...")

    remote = list(api.list_repo_tree(repo_id, repo_type="dataset", recursive=True, revision=commit.oid))
    problems = remote_differences(expected, remote)
    for p in problems[:20]:
        log(f"  {p}")
    if problems:
        log(f"upload: FAIL, {len(problems)} files on the Hub don't match the checked dataset folder")
        return 1
    with b.uploads.open("a") as f:
        f.write(json.dumps({"repo_id": repo_id, "commit": commit.oid, "private": private,
                            "uploaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                            "converter": gcod.CONVERTER_VERSION, "conventions": C.VERSION,
                            "files": len(expected), "bytes": sum(e["size"] for e in expected.values())}) + "\n")
    shutil.rmtree(folder)
    log(f"upload: PASS, all {len(expected):,} files on the Hub match ({url})")
    return 0


def _load_validator():
    # A regular import (not by file path), so the validator's worker processes can import it too.
    sys.path.insert(0, str(HERE.parents[1] / "tools"))
    import validate

    return validate


# --- main ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--workdir", type=Path, default=DEFAULT_WORKDIR, help="default: huggingface/data")
    sub = ap.add_subparsers(dest="stage", required=True)
    sub.add_parser("listing", help="fetch the OSF file listing")
    m = sub.add_parser("mirror", help="download and verify all athlete zips")
    m.add_argument("--workers", type=int, default=3, help="parallel downloads (default 3)")
    m.add_argument("--max-passes", type=int, default=8, help="retry rounds for failed zips (default 8)")
    m.add_argument("--refresh-listing", action="store_true", help="re-fetch the OSF listing first")
    m.add_argument("--athletes", nargs="+", metavar="UUID", help="only these athletes (default: all)")
    c = sub.add_parser("convert", help="convert mirrored zips into the dataset folder")
    c.add_argument("--jobs", type=int, default=DEFAULT_JOBS, help=f"parallel processes (default {DEFAULT_JOBS})")
    c.add_argument("--athletes", nargs="+", metavar="UUID", help="only these athletes (default: all)")
    c.add_argument("--athletes-from", type=Path, metavar="FILE",
                   help="only the athletes in FILE (one UUID per line, or a parquet with athlete_id)")
    c.add_argument("--allow-errors", action="store_true", help="leave out athletes that can't be converted")
    k = sub.add_parser("check", help="validate the dataset folder and check exactness against the zips")
    k.add_argument("--jobs", type=int, default=DEFAULT_JOBS, help=f"parallel processes (default {DEFAULT_JOBS})")
    k.add_argument("--text-fraction", type=float, default=0.05,
                   help="share of athletes also checked at the text level (slow; default 0.05)")
    k.add_argument("--seed", type=int, default=0)
    u = sub.add_parser("upload", help="upload the checked dataset folder to a Hugging Face dataset repo")
    u.add_argument("--repo", required=True, metavar="ORG/NAME", help="e.g. GoldenCheetah/opendata-test")
    a = ap.parse_args()

    def request_stop(signum, _frame):
        if STOP.is_set():
            raise SystemExit(130)  # second signal: exit now
        log(f"received {signal.Signals(signum).name}; finishing up (work done so far is kept) ...")
        STOP.set()

    if a.stage != "upload":  # upload: Ctrl-C stops at once (it resumes when run again)
        signal.signal(signal.SIGINT, request_stop)
        signal.signal(signal.SIGTERM, request_stop)

    a.workdir.mkdir(parents=True, exist_ok=True)
    log(f"migrate {a.stage}, workdir {a.workdir}, pid {os.getpid()}")
    if a.stage == "listing":
        stage_listing(a.workdir)
    elif a.stage == "mirror":
        sys.exit(stage_mirror(a.workdir, a.workers, a.max_passes, a.refresh_listing, a.athletes))
    elif a.stage == "convert":
        sys.exit(stage_convert(a.workdir, a.jobs, a.athletes, a.athletes_from, a.allow_errors))
    elif a.stage == "check":
        sys.exit(stage_check(a.workdir, a.jobs, a.text_fraction, a.seed))
    elif a.stage == "upload":
        sys.exit(stage_upload(a.workdir, a.repo))


if __name__ == "__main__":
    main()
