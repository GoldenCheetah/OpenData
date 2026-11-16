# /// script
# requires-python = ">=3.11"
# dependencies = ["httpx>=0.27", "polars>=1.20"]
# ///
"""GoldenCheetah OpenData (GCOD): migrate the OSF release to Hugging Face.

Every stage is resumable and safe to re-run; the files in the work directory are the state.

  listing   fetch the OSF file listing (ids, sizes, sha256)  -> <workdir>/osf/listing.parquet
  mirror    download every athlete zip and verify its sha256 -> <workdir>/osf/athletes/<uuid>.zip
  (convert, check, upload: to follow)

Usage (from huggingface/):

  uv run datasets/opendata/migrate.py listing
  uv run datasets/opendata/migrate.py mirror [--workers 3]

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
import shutil
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import httpx
import polars as pl

HERE = Path(__file__).resolve().parent
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
    a = ap.parse_args()

    def request_stop(signum, _frame):
        if STOP.is_set():
            raise SystemExit(130)  # second signal: exit now
        log(f"received {signal.Signals(signum).name}; finishing up (partial downloads are kept) ...")
        STOP.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    a.workdir.mkdir(parents=True, exist_ok=True)
    log(f"migrate {a.stage}, workdir {a.workdir}, pid {os.getpid()}")
    if a.stage == "listing":
        stage_listing(a.workdir)
    elif a.stage == "mirror":
        sys.exit(stage_mirror(a.workdir, a.workers, a.max_passes, a.refresh_listing, a.athletes))


if __name__ == "__main__":
    main()
