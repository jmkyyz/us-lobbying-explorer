"""Daily refresh orchestrator: incremental ingest -> re-stage parquet -> optional R2 upload.

Designed for unattended launchd runs (see make_refresh_plist.py):
- A pid lockfile prevents overlapping runs; a run that finds the DuckDB file locked by
  something else (e.g. a manual backfill) logs and exits cleanly instead of fighting for it.
- Watermark = newest dt_posted already in the DB, minus a safety overlap. LDA amendments
  arrive as NEW filings with fresh dt_posted (originals are never edited in place), so a
  posted-date watermark catches corrections too; the overlap re-pulls a few days of already-
  seen filings, which the uuid-keyed upsert makes harmless.
- Failures print a loud marker line — the sibling Canadian app's pipeline once failed
  silently for 13 days, hence the noise.

Usage:
    python3 refresh.py            # incremental ingest + stage (+ upload if R2 creds in .env)
    python3 refresh.py --no-upload
    python3 refresh.py --dry-run  # show the watermark and exit
"""

import argparse
import datetime as dt
import os
import sys

import duckdb

import schema
import publish
from ingest import load_dotenv, make_session, run_incremental, set_state, DEFAULT_BASE_URL

LOCK_PATH = ".refresh.lock"
OVERLAP_DAYS = 3


def log(msg: str) -> None:
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def acquire_lock() -> bool:
    if os.path.exists(LOCK_PATH):
        try:
            pid = int(open(LOCK_PATH).read().strip())
            os.kill(pid, 0)  # raises if that pid is gone
            log(f"another refresh (pid {pid}) is running — exiting")
            return False
        except (ValueError, ProcessLookupError, PermissionError):
            log("stale lockfile found — taking over")
    with open(LOCK_PATH, "w") as f:
        f.write(str(os.getpid()))
    return True


def release_lock() -> None:
    try:
        os.remove(LOCK_PATH)
    except FileNotFoundError:
        pass


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-upload", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_dotenv()
    if not acquire_lock():
        return 0

    try:
        try:
            con = schema.get_connection()
        except duckdb.IOException as e:
            log(f"DB is locked by another process (backfill?) — skipping this run: {e}")
            return 0

        row = con.execute("SELECT max(dt_posted) FROM filings").fetchone()
        if not row or row[0] is None:
            log("!!! REFRESH FAILED !!! DB is empty — run a backfill before scheduling refreshes")
            return 1
        since = (row[0] - dt.timedelta(days=OVERLAP_DAYS)).strftime("%Y-%m-%dT%H:%M:%S")
        log(f"newest filing in DB: {row[0]} — pulling since {since} ({OVERLAP_DAYS}d overlap)")
        if args.dry_run:
            return 0

        session = make_session()
        count = run_incremental(con, session, DEFAULT_BASE_URL, since)
        set_state(con, "last_incremental_dt_posted", since)
        log(f"incremental ingest done: {count} filings upserted")

        publish.stage(con)
        log("stage done")

        if args.no_upload:
            log("upload skipped (--no-upload)")
        elif os.environ.get("R2_ACCOUNT_ID") and os.environ.get("R2_BUCKET"):
            publish.upload()
            log("R2 upload done")
        else:
            log("upload skipped (R2 credentials not configured)")

        log("refresh complete")
        return 0
    except Exception as e:
        log(f"!!! REFRESH FAILED !!! {type(e).__name__}: {e}")
        return 1
    finally:
        release_lock()


if __name__ == "__main__":
    sys.exit(main())
