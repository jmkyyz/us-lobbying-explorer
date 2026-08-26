"""Daily refresh orchestrator: incremental ingest -> re-stage parquet -> optional R2 upload.

Designed for unattended launchd runs (see make_refresh_plist.py):
- A pid lockfile prevents overlapping runs; a run that finds the DuckDB file locked by
  something else (e.g. a manual backfill) logs and exits cleanly instead of fighting for it.
- Source is the House Clerk's bulk XML (ingest_house.py), NOT the lda.gov REST API. The API
  is unreachable from any non-browser client — see the header of ingest_house.py for the
  full diagnosis. The old dt_posted watermark went with it: freshness is now decided by the
  House manifest, which timestamps each quarterly ZIP, so an unchanged quarter costs a
  single JSON fetch and a changed one is re-parsed in full.
- Only the current and previous filing year are checked by default. The House does
  occasionally re-touch much older ZIPs, and re-parsing all 70 of them would turn a daily
  job into a multi-GB download; those are picked up by an explicit --years run instead.
- Failures print a loud marker line — the sibling Canadian app's pipeline once failed
  silently for 13 days, hence the noise. That noise is what surfaced the API block here,
  after 22 consecutive failed mornings.

Usage:
    python3 refresh.py            # incremental ingest + stage (+ upload if R2 creds in .env)
    python3 refresh.py --no-upload
    python3 refresh.py --dry-run  # show which ZIPs would be re-ingested and exit
    python3 refresh.py --years 2013-2026   # widen the window (picks up old-ZIP re-touches)
"""

import argparse
import datetime as dt
import os
import sys

import duckdb

import schema
import publish
import ingest_house
from ingest import load_dotenv

LOCK_PATH = ".refresh.lock"


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
    parser.add_argument("--years", help="year or range, e.g. 2026 or 2013-2026")
    args = parser.parse_args()

    this_year = dt.date.today().year
    if args.years:
        lo, _, hi = args.years.partition("-")
        years = set(range(int(lo), int(hi or lo) + 1))
    else:
        years = {this_year, this_year - 1}

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
        log(f"newest filing in DB: {row[0]} — checking House ZIPs for {sorted(years)}")

        if args.dry_run:
            import requests
            manifest = ingest_house.fetch_manifest(requests.Session())
            for _y, entry in ingest_house.entries_for_years(manifest, years):
                prior = con.execute(
                    "SELECT value FROM ingest_state WHERE key = ?",
                    [f"house_zip_{entry['file']}"],
                ).fetchone()
                state = "unchanged" if prior and prior[0] == entry["name"] else "WOULD RE-INGEST"
                log(f"  {entry['file']}: {state}")
            return 0

        count = ingest_house.run_refresh(con, years)
        log(f"House ingest done: {count} filings upserted")

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
