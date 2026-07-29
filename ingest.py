"""Pull LDA filings from the REST API into the local DuckDB store (data/lda.duckdb).

Auth: Authorization: Token <LDA_API_KEY> header (confirmed format; register a free key at
https://lda.senate.gov/api/register/ — that signup step is manual, not automated here).
Anonymous access works but is capped at 15 req/min vs. 120 req/min with a key.

Base URL: lda.senate.gov (the documented "successor" lda.gov currently 403s regardless of
User-Agent as of 2026-07-28 — not actually live yet despite the deprecation/Link headers
pointing to it). Override via LDA_API_BASE if/when lda.gov starts working, ideally before its
sunset date. This is a one-line env var change, no code change needed.

Usage:
    python3 ingest.py --backfill --filing-year 2024
    python3 ingest.py --backfill --filing-year 2024 --max-pages 3     # smoke test
    python3 ingest.py --backfill --start-year 2013 --end-year 2026   # ranged backfill
    python3 ingest.py --backfill                                     # full history since 1999
    python3 ingest.py --since 2026-07-20T00:00:00-05:00               # incremental
"""

import argparse
import hashlib
import os
import time

import requests

import schema
from normalize import norm_name

DEFAULT_BASE_URL = os.environ.get("LDA_API_BASE", "https://lda.senate.gov/api/v1")
EARLIEST_FILING_YEAR = 1999  # confirmed via API: filing_year=1996..1998 rejected as invalid


def load_dotenv(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())


def make_session() -> requests.Session:
    s = requests.Session()
    api_key = os.environ.get("LDA_API_KEY")
    if api_key:
        s.headers["Authorization"] = f"Token {api_key}"
    return s


def rate_sleep() -> float:
    """Extra delay between requests, on top of the API's own ~1s response latency.

    Limits are 120 req/min with a key, 15/min anonymous. Measured round-trip is ~1.0s, so with
    a key we're naturally at ~60 req/min — already half the budget — and need no added delay;
    a small sleep is kept purely as headroom. Anonymous needs a real delay to stay under 15/min.
    """
    has_key = bool(os.environ.get("LDA_API_KEY"))
    return 0.05 if has_key else 4.2


def synth_id(*parts) -> int:
    """Deterministic synthetic bigint id — same inputs always produce the same id, so
    re-ingesting the same filing (e.g. during an incremental re-pull) is idempotent."""
    h = hashlib.sha1("|".join(str(p) for p in parts).encode()).digest()
    return int.from_bytes(h[:8], "big") & 0x7FFFFFFFFFFFFFFF


def safe_int(v):
    """Coerce an API id field to int, or None. The live API occasionally returns
    non-numeric placeholders in integer fields (e.g. client_id="New")."""
    if v is None or isinstance(v, int):
        return v
    s = str(v).strip()
    return int(s) if s.lstrip("-").isdigit() else None


def fetch_with_retry(session: requests.Session, url: str, params, max_retries=8):
    """Retry transient failures with exponential backoff: 429 (rate limit), 5xx (the live API
    throws occasional 503s), AND connection-level exceptions (read timeouts, resets — both seen
    in real overnight runs). Page counting lives in the caller, not here, so a retry can never
    desync the checkpoint numbering from the actual page position."""
    backoff = 5
    for attempt in range(max_retries):
        try:
            resp = session.get(url, params=params, timeout=60)
        except requests.RequestException as e:
            print(f"{type(e).__name__} on attempt {attempt + 1}/{max_retries}, backing off {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)
            continue
        if resp.status_code == 429 or resp.status_code >= 500:
            print(f"{resp.status_code} on attempt {attempt + 1}/{max_retries}, backing off {backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 120)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"giving up on {url} after {max_retries} retries")


def fetch_pages(session: requests.Session, base_url: str, params: dict, max_pages=None,
                start_page=1):
    """Yield (page_number, payload). start_page jumps straight to that page via the API's own
    ?page= parameter so a resumed run doesn't re-download everything before it."""
    url = f"{base_url}/filings/"
    page = start_page - 1
    pages_this_run = 0
    sleep_s = rate_sleep()
    request_params = dict(params)
    if start_page > 1:
        request_params["page"] = start_page
    while url:
        data = fetch_with_retry(session, url, request_params)
        page += 1
        pages_this_run += 1
        yield page, data
        url = data.get("next")
        request_params = None  # params are baked into `next` already
        if max_pages and pages_this_run >= max_pages:
            break
        time.sleep(sleep_s)


def new_buffers():
    """Accumulators for one page of filings. Dicts (not lists) for the PK-keyed tables so a
    duplicate within the same batch collapses to one row — DuckDB errors if a single
    INSERT ... ON CONFLICT statement tries to update the same row twice."""
    return {
        "registrants": {}, "clients": {}, "lobbyists": {},
        "filings": [], "filing_uuids": [],
        "activities": [], "gov_entities": [], "activity_lobbyists": [],
    }


def collect_filing(filing: dict, buf: dict) -> None:
    """Append one API filing into the page buffers. No DB I/O — see flush_buffers().

    Batching matters a lot here: the previous row-at-a-time version issued ~550 individual
    statements per 25-filing page, which cost more wall-clock than the API request itself.
    DuckDB is columnar and has high per-statement overhead, so one executemany per table is
    dramatically faster than many small INSERTs.
    """
    registrant = filing.get("registrant") or {}
    client = filing.get("client") or {}

    registrant_id = registrant.get("id")
    if registrant_id is not None:
        buf["registrants"][registrant_id] = (
            registrant_id, safe_int(registrant.get("house_registrant_id")),
            registrant.get("name"), norm_name(registrant.get("name")),
            registrant.get("description"), registrant.get("address_1"),
            registrant.get("address_2"), registrant.get("city"), registrant.get("state"),
            registrant.get("zip"), registrant.get("country"), registrant.get("ppb_country"),
            registrant.get("contact_name"), registrant.get("dt_updated"),
        )

    # Key on client["id"] — the API's GLOBAL client PK (resource-addressable at /clients/<id>/).
    # Do NOT key on client["client_id"]: despite the name, that is a PER-REGISTRANT sequence
    # number (Pfizer's seq 12 = "Pfizer Inc.", another registrant's seq 12 = a different org
    # entirely — verified live 2026-07-28). Keying on it collapsed thousands of unrelated
    # clients into single rows. If id is ever missing, synthesize a deterministic NEGATIVE
    # surrogate from the normalized name (can't collide with real positive ids; same client
    # dedupes to same surrogate; a later re-pull with a real id supersedes it).
    client_id = safe_int(client.get("id"))
    if client_id is None and client.get("name"):
        client_id = -synth_id("client", norm_name(client.get("name")))
    if client_id is not None:
        buf["clients"][client_id] = (
            client_id, client.get("name"), norm_name(client.get("name")),
            client.get("general_description"), client.get("client_government_entity"),
            client.get("client_self_select"), client.get("state"), client.get("country"),
            client.get("ppb_state"), client.get("ppb_country"), client.get("effective_date"),
        )

    filing_uuid = filing["filing_uuid"]
    buf["filing_uuids"].append(filing_uuid)
    buf["filings"].append((
        filing_uuid, filing.get("filing_type"), filing.get("filing_type_display"),
        filing.get("filing_year"), filing.get("filing_period"),
        filing.get("filing_period_display"), filing.get("filing_document_url"),
        filing.get("dt_posted"), filing.get("termination_date"), filing.get("income"),
        filing.get("expenses"), filing.get("expenses_method"), filing.get("posted_by_name"),
        registrant_id, client_id,
    ))

    for i, activity in enumerate(filing.get("lobbying_activities") or []):
        activity_id = synth_id(filing_uuid, i)
        buf["activities"].append((
            activity_id, filing_uuid, activity.get("general_issue_code"),
            activity.get("general_issue_code_display"), activity.get("description"),
            activity.get("foreign_entity_issues"),
        ))
        for entity in activity.get("government_entities") or []:
            buf["gov_entities"].append((activity_id, entity.get("id"), entity.get("name")))
        for lob in activity.get("lobbyists") or []:
            person = lob.get("lobbyist") or {}
            lobbyist_id = person.get("id")
            if lobbyist_id is None:
                continue
            full_name = " ".join(
                p for p in [person.get("first_name"), person.get("last_name")] if p
            )
            buf["lobbyists"][lobbyist_id] = (
                lobbyist_id, person.get("prefix"), person.get("first_name"),
                person.get("middle_name"), person.get("last_name"), person.get("suffix"),
                norm_name(full_name),
            )
            buf["activity_lobbyists"].append(
                (activity_id, lobbyist_id, lob.get("covered_position"), lob.get("new"))
            )


def flush_buffers(con, buf: dict) -> None:
    """Write one page of buffered rows in a single transaction, one executemany per table."""
    if not buf["filings"]:
        return

    con.execute("BEGIN TRANSACTION")
    try:
        if buf["registrants"]:
            con.executemany(
                """
                INSERT INTO registrants
                    (registrant_id, house_registrant_id, name, name_norm, description,
                     address_1, address_2, city, state, zip, country, ppb_country,
                     contact_name, dt_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (registrant_id) DO UPDATE SET
                    name = excluded.name, name_norm = excluded.name_norm,
                    description = excluded.description, address_1 = excluded.address_1,
                    address_2 = excluded.address_2, city = excluded.city,
                    state = excluded.state, zip = excluded.zip, country = excluded.country,
                    ppb_country = excluded.ppb_country, contact_name = excluded.contact_name,
                    dt_updated = excluded.dt_updated
                """,
                list(buf["registrants"].values()),
            )

        if buf["clients"]:
            con.executemany(
                """
                INSERT INTO clients
                    (client_id, name, name_norm, general_description, client_government_entity,
                     client_self_select, state, country, ppb_state, ppb_country, effective_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (client_id) DO UPDATE SET
                    name = excluded.name, name_norm = excluded.name_norm,
                    general_description = excluded.general_description,
                    client_government_entity = excluded.client_government_entity,
                    client_self_select = excluded.client_self_select, state = excluded.state,
                    country = excluded.country, ppb_state = excluded.ppb_state,
                    ppb_country = excluded.ppb_country, effective_date = excluded.effective_date
                """,
                list(buf["clients"].values()),
            )

        if buf["lobbyists"]:
            con.executemany(
                """
                INSERT INTO lobbyists
                    (lobbyist_id, prefix, first_name, middle_name, last_name, suffix, name_norm)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (lobbyist_id) DO UPDATE SET
                    prefix = excluded.prefix, first_name = excluded.first_name,
                    middle_name = excluded.middle_name, last_name = excluded.last_name,
                    suffix = excluded.suffix, name_norm = excluded.name_norm
                """,
                list(buf["lobbyists"].values()),
            )

        con.executemany(
            """
            INSERT INTO filings
                (filing_uuid, filing_type, filing_type_display, filing_year, filing_period,
                 filing_period_display, filing_document_url, dt_posted, termination_date,
                 income, expenses, expenses_method, posted_by_name, registrant_id, client_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (filing_uuid) DO UPDATE SET
                filing_type = excluded.filing_type,
                filing_type_display = excluded.filing_type_display,
                filing_year = excluded.filing_year, filing_period = excluded.filing_period,
                filing_period_display = excluded.filing_period_display,
                filing_document_url = excluded.filing_document_url,
                dt_posted = excluded.dt_posted, termination_date = excluded.termination_date,
                income = excluded.income, expenses = excluded.expenses,
                expenses_method = excluded.expenses_method,
                posted_by_name = excluded.posted_by_name,
                registrant_id = excluded.registrant_id, client_id = excluded.client_id
            """,
            buf["filings"],
        )

        # Re-ingesting a filing (incremental re-pull of a still-mutable period) must be
        # idempotent: wipe this batch's activity tree and rebuild it rather than diffing.
        placeholders = ",".join("?" for _ in buf["filing_uuids"])
        uuids = buf["filing_uuids"]
        for child in ("activity_government_entities", "activity_lobbyists", "activity_topics"):
            con.execute(
                f"""DELETE FROM {child} WHERE activity_id IN
                    (SELECT activity_id FROM lobbying_activities
                     WHERE filing_uuid IN ({placeholders}))""",
                uuids,
            )
        con.execute(
            f"DELETE FROM lobbying_activities WHERE filing_uuid IN ({placeholders})", uuids
        )

        if buf["activities"]:
            con.executemany(
                """
                INSERT INTO lobbying_activities
                    (activity_id, filing_uuid, general_issue_code, general_issue_code_display,
                     description, foreign_entity_issues)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                buf["activities"],
            )
        if buf["gov_entities"]:
            con.executemany(
                "INSERT INTO activity_government_entities (activity_id, entity_id, entity_name) "
                "VALUES (?, ?, ?)",
                buf["gov_entities"],
            )
        if buf["activity_lobbyists"]:
            con.executemany(
                "INSERT INTO activity_lobbyists "
                "(activity_id, lobbyist_id, covered_position, is_new) VALUES (?, ?, ?, ?)",
                buf["activity_lobbyists"],
            )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def get_state(con, key: str):
    row = con.execute("SELECT value FROM ingest_state WHERE key = ?", [key]).fetchone()
    return row[0] if row else None


def set_state(con, key: str, value: str) -> None:
    con.execute(
        "INSERT INTO ingest_state (key, value) VALUES (?, ?) "
        "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        [key, value],
    )


def run_backfill(con, session, base_url, filing_year=None, max_pages=None, page_size=100,
                  start_year=None, end_year=None):
    if filing_year:
        years = [filing_year]
    elif start_year or end_year:
        years = range(start_year or EARLIEST_FILING_YEAR, (end_year or 2026) + 1)
    else:
        years = range(EARLIEST_FILING_YEAR, 2027)
    for year in years:
        done_key = f"backfill_done_{year}"
        if get_state(con, done_key) == "1" and not max_pages:
            print(f"{year}: already fully backfilled, skipping (delete ingest_state row to redo)")
            continue

        # Resume by asking the API for the next page directly. Previously this walked from
        # page 1 and discarded already-ingested pages in Python, which re-downloaded thousands
        # of pages on every resume.
        resume_page = int(get_state(con, f"backfill_page_{year}") or 0)
        start_page = resume_page + 1
        params = {"filing_year": year, "page_size": page_size, "ordering": "dt_posted"}
        if start_page > 1:
            print(f"{year}: resuming at page {start_page}")
        count = 0
        for page, data in fetch_pages(session, base_url, params, max_pages=max_pages,
                                      start_page=start_page):
            buf = new_buffers()
            for filing in data["results"]:
                collect_filing(filing, buf)
                count += 1
            flush_buffers(con, buf)
            set_state(con, f"backfill_page_{year}", str(page))
            print(f"{year}: page {page}, {count} filings so far (total {data.get('count')})",
                  flush=True)

        if not max_pages:
            set_state(con, done_key, "1")
        print(f"{year}: done, {count} filings ingested this run", flush=True)


def run_incremental(con, session, base_url, since: str, page_size=100):
    params = {"filing_dt_posted_after": since, "page_size": page_size, "ordering": "dt_posted"}
    count = 0
    for page, data in fetch_pages(session, base_url, params):
        buf = new_buffers()
        for filing in data["results"]:
            collect_filing(filing, buf)
            count += 1
        flush_buffers(con, buf)
        print(f"incremental: page {page}, {count} filings so far (total {data.get('count')})",
              flush=True)
    return count


def main():
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", action="store_true")
    parser.add_argument("--filing-year", type=int)
    parser.add_argument("--start-year", type=int, help="backfill from this year (inclusive)")
    parser.add_argument("--end-year", type=int, help="backfill through this year (inclusive)")
    parser.add_argument("--since", help="ISO timestamp, e.g. 2026-07-20T00:00:00-05:00")
    parser.add_argument("--max-pages", type=int, default=None, help="limit pages (smoke tests)")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--db", default=schema.DB_PATH)
    args = parser.parse_args()

    if not args.backfill and not args.since:
        parser.error("pass --backfill (with optional --filing-year) or --since <timestamp>")

    con = schema.get_connection(args.db)
    schema.init_schema(con)
    session = make_session()
    base_url = DEFAULT_BASE_URL
    print(f"using API base: {base_url} ({'with key' if 'Authorization' in session.headers else 'anonymous'})")

    if args.backfill:
        run_backfill(con, session, base_url, args.filing_year, args.max_pages, args.page_size,
                     args.start_year, args.end_year)
    else:
        run_incremental(con, session, base_url, args.since, args.page_size)
        set_state(con, "last_incremental_dt_posted", args.since)


if __name__ == "__main__":
    main()
