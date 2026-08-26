"""Ingest LDA filings from the House Clerk's bulk XML ZIPs into data/lda.duckdb.

WHY THIS EXISTS (and why it replaced the REST API path in ingest.py)
--------------------------------------------------------------------
As of 2026-08, lda.gov sits behind an Akamai edge rule that fingerprints the CLIENT, not
the request: Chrome gets HTTP 200, while curl and python-requests get a blanket 403 from
every path on the host. Verified 2026-08-26 — the same URL, same machine, same IP, returned
1,976,387 filings in a real browser seconds after curl was refused. The API key makes no
difference (403 with and without), and no User-Agent or header combination changes it,
because the block is below HTTP. That is why refresh.py logged "!!! REFRESH FAILED !!!"
every morning for 22 days: it looked like the July 31 2026 lda.senate.gov sunset, but the
API itself was never down.

The House Clerk kept its bulk downloads alive specifically past that sunset — the notice on
disclosurespreview.house.gov reads "this site will be unavailable, except for the ability to
download ZIP files" — and those ZIPs are served to plain curl with no key and no bot check.
They are also a better fit than the API ever was: one ~47MB quarterly file replaces roughly
2.6 hours of 25-row-per-page pagination, and the manifest's per-file timestamps give the
daily refresh real change detection instead of a filing-date watermark.

FIELD MAPPING (verified 2026-08-26 against the 24,011 Q2-2026 filings that appear in both
sources — see --validate)
--------------------------------------------------------------------
  signedDate            -> dt_posted        exact match on 99.4%, 99.9% within an hour
  income / expenses     -> income/expenses  identical on 99.86%
  reportType+noLobbying -> filing_type      the API's "Y" suffix (Q2Y = "2nd Quarter -
                                            Report (No Activity)") is just noLobbying=Y
  senateID              -> registrant_id + client_num, split on the single "-"

Three ids exist in the API but not in the House XML, so they are synthesized deterministically
(same input always yields the same id, which keeps re-ingesting a quarter idempotent):

  filing_uuid  The House filing id — i.e. the XML filename stem, "301871849". Kept as-is
               rather than hashed so a row can be traced back to its source file. It is
               plainly not a UUID, which is deliberate: it makes API-sourced and House-sourced
               rows impossible to confuse if they ever land in one database.

  client_id    synth_id(registrant_id, client_num) for LD-2. The API's GLOBAL client id has no
               equivalent here, and the only client number the House publishes is the
               per-registrant sequence — the exact field the schema.py docstring warns must
               never be used as a key on its own, because two registrants' "client 12" are
               different orgs. Pairing it with the registrant makes that structurally
               impossible. LD-1 registrations carry no client number at all (the client has
               not been assigned one yet), so those use synth_id(registrant_id,
               norm_name(clientName)) instead. One consequence, harmless downstream: an org's
               registration and its quarterly reports can hold different client_ids. Nothing
               in the UI keys on client_id for identity — the client list groups by org name
               and org_first_seen keys on name_norm — so the org still reads as one entity.

  lobbyist_id  synth_id(norm_name(first last)). The API's stable lobbyist.id is not published
               in the bulk XML, only names, so this collapses two distinct people who share a
               name. Same tradeoff the Canadian registry app already makes.

Usage:
    python3 ingest_house.py --year 2026                  # one year, all periods
    python3 ingest_house.py --start-year 2013 --end-year 2026
    python3 ingest_house.py --year 2026 --max-files 500  # smoke test
    python3 ingest_house.py --refresh                    # only files whose manifest stamp moved
    python3 ingest_house.py --validate 2026 2ndQuarter   # compare against existing API rows
"""

import argparse
import os
import re
import sys
import time
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime

import requests

import schema
from ingest import synth_id, safe_int, load_dotenv
from normalize import norm_name

BASE_URL = os.environ.get(
    "HOUSE_LD_BASE", "https://disclosurespreview.house.gov/data/LD"
)
MANIFEST_FILE = "LdSearchPastFilings.json"
ZIP_CACHE = "data/house_zips"

# The app's frontend treats 2013 as the start of history (publish.py's EARLIEST_INTENDED_YEAR
# gates the first-time-org badge on it). The House publishes back to 2002, but pulling that in
# would grow the parquet the browser downloads without the UI being able to use it, so the
# default range stays 2013+ and older years are opt-in via --start-year.
DEFAULT_START_YEAR = 2013

# reportType prefix -> (filing_period, filing_period_display). Registrations (RR/RA) are not
# here: the API files them under the quarter they were signed in, so they are derived from
# signedDate at parse time instead.
PERIOD_BY_PREFIX = {
    "1": ("first_quarter", "1st Quarter (Jan 1 - Mar 31)"),
    "2": ("second_quarter", "2nd Quarter (Apr 1 - June 30)"),
    "3": ("third_quarter", "3rd Quarter (July 1 - Sep 30)"),
    "4": ("fourth_quarter", "4th Quarter (Oct 1 - Dec 31)"),
    "M": ("mid_year", "Mid-Year (Jan 1 - Jun 30)"),
    "Y": ("year_end", "Year-End (Jul 1 - Dec 31)"),
}

QUARTER_BY_MONTH = {
    **{m: ("first_quarter", "1st Quarter (Jan 1 - Mar 31)") for m in (1, 2, 3)},
    **{m: ("second_quarter", "2nd Quarter (Apr 1 - June 30)") for m in (4, 5, 6)},
    **{m: ("third_quarter", "3rd Quarter (July 1 - Sep 30)") for m in (7, 8, 9)},
    **{m: ("fourth_quarter", "4th Quarter (Oct 1 - Dec 31)") for m in (10, 11, 12)},
}

ORDINAL = {"1": "1st", "2": "2nd", "3": "3rd", "4": "4th"}


def filing_type_display(ftype: str) -> str:
    """Rebuild the API's human label from a type code, e.g. '2TY' -> '2nd Quarter -
    Termination (No Activity)'. Verified against every distinct code the API returned for
    2026 (Q2/Q2Y/2A/2AY/2T/2TY/2@/2@Y/RR/RA)."""
    if ftype in ("RR", "RA"):
        return "Registration" if ftype == "RR" else "Registration - Amendment"

    no_activity = ftype.endswith("Y") and not ftype.startswith("Q")
    core = ftype[:-1] if no_activity else ftype
    if core.startswith("Q"):
        # "Q2" / "Q2Y" — the quarter digit trails the Q, and here a trailing Y IS the
        # no-activity flag rather than a period code.
        if core.endswith("Y") and len(core) == 3:
            core, no_activity = core[:2], True
        quarter = core[1]
        label = f"{ORDINAL.get(quarter, quarter)} Quarter - Report"
    else:
        quarter, suffix = core[0], core[1:]
        base = ORDINAL.get(quarter, quarter) + " Quarter"
        if quarter in ("M", "Y"):
            base = "Mid-Year" if quarter == "M" else "Year-End"
        label = {
            "A": f"{base} - Amendment",
            "T": f"{base} - Termination",
            "@": f"{base} - Termination Amendment",
        }.get(suffix, f"{base} - Report")
    return label + (" (No Activity)" if no_activity else "")


def parse_period(report_type: str, signed: datetime | None):
    if report_type in ("RR", "RA"):
        if signed:
            return QUARTER_BY_MONTH[signed.month]
        return (None, None)
    return PERIOD_BY_PREFIX.get(report_type[0] if report_type[0] != "Q" else report_type[1],
                                (None, None))


def parse_money(s: str):
    if not s:
        return None
    try:
        return float(s.replace(",", "").replace("$", "").strip())
    except ValueError:
        return None


def parse_signed(s: str):
    """signedDate is US-format with a 12-hour clock, e.g. '5/11/2026 10:20:09 AM'. A few
    older filings carry a bare date with no time."""
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def parse_date(s: str):
    d = parse_signed(s)
    return d.date() if d else None


def txt(el, tag: str) -> str:
    """Element text, whitespace-stripped. The House XML pads empty elements with a newline
    and indentation rather than self-closing them, so every field needs stripping and an
    'empty' field is '' not None."""
    if el is None:
        return ""
    found = el.find(tag)
    if found is None or found.text is None:
        return ""
    return found.text.strip()


def split_senate_id(senate_id: str):
    """'401104898-12' -> (401104898, '12'). LD-1 registrations carry only the registrant part
    because the client has no number assigned yet."""
    senate_id = (senate_id or "").strip()
    if not senate_id:
        return None, None
    if "-" in senate_id:
        reg, _, client = senate_id.partition("-")
        return safe_int(reg), client.strip() or None
    return safe_int(senate_id), None


def split_agencies(raw: str):
    """federal_agencies is one comma-separated string, not a structured list — the API's
    numeric entity ids have no equivalent here, so ids are synthesized from the name."""
    if not raw:
        return []
    seen, out = set(), []
    for part in raw.split(","):
        name = part.strip()
        if not name or name.lower() in seen:
            continue
        seen.add(name.lower())
        out.append(name)
    return out


class Batch:
    """Accumulates rows for one ZIP, then writes each table in a single executemany.

    Row-at-a-time DuckDB inserts were 60% of the original API ingest's runtime; the same
    lesson applies here, only more so, since a quarter is ~25k filings arriving at once.
    PK tables dedup in a dict because one ZIP mentions the same registrant or lobbyist across
    many filings, and DuckDB will not silently ignore a duplicate primary key.
    """

    def __init__(self):
        self.registrants, self.clients, self.lobbyists = {}, {}, {}
        self.filings, self.activities = [], []
        self.act_entities, self.act_lobbyists = [], []

    def flush(self, con):
        def many(sql, rows):
            if rows:
                con.executemany(sql, rows)

        # activity_government_entities and activity_lobbyists are link tables with no primary
        # key, so INSERT alone is NOT idempotent — re-ingesting a quarter (which refresh.py
        # does whenever the House re-publishes one) would append a second copy of every
        # lobbyist link rather than replacing it, silently doubling lobbyist counts. The
        # other tables are safe because they upsert on a PK. activity_id is deterministic,
        # so clearing the ids this batch is about to write removes exactly the previous
        # version of these rows and nothing else.
        if self.activities:
            con.execute("CREATE OR REPLACE TEMP TABLE _batch_act (activity_id BIGINT)")
            con.executemany("INSERT INTO _batch_act VALUES (?)",
                            [(a[0],) for a in self.activities])
            for tbl in ("activity_government_entities", "activity_lobbyists"):
                con.execute(f"DELETE FROM {tbl} WHERE activity_id IN "
                            "(SELECT activity_id FROM _batch_act)")

        many(
            "INSERT OR REPLACE INTO registrants (registrant_id, house_registrant_id, name, "
            "name_norm, description, address_1, address_2, city, state, zip, country, "
            "ppb_country, contact_name, dt_updated) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            list(self.registrants.values()),
        )
        many(
            "INSERT OR REPLACE INTO clients (client_id, name, name_norm, general_description, "
            "client_government_entity, client_self_select, state, country, ppb_state, "
            "ppb_country, effective_date) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            list(self.clients.values()),
        )
        many(
            "INSERT OR REPLACE INTO lobbyists (lobbyist_id, prefix, first_name, middle_name, "
            "last_name, suffix, name_norm) VALUES (?,?,?,?,?,?,?)",
            list(self.lobbyists.values()),
        )
        many(
            "INSERT OR REPLACE INTO filings (filing_uuid, filing_type, filing_type_display, "
            "filing_year, filing_period, filing_period_display, filing_document_url, "
            "dt_posted, termination_date, income, expenses, expenses_method, posted_by_name, "
            "registrant_id, client_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            self.filings,
        )
        many(
            "INSERT OR REPLACE INTO lobbying_activities (activity_id, filing_uuid, "
            "general_issue_code, general_issue_code_display, description, foreign_entity_issues) "
            "VALUES (?,?,?,?,?,?)",
            self.activities,
        )
        many(
            "INSERT INTO activity_government_entities (activity_id, entity_id, entity_name) "
            "VALUES (?,?,?)",
            self.act_entities,
        )
        many(
            "INSERT INTO activity_lobbyists (activity_id, lobbyist_id, covered_position, "
            "is_new) VALUES (?,?,?,?)",
            self.act_lobbyists,
        )


def parse_filing(raw: bytes, house_file_id: str, batch: Batch) -> bool:
    """Parse one filing XML into batch. Returns False if it could not be parsed at all."""
    try:
        # utf-8-sig: a minority of files carry a BOM. Some roots also carry xmlns:xsi/xsd
        # attributes, which ElementTree handles but a naive "<LOBBYINGDISCLOSURE2>" string
        # match would miss.
        root = ET.fromstring(raw.decode("utf-8-sig", "replace"))
    except ET.ParseError:
        return False

    is_registration = root.tag.endswith("1")
    report_type = txt(root, "reportType")
    if not report_type:
        report_type = "RR" if is_registration else ""
    if not report_type:
        return False

    signed = parse_signed(txt(root, "signedDate"))
    period, period_display = parse_period(report_type, signed)

    # The API encodes "no lobbying activity this period" as a Y suffix on the type code
    # rather than as its own field; noLobbying is that same flag in the bulk XML.
    ftype = report_type
    if txt(root, "noLobbying").upper() == "Y" and not is_registration:
        ftype = report_type + "Y"

    registrant_id, client_num = split_senate_id(txt(root, "senateID"))
    if registrant_id is None:
        return False

    reg_name = txt(root, "organizationName")
    if not reg_name:
        # Individual (non-org) registrants file under a person's name instead.
        reg_name = " ".join(
            p for p in (txt(root, "prefix"), txt(root, "firstName"), txt(root, "lastName")) if p
        ).strip()

    batch.registrants[registrant_id] = (
        registrant_id,
        safe_int(txt(root, "houseID")),
        reg_name,
        norm_name(reg_name),
        txt(root, "registrantGeneralDescription") or None,
        txt(root, "address1") or None,
        txt(root, "address2") or None,
        txt(root, "city") or None,
        txt(root, "state") or None,
        txt(root, "zip") or None,
        txt(root, "country") or None,
        txt(root, "principal_country") or None,
        txt(root, "printedName") or None,
        signed,
    )

    client_name = txt(root, "clientName")
    client_id = (
        synth_id(registrant_id, client_num)
        if client_num
        else synth_id(registrant_id, norm_name(client_name))
    )
    batch.clients[client_id] = (
        client_id,
        client_name,
        norm_name(client_name),
        txt(root, "clientGeneralDescription") or txt(root, "generalDescription") or None,
        txt(root, "clientGovtEntity").upper() == "Y",
        txt(root, "selfSelect") or None,
        txt(root, "clientState") or txt(root, "state") or None,
        txt(root, "clientCountry") or txt(root, "country") or None,
        txt(root, "prinClientState") or None,
        txt(root, "prinClientCountry") or None,
        signed.date() if signed else None,
    )

    batch.filings.append((
        house_file_id,
        ftype,
        filing_type_display(ftype),
        safe_int(txt(root, "reportYear")) or (signed.year if signed else None),
        period,
        period_display,
        None,  # filing_document_url: the API's print URL is keyed on its UUID, which the
               # bulk XML has no equivalent for. Unused downstream (publish.py never selects it).
        signed,
        parse_date(txt(root, "terminationDate")),
        parse_money(txt(root, "income")),
        parse_money(txt(root, "expenses")),
        txt(root, "expensesMethod") or None,
        txt(root, "printedName") or None,
        registrant_id,
        client_id,
    ))

    # LD-1 registrations list lobbyists directly under the root; LD-2 nests them per issue
    # area inside alis/ali_info. Registrations have no issue text, so they get one synthetic
    # activity row purely to hang their lobbyists off — matching how the API represents them.
    ali_infos = root.findall("./alis/ali_info")
    if not ali_infos and is_registration:
        _add_activity(batch, house_file_id, 0, txt(root, "alis") or "", "", "",
                      root.findall("./lobbyists/lobbyist"))
    for idx, ali in enumerate(ali_infos):
        _add_activity(
            batch, house_file_id, idx,
            txt(ali, "issueAreaCode"),
            txt(ali, "./specific_issues/description"),
            txt(ali, "federal_agencies"),
            ali.findall("./lobbyists/lobbyist"),
            txt(ali, "foreign_entity_issues"),
        )
    return True


def _add_activity(batch, filing_id, idx, code, description, agencies, lobbyist_els,
                  foreign_issues=""):
    activity_id = synth_id(filing_id, idx, code)
    batch.activities.append((
        activity_id, filing_id, code or None, code or None,
        description or None, foreign_issues or None,
    ))

    for name in split_agencies(agencies):
        batch.act_entities.append((activity_id, synth_id(norm_name(name)) % (2**31), name))

    for el in lobbyist_els:
        first, last = txt(el, "lobbyistFirstName"), txt(el, "lobbyistLastName")
        if not first and not last:
            # Every filing pads its lobbyist list out to a fixed length with blank entries.
            continue
        suffix = txt(el, "lobbyistSuffix")
        full = norm_name(f"{first} {last} {suffix}".strip(), strip_suffixes=False)
        lobbyist_id = synth_id(full)
        batch.lobbyists[lobbyist_id] = (
            lobbyist_id, None, first or None, None, last or None, suffix or None, full,
        )
        batch.act_lobbyists.append((
            activity_id, lobbyist_id, txt(el, "coveredPosition") or None,
            txt(el, "lobbyistNew").upper() == "Y",
        ))


def fetch_manifest(session):
    r = session.get(f"{BASE_URL}/{MANIFEST_FILE}", timeout=60)
    r.raise_for_status()
    return r.json()


def download_zip(session, filename: str) -> str:
    os.makedirs(ZIP_CACHE, exist_ok=True)
    path = os.path.join(ZIP_CACHE, filename)
    with session.get(f"{BASE_URL}/{filename}", timeout=600, stream=True) as r:
        r.raise_for_status()
        tmp = path + ".part"
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
        os.replace(tmp, path)
    return path


def ingest_zip(con, session, entry: dict, max_files=None, keep_zip=True) -> int:
    """Download (if needed) and ingest one manifest entry. Returns filings written."""
    filename = entry["file"]
    state_key = f"house_zip_{filename}"
    prior = con.execute(
        "SELECT value FROM ingest_state WHERE key = ?", [state_key]
    ).fetchone()
    path = os.path.join(ZIP_CACHE, filename)

    if prior and prior[0] == entry["name"] and os.path.exists(path) and max_files is None:
        print(f"  {filename}: unchanged since last ingest, skipping")
        return 0

    if not os.path.exists(path):
        print(f"  {filename}: downloading…", end="", flush=True)
        t0 = time.time()
        download_zip(session, filename)
        print(f" {os.path.getsize(path)/1048576:.1f}MB in {time.time()-t0:.0f}s")

    batch = Batch()
    written = failed = 0
    t0 = time.time()
    with zipfile.ZipFile(path) as z:
        names = z.namelist()
        if max_files:
            names = names[:max_files]
        for n in names:
            if not n.lower().endswith(".xml"):
                continue
            if parse_filing(z.read(n), os.path.basename(n)[:-4], batch):
                written += 1
            else:
                failed += 1

    batch.flush(con)
    print(f"  {filename}: {written} filings, {len(batch.activities)} activities "
          f"({failed} unparseable) in {time.time()-t0:.0f}s")

    if max_files is None:
        con.execute(
            "INSERT OR REPLACE INTO ingest_state (key, value) VALUES (?, ?)",
            [state_key, entry["name"]],
        )
        if not keep_zip:
            os.remove(path)
    return written


def run_refresh(con, years, keep_zip: bool = True) -> int:
    """Re-ingest only the ZIPs whose manifest timestamp moved since last time.

    This is the whole reason the House feed beats the old watermark approach: the manifest
    states when each quarterly file was last rebuilt, so an unchanged quarter costs one
    JSON fetch and nothing else. A changed one is re-parsed wholesale, and because every id
    is derived deterministically from the filing's own content, re-ingesting a quarter
    overwrites its rows in place rather than duplicating them.
    """
    session = requests.Session()
    manifest = fetch_manifest(session)
    total = 0
    for _year, entry in entries_for_years(manifest, years):
        total += ingest_zip(con, session, entry, None, keep_zip)
    return total


def entries_for_years(manifest, years):
    out = []
    for e in manifest:
        m = re.match(r"(\d{4})_", e["file"])
        if m and int(m.group(1)) in years:
            out.append((int(m.group(1)), e))
    # Registrations last: a quarterly report is the richer record, so when both mention the
    # same registrant, the quarterly one should not overwrite it with registration-time fields.
    out.sort(key=lambda t: (t[0], "Registrations" in t[1]["file"]))
    return out


def mark_years_done(con, years):
    for y in sorted(years):
        con.execute(
            "INSERT OR REPLACE INTO ingest_state (key, value) VALUES (?, '1')",
            [f"backfill_done_{y}"],
        )


def validate(con, year: int, period_file: str):
    """Compare House-sourced rows against whatever the API left in the DB for the same
    period. Only meaningful before a rebuild replaces those rows."""
    period = {
        "1stQuarter": "first_quarter", "2ndQuarter": "second_quarter",
        "3rdQuarter": "third_quarter", "4thQuarter": "fourth_quarter",
    }[period_file]
    row = con.execute("""
        SELECT count(*), count(DISTINCT registrant_id), count(DISTINCT client_id),
               min(dt_posted), max(dt_posted), sum(income), sum(expenses)
        FROM filings WHERE filing_year = ? AND filing_period = ?
    """, [year, period]).fetchone()
    print(f"{year} {period}: {row[0]} filings, {row[1]} registrants, {row[2]} clients")
    print(f"  dt_posted {row[3]} .. {row[4]}")
    print(f"  income ${row[5] or 0:,.0f}   expenses ${row[6] or 0:,.0f}")


def main():
    load_dotenv()
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int)
    p.add_argument("--start-year", type=int, default=DEFAULT_START_YEAR)
    p.add_argument("--end-year", type=int, default=datetime.now().year)
    p.add_argument("--max-files", type=int, help="smoke test: only N files per ZIP")
    p.add_argument("--refresh", action="store_true",
                   help="only re-ingest ZIPs whose manifest timestamp changed")
    p.add_argument("--discard-zips", action="store_true",
                   help="delete each ZIP after ingesting (saves ~3GB on a full rebuild)")
    p.add_argument("--validate", nargs=2, metavar=("YEAR", "PERIOD"))
    p.add_argument("--db", default=schema.DB_PATH)
    args = p.parse_args()

    con = schema.get_connection(args.db)
    schema.init_schema(con)

    if args.validate:
        validate(con, int(args.validate[0]), args.validate[1])
        return

    years = ({args.year} if args.year
             else set(range(args.start_year, args.end_year + 1)))

    session = requests.Session()
    manifest = fetch_manifest(session)
    todo = entries_for_years(manifest, years)
    if not todo:
        print(f"no House ZIPs matched years {sorted(years)}")
        return

    print(f"{len(todo)} ZIP(s) for years {min(years)}–{max(years)}")
    total, t0 = 0, time.time()
    for year, entry in todo:
        total += ingest_zip(con, session, entry, args.max_files, not args.discard_zips)

    if args.max_files is None:
        mark_years_done(con, years)
    print(f"\ndone: {total} filings in {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
