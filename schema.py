"""DuckDB schema for the local LDA filings store (data/lda.duckdb).

Field names below were confirmed against a live response from
https://lda.senate.gov/api/v1/filings/ (Phase 0 spike, 2026-07-28), not guessed from docs.
Two API quirks drove design choices here:

- clients are keyed on the API's `client["id"]` — the GLOBAL client PK (resource-addressable
  at /clients/<id>/). The confusingly-named `client["client_id"]` field is a PER-REGISTRANT
  sequence number (two registrants' "client 12" are different orgs — verified live) and must
  never be used as a key; an early build did and collapsed thousands of unrelated clients.
  Note one real org can still hold several client ids (one per registrant relationship) —
  org-level identity across those is what org_first_seen's name_norm key is for.
- `lobbyist.id` IS a stable numeric id (unlike the Canadian registry, which has no such id and
  has to synthesize one from normalized name) — use it directly as the primary key.
"""

import duckdb

DB_PATH = "data/lda.duckdb"


def get_connection(path: str = DB_PATH) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(path)


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS registrants (
            registrant_id       BIGINT PRIMARY KEY,
            house_registrant_id BIGINT,
            name                VARCHAR,
            name_norm           VARCHAR,
            description         VARCHAR,
            address_1           VARCHAR,
            address_2           VARCHAR,
            city                VARCHAR,
            state               VARCHAR,
            zip                 VARCHAR,
            country             VARCHAR,
            ppb_country         VARCHAR,
            contact_name        VARCHAR,
            dt_updated          TIMESTAMP
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS clients (
            client_id                BIGINT PRIMARY KEY,
            name                     VARCHAR,
            name_norm                VARCHAR,
            general_description      VARCHAR,
            client_government_entity BOOLEAN,
            client_self_select       VARCHAR,
            state                    VARCHAR,
            country                  VARCHAR,
            ppb_state                VARCHAR,
            ppb_country              VARCHAR,
            effective_date           DATE
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS filings (
            filing_uuid          VARCHAR PRIMARY KEY,
            filing_type          VARCHAR,
            filing_type_display  VARCHAR,
            filing_year          INTEGER,
            filing_period        VARCHAR,
            filing_period_display VARCHAR,
            filing_document_url  VARCHAR,
            dt_posted            TIMESTAMP,
            termination_date     DATE,
            income               DOUBLE,
            expenses             DOUBLE,
            expenses_method      VARCHAR,
            posted_by_name       VARCHAR,
            registrant_id        BIGINT,
            client_id            BIGINT
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_filings_dt_posted ON filings(dt_posted)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_filings_client ON filings(client_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_filings_registrant ON filings(registrant_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_filings_year ON filings(filing_year)")

    con.execute("""
        CREATE TABLE IF NOT EXISTS lobbying_activities (
            activity_id           BIGINT PRIMARY KEY,
            filing_uuid           VARCHAR,
            general_issue_code    VARCHAR,
            general_issue_code_display VARCHAR,
            description           VARCHAR,
            foreign_entity_issues VARCHAR
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_activities_filing ON lobbying_activities(filing_uuid)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_activities_issue ON lobbying_activities(general_issue_code)")

    con.execute("""
        CREATE TABLE IF NOT EXISTS activity_government_entities (
            activity_id BIGINT,
            entity_id   INTEGER,
            entity_name VARCHAR
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_age_activity ON activity_government_entities(activity_id)")

    con.execute("""
        CREATE TABLE IF NOT EXISTS lobbyists (
            lobbyist_id BIGINT PRIMARY KEY,
            prefix      VARCHAR,
            first_name  VARCHAR,
            middle_name VARCHAR,
            last_name   VARCHAR,
            suffix      VARCHAR,
            name_norm   VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS activity_lobbyists (
            activity_id      BIGINT,
            lobbyist_id      BIGINT,
            covered_position VARCHAR,
            is_new           BOOLEAN
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_al_activity ON activity_lobbyists(activity_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_al_lobbyist ON activity_lobbyists(lobbyist_id)")

    # Populated by normalize.py's tag_topics() against a hand-maintained topics.json dictionary.
    con.execute("""
        CREATE TABLE IF NOT EXISTS activity_topics (
            activity_id BIGINT,
            topic_key   VARCHAR
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_at_activity ON activity_topics(activity_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_at_topic ON activity_topics(topic_key)")

    # "First-time organization" detection — a filing counts as first-time only when its
    # dt_posted equals its client's global first-seen date, not merely the first filing_uuid
    # for that client_id (client_id can persist across a long-standing org just hiring a new
    # registrant/firm, which should NOT count as a debut).
    con.execute("""
        CREATE TABLE IF NOT EXISTS org_first_seen (
            name_norm       VARCHAR PRIMARY KEY,
            first_date      DATE,
            display_name    VARCHAR,
            first_client_id BIGINT
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS ingest_state (
            key   VARCHAR PRIMARY KEY,
            value VARCHAR
        )
    """)


if __name__ == "__main__":
    con = get_connection()
    init_schema(con)
    print("schema initialized at", DB_PATH)
