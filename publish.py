"""DuckDB -> consolidated Parquet (--stage) -> Cloudflare R2 (--upload).

One file per slice, not per year/period — a per-year-file layout was tried and abandoned in
the sibling Canada Trade Explorer app: it meant DuckDB-WASM made hundreds of small HTTP range
requests per query in production (~1min/query over R2) until consolidated into one big file
per slice with a large ROW_GROUP_SIZE. Applying that lesson here from day one.
"""

import argparse
import json
import os
import time

import schema
from normalize import tag_topics, build_org_first_seen

STAGE_DIR = "data/publish/parquet"
WASM_PATH = "vendor/duckdb-eh.wasm"
ROW_GROUP_SIZE = 500_000

# Every visitor downloads these files in full before the app is usable, so bytes here are
# the single biggest lever on load time. Level 19 costs minutes of publish time once a day
# and takes activities.parquet from 145MB to 74MB; zstd decompression speed does not vary
# with the level it was compressed at, so queries are unaffected (measured: 0.42s vs 0.43s
# for a full description scan).
COMPRESSION_LEVEL = 19


def stage(con) -> dict:
    os.makedirs(STAGE_DIR, exist_ok=True)

    tagged = tag_topics(con)
    first_seen = build_org_first_seen(con)
    print(f"tag_topics: {tagged} activity-topic rows; build_org_first_seen: {first_seen} orgs")

    # One display name per organisation.
    #
    # The frontend's client list does GROUP BY client_name (deliberately — one org holds a
    # separate client_id per registrant relationship), so any org whose name is spelled two
    # ways renders as two separate organisations. Filers retype their own name every quarter
    # and are inconsistent about capitalisation, and the two upstream sources differ
    # systematically as well: the old REST API uppercased names ("THE HOME DEPOT") while the
    # House bulk XML preserves what the filer typed ("The Home Depot"). Left alone, that split
    # 66% of recently-active orgs in two.
    #
    # Resolve it once here rather than in each query: every filing for an org gets the same
    # rendering — the spelling that org's filings use most often, most recent breaking ties.
    for tbl, key in (("clients", "client_id"), ("registrants", "registrant_id")):
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE {tbl}_display AS
            SELECT name_norm, display FROM (
                SELECT t.name_norm, t.name AS display,
                       row_number() OVER (PARTITION BY t.name_norm
                                          ORDER BY count(*) DESC, max(f.dt_posted) DESC) rn
                FROM filings f JOIN {tbl} t ON t.{key} = f.{key}
                WHERE t.name IS NOT NULL AND t.name_norm IS NOT NULL AND t.name_norm != ''
                GROUP BY t.name_norm, t.name
            ) WHERE rn = 1
        """)
    merged = con.execute("""
        SELECT count(*) FROM (SELECT name_norm FROM clients WHERE name_norm != ''
                              GROUP BY 1 HAVING count(DISTINCT name) > 1)
    """).fetchone()[0]
    print(f"canonical display names: collapsed {merged} multi-spelling client orgs")

    con.execute(f"""
        COPY (
            WITH issues AS (
                SELECT filing_uuid, list_distinct(list(general_issue_code)) AS issue_codes
                FROM lobbying_activities
                GROUP BY filing_uuid
            ),
            entities AS (
                SELECT la.filing_uuid, list_distinct(list(age.entity_name)) AS government_entities
                FROM lobbying_activities la
                JOIN activity_government_entities age ON age.activity_id = la.activity_id
                GROUP BY la.filing_uuid
            )
            SELECT
                f.filing_uuid, f.filing_type, f.filing_type_display, f.filing_year,
                f.filing_period, f.filing_period_display, f.dt_posted, f.termination_date,
                f.income, f.expenses,
                coalesce(rd.display, r.name) AS registrant_name, f.registrant_id,
                coalesce(cd.display, c.name) AS client_name, f.client_id, c.state AS client_state,
                c.country AS client_country,
                i.issue_codes,
                coalesce(e.government_entities, []) AS government_entities,
                coalesce(ofs.first_date = f.dt_posted::DATE, false) AS is_first_time_org
            FROM filings f
            LEFT JOIN issues i ON i.filing_uuid = f.filing_uuid
            LEFT JOIN entities e ON e.filing_uuid = f.filing_uuid
            LEFT JOIN registrants r ON r.registrant_id = f.registrant_id
            LEFT JOIN clients c ON c.client_id = f.client_id
            LEFT JOIN clients_display cd ON cd.name_norm = c.name_norm
            LEFT JOIN registrants_display rd ON rd.name_norm = r.name_norm
            LEFT JOIN org_first_seen ofs ON ofs.name_norm = c.name_norm
            ORDER BY f.dt_posted DESC
        ) TO '{STAGE_DIR}/filings.parquet'
        (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL {COMPRESSION_LEVEL}, ROW_GROUP_SIZE {ROW_GROUP_SIZE})
    """)

    # Activities are written as TWO files, because description alone is 72% of the bytes and
    # nothing needs it until someone runs a text search or opens a client. The chart matches
    # predefined topics on the precomputed `topics` column, so the default view never touches
    # description at all. The frontend loads the core file up front and pulls the text file
    # only on first use, which is what takes first load from ~131MB to ~78MB.
    #
    # Materialised to a temp table first so BOTH files get the same rid for the same row.
    # Assigning row numbers separately per COPY would rely on two queries producing identical
    # ordering, which nothing guarantees — and a silent mismatch would attach every filing's
    # text to the wrong activity.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _activities_out AS
        WITH topics_agg AS (
            SELECT activity_id, list(topic_key) AS topics
            FROM activity_topics
            GROUP BY activity_id
        )
        SELECT
            row_number() OVER (ORDER BY f.dt_posted DESC) - 1 AS rid,
            la.filing_uuid, la.general_issue_code, la.description,
            coalesce(ta.topics, []) AS topics,
            f.filing_year, f.filing_period, f.filing_period_display, f.dt_posted,
            f.client_id, coalesce(cd.display, c.name) AS client_name,
            f.registrant_id, coalesce(rd.display, r.name) AS registrant_name
        FROM lobbying_activities la
        JOIN filings f ON f.filing_uuid = la.filing_uuid
        LEFT JOIN topics_agg ta ON ta.activity_id = la.activity_id
        LEFT JOIN clients c ON c.client_id = f.client_id
        LEFT JOIN registrants r ON r.registrant_id = f.registrant_id
        LEFT JOIN clients_display cd ON cd.name_norm = c.name_norm
        LEFT JOIN registrants_display rd ON rd.name_norm = r.name_norm
    """)

    con.execute(f"""
        COPY (
            SELECT rid, filing_uuid, general_issue_code, topics, filing_year, filing_period,
                   filing_period_display, dt_posted, client_id, client_name,
                   registrant_id, registrant_name
            FROM _activities_out ORDER BY rid
        ) TO '{STAGE_DIR}/activities.parquet'
        (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL {COMPRESSION_LEVEL}, ROW_GROUP_SIZE {ROW_GROUP_SIZE})
    """)

    con.execute(f"""
        COPY (SELECT rid, description FROM _activities_out ORDER BY rid)
        TO '{STAGE_DIR}/activity_text.parquet'
        (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL {COMPRESSION_LEVEL}, ROW_GROUP_SIZE {ROW_GROUP_SIZE})
    """)

    con.execute(f"""
        COPY (SELECT o.name_norm, o.first_date,
                     coalesce(cd.display, o.display_name) AS display_name, o.first_client_id
              FROM org_first_seen o
              LEFT JOIN clients_display cd ON cd.name_norm = o.name_norm
              ORDER BY o.first_date DESC)
        TO '{STAGE_DIR}/org_first_seen.parquet'
        (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL {COMPRESSION_LEVEL})
    """)

    counts = {
        t: con.execute(
            f"SELECT count(*) FROM '{STAGE_DIR}/{t}.parquet'"
        ).fetchone()[0]
        for t in ["filings", "activities", "org_first_seen", "activity_text"]
    }
    # The two activity files are joined by rid on the client, so a row-count mismatch would
    # silently drop or mis-pair description text. Cheap to assert, expensive to discover later.
    if counts["activities"] != counts["activity_text"]:
        raise RuntimeError(
            f"activities ({counts['activities']}) and activity_text "
            f"({counts['activity_text']}) row counts differ — refusing to publish"
        )
    date_range = con.execute(
        f"SELECT min(dt_posted), max(dt_posted) FROM '{STAGE_DIR}/filings.parquet'"
    ).fetchone()

    # Which filing_years have a COMPLETE backfill (per ingest_state done-flags)? The frontend
    # uses this to (a) break the trend line across un-backfilled gap years instead of plotting
    # fabricated zeros, and (b) decide whether "first-time org" detection is trustworthy —
    # a debut computed against gapped history is wrong (an org that started lobbying inside
    # the gap looks like a debut when it reappears), so the badge only shows when the loaded
    # years form a contiguous run.
    years_covered = sorted(
        int(k.rsplit("_", 1)[1])
        for (k,) in con.execute(
            "SELECT key FROM ingest_state WHERE key LIKE 'backfill_done_%' AND value = '1'"
        ).fetchall()
    )
    # Reliable only when the covered years are contiguous AND reach back to the intended
    # start of history — "first-time" measured against a window that only starts in 2024
    # would flag long-established lobbying orgs as debuts.
    EARLIEST_INTENDED_YEAR = 2013
    first_time_reliable = (
        len(years_covered) > 0
        and years_covered[-1] - years_covered[0] + 1 == len(years_covered)
        and years_covered[0] <= EARLIEST_INTENDED_YEAR
    )

    manifest = {
        "version": int(time.time()),
        "built_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "files": {
            "filings": "filings.parquet",
            "activities": "activities.parquet",
            "org_first_seen": "org_first_seen.parquet",
        },
        # Fetched on demand, not at startup. An older frontend simply ignores this key and
        # keeps working — it just never gets description, so keep `files` above eager-only.
        "lazy_files": {
            "activity_text": "activity_text.parquet",
        },
        "row_counts": counts,
        "date_range": {
            "min_dt_posted": str(date_range[0]) if date_range[0] else None,
            "max_dt_posted": str(date_range[1]) if date_range[1] else None,
        },
        "years_covered": years_covered,
        "first_time_reliable": first_time_reliable,
    }
    with open(f"{STAGE_DIR}/manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)

    print("staged:", json.dumps(manifest, indent=2))
    return manifest


def upload():
    import boto3

    account_id = os.environ["R2_ACCOUNT_ID"]
    bucket = os.environ["R2_BUCKET"]
    prefix = os.environ.get("R2_PREFIX", "lda").rstrip("/")

    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
    )

    local_files = [f for f in os.listdir(STAGE_DIR) if not f.startswith(".")]

    # Sync: delete stale data files sitting DIRECTLY under the prefix. Never touch
    # subdirectories — lda/vendor/ holds the duckdb wasm binary, which isn't part of the
    # staged data set and was silently deleted by an earlier version of this sweep.
    existing = s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/").get("Contents", [])
    keep_keys = {f"{prefix}/{name}" for name in local_files}
    stale = [
        obj["Key"] for obj in existing
        if obj["Key"] not in keep_keys and "/" not in obj["Key"][len(prefix) + 1:]
    ]
    if stale:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in stale]})
        print(f"deleted {len(stale)} stale object(s)")

    for name in local_files:
        key = f"{prefix}/{name}"
        cache_control = "no-cache" if name == "manifest.json" else "public, max-age=3600"
        content_type = "application/json" if name.endswith(".json") else "application/octet-stream"
        s3.upload_file(
            f"{STAGE_DIR}/{name}", bucket, key,
            ExtraArgs={"CacheControl": cache_control, "ContentType": content_type},
        )
        print(f"uploaded {key}")

    # The duckdb wasm engine ships from R2 too (34MB exceeds Cloudflare Workers' 25MiB
    # per-asset cap). Upload it if missing so a fresh bucket is self-sufficient; it's
    # immutable, so skip the transfer when it's already there.
    wasm_key = f"{prefix}/vendor/{os.path.basename(WASM_PATH)}"
    try:
        s3.head_object(Bucket=bucket, Key=wasm_key)
    except s3.exceptions.ClientError:
        s3.upload_file(
            WASM_PATH, bucket, wasm_key,
            ExtraArgs={"ContentType": "application/wasm",
                       "CacheControl": "public, max-age=31536000"},
        )
        print(f"uploaded {wasm_key}")


def main():
    from ingest import load_dotenv
    load_dotenv()  # R2_* creds live in .env; direct CLI runs need them too, not just refresh.py

    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", action="store_true")
    parser.add_argument("--upload", action="store_true")
    parser.add_argument("--db", default=schema.DB_PATH)
    args = parser.parse_args()

    if not args.stage and not args.upload:
        parser.error("pass --stage and/or --upload")

    if args.stage:
        con = schema.get_connection(args.db)
        stage(con)

    if args.upload:
        upload()


if __name__ == "__main__":
    main()
