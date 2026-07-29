# US Lobbying Explorer

Search, browse, and track U.S. federal lobbying disclosures (Senate LDA database — the
joint House/Senate lobbying disclosure system) with a topic-level drill-down: e.g. how much
lobbying activity mentions tariffs, or references Canada, and who's behind it.

Static site — no server. A local Python pipeline pulls from the LDA REST API into DuckDB,
exports consolidated Parquet, and publishes it to Cloudflare R2. The frontend
(`web/index.html`) queries the Parquet files directly in the browser via DuckDB-WASM.

## Pipeline

```
ingest.py --backfill --filing-year 2024   # pull filings from the LDA API into data/lda.duckdb
ingest.py --since <timestamp>             # incremental pull (used by refresh.py)
publish.py --stage                        # DuckDB -> consolidated Parquet under data/publish/
publish.py --upload                       # sync data/publish/parquet to Cloudflare R2
refresh.py                                # incremental ingest + publish, launchd-scheduled
```

## Local dev

```
pip install -r requirements.txt
cp .env.example .env   # fill in LDA_API_KEY (register at https://lda.gov/api/register/)
python3 ingest.py --backfill --filing-year 2024
python3 publish.py --stage
cd web && python3 -m http.server 8010
# open http://localhost:8010/?r2=http://localhost:8010/../data/publish/parquet
```

See `../.claude/plans/adaptive-tumbling-gray.md` (StatCanApp repo, this app's design doc) for
the full architecture writeup.
