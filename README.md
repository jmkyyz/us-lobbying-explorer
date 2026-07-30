# US Lobbying Explorer

Search, browse and track U.S. federal lobbying disclosures from the Senate LDA database —
the single joint filing system both the House Clerk and the Secretary of the Senate receive,
so it covers House and Senate lobbying alike.

**Live: https://us-lobbying.jasonkirby.workers.dev**

The Topic Explorer answers "how much lobbying mentions tariffs (or Canada, or softwood
lumber), how has it moved over time, and which clients are behind it" — with drill-down into
the exact text each client filed.

## Architecture

No server. A local Python pipeline pulls the LDA REST API into DuckDB, exports consolidated
Parquet, and publishes to Cloudflare R2. The frontend (`web/index.html`) queries those
Parquet files **in the browser** via DuckDB-WASM, so every filter and chart is client-side.

```
LDA REST API → ingest.py → data/lda.duckdb → publish.py → Parquet + manifest → Cloudflare R2
                                                                                     ↓
                                          web/index.html (DuckDB-WASM, queries in-browser)
                                          served by Cloudflare Workers (wrangler.jsonc)
```

## Just want to use it?

Open the live URL. Nothing to install. First load pulls ~215MB (engine + data); everything
after that is instant.

## Clone and run locally

```bash
git clone https://github.com/jmkyyz/us-lobbying-explorer.git
cd us-lobbying-explorer
python3 -m http.server 8010          # serve from the REPO ROOT, not web/
# open http://localhost:8010/web/index.html
```

That works immediately with **no data files and no API key** — the frontend falls back to
the public R2 bucket when no local Parquet is present. (Serve from the repo root: the page
loads the WASM engine from `vendor/`, one level above `web/`.)

To point at a different data source: `?data=<base-url>`.

## Rebuilding the data yourself (optional)

Only needed to change the pipeline, add topics that require re-tagging, or run your own
refresh. Requires a free LDA API key — register at https://lda.senate.gov/api/register/
(manual signup; the key raises the rate limit from 15 to 120 requests/min).

```bash
pip install -r requirements.txt
cp .env.example .env                  # add LDA_API_KEY (+ R2_* only if you publish)
python3 ingest.py --backfill --start-year 2024 --end-year 2026   # ~3h for 260k filings
python3 publish.py --stage            # writes data/publish/parquet/
python3 publish.py --upload           # optional: sync to R2 (needs R2_* in .env)
```

Full history (1999 onward) is available; 2013–2026 is ~1.16M filings, a ~3GB DuckDB file and
~215MB of published Parquet. Backfills are resumable — interrupt and re-run the same command.

`refresh.py` does incremental ingest → stage → upload for scheduled runs;
`make_refresh_plist.py` generates the launchd job.

## What's in git vs. not

Committed: all code, the DuckDB-WASM engine (`vendor/`, `web/static/vendor/`), topic
definitions, deploy config.

Gitignored: `.env` (API key, R2 credentials), `data/lda.duckdb`, `data/publish/`. The data is
reproducible from the API, and the published copy lives in R2.

## Topics

`web/topics.json` defines each topic as keywords matched against filing description text
(word-boundary, case-insensitive), optionally pre-filtered by official LDA issue codes.
Editing it requires only `publish.py --stage --upload` — no re-ingest.

Topics are **text-derived**: they measure what lobbyists wrote, not ground truth. There is no
country code in the LDA schema, so country topics like Canada are keyword-only and
recall-limited. See `DEPLOY.md` for deployment; see the commit history for methodology
decisions (e.g. why `usmca`/`nafta` were removed from the Canada topic, then partly restored).

## Ad-hoc search syntax

The Topic Explorer's search box supports `AND` · `OR` · `NOT` (or `-word`) ·
`"exact phrase"` · `(grouping)`, plus `code:TAR` to filter by official issue code.
Spaces mean AND. Example: `code:TRD AND canada -china`.
