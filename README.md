# US Lobbying Explorer

Search, browse and track U.S. federal lobbying disclosures filed under the Lobbying
Disclosure Act — the single joint filing system both the House Clerk and the Secretary of the
Senate receive, so it covers House and Senate lobbying alike.

**Live: https://us-lobbying.jasonkirby.workers.dev**

The Topic Explorer answers "how much lobbying mentions tariffs (or Canada, or softwood
lumber), how has it moved over time, and which clients are behind it" — with drill-down into
the exact text each client filed.

## Architecture

No server. A local Python pipeline parses the House Clerk's bulk LDA filings into DuckDB,
exports consolidated Parquet, and publishes to Cloudflare R2. The frontend (`web/index.html`)
queries those Parquet files **in the browser** via DuckDB-WASM, so every filter and chart is
client-side.

```
House bulk XML → ingest_house.py → data/lda.duckdb → publish.py → Parquet + manifest → R2
                                                                                        ↓
                                          web/index.html (DuckDB-WASM, queries in-browser)
                                          served by Cloudflare Workers (wrangler.jsonc)
```

**Why bulk XML and not the REST API?** The pipeline used the lda.gov REST API until August
2026, when it began returning HTTP 403 to every non-browser client. The API is not down — it
answers normally in a browser — but Akamai fingerprints the client below the HTTP layer, so
curl and python-requests are refused no matter what key or headers they send. The House Clerk
publishes the same filings as quarterly XML ZIPs and kept those downloads alive past the
July 31 2026 sunset of the old site. They are also simply a better source here: one ~47MB
quarterly file replaces roughly 2.6 hours of 25-rows-per-page API pagination, no key is
needed, and the download manifest timestamps each file so a refresh knows exactly what
changed. `ingest.py` (the API path) is kept for the day access returns.

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
refresh. **No API key required** — the House bulk downloads are public.

```bash
pip install -r requirements.txt
cp .env.example .env                  # R2_* only, and only if you publish
python3 ingest_house.py --start-year 2013 --end-year 2026   # ~1.5h, ~3GB of ZIPs
python3 publish.py --stage            # writes data/publish/parquet/
python3 publish.py --upload           # optional: sync to R2 (needs R2_* in .env)
```

The House publishes back to 2002, but the frontend treats 2013 as the start of history (it
gates the first-time-org badge on contiguous coverage), so that is the default range. Pass
`--start-year 2002` to go deeper, `--discard-zips` to delete each ZIP after parsing, and
`--max-files 300` for a smoke test. Re-running is safe: every id is derived deterministically
from the filing itself, so a re-ingested quarter overwrites in place rather than duplicating.

`refresh.py` does incremental ingest → stage → upload for scheduled runs, re-parsing only the
quarters whose manifest timestamp moved (current + previous year by default);
`make_refresh_plist.py` generates the launchd job.

## What's in git vs. not

Committed: all code, the DuckDB-WASM engine (`vendor/`, `web/static/vendor/`), topic
definitions, deploy config.

Gitignored: `.env` (R2 credentials), `data/lda.duckdb`, `data/publish/`, `data/house_zips/`.
The data is reproducible from the House bulk downloads, and the published copy lives in R2.

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
