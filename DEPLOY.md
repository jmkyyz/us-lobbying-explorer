# Deploying to Cloudflare (R2 + Pages)

Same architecture as the Canada Trade Explorer: parquet in R2, static frontend on
Cloudflare, all querying in the visitor's browser. No server.

## 1. R2 bucket (data)

1. Cloudflare dashboard → R2 → **Create bucket** → name `us-lobbying` (any name works).
2. **Settings → Public access → Allow** (r2.dev subdomain) — note the `https://pub-….r2.dev` URL.
3. **Settings → CORS policy** — required for DuckDB-WASM's range requests:

```json
[
  {
    "AllowedOrigins": ["https://<your-pages-domain>", "http://localhost:8010"],
    "AllowedMethods": ["GET", "HEAD"],
    "AllowedHeaders": ["Range"],
    "ExposeHeaders": ["Content-Range", "Accept-Ranges", "Content-Length"],
    "MaxAgeSeconds": 3600
  }
]
```

4. R2 → **Manage API tokens → Create token** (Object Read & Write, scoped to this bucket).
5. Add to `.env` (never committed):

```
R2_ACCOUNT_ID=<32-hex account id, NOT the full endpoint URL>
R2_ACCESS_KEY_ID=…
R2_SECRET_ACCESS_KEY=…
R2_BUCKET=us-lobbying
R2_PREFIX=lda
```

6. Upload: `python3 publish.py --stage --upload`
7. Verify: `curl -sI https://pub-….r2.dev/lda/manifest.json` → 200 with `cache-control: no-cache`.

## 2. Cloudflare Workers (frontend)

Live at https://us-lobbying.jasonkirby.workers.dev — served as **Workers static assets**
via `wrangler.jsonc`, not Pages. Cloudflare no longer offers the classic Pages
connect-to-Git flow for new projects, so these steps describe what is actually deployed:

1. Dashboard → Workers & Pages → project `us-lobbying` → **Build** section, connected to
   `jmkyyz/us-lobbying-explorer`.
2. No build command; `wrangler.jsonc` declares the asset directory.
3. Every push to `main` auto-deploys.

The 34MB DuckDB-WASM engine is served from R2, not from the Worker — it exceeds Workers'
25MiB per-asset cap. `publish.py --upload` puts it there on first run.

## 3. Point the frontend at R2

`web/index.html` resolves its data base as
`?data= param → window.DATA_BASE → ../data/publish/parquet` (local dev fallback).
For production, set the R2 URL as the non-localhost default near the top of the script:

```js
const DATA_BASE = params.get('data') || window.DATA_BASE ||
  (location.hostname === 'localhost' ? '../data/publish/parquet'
                                     : 'https://pub-XXXX.r2.dev/lda');
```

(One-line edit once the pub URL exists; commit + push and Pages redeploys.)

## 4. Recurring refresh (local Mac → R2)

```
python3 make_refresh_plist.py     # writes the launchd plist; prints install commands
```

The daily job re-parses any House quarterly ZIP whose manifest timestamp moved, re-stages
parquet, and — once the `R2_*` vars are in `.env` — uploads automatically. Browsers pick up
new data via the `no-cache` manifest + `?v=<version>` cache-busting; no redeploy needed for
data updates. No API key is involved: the source is the House Clerk's public bulk downloads.

Check what a run would do without doing it:

```
python3 refresh.py --dry-run              # lists each ZIP as unchanged / WOULD RE-INGEST
python3 refresh.py --years 2013-2026      # widen beyond the default current+previous year
```

**Watch for silent staleness.** The job logs `!!! REFRESH FAILED !!!` on any error, but
nothing alerts on it — an earlier API-side block failed every morning for 22 days unnoticed.
A cheap health check, which needs no download:

```
curl -s https://pub-….r2.dev/lda/manifest.json | python3 -c \
  "import json,sys; m=json.load(sys.stdin); print(m['built_at'], m['date_range'])"
```

## Gotchas (learned on sibling apps, pre-applied here)

- r2.dev sends no Cache-Control by default; `publish.py` sets `no-cache` on the manifest
  and `max-age=3600` on parquet explicitly.
- The frontend fetches the manifest with `{cache:"no-cache"}` and appends `?v=<version>`
  to parquet URLs — do not remove either (stale-manifest incident on the trade app).
- Keep parquet consolidated (one file per slice): many small files over R2 = hundreds of
  range requests per query (~1 min/query regression on the trade app).
- `R2_ACCOUNT_ID` is the bare 32-hex id; pasting the full endpoint URL breaks boto3.
