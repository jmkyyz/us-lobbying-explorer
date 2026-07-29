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

## 2. Cloudflare Pages (frontend)

1. Dashboard → Workers & Pages → **Create → Pages → Connect to Git** →
   `jmkyyz/us-lobbying-explorer`.
2. Build settings: **no build command**, output directory = `web`.
3. Every push to `main` auto-deploys.

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

The daily job pulls new filings, re-stages parquet, and — once the `R2_*` vars are in
`.env` — uploads automatically. Browsers pick up new data via the `no-cache` manifest +
`?v=<version>` cache-busting; no redeploy needed for data updates.

## Gotchas (learned on sibling apps, pre-applied here)

- r2.dev sends no Cache-Control by default; `publish.py` sets `no-cache` on the manifest
  and `max-age=3600` on parquet explicitly.
- The frontend fetches the manifest with `{cache:"no-cache"}` and appends `?v=<version>`
  to parquet URLs — do not remove either (stale-manifest incident on the trade app).
- Keep parquet consolidated (one file per slice): many small files over R2 = hundreds of
  range requests per query (~1 min/query regression on the trade app).
- `R2_ACCOUNT_ID` is the bare 32-hex id; pasting the full endpoint URL breaks boto3.
