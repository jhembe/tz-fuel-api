# Tanzania Fuel Price API — CLAUDE.md

## What this project is
Single-file FastAPI service that scrapes EWURA (Tanzania's energy regulator) monthly PDF price bulletins and exposes them as a clean REST API. Covers petrol, diesel, kerosene across all mainland + Zanzibar districts, with full price history back to 2009.

## File layout
```
main.py                  # entire application — models, scraper, routes
requirements.txt
.env / .env.example      # DATABASE_URL, ADMIN_SECRET_KEY, RATE_LIMIT_PER_MINUTE, CACHE_TTL_SECONDS
Dockerfile               # multi-stage, non-root, HEALTHCHECK
docker-compose.yml       # API + Postgres 16
tz-fuel-api.service      # hardened systemd unit (www-data, PrivateTmp, no-new-privs)
nginx.conf               # reverse proxy to 127.0.0.1:8000
deploy.sh                # one-shot Ubuntu 24.04 deployment script
```

## Running locally (SQLite, zero config)
```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # edit ADMIN_SECRET_KEY at minimum
export $(grep -v '^#' .env | grep -v '^$' | xargs)   # .env is NOT auto-loaded by uvicorn
venv/bin/uvicorn main:app --host 127.0.0.1 --port 8888 --reload
# → http://127.0.0.1:8888/docs
# Note: ports 8000 and 8001 are occupied by other Docker services on this host
```

## Running with Docker
```bash
ADMIN_SECRET_KEY=mysecret docker compose up --build
```

## Production deploy (Ubuntu 24.04)
```bash
sudo bash deploy.sh
# Installs system deps, venv, sets up systemd + nginx, prints health status
```

## Triggering the first data sync
```bash
# Latest bulletin only:
curl -X POST http://127.0.0.1:8888/api/v1/admin/trigger-sync \
     -H "X-API-KEY: <ADMIN_SECRET_KEY>"

# Full historical backfill (2009 → present, ~10 min):
curl -X POST http://127.0.0.1:8888/api/v1/admin/backfill \
     -H "X-API-KEY: <ADMIN_SECRET_KEY>"

# Monitor backfill progress:
curl http://127.0.0.1:8888/api/v1/admin/backfill/status \
     -H "X-API-KEY: <ADMIN_SECRET_KEY>"

# Monitor sync logs:
curl http://127.0.0.1:8888/api/v1/admin/sync-logs \
     -H "X-API-KEY: <ADMIN_SECRET_KEY>"
```

## Key architecture decisions

### DB URL handling
`_resolve_db_url()` in `main.py` rewrites `postgres://` and `postgresql://` to `postgresql+psycopg2://` automatically — required for Supabase connection strings. SQLite is the default fallback.

### District deduplication
EWURA PDFs use inconsistent district names ("Tanga CC", "Tanga", "tanga cc"). The `district_key()` function normalises → aliases → strips suffixes to produce a stable unique key stored in `RegionDistrict.district_key`. Never use raw name as the dedup key.

### Historical data model
`FuelPrice` has a `(district_id, effective_date)` unique constraint — each row is one district for one pricing period. This supports full history. The backfill imports every PDF listed on the EWURA page, skipping months already in DB. As of June 2026: ~19,900 records across 130 pricing periods (2011–2026). 2022 Jul–Dec is temporarily missing (accidentally deleted during 2009 data cleanup — will recover when EWURA server is reachable via `POST /api/v1/admin/backfill`).

### PDF table formats
EWURA has used two table structures over the years. `parse_ewura_pdf()` detects both:
- **Format A (2024–present):** 5+ cols — `SN | Town | Petrol | Diesel | Kerosene` (many None cells from merged columns)
- **Format B (2021–2023):** 4 cols — `Town | Petrol | Diesel | Kerosene` (no SN or region column)

The `len(cells) < 4` guard (not `< 5`) allows Format B rows through. Format B is identified by the 4-cell path when Format A fails.

Always call `db.flush()` after each `FuelPrice` insert — without it the SELECT check for existing records within the same unflushed transaction can miss pending inserts and hit UNIQUE constraint.

### Effective date extraction
`extract_date_from_pdf()` scans the first two pages for patterns (highest to lowest priority):
1. `"Effective Date: 1st July 2026"` — explicit label
2. `"Effective from DD/MM/YYYY"` — numeric
3. `"EFFECTIVE WEDNESDAY, 5th NOVEMBER 2025"` — weekday between keyword and date
4. `"TAREHE 7 FEBRUARI 2024"` / `"5th NOVEMBER 2025"` — Swahili tarehe prefix or plain ordinal
5. `"Mwezi Julai 2026"` — Swahili month-year
6. ISO `YYYY-MM-DD`

**Do NOT add a `PPR/YEAR - NN/NN` ref-code pattern.** The `NN` after the dash is a document category code, not the month number — it caused all 2024 PDFs to be misdated as January 2024.

In the backfill, never fall back to `date.today()` if extraction returns None — skip the PDF instead.

### Backfill logic
`background_backfill_task()` iterates all scored PDF URLs from the EWURA page (oldest-first), HEAD-probes each, skips 404s and already-imported dates, and writes a `SyncLog` entry per PDF. Progress is exposed at `GET /api/v1/admin/backfill/status` without needing the X-API-KEY (read-only); the trigger endpoint does require it.

The `_backfill_state` dict is module-level — one active job at a time. A second POST while running returns `already_running` with current progress.

### Caching
In-process TTL dict (`_CACHE`). Cleared automatically after every successful sync. TTL configurable via `CACHE_TTL_SECONDS` env var (default 300 s). Flush manually: `DELETE /api/v1/admin/cache`.

### Rate limiting
Per-IP token bucket in `_check_rate_limit()`. Configurable via `RATE_LIMIT_PER_MINUTE` (default 120). Stored in `_RATE_BUCKETS` dict — resets if process restarts.

### EWURA scraper quirks
- **June 2026** link on the EWURA page is currently 404 — `fetch_ewura_pdf_url()` HEAD-probes all candidates and returns the first reachable one (May 2026 as of June 2026)
- Some URLs on the EWURA page are truncated (missing `.pdf`) — these 404 and are skipped
- EWURA sometimes uploads the same month's PDF twice with different filenames — the `existing_dates` set in the backfill deduplicates by effective date
- 2009 bulletins were fortnightly, not monthly — expect multiple dates per calendar month
- **2009/2010 PDFs use a different table format** that `parse_ewura_pdf()` cannot handle — it misidentifies numeric cell values as district names, creating fake `region_districts` entries like `"1084.00"`. These can be cleaned with `DELETE FROM fuel_prices fp USING region_districts rd WHERE fp.district_id=rd.id AND rd.district_name ~ '^[0-9,\\.]+$'` — but be careful: this regex also matches any legitimate fuel_price records linked to those fake IDs.

### Wayback Machine backfill (2011–2020)
Old EWURA WordPress paths (`/wp-content/uploads/`) all 404 on the live site but are accessible via `https://web.archive.org/web/2021/{original_url}`. Scripts for this are in `/opt/apps/fuel/tz-fuel-api/backfill_historical.py` and friends. Use at least 4s delay between requests to avoid WB rate limiting; 8s is reliable. The WB CDX API can find archived URLs but itself rate-limits on large queries.

## Environment variables
| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `sqlite:///./tanzania_fuel.db` | DB connection string |
| `ADMIN_SECRET_KEY` | *(empty — disables admin)* | `X-API-KEY` value for admin endpoints |
| `RATE_LIMIT_PER_MINUTE` | `120` | Per-IP request cap |
| `CACHE_TTL_SECONDS` | `300` | Response cache lifetime |

## Frontend — fuel.mahembega.com

React SPA at `/opt/apps/tz-fuel-ui`. Built with Vite + React 18 + TypeScript + Tailwind CSS.
API served at `fuelapi.mahembega.com` → proxied by nginx to `127.0.0.1:8888`.

### Pages
| Route | Description |
|---|---|
| `/` | Overview — national stats strip, latest price table, sparkline summary |
| `/map` | Interactive Tanzania district choropleth map; toggle petrol/diesel/kerosene; click district → history popup |
| `/trends` | Area/line charts for full 2009–present price history; date range brush; MoM change bars |
| `/analytics` | Volatility ranking, regional gap chart, inflation index (base-date picker), forecast with CI bands, correlation matrix |
| `/regions` | Regional breakdown — avg prices by region for latest period; drilldown to districts |
| `/districts` | Searchable/sortable district table with mini sparklines |
| `/compare` | Side-by-side district comparison (up to 3 districts) with overlaid history charts |
| `/docs` | Full API documentation with live try-it-out for every endpoint |

### Design system
- **Light mode primary** — warm white `#FAFAF9` background, slate text `#1E293B`
- **Dark mode secondary** — `#0F172A` background, `#1E293B` cards
- **Accent** — Tanzanian gold `#F59E0B` (amber-400)
- **Fuel colors** — Petrol: `#3B82F6` (blue), Diesel: `#10B981` (emerald), Kerosene: `#F59E0B` (amber)
- Charts: Recharts; Map: React-Leaflet + leaflet.heat; Animations: Framer Motion
- Data fetching: TanStack Query v5 with 5-min stale time

### Build & serve
```bash
cd /opt/apps/tz-fuel-ui
npm install
npm run build
# Output: dist/ — served by nginx as static files
```

### nginx (fuel.mahembega.com)
Static SPA — all paths → `dist/index.html` with SPA fallback. Gzip + cache headers on assets.

### New backend endpoints added for frontend
- `GET /api/v1/analytics/regional-summary` — avg petrol/diesel/kerosene grouped by region for latest (or given) date

## Analytics endpoints

Seven deep-analysis endpoints under `/api/v1/analytics/`. All are cached, rate-limited, and accept optional `from_date`/`to_date` query params where relevant.

| Endpoint | Description |
|---|---|
| `GET /api/v1/analytics/trend` | National avg petrol/diesel/kerosene per pricing period with MoM % change |
| `GET /api/v1/analytics/volatility` | District price volatility ranking by coefficient of variation |
| `GET /api/v1/analytics/regional-gap` | Cheapest vs most expensive district spread over time |
| `GET /api/v1/analytics/peak` | All-time records: highest/lowest national avg, biggest single-period jump/drop, single-district extremes |
| `GET /api/v1/analytics/inflation` | Cumulative price index (base=100) + CAGR per fuel type |
| `GET /api/v1/analytics/correlation` | Pearson r between petrol/diesel/kerosene national averages |
| `GET /api/v1/analytics/forecast` | Multi-model forecast (Holt-Winters ETS, ARIMA, SARIMA, SARIMAX-X) with 95% CI |
| `GET /api/v1/analytics/brent` | Brent crude price history (200+ months via EIA API) |
| `GET /api/v1/analytics/regional-summary` | Per-region averages for a given date |
| `GET /api/v1/documents` | List all imported EWURA PDF bulletins |
| `GET /api/v1/documents/{date}/pdf` | Serve stored bulletin PDF; proxies EWURA as fallback |

Math helpers (internal, in `main.py`):
- `_ols(xs, ys) → (slope, intercept, r_squared)` — ordinary least squares
- `_pearson(xs, ys) → float` — Pearson correlation coefficient
- `_cagr(start, end, years) → float` — compound annual growth rate as %
- `_compute_r2(ys, fitted_values)` — filters NaN/inf before computing; clamps to [0, 1]
- `_project_brent(series, periods)` — ARIMA(1,1,0) projection of Brent crude forward
- `_best_forecast(ys, periods, brent_exog, brent_future)` — fits all models, picks lowest AIC

### Forecast model details
- Default `training_periods=36` (last 3 years of monthly data)
- Model selection: lowest AIC wins among ARIMA(1,1,1), SARIMA(1,1,1)(1,0,0,12), Holt-Winters ETS (damped trend), SARIMAX-X with Brent
- `damped_trend=True` on Holt-Winters — prevents over-projecting short-term spikes over long horizons
- Brent quality check: if any recent month-on-month Brent change > 75%, SARIMAX-X is skipped (EIA DEMO_KEY sometimes returns impossibly stale data; real geopolitical shocks like Strait of Hormuz 2026 are +45% and must pass through)
- R² for ARIMA/SARIMA often shows 0.0 on non-stationary data with structural breaks — this is correct behaviour, not a bug

### PDF storage
- `PDF_STORAGE_DIR` env var (default `/data/pdfs`), mounted as Docker volume `./pdfs:/data/pdfs`
- After each successful sync/backfill, PDF is copied to `ewura_{effective_date}.pdf`
- `GET /api/v1/documents/{date}/pdf` serves local file or proxies EWURA URL as fallback
- EWURA server intermittently goes into maintenance ("Be right back." page) — not an IP block

## Coding conventions
- Keep everything in `main.py` — single unified file is intentional
- No inline comments unless the WHY is non-obvious
- No docstrings — endpoint descriptions go in the `description=` param of route decorators
- Add new district aliases to `_DISTRICT_ALIASES` dict, not inline logic
- Add new Swahili month spellings to `_SW_MONTHS` dict
- Add new date patterns to `_DATE_PATTERNS` list; keep them ordered most-specific first
