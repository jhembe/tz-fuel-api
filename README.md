# 🇹🇿 Tanzania Fuel Price API

> **Live:** [fuelapi.mahembega.com](https://fuelapi.mahembega.com) · **Docs:** [fuelapi.mahembega.com/docs](https://fuelapi.mahembega.com/docs)

A production-grade REST API that scrapes EWURA (Tanzania's Energy and Water Utilities Regulatory Authority) monthly fuel price bulletins and exposes them as clean, structured data — complete with full history back to 2009, time-series forecasting, and machine learning district clustering.

---

## What It Does

Tanzania's official fuel prices are published as PDF files on the EWURA website. This API:

1. **Scrapes** every monthly PDF bulletin (2009 → present, ~80 bulletins)
2. **Parses** district-level petrol, diesel, and kerosene cap prices
3. **Normalises** inconsistent district names across 17 years of PDFs
4. **Stores** ~11,000 price records in a relational database
5. **Exposes** clean REST endpoints with filtering, history, exports, and analytics
6. **Forecasts** future prices using automated time-series model selection (ARIMA, SARIMA, Holt-Winters ETS)
7. **Clusters** districts by price behaviour using K-means machine learning

---

## Coverage

| Metric | Value |
|---|---|
| Price records | ~10,996 |
| Pricing dates | 80 (2009–2026) |
| Districts covered | 185 |
| Fuel types | Petrol, Diesel, Kerosene |
| Geography | All Tanzania mainland regions + Zanzibar |
| Update frequency | Monthly (EWURA publishes ~1st of each month) |

---

## API Reference

### Core Endpoints

```
GET /api/v1/prices                     # All prices for latest period
GET /api/v1/prices/{district}          # Latest prices for a specific district
GET /api/v1/history/{district}         # Full price history for a district
GET /api/v1/stats                      # National summary statistics
GET /api/v1/search?q=dar               # Search districts by name
GET /api/v1/cheapest?fuel=petrol       # Cheapest districts for a fuel type
GET /api/v1/most-expensive             # Most expensive districts
GET /api/v1/compare?districts=a,b,c    # Side-by-side district comparison
GET /api/v1/regions                    # Prices grouped by region
GET /api/v1/export/csv                 # Full dataset export as CSV
```

### Analytics — Deep

```
GET /api/v1/analytics/trend            # National avg per period + MoM % change
GET /api/v1/analytics/volatility       # District price volatility ranking (CV)
GET /api/v1/analytics/regional-gap     # Cheapest vs most expensive spread over time
GET /api/v1/analytics/peak             # All-time records: highs, lows, biggest jumps
GET /api/v1/analytics/inflation        # Cumulative price index (base=100) + CAGR
GET /api/v1/analytics/correlation      # Pearson r between fuel type national averages
GET /api/v1/analytics/forecast         # Multi-model time-series price forecast
GET /api/v1/analytics/clusters         # K-means district clustering by price trajectory
GET /api/v1/analytics/regional-summary # Avg prices grouped by region for any date
```

---

## Machine Learning: Forecast Endpoint

`GET /api/v1/analytics/forecast`

The forecast endpoint doesn't just use one model — it **automatically selects the best** by fitting three competing time-series models and picking the lowest AIC (Akaike Information Criterion):

| Model | What it captures | Min data |
|---|---|---|
| **ARIMA(1,1,1)** | Trend + autocorrelation, first-order differencing | 6 periods |
| **SARIMA(1,1,1)(1,0,0,12)** | + Annual seasonal cycle | 24 periods |
| **Holt-Winters ETS** | Level + additive trend + additive seasonality | 24 periods |

**Example response:**
```json
{
  "model": "Holt-Winters (ETS)",
  "model_aic": 256.14,
  "model_comparison": {
    "ARIMA(1,1,1)":             { "aic": 315.27, "r_squared": 0.0,    "converged": true, "selected": false },
    "SARIMA(1,1,1)(1,0,0,12)": { "aic": 316.12, "r_squared": 0.0,    "converged": true, "selected": false },
    "Holt-Winters (ETS)":      { "aic": 256.14, "r_squared": 0.6265, "converged": true, "selected": true  }
  },
  "forecast": [
    {
      "projected_date": "2026-07-11",
      "petrol_forecast": 4365.70,
      "petrol_ci_low": 3867.66,
      "petrol_ci_high": 4904.59,
      "diesel_forecast": 4412.30,
      "kerosene_forecast": 4780.50
    }
  ]
}
```

The `model_comparison` field is always included so analysts can audit the selection — every model's AIC and R² are returned, not just the winner's. Holt-Winters consistently wins on this dataset because Tanzania fuel prices carry both a rising trend and an annual cycle tied to global oil markets.

> **Important:** These are statistical projections based on historical EWURA data. They do not account for geopolitical shocks, OPEC decisions, exchange rate movements, or regulatory changes. Do not use for financial or policy decisions without additional context.

---

## Machine Learning: District Clustering

`GET /api/v1/analytics/clusters`

Groups all 185 districts into behavioural clusters using **K-means on z-score-normalised price trajectories**. Normalisation means districts are clustered by *shape* (do they rise together? fall together? diverge?) not by absolute price level.

**Parameters:**
- `fuel_type`: `petrol` | `diesel` | `kerosene` (default: `petrol`)
- `n_clusters`: 2–8 (default: 5)
- `min_periods`: minimum pricing periods a district must have to qualify (default: 12)

**Example response:**
```json
{
  "fuel_type": "petrol",
  "n_clusters": 5,
  "districts_included": 185,
  "centroids": [
    { "cluster_id": 0, "label": "Lowest Price, Rising",  "district_count": 17, "avg_latest_price": 3499.29, "trend": "rising" },
    { "cluster_id": 1, "label": "Low Price, Rising",     "district_count": 53, "avg_latest_price": 3507.74, "trend": "rising" },
    { "cluster_id": 2, "label": "Mid Price, Rising",     "district_count": 49, "avg_latest_price": 3652.18, "trend": "rising" },
    { "cluster_id": 3, "label": "High Price, Rising",    "district_count": 21, "avg_latest_price": 3782.24, "trend": "rising" },
    { "cluster_id": 4, "label": "Highest Price, Rising", "district_count": 45, "avg_latest_price": 3935.71, "trend": "rising" }
  ],
  "dates": ["2009-01-06", "2009-01-21", "..."]
}
```

Each cluster includes `centroid_values` — the normalised trajectory array — for rendering on a chart to visualise the cluster's characteristic price pattern.

---

## Architecture

```
┌───────────────────────────────────────────────────┐
│                  main.py (single file)             │
│                                                    │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────┐ │
│  │   Scraper   │  │  Analytics   │  │  Admin   │ │
│  │  EWURA PDF  │  │  + ML models │  │ sync/    │ │
│  │  parser     │  │  ARIMA/ETS/  │  │ backfill │ │
│  │  date extx  │  │  K-means     │  │ cache    │ │
│  └──────┬──────┘  └──────┬───────┘  └──────────┘ │
│         │                │                        │
│  ┌──────▼────────────────▼──────────────────────┐ │
│  │              SQLAlchemy ORM                   │ │
│  │     SQLite (dev) · PostgreSQL (prod)          │ │
│  └───────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────┘
         ↑
   FastAPI + Uvicorn
   In-process TTL cache
   Per-IP rate limiting
   Docker / systemd
```

### Key Design Decisions

- **Single-file** (`main.py`) — intentional. The entire application is one file: models, scraper, routes, ML helpers. Easy to audit, deploy, and hand to a government analyst.
- **Zero-config SQLite default** — run with no external dependencies for development; switch to PostgreSQL via `DATABASE_URL` for production.
- **District normalisation** — EWURA PDFs use inconsistent names across 17 years ("Tanga CC", "Tanga", "tanga cc"). `district_key()` normalises and deduplicates using a canonical alias table.
- **Dual PDF format support** — EWURA changed table format in 2024. The parser detects both the old 4-column and new 5-column layouts.
- **Effective date extraction** — handles 6 different date formats across PDFs including Swahili (`TAREHE 7 FEBRUARI 2024`, `Mwezi Julai 2026`).

---

## Running Locally

```bash
# Clone and set up
git clone https://github.com/jhembe/tz-fuel-api.git
cd tz-fuel-api

python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env          # set ADMIN_SECRET_KEY at minimum
export $(grep -v '^#' .env | grep -v '^$' | xargs)

uvicorn main:app --host 127.0.0.1 --port 8888 --reload
# → http://127.0.0.1:8888/docs
```

### Trigger the first data sync

```bash
# Latest bulletin only (~30 seconds):
curl -X POST http://127.0.0.1:8888/api/v1/admin/trigger-sync \
     -H "X-API-KEY: <your-admin-key>"

# Full historical backfill 2009 → present (~10 minutes):
curl -X POST http://127.0.0.1:8888/api/v1/admin/backfill \
     -H "X-API-KEY: <your-admin-key>"

# Monitor progress:
curl http://127.0.0.1:8888/api/v1/admin/backfill/status \
     -H "X-API-KEY: <your-admin-key>"
```

---

## Running with Docker

```bash
ADMIN_SECRET_KEY=your-secret docker compose up --build
# API available at http://localhost:8000
# Swagger UI at http://localhost:8000/docs
```

---

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `sqlite:///./tanzania_fuel.db` | DB connection (PostgreSQL or SQLite) |
| `ADMIN_SECRET_KEY` | *(empty — disables admin)* | `X-API-KEY` for admin endpoints |
| `RATE_LIMIT_PER_MINUTE` | `120` | Per-IP request cap |
| `CACHE_TTL_SECONDS` | `300` | In-process TTL cache lifetime |

---

## Dependencies

| Package | Purpose |
|---|---|
| `fastapi` | Web framework |
| `uvicorn` | ASGI server |
| `pdfplumber` | PDF parsing |
| `beautifulsoup4` | EWURA page scraping |
| `sqlalchemy` | ORM (SQLite + PostgreSQL) |
| `statsmodels` | ARIMA, SARIMA, Holt-Winters time-series models |
| `scikit-learn` | K-means clustering |
| `numpy` | Numerical arrays (used by statsmodels/sklearn) |

---

## Production Deployment (Ubuntu 24.04)

```bash
sudo bash deploy.sh
# Installs: Python 3.12 venv, systemd service, nginx reverse proxy
# Reads config from: /opt/apps/tz-fuel-api/.env
```

The systemd service runs as `www-data` with `NoNewPrivileges`, `PrivateTmp`, and `ProtectSystem=strict` for hardening.

---

## Data Source

All price data is sourced from **EWURA's publicly published monthly fuel price bulletins**:
- [https://www.ewura.go.tz](https://www.ewura.go.tz) → Energy → Petroleum → Petroleum Prices

Prices are **Maximum Retail Prices** (cap prices) set by the government. Actual pump prices at any given station may be at or below these caps.

---

## Disclaimer

This project is not affiliated with or endorsed by EWURA or the Government of Tanzania. It is an independent tool built to make public data more accessible. All data originates from EWURA's official publications. The forecasting models are statistical projections for informational purposes only.

---

## License

MIT — use freely, attribute appreciated.
