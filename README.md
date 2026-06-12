# Tanzania Fuel Price API

A production-grade REST API that scrapes **EWURA** (Energy and Water Utilities Regulatory Authority) monthly PDF bulletins and serves Tanzania's official fuel cap prices as clean, structured data — covering petrol, diesel, and kerosene across all mainland and Zanzibar districts since 2009.

Live at **[fuelapi.mahembega.com](https://fuelapi.mahembega.com)**  
Dashboard at **[fuel.mahembega.com](https://fuel.mahembega.com)**

---

## Screenshots

| Overview | Price Map |
|---|---|
| ![Overview](docs/screenshots/ss_overview.png) | ![Map](docs/screenshots/ss_map.png) |

| Deep Analytics | EWURA Bulletins |
|---|---|
| ![Analytics](docs/screenshots/ss_analytics.png) | ![Bulletins](docs/screenshots/ss_bulletins.png) |

| Historical Trends | Mobile |
|---|---|
| ![Trends](docs/screenshots/ss_trends.png) | ![Mobile](docs/screenshots/ss_mobile.png) |

---

## Features

- **10,996+ fuel price records** from 80 pricing periods (2009–present)
- **192 districts** across mainland Tanzania and Zanzibar
- **Monthly PDF scraping** from EWURA's official website
- **Automated scheduling** — new prices fetched on the 6th of each month
- **Multi-model forecasting** — auto-selects best model (Holt-Winters ETS, ARIMA, SARIMA, SARIMAX-X with Brent crude)
- **Brent crude integration** — 200+ months of oil price history via EIA API
- **PDF hosting** — bulletins stored and served locally; no EWURA URL dependency for end users
- **7 analytics endpoints** — volatility, inflation, regional gap, correlation, forecast, peak records
- Rate-limited, cached, and production-ready

---

## API Endpoints

### Core

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/prices/latest` | Latest fuel prices for all districts |
| `GET` | `/api/v1/prices/district/{name}/history` | Full price history for one district |
| `GET` | `/api/v1/stats` | National averages (min/max/cheapest/dearest) |
| `GET` | `/api/v1/search` | Search districts by name |
| `GET` | `/api/v1/compare` | Side-by-side comparison (up to 3 districts) |
| `GET` | `/api/v1/cheapest` | Cheapest districts for a fuel type |
| `GET` | `/api/v1/dates` | All available pricing period dates |
| `GET` | `/api/v1/export/csv` | Download full dataset as CSV |

### Analytics

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/analytics/trend` | National averages per period + MoM % change |
| `GET` | `/api/v1/analytics/volatility` | District price volatility ranking |
| `GET` | `/api/v1/analytics/regional-gap` | Cheapest vs most expensive spread over time |
| `GET` | `/api/v1/analytics/peak` | All-time records |
| `GET` | `/api/v1/analytics/inflation` | Cumulative price index + CAGR |
| `GET` | `/api/v1/analytics/correlation` | Pearson r between fuel types |
| `GET` | `/api/v1/analytics/forecast` | Multi-model price forecast with 95% CI |
| `GET` | `/api/v1/analytics/brent` | Brent crude oil price history |
| `GET` | `/api/v1/analytics/regional-summary` | Per-region averages for a given date |

### Documents

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/documents` | List all imported EWURA PDF bulletins |
| `GET` | `/api/v1/documents/{date}/pdf` | Serve stored bulletin PDF by effective date |

Interactive API docs: **[fuelapi.mahembega.com/docs](https://fuelapi.mahembega.com/docs)**

---

## Forecasting

The `/api/v1/analytics/forecast` endpoint fits four competing models and picks the one with the lowest AIC:

| Model | Description |
|---|---|
| **Holt-Winters (ETS)** | Exponential smoothing — captures level, trend, and seasonality |
| **ARIMA(1,1,1)** | Classic differenced autoregressive model |
| **SARIMA(1,1,1)(1,0,0,12)** | ARIMA with 12-month seasonal component |
| **SARIMAX-X (Brent crude)** | SARIMA with Brent crude oil price as exogenous variable |

As of June 2026, **Holt-Winters ETS wins** (AIC 256 vs 315+ for others) with R² ≈ 0.90 across all fuel types.

> For best SARIMAX-X accuracy, register a free EIA API key at [eia.gov/opendata](https://www.eia.gov/opendata/) and set `EIA_API_KEY` in your environment. The default `DEMO_KEY` can return unreliable recent data.

---

## Quick Start

### Local development (SQLite, zero config)

```bash
git clone https://github.com/jhembe/tz-fuel-api.git && cd tz-fuel-api
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # set ADMIN_SECRET_KEY at minimum
export $(grep -v '^#' .env | grep -v '^$' | xargs)
venv/bin/uvicorn main:app --host 127.0.0.1 --port 8888 --reload
# → http://127.0.0.1:8888/docs
```

### Docker

```bash
ADMIN_SECRET_KEY=mysecret docker compose up --build
```

### Trigger initial data import

```bash
# Latest bulletin only:
curl -X POST http://localhost:8888/api/v1/admin/trigger-sync \
     -H "X-API-KEY: your_admin_key"

# Full historical backfill (2009–present, ~10 min):
curl -X POST http://localhost:8888/api/v1/admin/backfill \
     -H "X-API-KEY: your_admin_key"
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `sqlite:///./tanzania_fuel.db` | PostgreSQL or SQLite connection string |
| `ADMIN_SECRET_KEY` | *(empty — disables admin endpoints)* | Secret for admin/sync endpoints |
| `EIA_API_KEY` | `DEMO_KEY` | EIA API key for Brent crude prices |
| `PDF_STORAGE_DIR` | `/data/pdfs` | Directory to store downloaded EWURA PDFs |
| `RATE_LIMIT_PER_MINUTE` | `120` | Per-IP request cap |
| `CACHE_TTL_SECONDS` | `300` | In-process cache TTL |

---

## Stack

- **FastAPI** — web framework
- **SQLAlchemy** + **PostgreSQL** (or SQLite for local dev)
- **pdfplumber** — PDF table extraction from EWURA bulletins
- **statsmodels** — Holt-Winters ETS, ARIMA, SARIMA, SARIMAX
- **APScheduler** — automated monthly cron-style sync jobs
- **Docker Compose** — containerised deployment

---

## Data Source

All fuel prices are sourced from **EWURA** (Energy and Water Utilities Regulatory Authority), Tanzania's official energy regulator. Prices represent the official **maximum retail cap prices** set each month for petrol, diesel, and kerosene.

[ewura.go.tz](https://www.ewura.go.tz)

---

## License

MIT
