# Tanzania Fuel API — Next Phase Vision

## 1. Predictive Modelling

Upgrade the current OLS forecast with real time-series and clustering models.

### Phase 1 — ARIMA / Prophet per fuel type
- Replace `GET /api/v1/analytics/forecast` OLS with ARIMA or Prophet
- Train on national average per fuel type (80 monthly points)
- Output: point forecast + 95% CI for next N periods (same shape as current response)
- Expose model metadata: algorithm used, R², training window

### Phase 2 — District clustering
- K-means (or DTW-based) clustering of districts by price trajectory
- New endpoint: `GET /api/v1/analytics/clusters`
- UI: colour districts on the map by cluster; "similar districts" panel in `/compare`

### Phase 3 — Panel regression (optional / stretch)
- Treat all districts as a panel — explain price spread using region, coastal/inland, population centre features
- Requires building a static district-metadata table (lat/lon, region tier, etc.)

### What to avoid
- Deep learning (LSTM, Transformer) — 80 time points is not enough; will overfit
- Black-box models with no explainability — this is public data, outputs should be auditable

---

## 2. PDF Archive & Source Access

Preserve original EWURA bulletins so data is auditable and survives link rot.

### Backend changes
- During backfill and sync: download the PDF bytes and store to `./pdfs/YYYY-MM-DD.pdf`
- New column on `SyncLog`: `pdf_stored: bool`
- New endpoint: `GET /api/v1/bulletins/{date}/pdf` → streams the stored file (or 404 if not archived)
- New endpoint: `GET /api/v1/bulletins` → list all pricing dates with `pdf_available` flag

### Frontend changes
- Each pricing period row in the UI gets a download/view icon
- Bulletin browser page (or section on `/docs`) listing all dates + PDF links
- Badge: "Source PDF available" for periods with archived files

### Storage
- ~80 PDFs × ~300 KB avg = ~24 MB total — negligible
- Store on disk alongside the app; back up with the database
- No CDN or object storage needed at this scale

---

## Priority Order

1. **PDF archive** — smaller scope, high trust value, unblocks auditability
2. **ARIMA/Prophet forecast** — direct upgrade to existing endpoint, visible user impact
3. **District clustering** — new analytical surface, feeds into map and compare pages
