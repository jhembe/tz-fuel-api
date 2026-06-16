"""
Tanzania Public Fuel Price API — v2.0
Production-grade FastAPI service sourcing cap prices from EWURA PDFs.
"""

import asyncio
import csv
import io
import logging
import math
import os
import re
import shutil
import statistics
import tempfile
import time
import unicodedata
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

import pdfplumber
import requests
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from bs4 import BeautifulSoup
from fastapi import (
    BackgroundTasks,
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
    Response,
    Security,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import (
    Column,
    Date,
    Float,
    ForeignKey,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    create_engine,
    func,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Session, relationship, sessionmaker

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("tz-fuel-api")

# ─────────────────────────────────────────────────────────────────────────────
# Config from environment
# ─────────────────────────────────────────────────────────────────────────────

_RAW_DB_URL = os.getenv("DATABASE_URL", "sqlite:///./tanzania_fuel.db")
ADMIN_SECRET_KEY = os.getenv("ADMIN_SECRET_KEY", "")
EWURA_URL = "https://www.ewura.go.tz/pages/petroleum-product-pricing"
RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "120"))
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "300"))  # 5 min default
EIA_API_KEY = os.getenv("EIA_API_KEY", "DEMO_KEY")  # free demo key works for ~100 req/day
FRED_API_KEY = os.getenv("FRED_API_KEY", "")  # optional — register free at fred.stlouisfed.org for monthly TZS/USD data
PDF_STORAGE_DIR = os.getenv("PDF_STORAGE_DIR", "/data/pdfs")


# ─────────────────────────────────────────────────────────────────────────────
# Database URL normalisation
# ─────────────────────────────────────────────────────────────────────────────


def _resolve_db_url(url: str) -> str:
    """Rewrite Supabase/Heroku postgres:// → postgresql+psycopg2://."""
    if url.startswith("postgres://"):
        return "postgresql+psycopg2://" + url[len("postgres://"):]
    if url.startswith("postgresql://") and "+psycopg2" not in url:
        return "postgresql+psycopg2://" + url[len("postgresql://"):]
    return url


DATABASE_URL = _resolve_db_url(_RAW_DB_URL)
IS_SQLITE = "sqlite" in DATABASE_URL


def _make_engine():
    kwargs: dict = {"pool_pre_ping": True}
    if IS_SQLITE:
        kwargs["connect_args"] = {"check_same_thread": False}
    else:
        kwargs.update({"pool_size": 5, "max_overflow": 10, "pool_recycle": 1800})
    return create_engine(DATABASE_URL, **kwargs)


engine = _make_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# ─────────────────────────────────────────────────────────────────────────────
# ORM Models
# ─────────────────────────────────────────────────────────────────────────────


class Base(DeclarativeBase):
    pass


class RegionDistrict(Base):
    __tablename__ = "region_districts"
    __table_args__ = (UniqueConstraint("district_key", name="uq_district_key"),)

    id = Column(Integer, primary_key=True, index=True)
    region_name = Column(String(120), index=True, nullable=False)
    district_name = Column(String(120), index=True, nullable=False)
    district_key = Column(String(120), unique=True, index=True, nullable=False)
    prices = relationship(
        "FuelPrice", back_populates="district", cascade="all, delete-orphan"
    )


class FuelPrice(Base):
    __tablename__ = "fuel_prices"
    __table_args__ = (
        UniqueConstraint("district_id", "effective_date", name="uq_district_date"),
    )

    id = Column(Integer, primary_key=True, index=True)
    district_id = Column(
        Integer,
        ForeignKey("region_districts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    petrol_price = Column(Float, nullable=False)
    diesel_price = Column(Float, nullable=False)
    kerosene_price = Column(Float, nullable=False)
    effective_date = Column(Date, index=True, nullable=False)
    district = relationship("RegionDistrict", back_populates="prices")


class SyncLog(Base):
    __tablename__ = "sync_logs"

    id = Column(Integer, primary_key=True, index=True)
    triggered_at = Column(String(40), nullable=False)
    status = Column(String(20), nullable=False)  # running | success | failed
    pdf_url = Column(String(600), nullable=True)
    effective_date = Column(Date, nullable=True)
    records_upserted = Column(Integer, default=0)
    error_message = Column(String(1000), nullable=True)


class BrentCrude(Base):
    __tablename__ = "brent_crude"
    __table_args__ = (UniqueConstraint("price_date", name="uq_brent_date"),)

    id = Column(Integer, primary_key=True, index=True)
    price_date = Column(Date, unique=True, index=True, nullable=False)
    price_usd = Column(Float, nullable=False)


class TzsUsdRate(Base):
    __tablename__ = "tzs_usd_rate"
    __table_args__ = (UniqueConstraint("rate_date", name="uq_tzs_rate_date"),)

    id = Column(Integer, primary_key=True, index=True)
    rate_date = Column(Date, unique=True, index=True, nullable=False)
    rate_tzs_per_usd = Column(Float, nullable=False)


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic Schemas
# ─────────────────────────────────────────────────────────────────────────────


class PriceResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    region_name: str
    district_name: str
    petrol_price: float
    diesel_price: float
    kerosene_price: float
    effective_date: date


class PriceComparisonResponse(BaseModel):
    district_1: PriceResponse
    district_2: PriceResponse
    petrol_difference: float = Field(description="Absolute TZS difference")
    diesel_difference: float
    cheaper_district_petrol: str
    cheaper_district_diesel: str


class TrendEntry(BaseModel):
    effective_date: date
    petrol_price: float
    diesel_price: float
    kerosene_price: float
    petrol_change_pct: Optional[float] = Field(None, description="% change vs previous period")
    diesel_change_pct: Optional[float] = None
    kerosene_change_pct: Optional[float] = None


class DistrictTrendResponse(BaseModel):
    district_name: str
    region_name: str
    trend: List[TrendEntry]


class FuelStats(BaseModel):
    avg: float
    min: float
    max: float
    cheapest_district: str
    most_expensive_district: str


class NationalStatsResponse(BaseModel):
    effective_date: date
    district_count: int
    petrol: FuelStats
    diesel: FuelStats
    kerosene: FuelStats


class DistrictListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    district_name: str
    region_name: str


class LatestPricesResponse(BaseModel):
    effective_date: date
    total_districts: int
    prices: List[PriceResponse]


class AvailableDatesResponse(BaseModel):
    dates: List[date]
    total: int


class SyncStatusResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    triggered_at: str
    status: str
    pdf_url: Optional[str]
    effective_date: Optional[date]
    records_upserted: int
    error_message: Optional[str]


# ── Analytics schemas ─────────────────────────────────────────────────────────

class TrendPoint(BaseModel):
    effective_date: date
    petrol_avg: float
    diesel_avg: float
    kerosene_avg: float
    district_count: int
    petrol_change_pct: Optional[float] = Field(None, description="% change vs previous period")
    diesel_change_pct: Optional[float] = None
    kerosene_change_pct: Optional[float] = None


class NationalTrendResponse(BaseModel):
    from_date: date
    to_date: date
    periods: int
    data: List[TrendPoint]


class VolatilityEntry(BaseModel):
    district_name: str
    region_name: str
    periods_analysed: int
    petrol_cv: float = Field(description="Coefficient of variation % — stdev/mean")
    diesel_cv: float
    kerosene_cv: float
    petrol_max_swing_pct: float = Field(description="Largest single-period % price change")
    diesel_max_swing_pct: float
    kerosene_max_swing_pct: float


class VolatilityResponse(BaseModel):
    periods_analysed: int
    min_periods_required: int
    most_volatile: List[VolatilityEntry]
    most_stable: List[VolatilityEntry]


class RegionalGapPoint(BaseModel):
    effective_date: date
    petrol_min: float
    petrol_max: float
    petrol_avg: float
    petrol_spread: float = Field(description="Max minus min (TZS/L)")
    petrol_spread_pct: float = Field(description="Spread as % of national average")
    diesel_spread: float
    diesel_spread_pct: float
    kerosene_spread: float
    kerosene_spread_pct: float
    district_count: int


class RegionalGapResponse(BaseModel):
    from_date: date
    to_date: date
    periods: int
    avg_petrol_spread: float
    max_petrol_spread: float
    max_petrol_spread_date: date
    data: List[RegionalGapPoint]


class AllTimeRecord(BaseModel):
    fuel_type: str
    record_type: str
    value: float
    effective_date: date
    district_name: Optional[str] = None
    region_name: Optional[str] = None
    note: str


class PeakRecordsResponse(BaseModel):
    generated_at: str
    records: List[AllTimeRecord]


class InflationPoint(BaseModel):
    effective_date: date
    petrol_avg: float
    diesel_avg: float
    kerosene_avg: float
    petrol_index: float = Field(description="Index vs base date (base = 100)")
    diesel_index: float
    kerosene_index: float
    petrol_cumulative_pct: float = Field(description="Cumulative % change since base date")
    diesel_cumulative_pct: float
    kerosene_cumulative_pct: float


class InflationResponse(BaseModel):
    base_date: date
    base_petrol_avg: float
    base_diesel_avg: float
    base_kerosene_avg: float
    total_periods: int
    petrol_annualised_rate: float = Field(description="Compound annual growth rate %")
    diesel_annualised_rate: float
    kerosene_annualised_rate: float
    data: List[InflationPoint]


class CorrelationMatrix(BaseModel):
    petrol_diesel: float = Field(description="Pearson r")
    petrol_kerosene: float
    diesel_kerosene: float


class CorrelationResponse(BaseModel):
    periods_analysed: int
    from_date: date
    to_date: date
    national_average_correlation: CorrelationMatrix
    interpretation: Dict[str, str]


class ForecastPoint(BaseModel):
    projected_date: date
    petrol_forecast: float
    diesel_forecast: float
    kerosene_forecast: float
    petrol_ci_low: float = Field(description="95% confidence interval lower bound")
    petrol_ci_high: float
    diesel_ci_low: float
    diesel_ci_high: float
    kerosene_ci_low: float
    kerosene_ci_high: float


class ForecastResponse(BaseModel):
    model: str
    model_aic: Optional[float] = None
    model_comparison: Dict[str, Dict] = {}
    periods_used: int
    from_date: date
    to_date: date
    avg_period_days: int
    r_squared: Dict[str, float]
    trend_direction: Dict[str, str]
    brent_last_known: Optional[float] = None
    brent_forecast_inputs: Optional[List[float]] = None
    fx_last_known: Optional[float] = None
    fx_forecast_inputs: Optional[List[float]] = None
    forecast: List[ForecastPoint]
    disclaimer: str


class BrentPoint(BaseModel):
    price_date: date
    price_usd: float


class BrentResponse(BaseModel):
    count: int
    from_date: date
    to_date: date
    latest_price_usd: float
    series: List[BrentPoint]


class FxPoint(BaseModel):
    rate_date: date
    rate_tzs_per_usd: float


class FxResponse(BaseModel):
    count: int
    from_date: date
    to_date: date
    latest_rate_tzs_per_usd: float
    source: str
    series: List[FxPoint]


class ClusterDistrict(BaseModel):
    district_name: str
    region_name: str
    cluster_id: int
    periods_with_data: int
    latest_price: float


class ClusterCentroid(BaseModel):
    cluster_id: int
    label: str
    district_count: int
    avg_latest_price: float
    trend: str
    centroid_values: List[float]


class ClusterResponse(BaseModel):
    fuel_type: str
    n_clusters: int
    periods_used: int
    from_date: date
    to_date: date
    districts_included: int
    districts: List[ClusterDistrict]
    centroids: List[ClusterCentroid]
    dates: List[date]


class RegionalSummaryEntry(BaseModel):
    region_name: str
    district_count: int
    petrol_avg: float
    diesel_avg: float
    kerosene_avg: float
    petrol_min: float
    petrol_max: float


class RegionalSummaryResponse(BaseModel):
    effective_date: date
    regions: List[RegionalSummaryEntry]


# ─────────────────────────────────────────────────────────────────────────────
# Security
# ─────────────────────────────────────────────────────────────────────────────

_API_KEY_HEADER = APIKeyHeader(name="X-API-KEY", auto_error=True)


def verify_admin_key(api_key: str = Security(_API_KEY_HEADER)) -> str:
    if not ADMIN_SECRET_KEY:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="ADMIN_SECRET_KEY is not configured on this server.",
        )
    if api_key != ADMIN_SECRET_KEY:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid API key.")
    return api_key


# ─────────────────────────────────────────────────────────────────────────────
# In-process TTL cache
# ─────────────────────────────────────────────────────────────────────────────

_CACHE: Dict[str, Tuple[float, object]] = {}


def cache_get(key: str) -> Optional[object]:
    entry = _CACHE.get(key)
    if entry and (time.monotonic() - entry[0]) < CACHE_TTL_SECONDS:
        return entry[1]
    return None


def cache_set(key: str, value: object) -> None:
    _CACHE[key] = (time.monotonic(), value)


def cache_clear() -> None:
    _CACHE.clear()


# ─────────────────────────────────────────────────────────────────────────────
# Per-IP rate limiting (token bucket, in-process)
# ─────────────────────────────────────────────────────────────────────────────

_RATE_BUCKETS: Dict[str, List[float]] = defaultdict(list)


def _check_rate_limit(request: Request) -> None:
    ip = request.client.host if request.client else "unknown"
    now = time.monotonic()
    window = 60.0
    bucket = _RATE_BUCKETS[ip]
    # Drop timestamps older than the window
    _RATE_BUCKETS[ip] = [t for t in bucket if now - t < window]
    if len(_RATE_BUCKETS[ip]) >= RATE_LIMIT_PER_MINUTE:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded. Max {RATE_LIMIT_PER_MINUTE} requests/minute.",
        )
    _RATE_BUCKETS[ip].append(now)


# ─────────────────────────────────────────────────────────────────────────────
# DB Dependency
# ─────────────────────────────────────────────────────────────────────────────


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Data Normalisation Utilities
# ─────────────────────────────────────────────────────────────────────────────

_DISTRICT_ALIASES: Dict[str, str] = {
    "dar es salaam": "dar es salaam",
    "dar-es-salaam": "dar es salaam",
    "dares salaam": "dar es salaam",
    "dsm": "dar es salaam",
    "ilala mc": "ilala",
    "kinondoni mc": "kinondoni",
    "temeke mc": "temeke",
    "ubungo mc": "ubungo",
    "kigamboni mc": "kigamboni",
    "dodoma cc": "dodoma",
    "arusha cc": "arusha",
    "mwanza cc": "mwanza",
    "tanga cc": "tanga",
    "mbeya cc": "mbeya",
    "morogoro mc": "morogoro",
    "zanzibar north": "kaskazini unguja",
    "zanzibar south": "kusini unguja",
    "zanzibar west": "mjini magharibi",
    "kaskazini-unguja": "kaskazini unguja",
    "kusini-unguja": "kusini unguja",
    "pemba north": "kaskazini pemba",
    "pemba south": "kusini pemba",
}

_REGION_ALIASES: Dict[str, str] = {
    "dar es salaam": "Dar Es Salaam",
    "dar-es-salaam": "Dar Es Salaam",
    "dares salaam": "Dar Es Salaam",
    "kaskazini pemba": "Kaskazini Pemba",
    "kusini pemba": "Kusini Pemba",
    "kaskazini unguja": "Kaskazini Unguja",
    "kusini unguja": "Kusini Unguja",
    "mjini magharibi": "Mjini Magharibi",
    "pwani": "Pwani",
}

_SUFFIX_RE = re.compile(
    r"\s+(mc|cc|dc|tc|cc/mc|municipal council|municipal|city council|town council)$",
    re.IGNORECASE,
)


def _normalize(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", s.lower().strip())


def district_key(raw: str) -> str:
    n = _SUFFIX_RE.sub("", _normalize(raw))
    return _DISTRICT_ALIASES.get(n, n)


def district_display(raw: str) -> str:
    return district_key(raw).title()


def region_display(raw: str) -> str:
    n = _normalize(raw)
    return _REGION_ALIASES.get(n, raw.title())


# ─────────────────────────────────────────────────────────────────────────────
# Resilient Date Extraction from PDF Header
# ─────────────────────────────────────────────────────────────────────────────

_SW_MONTHS: Dict[str, int] = {
    "januari": 1, "january": 1,
    "februari": 2, "february": 2,
    "machi": 3, "march": 3,
    "aprili": 4, "april": 4,
    "mei": 5, "may": 5,
    "juni": 6, "june": 6,
    "julai": 7, "july": 7,
    "agosti": 8, "august": 8,
    "septemba": 9, "september": 9,
    "oktoba": 10, "october": 10,
    "novemba": 11, "november": 11,
    "desemba": 12, "december": 12,
}

_DATE_PATTERNS = [
    (r"effective\s+date[:\s]+(\d{1,2})(?:st|nd|rd|th)?\s+(\w+)\s+(\d{4})", "dmy"),
    (r"effective\s+(?:from\s+)?(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})", "dmy_num"),
    # "EFFECTIVE WEDNESDAY, 5th NOVEMBER 2025" — weekday between "effective" and the date
    (r"effective\s+\w+,?\s+(\d{1,2})(?:st|nd|rd|th)\s+(\w+)\s+(\d{4})", "dmy"),
    # "5th NOVEMBER 2025" or "TAREHE 7 FEBRUARI 2024" — ordinal or plain day+month+year
    (r"(?:tarehe\s+)?(\d{1,2})(?:st|nd|rd|th)?\s+(\w+)\s+(\d{4})", "dmy"),
    (r"mwezi\s+(\w+)\s+(\d{4})", "my"),
    (r"(\d{4})[/\-](\d{2})[/\-](\d{2})", "ymd"),
    (r"^(\w+)\s+(\d{4})$", "my"),
]


def _try_parse_date(text_block: str) -> Optional[date]:
    for line in text_block.splitlines():
        line = line.strip()
        for pattern, fmt in _DATE_PATTERNS:
            m = re.search(pattern, line, re.IGNORECASE)
            if not m:
                continue
            try:
                if fmt == "dmy":
                    d, mon, y = int(m.group(1)), m.group(2).lower(), int(m.group(3))
                    mo = _SW_MONTHS.get(mon)
                    if mo:
                        return date(y, mo, d)
                elif fmt == "dmy_num":
                    d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
                    if 1 <= mo <= 12:
                        return date(y, mo, d)
                elif fmt == "my":
                    mon, y = m.group(1).lower(), int(m.group(2))
                    mo = _SW_MONTHS.get(mon)
                    if mo:
                        return date(y, mo, 1)
                elif fmt == "ymd":
                    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
                    if 1 <= mo <= 12 and 1 <= d <= 31:
                        return date(y, mo, d)
            except (ValueError, TypeError):
                continue
    return None


def extract_date_from_pdf(pdf_path: str) -> Optional[date]:
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages[:2]:
                block = page.extract_text() or ""
                found = _try_parse_date(block)
                if found:
                    log.info("PDF header date: %s", found)
                    return found
    except Exception as exc:
        log.warning("Date extraction failed: %s", exc)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# EWURA Scraping
# ─────────────────────────────────────────────────────────────────────────────

_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,sw;q=0.5",
}

_LINK_KW = frozenset(["cap price", "petroleum products", "pump price", "fuel price", "bei ya"])


def _scrape_all_candidate_urls() -> List[str]:
    """Scrape EWURA page and return all scored PDF URLs, deduped, score-sorted."""
    resp = requests.get(EWURA_URL, headers=_HTTP_HEADERS, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    seen: set = set()
    candidates: List[Tuple[int, str]] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if ".pdf" not in href.lower():
            continue
        text_lower = a.get_text(" ", strip=True).lower()
        score = sum(1 for kw in _LINK_KW if kw in text_lower)
        if score > 0:
            full = href if href.startswith("http") else f"https://www.ewura.go.tz{href}"
            if full not in seen:
                seen.add(full)
                candidates.append((score, full))
    candidates.sort(reverse=True)
    return [url for _, url in candidates]


def fetch_ewura_pdf_url() -> Optional[str]:
    """Return the first reachable EWURA PDF URL, skipping any that 404."""
    try:
        urls = _scrape_all_candidate_urls()
        log.info("EWURA candidates: %d", len(urls))
        for url in urls:
            try:
                head = requests.head(url, headers=_HTTP_HEADERS, timeout=10, allow_redirects=True)
                if head.status_code == 200:
                    log.info("EWURA PDF confirmed reachable: %s", url)
                    return url
                log.warning("EWURA PDF %s → HTTP %s, skipping", url, head.status_code)
            except Exception as probe_exc:
                log.warning("EWURA PDF probe failed (%s): %s", url, probe_exc)
    except Exception as exc:
        log.error("EWURA scrape error: %s", exc)
    return None


def download_pdf(url: str) -> str:
    resp = requests.get(url, headers=_HTTP_HEADERS, timeout=60, stream=True)
    resp.raise_for_status()
    fd, path = tempfile.mkstemp(suffix=".pdf", prefix="ewura_")
    with os.fdopen(fd, "wb") as f:
        for chunk in resp.iter_content(chunk_size=65536):
            f.write(chunk)
    log.info("Downloaded → %s (%.1f KB)", path, os.path.getsize(path) / 1024)
    return path


# ─────────────────────────────────────────────────────────────────────────────
# PDF Table Parsing
# ─────────────────────────────────────────────────────────────────────────────

_PRICE_RE = re.compile(r"[\d,]+(?:\.\d+)?")
_HEADER_WORDS = frozenset(
    ["region", "district", "petrol", "diesel", "kerosene", "mafuta",
     "mkoa", "wilaya", "bei", "s/no", "no.", "#", "town", "sn"]
)


def _is_header_row(cells: List[str]) -> bool:
    joined = " ".join(cells).lower()
    return sum(1 for w in _HEADER_WORDS if w in joined) >= 2


def _parse_price(cell: str) -> Optional[float]:
    cell = cell.strip().replace(",", "")
    m = _PRICE_RE.search(cell)
    if m:
        try:
            val = float(m.group())
            if 100 <= val <= 50000:
                return val
        except ValueError:
            pass
    return None


def _get_stored_pdf_path(effective_date: date) -> str:
    return os.path.join(PDF_STORAGE_DIR, f"ewura_{effective_date.isoformat()}.pdf")


def _save_pdf_to_storage(temp_path: str, effective_date: date) -> None:
    try:
        os.makedirs(PDF_STORAGE_DIR, exist_ok=True)
        dest = _get_stored_pdf_path(effective_date)
        shutil.copy2(temp_path, dest)
        log.info("PDF saved → %s", dest)
    except Exception as exc:
        log.warning("Could not save PDF to storage: %s", exc)


def parse_ewura_pdf(pdf_path: str, fallback_date: date, db: Session) -> Tuple[int, date]:
    effective_date = extract_date_from_pdf(pdf_path) or fallback_date
    log.info("Effective date: %s", effective_date)
    upserted = 0

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables() or []:
                for row in table:
                    if not row:
                        continue
                    cells = [str(c).strip() if c else "" for c in row]
                    cells = [c for c in cells if c]

                    if len(cells) < 4 or _is_header_row(cells):
                        continue

                    parsed = None

                    # Format A — 5+ cols: SN/region, district, petrol, diesel, kerosene
                    for off in (0, 1):
                        if off + 4 >= len(cells):
                            continue
                        r_raw, d_raw = cells[off], cells[off + 1]
                        p = _parse_price(cells[off + 2])
                        d = _parse_price(cells[off + 3])
                        k = _parse_price(cells[off + 4])
                        if p and d and k and r_raw and len(d_raw) > 1:
                            parsed = (r_raw, d_raw, p, d, k)
                            break

                    # Format B — 4 cols: town, petrol, diesel, kerosene (no region/SN)
                    if not parsed and len(cells) == 4:
                        p = _parse_price(cells[1])
                        d = _parse_price(cells[2])
                        k = _parse_price(cells[3])
                        town = cells[0]
                        if p and d and k and len(town) > 1 and not _parse_price(town):
                            parsed = ("", town, p, d, k)

                    if not parsed:
                        continue

                    r_raw, d_raw, petrol, diesel, kerosene = parsed
                    d_key = district_key(d_raw)
                    d_disp = district_display(d_raw)
                    r_disp = region_display(r_raw)

                    db_district = (
                        db.query(RegionDistrict)
                        .filter(RegionDistrict.district_key == d_key)
                        .first()
                    )
                    if not db_district:
                        db_district = RegionDistrict(
                            region_name=r_disp,
                            district_name=d_disp,
                            district_key=d_key,
                        )
                        db.add(db_district)
                        db.flush()
                    else:
                        db_district.region_name = r_disp
                        db_district.district_name = d_disp

                    existing = (
                        db.query(FuelPrice)
                        .filter(
                            FuelPrice.district_id == db_district.id,
                            FuelPrice.effective_date == effective_date,
                        )
                        .first()
                    )
                    if existing:
                        existing.petrol_price = petrol
                        existing.diesel_price = diesel
                        existing.kerosene_price = kerosene
                    else:
                        new_fp = FuelPrice(
                            district_id=db_district.id,
                            petrol_price=petrol,
                            diesel_price=diesel,
                            kerosene_price=kerosene,
                            effective_date=effective_date,
                        )
                        db.add(new_fp)
                        db.flush()
                    upserted += 1

        db.commit()

    cache_clear()
    log.info("Upserted %d price records (date=%s)", upserted, effective_date)
    return upserted, effective_date


# ─────────────────────────────────────────────────────────────────────────────
# Background Sync Task
# ─────────────────────────────────────────────────────────────────────────────


def background_scraper_task(factory) -> None:
    db = factory()
    log_entry = SyncLog(
        triggered_at=datetime.utcnow().isoformat(),
        status="running",
        records_upserted=0,
    )
    db.add(log_entry)
    db.commit()
    db.refresh(log_entry)

    pdf_path: Optional[str] = None
    try:
        pdf_url = fetch_ewura_pdf_url()
        if not pdf_url:
            log_entry.status = "failed"
            log_entry.error_message = "PDF not found on EWURA website."
            db.commit()
            return

        log_entry.pdf_url = pdf_url
        db.commit()

        pdf_path = download_pdf(pdf_url)
        count, eff_date = parse_ewura_pdf(pdf_path, date.today(), db)

        log_entry.status = "success"
        log_entry.records_upserted = count
        log_entry.effective_date = eff_date
        db.commit()
        log.info("Sync done — %d records.", count)

    except Exception as exc:
        log.exception("Sync failed: %s", exc)
        log_entry.status = "failed"
        log_entry.error_message = str(exc)[:900]
        db.commit()
    finally:
        if pdf_path and os.path.exists(pdf_path):
            if log_entry.effective_date:
                _save_pdf_to_storage(pdf_path, log_entry.effective_date)
            os.remove(pdf_path)
        db.close()


# Backfill state — one active job at a time
_backfill_state: Dict[str, object] = {}


def background_backfill_task(factory) -> None:
    """Download every EWURA PDF listed on the pricing page and import it."""
    global _backfill_state
    _backfill_state = {
        "status": "running",
        "started_at": datetime.utcnow().isoformat(),
        "total": 0,
        "done": 0,
        "skipped": 0,
        "failed_urls": [],
        "total_records": 0,
    }

    db = factory()
    try:
        all_urls = _scrape_all_candidate_urls()
        log.info("Backfill: found %d candidate PDFs", len(all_urls))
        _backfill_state["total"] = len(all_urls)

        # collect dates already in DB so we can skip them
        existing_dates: set = {
            r[0]
            for r in db.query(FuelPrice.effective_date).distinct().all()
        }

        for url in reversed(all_urls):  # oldest first (list is newest-first)
            pdf_path: Optional[str] = None
            try:
                head = requests.head(url, headers=_HTTP_HEADERS, timeout=10, allow_redirects=True)
                if head.status_code != 200:
                    log.warning("Backfill: skip %s → %s", url, head.status_code)
                    _backfill_state["skipped"] = _backfill_state["skipped"] + 1  # type: ignore[operator]
                    continue

                pdf_path = download_pdf(url)
                eff_date = extract_date_from_pdf(pdf_path)

                if eff_date is None:
                    log.warning("Backfill: cannot extract date from %s, skipping", url)
                    _backfill_state["skipped"] = _backfill_state["skipped"] + 1  # type: ignore[operator]
                    continue

                if eff_date in existing_dates:
                    log.info("Backfill: date %s already imported, skipping %s", eff_date, url)
                    _backfill_state["skipped"] = _backfill_state["skipped"] + 1  # type: ignore[operator]
                    continue

                count, eff_date = parse_ewura_pdf(pdf_path, eff_date, db)
                existing_dates.add(eff_date)
                _backfill_state["total_records"] = _backfill_state["total_records"] + count  # type: ignore[operator]

                log_entry = SyncLog(
                    triggered_at=datetime.utcnow().isoformat(),
                    status="success",
                    pdf_url=url,
                    effective_date=eff_date,
                    records_upserted=count,
                )
                db.add(log_entry)
                db.commit()
                log.info("Backfill: imported %s → %d records", eff_date, count)

            except Exception as exc:
                log.warning("Backfill: failed %s — %s", url, exc)
                _backfill_state["failed_urls"].append(url)  # type: ignore[union-attr]
                try:
                    log_entry = SyncLog(
                        triggered_at=datetime.utcnow().isoformat(),
                        status="failed",
                        pdf_url=url,
                        error_message=str(exc)[:900],
                    )
                    db.add(log_entry)
                    db.commit()
                except Exception:
                    db.rollback()
            finally:
                if pdf_path and os.path.exists(pdf_path):
                    if eff_date:
                        _save_pdf_to_storage(pdf_path, eff_date)
                    os.remove(pdf_path)
                _backfill_state["done"] = _backfill_state["done"] + 1  # type: ignore[operator]

    except Exception as exc:
        log.exception("Backfill outer error: %s", exc)
        _backfill_state["status"] = "failed"
    else:
        _backfill_state["status"] = "complete"
        _backfill_state["finished_at"] = datetime.utcnow().isoformat()
        log.info(
            "Backfill complete — %d records across %d PDFs",
            _backfill_state["total_records"],
            _backfill_state["done"],
        )
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Background Scheduler — automatic Brent + EWURA syncing
# ─────────────────────────────────────────────────────────────────────────────

_scheduler = AsyncIOScheduler()


def _sync_brent_prices() -> dict:
    """Pull Brent crude monthly prices from EIA and upsert into DB."""
    url = f"https://api.eia.gov/v2/seriesid/PET.RBRTE.M?api_key={EIA_API_KEY}&length=200"
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.error("Brent auto-sync: EIA request failed — %s", exc)
        return {"status": "failed", "error": str(exc)}

    rows = data.get("response", {}).get("data", [])
    if not rows:
        log.warning("Brent auto-sync: EIA returned no data rows")
        return {"status": "no_data"}

    db = SessionLocal()
    upserted = updated = 0
    try:
        for row in rows:
            period = row.get("period", "")
            value = row.get("value")
            if not period or value is None:
                continue
            try:
                yr, mo = int(period[:4]), int(period[5:7])
                price_date = date(yr, mo, 1)
                price_usd = float(value)
            except (ValueError, TypeError):
                continue
            existing = db.query(BrentCrude).filter(BrentCrude.price_date == price_date).first()
            if existing:
                existing.price_usd = price_usd
                updated += 1
            else:
                db.add(BrentCrude(price_date=price_date, price_usd=price_usd))
                upserted += 1
        db.commit()
        cache_clear()
        log.info("Brent auto-sync done — inserted=%d updated=%d", upserted, updated)
        return {"status": "ok", "inserted": upserted, "updated": updated}
    except Exception as exc:
        db.rollback()
        log.exception("Brent auto-sync DB error: %s", exc)
        return {"status": "failed", "error": str(exc)}
    finally:
        db.close()


def _sync_brent_spot_yf() -> dict:
    """Fetch recent Brent crude daily closes from Yahoo Finance (BZ=F ticker)
    and upsert the month-to-date average for the current month only.

    No API key needed. Runs daily to keep the current (incomplete) month accurate
    since EIA publishes with a ~30-day lag. Completed months are left to EIA.
    """
    try:
        import yfinance as yf
    except ImportError:
        log.warning("Brent YF sync: yfinance not installed — skipping")
        return {"status": "skipped", "reason": "yfinance not installed"}

    try:
        ticker = yf.Ticker("BZ=F")
        hist = ticker.history(period="40d")
    except Exception as exc:
        log.error("Brent YF sync: fetch failed — %s", exc)
        return {"status": "failed", "error": str(exc)}

    if hist.empty:
        log.warning("Brent YF sync: Yahoo Finance returned empty history")
        return {"status": "no_data"}

    today = date.today()
    # Collect only trading days in the current month
    current_month_prices: List[float] = []
    for dt, row in hist.iterrows():
        if dt.year == today.year and dt.month == today.month:
            close = float(row["Close"])
            if close > 0:
                current_month_prices.append(close)

    if not current_month_prices:
        return {"status": "no_data", "reason": "No trading days found for current month"}

    avg = round(sum(current_month_prices) / len(current_month_prices), 2)
    price_date = date(today.year, today.month, 1)

    db = SessionLocal()
    try:
        existing = db.query(BrentCrude).filter(BrentCrude.price_date == price_date).first()
        if existing:
            existing.price_usd = avg
            action = "updated"
        else:
            db.add(BrentCrude(price_date=price_date, price_usd=avg))
            action = "inserted"
        db.commit()
        cache_clear()
        log.info(
            "Brent YF sync done — %s %s = $%.2f (%d trading days)",
            action, price_date, avg, len(current_month_prices),
        )
        return {
            "status": "ok",
            action: 1,
            "month": str(price_date),
            "price_usd": avg,
            "trading_days": len(current_month_prices),
        }
    except Exception as exc:
        db.rollback()
        log.exception("Brent YF sync DB error: %s", exc)
        return {"status": "failed", "error": str(exc)}
    finally:
        db.close()


def _sync_tzs_usd_rates() -> dict:
    """Fetch TZS/USD exchange rates and upsert into DB.

    Sources (tried in order):
    1. FRED monthly series DEXTANUS (requires FRED_API_KEY env var — free at fred.stlouisfed.org)
    2. World Bank PA.NUS.FCRF annual official rate + linear interpolation to monthly (full history)
    3. open.er-api.com for the current month's spot rate (no key needed)
    """
    from calendar import monthrange as _monthrange

    rates: Dict[Tuple[int, int], float] = {}  # (year, month) -> TZS/USD

    # ── 1. FRED monthly (most accurate if key available) ──────────────────────
    if FRED_API_KEY:
        try:
            url = (
                "https://api.stlouisfed.org/fred/series/observations"
                f"?series_id=DEXTANUS&api_key={FRED_API_KEY}&file_type=json"
                "&frequency=m&aggregation_method=avg&observation_start=1960-01-01"
            )
            resp = requests.get(url, timeout=20)
            if resp.status_code == 200:
                for obs in resp.json().get("observations", []):
                    v = obs.get("value", ".")
                    if v == ".":
                        continue
                    try:
                        d = date.fromisoformat(obs["date"])
                        rates[(d.year, d.month)] = float(v)
                    except (ValueError, KeyError, TypeError):
                        continue
                log.info("FX sync: fetched %d monthly rows from FRED", len(rates))
        except Exception as exc:
            log.warning("FX sync: FRED failed (%s), falling back to World Bank", exc)

    # ── 2. World Bank annual (no key needed, full history back to 1960) ───────
    if not rates:
        try:
            url = (
                "https://api.worldbank.org/v2/country/TZA/indicator/PA.NUS.FCRF"
                "?format=json&per_page=100&mrv=70"
            )
            resp = requests.get(url, timeout=20)
            if resp.status_code == 200:
                payload = resp.json()
                annual: Dict[int, float] = {}
                for entry in (payload[1] if len(payload) > 1 else []):
                    yr_str = entry.get("date", "")
                    val = entry.get("value")
                    if val and yr_str:
                        try:
                            annual[int(yr_str)] = float(val)
                        except (ValueError, TypeError):
                            pass

                # Linear interpolation from annual to monthly
                sorted_years = sorted(annual.keys())
                for i, yr in enumerate(sorted_years):
                    rate_start = annual[yr]
                    rate_end = annual[sorted_years[i + 1]] if i + 1 < len(sorted_years) else rate_start
                    for mo in range(1, 13):
                        # Blend linearly within the year towards next year's rate
                        frac = (mo - 1) / 12.0
                        rates[(yr, mo)] = round(rate_start + frac * (rate_end - rate_start), 2)

                log.info("FX sync: interpolated %d monthly points from %d World Bank annual rows", len(rates), len(annual))
        except Exception as exc:
            log.warning("FX sync: World Bank failed — %s", exc)

    # ── 3. Current spot rate to fill the most recent months ───────────────────
    try:
        resp = requests.get("https://open.er-api.com/v6/latest/USD", timeout=10)
        if resp.status_code == 200:
            tzs_rate = resp.json().get("rates", {}).get("TZS")
            if tzs_rate:
                today = date.today()
                # Stamp to the 1st of current month
                rates[(today.year, today.month)] = round(float(tzs_rate), 2)
                log.info("FX sync: current spot rate %.2f TZS/USD from open.er-api.com", tzs_rate)
    except Exception as exc:
        log.warning("FX sync: open.er-api.com failed — %s", exc)

    if not rates:
        return {"status": "no_data", "error": "All FX data sources failed"}

    db = SessionLocal()
    inserted = updated = 0
    try:
        for (yr, mo), rate_val in rates.items():
            try:
                rate_date = date(yr, mo, 1)
            except ValueError:
                continue
            existing = db.query(TzsUsdRate).filter(TzsUsdRate.rate_date == rate_date).first()
            if existing:
                existing.rate_tzs_per_usd = rate_val
                updated += 1
            else:
                db.add(TzsUsdRate(rate_date=rate_date, rate_tzs_per_usd=rate_val))
                inserted += 1
        db.commit()
        cache_clear()
        log.info("FX sync done — inserted=%d updated=%d", inserted, updated)
        return {"status": "ok", "inserted": inserted, "updated": updated, "total": inserted + updated}
    except Exception as exc:
        db.rollback()
        log.exception("FX sync DB error: %s", exc)
        return {"status": "failed", "error": str(exc)}
    finally:
        db.close()


async def _scheduled_ewura_sync():
    """Monthly EWURA price bulletin sync — runs the scraper in a thread pool."""
    log.info("Scheduled EWURA sync triggered")
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, background_scraper_task, SessionLocal)


async def _scheduled_brent_sync():
    """Monthly Brent crude sync — EIA historical data, runs in a thread pool."""
    log.info("Scheduled Brent sync triggered")
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _sync_brent_prices)
    log.info("Scheduled Brent sync result: %s", result)


async def _scheduled_brent_spot_sync():
    """Daily Brent spot sync — Yahoo Finance current-month data, runs in a thread pool."""
    log.info("Scheduled Brent spot sync triggered")
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _sync_brent_spot_yf)
    log.info("Scheduled Brent spot sync result: %s", result)


async def _scheduled_fx_sync():
    """Monthly TZS/USD exchange rate sync — runs in a thread pool."""
    log.info("Scheduled FX sync triggered")
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _sync_tzs_usd_rates)
    log.info("Scheduled FX sync result: %s", result)


# ─────────────────────────────────────────────────────────────────────────────
# App Bootstrap
# ─────────────────────────────────────────────────────────────────────────────


def _seed_fx_on_boot():
    """Run FX sync on boot only if the table is empty."""
    db = SessionLocal()
    try:
        count = db.query(TzsUsdRate).count()
    finally:
        db.close()
    if count == 0:
        log.info("tzs_usd_rate table empty — seeding exchange rate history on boot")
        _sync_tzs_usd_rates()


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    os.makedirs(PDF_STORAGE_DIR, exist_ok=True)
    log.info(
        "DB tables ready (%s). ADMIN_SECRET_KEY %s. PDF storage: %s",
        "SQLite" if IS_SQLITE else "PostgreSQL",
        "SET" if ADMIN_SECRET_KEY else "NOT SET — admin endpoints disabled",
        PDF_STORAGE_DIR,
    )

    # Schedule automatic monthly syncs
    # EWURA publishes new bulletins around the 1st–5th; run on the 6th to be safe
    _scheduler.add_job(
        _scheduled_ewura_sync,
        CronTrigger(day=6, hour=7, minute=0, timezone="Africa/Dar_es_Salaam"),
        id="ewura_monthly",
        replace_existing=True,
    )
    # EIA Brent data is released ~30 days after month-end; fetch on the 5th
    _scheduler.add_job(
        _scheduled_brent_sync,
        CronTrigger(day=5, hour=6, minute=0, timezone="UTC"),
        id="brent_monthly",
        replace_existing=True,
    )
    # Daily Brent spot sync via Yahoo Finance — keeps current month accurate (EIA lags ~30 days)
    _scheduler.add_job(
        _scheduled_brent_spot_sync,
        CronTrigger(hour=8, minute=0, timezone="UTC"),
        id="brent_daily_spot",
        replace_existing=True,
    )
    # Exchange rate sync: World Bank updates annually; open.er-api gives current spot on the 5th
    _scheduler.add_job(
        _scheduled_fx_sync,
        CronTrigger(day=5, hour=6, minute=30, timezone="UTC"),
        id="fx_monthly",
        replace_existing=True,
    )
    _scheduler.start()
    log.info(
        "Scheduler started — EWURA sync: 6th 07:00 EAT · "
        "Brent EIA: 5th 06:00 UTC · Brent spot (YF): daily 08:00 UTC · FX sync: 5th 06:30 UTC"
    )

    # Seed exchange rate history on first boot if table is empty
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, _seed_fx_on_boot)

    yield

    _scheduler.shutdown(wait=False)
    log.info("Scheduler shut down")


app = FastAPI(
    title="Tanzania Fuel Price API",
    version="2.0.0",
    description=(
        "Real-time Tanzania fuel cap prices sourced from EWURA monthly PDFs. "
        "Covers petrol, diesel, and kerosene across all mainland and Zanzibar districts. "
        "Includes price trends, national statistics, CSV export, and district comparison."
    ),
    contact={
        "name": "Tanzania Fuel Price API",
        "url": "https://github.com/your-org/tz-fuel-api",
    },
    license_info={"name": "MIT"},
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
    expose_headers=["X-Cache", "X-Total-Count"],
)

_ANALYTICS_URL = os.getenv("ANALYTICS_URL", "http://analytics:8000")
_ANALYTICS_KEY = os.getenv("ANALYTICS_KEY", "")

def _post_hit_fuel(payload: dict) -> None:
    try:
        import urllib.request as _ur, json as _j
        data = _j.dumps(payload).encode()
        req = _ur.Request(
            f"{_ANALYTICS_URL}/hit", data=data,
            headers={"Content-Type": "application/json", "X-Analytics-Key": _ANALYTICS_KEY},
            method="POST",
        )
        _ur.urlopen(req, timeout=2)
    except Exception:
        pass

@app.middleware("http")
async def _analytics_mw(request: Request, call_next):
    response = await call_next(request)
    path = request.url.path
    if path not in {"/health", "/docs", "/redoc", "/openapi.json"}:
        import threading
        ip = (request.headers.get("x-forwarded-for", "").split(",")[0].strip()
              or (request.client.host if request.client else ""))
        threading.Thread(target=_post_hit_fuel, daemon=True, args=({
            "project": "tz-fuel-api", "path": path, "ip": ip,
            "user_agent": request.headers.get("user-agent", ""),
            "referrer": request.headers.get("referer", ""),
            "language": request.headers.get("accept-language", "").split(",")[0].strip(),
            "is_action": request.method == "POST",
            "status_code": response.status_code,
        },)).start()
    return response


# ─────────────────────────────────────────────────────────────────────────────
# Shared query helpers
# ─────────────────────────────────────────────────────────────────────────────


def _require_latest_date(db: Session) -> date:
    row = (
        db.query(FuelPrice.effective_date)
        .order_by(FuelPrice.effective_date.desc())
        .first()
    )
    if not row:
        raise HTTPException(
            status_code=404,
            detail="No fuel price data available yet. Trigger a sync first via POST /api/v1/admin/trigger-sync.",
        )
    return row[0]


def _price_query(db: Session, eff_date: date):
    return (
        db.query(
            RegionDistrict.region_name,
            RegionDistrict.district_name,
            FuelPrice.petrol_price,
            FuelPrice.diesel_price,
            FuelPrice.kerosene_price,
            FuelPrice.effective_date,
        )
        .join(FuelPrice, FuelPrice.district_id == RegionDistrict.id)
        .filter(FuelPrice.effective_date == eff_date)
    )


# ─────────────────────────────────────────────────────────────────────────────
# Status & Meta
# ─────────────────────────────────────────────────────────────────────────────


@app.get("/", tags=["Meta"])
def root():
    return {
        "service": "Tanzania Fuel Price API",
        "version": "2.0.0",
        "docs": "/docs",
        "redoc": "/redoc",
        "endpoints": {
            "latest_prices": "/api/v1/prices/latest",
            "stats": "/api/v1/stats",
            "cheapest": "/api/v1/cheapest",
            "search": "/api/v1/search?q=<name>",
            "compare": "/api/v1/compare?district1=X&district2=Y",
            "history": "/api/v1/prices/district/{name}/history",
            "export_csv": "/api/v1/export/csv",
        },
        "analytics": {
            "trend": "/api/v1/analytics/trend",
            "volatility": "/api/v1/analytics/volatility",
            "regional_gap": "/api/v1/analytics/regional-gap",
            "peak_records": "/api/v1/analytics/peak",
            "inflation_index": "/api/v1/analytics/inflation",
            "correlation": "/api/v1/analytics/correlation",
            "forecast": "/api/v1/analytics/forecast",
        },
    }


@app.get("/health", tags=["Meta"])
def health(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        db_ok = False
    return {
        "status": "ok" if db_ok else "degraded",
        "database": "ok" if db_ok else "error",
        "cache_entries": len(_CACHE),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Metadata / Discovery
# ─────────────────────────────────────────────────────────────────────────────


@app.get("/api/v1/dates", response_model=AvailableDatesResponse, tags=["Discovery"])
def available_dates(db: Session = Depends(get_db)):
    """All effective dates for which data exists, newest first."""
    cache_key = "dates"
    hit = cache_get(cache_key)
    if hit is not None:
        return hit
    rows = (
        db.query(FuelPrice.effective_date)
        .distinct()
        .order_by(FuelPrice.effective_date.desc())
        .all()
    )
    result = {"dates": [r[0] for r in rows], "total": len(rows)}
    cache_set(cache_key, result)
    return result


@app.get("/api/v1/regions", tags=["Discovery"])
def list_regions(db: Session = Depends(get_db)):
    """All distinct region names."""
    rows = (
        db.query(RegionDistrict.region_name)
        .distinct()
        .order_by(RegionDistrict.region_name)
        .all()
    )
    return {"regions": [r[0] for r in rows], "total": len(rows)}


@app.get("/api/v1/districts", response_model=List[DistrictListItem], tags=["Discovery"])
def list_districts(
    region: Optional[str] = Query(None, description="Filter by region (partial match)"),
    db: Session = Depends(get_db),
):
    """All districts, optionally filtered by region."""
    q = db.query(RegionDistrict).order_by(RegionDistrict.region_name, RegionDistrict.district_name)
    if region:
        q = q.filter(RegionDistrict.region_name.ilike(f"%{region}%"))
    return q.all()


# ─────────────────────────────────────────────────────────────────────────────
# Price Endpoints
# ─────────────────────────────────────────────────────────────────────────────


@app.get(
    "/api/v1/prices/latest",
    response_model=LatestPricesResponse,
    tags=["Prices"],
)
def latest_prices(
    page: int = Query(1, ge=1),
    page_size: int = Query(500, ge=1, le=500),
    request: Request = None,
    db: Session = Depends(get_db),
):
    """All district prices for the most recent effective date."""
    _check_rate_limit(request)
    eff_date = _require_latest_date(db)
    cache_key = f"latest:{page}:{page_size}"
    hit = cache_get(cache_key)
    if hit is not None:
        return hit
    rows = (
        _price_query(db, eff_date)
        .order_by(RegionDistrict.region_name, RegionDistrict.district_name)
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )
    result = LatestPricesResponse(
        effective_date=eff_date,
        total_districts=len(rows),
        prices=rows,
    )
    cache_set(cache_key, result)
    return result


@app.get("/api/v1/prices/date/{effective_date}", response_model=List[PriceResponse], tags=["Prices"])
def prices_by_date(
    effective_date: date,
    request: Request = None,
    db: Session = Depends(get_db),
):
    """All district prices for a specific historical date (YYYY-MM-DD)."""
    _check_rate_limit(request)
    rows = (
        _price_query(db, effective_date)
        .order_by(RegionDistrict.region_name, RegionDistrict.district_name)
        .all()
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data for {effective_date}.")
    return rows


@app.get("/api/v1/prices/region/{region}", response_model=List[PriceResponse], tags=["Prices"])
def prices_by_region(
    region: str,
    request: Request = None,
    db: Session = Depends(get_db),
):
    """Latest prices for all districts in a region (partial name match)."""
    _check_rate_limit(request)
    eff_date = _require_latest_date(db)
    rows = (
        _price_query(db, eff_date)
        .filter(RegionDistrict.region_name.ilike(f"%{region}%"))
        .order_by(RegionDistrict.district_name)
        .all()
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data for region '{region}'.")
    return rows


@app.get("/api/v1/prices/district/{district}", response_model=PriceResponse, tags=["Prices"])
def price_by_district(
    district: str,
    request: Request = None,
    db: Session = Depends(get_db),
):
    """Latest price for a single district (partial name match)."""
    _check_rate_limit(request)
    eff_date = _require_latest_date(db)
    row = (
        _price_query(db, eff_date)
        .filter(RegionDistrict.district_name.ilike(f"%{district}%"))
        .first()
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"District '{district}' not found.")
    return row


@app.get(
    "/api/v1/prices/district/{district}/history",
    tags=["Prices"],
)
def district_history(
    district: str,
    limit: int = Query(200, ge=1, le=300),
    request: Request = None,
    db: Session = Depends(get_db),
):
    """Price history for a district across all available dates, oldest first."""
    _check_rate_limit(request)
    rows = (
        db.query(
            RegionDistrict.region_name,
            RegionDistrict.district_name,
            FuelPrice.petrol_price,
            FuelPrice.diesel_price,
            FuelPrice.kerosene_price,
            FuelPrice.effective_date,
        )
        .join(FuelPrice, FuelPrice.district_id == RegionDistrict.id)
        .filter(RegionDistrict.district_name.ilike(f"%{district}%"))
        .order_by(FuelPrice.effective_date.asc())
        .limit(limit)
        .all()
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No history for '{district}'.")
    return {
        "district_name": rows[0][1],
        "region_name": rows[0][0],
        "total_records": len(rows),
        "history": [
            {
                "effective_date": r[5],
                "petrol_price": r[2],
                "diesel_price": r[3],
                "kerosene_price": r[4],
            }
            for r in rows
        ],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Analytics Endpoints
# ─────────────────────────────────────────────────────────────────────────────


@app.get("/api/v1/stats", response_model=NationalStatsResponse, tags=["Analytics"])
def national_stats(
    request: Request = None,
    db: Session = Depends(get_db),
):
    """National aggregate statistics (avg / min / max) for the latest effective date."""
    _check_rate_limit(request)
    cache_key = "stats:latest"
    hit = cache_get(cache_key)
    if hit is not None:
        return hit

    eff_date = _require_latest_date(db)
    agg = (
        db.query(
            func.count(FuelPrice.id),
            func.avg(FuelPrice.petrol_price),
            func.min(FuelPrice.petrol_price),
            func.max(FuelPrice.petrol_price),
            func.avg(FuelPrice.diesel_price),
            func.min(FuelPrice.diesel_price),
            func.max(FuelPrice.diesel_price),
            func.avg(FuelPrice.kerosene_price),
            func.min(FuelPrice.kerosene_price),
            func.max(FuelPrice.kerosene_price),
        )
        .filter(FuelPrice.effective_date == eff_date)
        .first()
    )

    def _cheapest(col):
        r = (
            db.query(RegionDistrict.district_name)
            .join(FuelPrice, FuelPrice.district_id == RegionDistrict.id)
            .filter(FuelPrice.effective_date == eff_date)
            .order_by(col.asc())
            .first()
        )
        return r[0] if r else ""

    def _priciest(col):
        r = (
            db.query(RegionDistrict.district_name)
            .join(FuelPrice, FuelPrice.district_id == RegionDistrict.id)
            .filter(FuelPrice.effective_date == eff_date)
            .order_by(col.desc())
            .first()
        )
        return r[0] if r else ""

    result = NationalStatsResponse(
        effective_date=eff_date,
        district_count=agg[0] or 0,
        petrol=FuelStats(
            avg=round(float(agg[1] or 0), 2),
            min=float(agg[2] or 0),
            max=float(agg[3] or 0),
            cheapest_district=_cheapest(FuelPrice.petrol_price),
            most_expensive_district=_priciest(FuelPrice.petrol_price),
        ),
        diesel=FuelStats(
            avg=round(float(agg[4] or 0), 2),
            min=float(agg[5] or 0),
            max=float(agg[6] or 0),
            cheapest_district=_cheapest(FuelPrice.diesel_price),
            most_expensive_district=_priciest(FuelPrice.diesel_price),
        ),
        kerosene=FuelStats(
            avg=round(float(agg[7] or 0), 2),
            min=float(agg[8] or 0),
            max=float(agg[9] or 0),
            cheapest_district=_cheapest(FuelPrice.kerosene_price),
            most_expensive_district=_priciest(FuelPrice.kerosene_price),
        ),
    )
    cache_set(cache_key, result)
    return result


@app.get("/api/v1/search", response_model=List[PriceResponse], tags=["Analytics"])
def search(
    q: str = Query(..., min_length=2, description="District or region name fragment"),
    request: Request = None,
    db: Session = Depends(get_db),
):
    """Search districts and regions by name fragment (latest prices)."""
    _check_rate_limit(request)
    eff_date = _require_latest_date(db)
    rows = (
        _price_query(db, eff_date)
        .filter(
            RegionDistrict.district_name.ilike(f"%{q}%")
            | RegionDistrict.region_name.ilike(f"%{q}%")
        )
        .order_by(RegionDistrict.region_name, RegionDistrict.district_name)
        .all()
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No matches for '{q}'.")
    return rows


@app.get("/api/v1/compare", response_model=PriceComparisonResponse, tags=["Analytics"])
def compare(
    district1: str = Query(..., description="First district name"),
    district2: str = Query(..., description="Second district name"),
    request: Request = None,
    db: Session = Depends(get_db),
):
    """Side-by-side fuel price comparison between two districts."""
    _check_rate_limit(request)
    eff_date = _require_latest_date(db)

    def _get(name: str):
        row = (
            _price_query(db, eff_date)
            .filter(RegionDistrict.district_name.ilike(f"%{name}%"))
            .first()
        )
        if not row:
            raise HTTPException(status_code=404, detail=f"District '{name}' not found.")
        return row

    d1, d2 = _get(district1), _get(district2)
    return {
        "district_1": d1,
        "district_2": d2,
        "petrol_difference": round(abs(d1.petrol_price - d2.petrol_price), 2),
        "diesel_difference": round(abs(d1.diesel_price - d2.diesel_price), 2),
        "cheaper_district_petrol": (
            d1.district_name if d1.petrol_price <= d2.petrol_price else d2.district_name
        ),
        "cheaper_district_diesel": (
            d1.district_name if d1.diesel_price <= d2.diesel_price else d2.district_name
        ),
    }


@app.get("/api/v1/cheapest", tags=["Analytics"])
def cheapest(
    fuel_type: str = Query("petrol", pattern="^(petrol|diesel|kerosene)$"),
    limit: int = Query(10, ge=1, le=50),
    request: Request = None,
    db: Session = Depends(get_db),
):
    """Districts with the lowest price for a given fuel type (latest date)."""
    _check_rate_limit(request)
    eff_date = _require_latest_date(db)
    col_map = {
        "petrol": FuelPrice.petrol_price,
        "diesel": FuelPrice.diesel_price,
        "kerosene": FuelPrice.kerosene_price,
    }
    col = col_map[fuel_type]
    rows = _price_query(db, eff_date).order_by(col.asc()).limit(limit).all()
    price_attr = f"{fuel_type}_price"
    return {
        "fuel_type": fuel_type,
        "effective_date": eff_date,
        "results": [
            {
                "district_name": r.district_name,
                "region_name": r.region_name,
                "price": getattr(r, price_attr),
                "effective_date": r.effective_date,
            }
            for r in rows
        ],
    }


@app.get("/api/v1/most-expensive", response_model=List[PriceResponse], tags=["Analytics"])
def most_expensive(
    fuel: str = Query("petrol", pattern="^(petrol|diesel|kerosene)$"),
    top: int = Query(10, ge=1, le=50),
    request: Request = None,
    db: Session = Depends(get_db),
):
    """Districts with the highest price for a given fuel type (latest date)."""
    _check_rate_limit(request)
    eff_date = _require_latest_date(db)
    col_map = {
        "petrol": FuelPrice.petrol_price,
        "diesel": FuelPrice.diesel_price,
        "kerosene": FuelPrice.kerosene_price,
    }
    return _price_query(db, eff_date).order_by(col_map[fuel].desc()).limit(top).all()


@app.get(
    "/api/v1/trends/district/{district}",
    response_model=DistrictTrendResponse,
    tags=["Analytics"],
)
def district_trends(
    district: str,
    periods: int = Query(6, ge=2, le=24, description="Number of months to include"),
    request: Request = None,
    db: Session = Depends(get_db),
):
    """
    Month-over-month price trends for a district with percentage change.
    Useful for charting fuel price evolution over time.
    """
    _check_rate_limit(request)
    rows = (
        db.query(
            RegionDistrict.region_name,
            RegionDistrict.district_name,
            FuelPrice.petrol_price,
            FuelPrice.diesel_price,
            FuelPrice.kerosene_price,
            FuelPrice.effective_date,
        )
        .join(FuelPrice, FuelPrice.district_id == RegionDistrict.id)
        .filter(RegionDistrict.district_name.ilike(f"%{district}%"))
        .order_by(FuelPrice.effective_date.asc())
        .limit(periods)
        .all()
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No trend data for '{district}'.")

    trend: List[TrendEntry] = []
    for i, row in enumerate(rows):
        entry = TrendEntry(
            effective_date=row.effective_date,
            petrol_price=row.petrol_price,
            diesel_price=row.diesel_price,
            kerosene_price=row.kerosene_price,
        )
        if i > 0:
            prev = rows[i - 1]
            def pct(curr, prev_val):
                return round((curr - prev_val) / prev_val * 100, 2) if prev_val else None
            entry.petrol_change_pct = pct(row.petrol_price, prev.petrol_price)
            entry.diesel_change_pct = pct(row.diesel_price, prev.diesel_price)
            entry.kerosene_change_pct = pct(row.kerosene_price, prev.kerosene_price)
        trend.append(entry)

    trend.reverse()  # newest first in response
    return {
        "district_name": rows[0].district_name,
        "region_name": rows[0].region_name,
        "trend": trend,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Analytics — Deep Historical Analysis
# ─────────────────────────────────────────────────────────────────────────────


def _ols(xs: List[float], ys: List[float]) -> Tuple[float, float, float]:
    """Ordinary least squares: returns (slope, intercept, r_squared)."""
    n = len(xs)
    if n < 2:
        return 0.0, ys[0] if ys else 0.0, 0.0
    x_mean = sum(xs) / n
    y_mean = sum(ys) / n
    num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    den = sum((x - x_mean) ** 2 for x in xs)
    slope = num / den if den else 0.0
    intercept = y_mean - slope * x_mean
    ss_res = sum((y - (slope * x + intercept)) ** 2 for x, y in zip(xs, ys))
    ss_tot = sum((y - y_mean) ** 2 for y in ys)
    r2 = 1.0 - ss_res / ss_tot if ss_tot else 1.0
    return slope, intercept, max(0.0, r2)


def _compute_r2(ys: List[float], fitted_values: List[float]) -> float:
    import math as _math
    actual = ys[-len(fitted_values):]
    # Drop pairs where the fitted value is NaN/inf (common at start of ARIMA/SARIMA series)
    pairs = [(a, f) for a, f in zip(actual, fitted_values) if _math.isfinite(a) and _math.isfinite(f)]
    if not pairs:
        return 0.0
    a_vals = [a for a, _ in pairs]
    f_vals = [f for _, f in pairs]
    y_mean = sum(a_vals) / len(a_vals)
    ss_res = sum((a - f) ** 2 for a, f in zip(a_vals, f_vals))
    ss_tot = sum((a - y_mean) ** 2 for a in a_vals)
    return max(0.0, round(1.0 - ss_res / ss_tot, 4)) if ss_tot > 0 else 1.0


def _project_brent(brent_series: List[float], periods: int) -> List[float]:
    """Project Brent crude N months forward using ARIMA(1,1,0) — simple random walk with drift."""
    try:
        import numpy as np, warnings
        from statsmodels.tsa.arima.model import ARIMA as _ARIMA
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            fit = _ARIMA(np.asarray(brent_series, dtype=float), order=(1, 1, 0)).fit()
        return [round(float(v), 2) for v in fit.get_forecast(steps=periods).predicted_mean]
    except Exception:
        n = len(brent_series)
        m, b, _ = _ols(list(range(n)), brent_series)
        return [round(m * (n + i) + b, 2) for i in range(1, periods + 1)]


def _project_fx(fx_series: List[float], periods: int) -> List[float]:
    """Project TZS/USD exchange rate N months forward using ARIMA(1,1,0)."""
    try:
        import numpy as np, warnings
        from statsmodels.tsa.arima.model import ARIMA as _ARIMA
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            fit = _ARIMA(np.asarray(fx_series, dtype=float), order=(1, 1, 0)).fit()
        return [round(float(v), 2) for v in fit.get_forecast(steps=periods).predicted_mean]
    except Exception:
        n = len(fx_series)
        m, b, _ = _ols(list(range(n)), fx_series)
        return [round(m * (n + i) + b, 2) for i in range(1, periods + 1)]


def _best_forecast(
    ys: List[float],
    periods: int,
    brent_exog: Optional[List[float]] = None,
    brent_future: Optional[List[float]] = None,
    fx_exog: Optional[List[float]] = None,
    fx_future: Optional[List[float]] = None,
) -> Tuple[List[float], List[float], List[float], float, str, Dict]:
    """
    Fits ARIMA(1,1,1), SARIMA(1,1,1)(1,0,0,12), Holt-Winters ETS, and (when Brent
    data is available) SARIMAX(1,1,1)(1,0,0,12) with oil cost in TZS (Brent × TZS/USD)
    as exogenous variable — or Brent alone if exchange rate data is unavailable.
    Selects the lowest-AIC model and returns its forecast.
    Returns (means, ci_lows, ci_highs, r_squared, model_name, comparison_dict).
    Falls back to OLS if all time-series models fail.
    """
    import warnings
    import numpy as np

    arr = np.asarray(ys, dtype=float)
    n = len(arr)
    candidates = []
    comparison: Dict[str, Dict] = {}

    def _try_arima():
        from statsmodels.tsa.arima.model import ARIMA as _ARIMA
        fit = _ARIMA(arr, order=(1, 1, 1)).fit()
        fcast = fit.get_forecast(steps=periods)
        ci = np.asarray(fcast.conf_int(alpha=0.05))
        means = [round(float(v), 2) for v in fcast.predicted_mean]
        ci_lows = [round(float(ci[i, 0]), 2) for i in range(periods)]
        ci_highs = [round(float(ci[i, 1]), 2) for i in range(periods)]
        r2 = _compute_r2(ys, [float(v) for v in fit.fittedvalues])
        return float(fit.aic), means, ci_lows, ci_highs, r2, "ARIMA(1,1,1)"

    def _try_sarima():
        from statsmodels.tsa.statespace.sarimax import SARIMAX as _SARIMAX
        fit = _SARIMAX(arr, order=(1, 1, 1), seasonal_order=(1, 0, 0, 12)).fit(disp=False)
        fcast = fit.get_forecast(steps=periods)
        ci = np.asarray(fcast.conf_int(alpha=0.05))
        means = [round(float(v), 2) for v in fcast.predicted_mean]
        ci_lows = [round(float(ci[i, 0]), 2) for i in range(periods)]
        ci_highs = [round(float(ci[i, 1]), 2) for i in range(periods)]
        r2 = _compute_r2(ys, [float(v) for v in fit.fittedvalues])
        return float(fit.aic), means, ci_lows, ci_highs, r2, "SARIMA(1,1,1)(1,0,0,12)"

    def _try_holtwinters():
        from statsmodels.tsa.holtwinters import ExponentialSmoothing as _ES
        fit_hw = _ES(
            arr, trend="add", damped_trend=True, seasonal="add", seasonal_periods=12,
            initialization_method="estimated",
        ).fit(optimized=True)
        fc = [round(float(v), 2) for v in fit_hw.forecast(steps=periods)]
        # Monte Carlo 95% CI via bootstrap simulation
        sims = fit_hw.simulate(nsimulations=periods, anchor="end", repetitions=400)
        # sims shape: (periods, 400)
        if sims.ndim == 2 and sims.shape[0] != periods:
            sims = sims.T
        ci_lows = [round(float(np.percentile(sims[i], 2.5)), 2) for i in range(periods)]
        ci_highs = [round(float(np.percentile(sims[i], 97.5)), 2) for i in range(periods)]
        r2 = _compute_r2(ys, [float(v) for v in fit_hw.fittedvalues])
        return float(fit_hw.aic), fc, ci_lows, ci_highs, r2, "Holt-Winters (ETS)"

    def _try_sarimax_x():
        from statsmodels.tsa.statespace.sarimax import SARIMAX as _SARIMAX

        has_fx = (
            fx_exog is not None
            and fx_future is not None
            and len(fx_exog) == n
            and len(fx_future) >= periods
            and sum(1 for v in fx_exog if v and v > 0) >= 12
        )

        if has_fx:
            # Combined variable: "what a barrel of oil actually costs in TZS"
            # This is the direct cost driver for EWURA prices — much stronger predictor than Brent alone
            exog_train = np.array(
                [b * f for b, f in zip(brent_exog, fx_exog)], dtype=float
            ).reshape(-1, 1)
            exog_fcast = np.array(
                [b * f for b, f in zip(brent_future[:periods], fx_future[:periods])], dtype=float
            ).reshape(-1, 1)
            model_label = "SARIMAX-X (oil cost in TZS)"
        else:
            exog_train = np.array(brent_exog, dtype=float).reshape(-1, 1)
            exog_fcast = np.array(brent_future[:periods], dtype=float).reshape(-1, 1)
            model_label = "SARIMAX-X (Brent crude)"

        fit = _SARIMAX(
            arr, exog=exog_train, order=(1, 1, 1), seasonal_order=(1, 0, 0, 12)
        ).fit(disp=False)
        fcast = fit.get_forecast(steps=periods, exog=exog_fcast)
        ci = np.asarray(fcast.conf_int(alpha=0.05))
        means = [round(float(v), 2) for v in fcast.predicted_mean]
        ci_lows = [round(float(ci[i, 0]), 2) for i in range(periods)]
        ci_highs = [round(float(ci[i, 1]), 2) for i in range(periods)]
        r2 = _compute_r2(ys, [float(v) for v in fit.fittedvalues])
        return float(fit.aic), means, ci_lows, ci_highs, r2, model_label

    has_brent = (
        brent_exog is not None
        and brent_future is not None
        and len(brent_exog) == n
        and len(brent_future) >= periods
        and sum(1 for v in brent_exog if v > 0) >= 12
    )

    # Brent quality check: reject only truly implausible jumps (> 75% MoM) caused by stale DEMO_KEY data.
    # Real geopolitical shocks (e.g. Strait of Hormuz 2026: Feb→Mar +45%) are legitimate and must pass.
    _sarimax_key = "SARIMAX-X (oil cost in TZS)" if (
        fx_exog is not None and fx_future is not None
        and len(fx_exog) == n and len(fx_future) >= periods
        and sum(1 for v in fx_exog if v and v > 0) >= 12
    ) else "SARIMAX-X (Brent crude)"

    if has_brent:
        recent_b = [v for v in (brent_exog or [])[-6:] if v and v > 0]
        if len(recent_b) >= 2:
            mom_jumps = [abs(recent_b[i] - recent_b[i-1]) / recent_b[i-1] for i in range(1, len(recent_b))]
            if max(mom_jumps) > 0.75:
                has_brent = False
                comparison[_sarimax_key] = {
                    "aic": None, "r_squared": None, "converged": False, "selected": False,
                    "note": "Skipped — recent Brent data has implausible jump (>75% MoM), likely stale EIA DEMO_KEY data.",
                }

    model_fns = [
        ("ARIMA(1,1,1)", _try_arima, 6),
        ("SARIMA(1,1,1)(1,0,0,12)", _try_sarima, 24),
        ("Holt-Winters (ETS)", _try_holtwinters, 24),
    ]
    if has_brent:
        model_fns.append((_sarimax_key, _try_sarimax_x, 24))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for name, fn, min_n in model_fns:
            if n < min_n:
                comparison[name] = {"aic": None, "r_squared": None, "converged": False, "selected": False, "note": f"requires ≥{min_n} periods"}
                continue
            try:
                aic, means, ci_lows, ci_highs, r2, label = fn()
                comparison[name] = {"aic": round(aic, 2), "r_squared": r2, "converged": True, "selected": False}
                candidates.append((aic, means, ci_lows, ci_highs, r2, label))
            except Exception as exc:
                comparison[name] = {"aic": None, "r_squared": None, "converged": False, "selected": False, "error": str(exc)[:120]}

    if candidates:
        best = min(candidates, key=lambda c: c[0])
        selected_name = best[5]
        for k in comparison:
            if comparison[k].get("converged"):
                comparison[k]["selected"] = (k == selected_name)
        _, means, ci_lows, ci_highs, r2, name = best
        return means, ci_lows, ci_highs, r2, name, comparison

    # OLS fallback when all time-series models fail
    xs = list(range(n))
    m, b, r2 = _ols(xs, ys)
    residuals = [y - (m * x + b) for x, y in zip(xs, ys)]
    rse = math.sqrt(sum(r ** 2 for r in residuals) / max(n - 2, 1)) * 1.96
    means = [round(m * (n + i) + b, 2) for i in range(1, periods + 1)]
    ci_lows = [round(v - rse, 2) for v in means]
    ci_highs = [round(v + rse, 2) for v in means]
    comparison["OLS (fallback)"] = {"aic": None, "r_squared": round(r2, 4), "converged": True, "selected": True}
    return means, ci_lows, ci_highs, round(r2, 4), "OLS (fallback)", comparison


def _pearson(xs: List[float], ys: List[float]) -> float:
    """Pearson correlation coefficient between two equal-length sequences."""
    n = len(xs)
    if n < 2:
        return 0.0
    x_mean = sum(xs) / n
    y_mean = sum(ys) / n
    num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    den = math.sqrt(
        sum((x - x_mean) ** 2 for x in xs) * sum((y - y_mean) ** 2 for y in ys)
    )
    return round(num / den, 4) if den else 0.0


def _interp(label: str, r: float) -> str:
    a = abs(r)
    direction = "positive" if r >= 0 else "negative"
    if a >= 0.95:
        strength = "near-perfect"
    elif a >= 0.85:
        strength = "very strong"
    elif a >= 0.70:
        strength = "strong"
    elif a >= 0.50:
        strength = "moderate"
    else:
        strength = "weak"
    return f"{strength} {direction} correlation (r={r:.3f})"


def _cagr(start: float, end: float, years: float) -> float:
    """Compound annual growth rate as a percentage."""
    if start <= 0 or years <= 0:
        return 0.0
    return round(((end / start) ** (1.0 / years) - 1) * 100, 2)


@app.get(
    "/api/v1/analytics/trend",
    response_model=NationalTrendResponse,
    tags=["Analytics — Deep"],
    description=(
        "National average petrol, diesel, and kerosene prices across every available "
        "pricing date. Includes month-on-month % change. Ideal for time-series charts "
        "showing how Tanzania's fuel prices have evolved since 2009."
    ),
)
def analytics_trend(
    from_date: Optional[date] = Query(None, description="Start date (YYYY-MM-DD). Defaults to earliest."),
    to_date: Optional[date] = Query(None, description="End date (YYYY-MM-DD). Defaults to latest."),
    request: Request = None,
    db: Session = Depends(get_db),
):
    _check_rate_limit(request)
    cache_key = f"analytics:trend:{from_date}:{to_date}"
    hit = cache_get(cache_key)
    if hit is not None:
        return hit

    q = (
        db.query(
            FuelPrice.effective_date,
            func.round(func.avg(FuelPrice.petrol_price).cast(Numeric), 2),
            func.round(func.avg(FuelPrice.diesel_price).cast(Numeric), 2),
            func.round(func.avg(FuelPrice.kerosene_price).cast(Numeric), 2),
            func.count(FuelPrice.id),
        )
        .group_by(FuelPrice.effective_date)
        .order_by(FuelPrice.effective_date.asc())
    )
    if from_date:
        q = q.filter(FuelPrice.effective_date >= from_date)
    if to_date:
        q = q.filter(FuelPrice.effective_date <= to_date)

    rows = q.all()
    if not rows:
        raise HTTPException(status_code=404, detail="No data in the requested date range.")

    data: List[TrendPoint] = []
    for i, (eff_date, p_avg, d_avg, k_avg, cnt) in enumerate(rows):
        p_avg, d_avg, k_avg = float(p_avg or 0), float(d_avg or 0), float(k_avg or 0)
        point = TrendPoint(
            effective_date=eff_date,
            petrol_avg=p_avg,
            diesel_avg=d_avg,
            kerosene_avg=k_avg,
            district_count=cnt,
        )
        if i > 0:
            prev = rows[i - 1]
            def _pct(curr, prev_val):
                pv = float(prev_val) if prev_val else 0
                return round((curr - pv) / pv * 100, 2) if pv else None
            point.petrol_change_pct = _pct(p_avg, prev[1])
            point.diesel_change_pct = _pct(d_avg, prev[2])
            point.kerosene_change_pct = _pct(k_avg, prev[3])
        data.append(point)

    result = NationalTrendResponse(
        from_date=rows[0][0],
        to_date=rows[-1][0],
        periods=len(rows),
        data=data,
    )
    cache_set(cache_key, result)
    return result


@app.get(
    "/api/v1/analytics/volatility",
    response_model=VolatilityResponse,
    tags=["Analytics — Deep"],
    description=(
        "Ranks districts by price volatility — how much their fuel prices swing between "
        "pricing periods. Uses coefficient of variation (stdev / mean) on the historical "
        "price series. Returns the most volatile and most stable districts. Useful for "
        "identifying geographic price instability."
    ),
)
def analytics_volatility(
    top: int = Query(10, ge=1, le=30, description="Number of districts to return in each group"),
    min_periods: int = Query(12, ge=3, le=80, description="Minimum pricing periods required"),
    request: Request = None,
    db: Session = Depends(get_db),
):
    _check_rate_limit(request)
    cache_key = f"analytics:volatility:{top}:{min_periods}"
    hit = cache_get(cache_key)
    if hit is not None:
        return hit

    rows = (
        db.query(
            RegionDistrict.district_name,
            RegionDistrict.region_name,
            FuelPrice.effective_date,
            FuelPrice.petrol_price,
            FuelPrice.diesel_price,
            FuelPrice.kerosene_price,
        )
        .join(FuelPrice, FuelPrice.district_id == RegionDistrict.id)
        .order_by(RegionDistrict.district_name, FuelPrice.effective_date.asc())
        .all()
    )

    # group by district
    districts: Dict[str, dict] = {}
    for district_name, region_name, eff_date, p, d, k in rows:
        if district_name not in districts:
            districts[district_name] = {
                "district_name": district_name,
                "region_name": region_name,
                "petrol": [], "diesel": [], "kerosene": [],
            }
        districts[district_name]["petrol"].append(p)
        districts[district_name]["diesel"].append(d)
        districts[district_name]["kerosene"].append(k)

    def _cv(prices: List[float]) -> float:
        if len(prices) < 2:
            return 0.0
        mean = statistics.mean(prices)
        return round(statistics.stdev(prices) / mean * 100, 3) if mean else 0.0

    def _max_swing(prices: List[float]) -> float:
        if len(prices) < 2:
            return 0.0
        changes = [abs(prices[i] - prices[i - 1]) / prices[i - 1] * 100
                   for i in range(1, len(prices)) if prices[i - 1]]
        return round(max(changes), 2) if changes else 0.0

    entries: List[VolatilityEntry] = []
    for info in districts.values():
        n = min(len(info["petrol"]), len(info["diesel"]), len(info["kerosene"]))
        if n < min_periods:
            continue
        entries.append(VolatilityEntry(
            district_name=info["district_name"],
            region_name=info["region_name"],
            periods_analysed=n,
            petrol_cv=_cv(info["petrol"]),
            diesel_cv=_cv(info["diesel"]),
            kerosene_cv=_cv(info["kerosene"]),
            petrol_max_swing_pct=_max_swing(info["petrol"]),
            diesel_max_swing_pct=_max_swing(info["diesel"]),
            kerosene_max_swing_pct=_max_swing(info["kerosene"]),
        ))

    entries.sort(key=lambda e: e.petrol_cv, reverse=True)

    result = VolatilityResponse(
        periods_analysed=len(
            db.query(FuelPrice.effective_date).distinct().all()
        ),
        min_periods_required=min_periods,
        most_volatile=entries[:top],
        most_stable=list(reversed(entries))[:top],
    )
    cache_set(cache_key, result)
    return result


@app.get(
    "/api/v1/analytics/regional-gap",
    response_model=RegionalGapResponse,
    tags=["Analytics — Deep"],
    description=(
        "Tracks the price spread (cheapest vs most expensive district) over every "
        "available pricing date. Shows whether geographic price inequality is growing "
        "or shrinking over time — a key fairness indicator for remote vs urban areas."
    ),
)
def analytics_regional_gap(
    from_date: Optional[date] = Query(None),
    to_date: Optional[date] = Query(None),
    request: Request = None,
    db: Session = Depends(get_db),
):
    _check_rate_limit(request)
    cache_key = f"analytics:gap:{from_date}:{to_date}"
    hit = cache_get(cache_key)
    if hit is not None:
        return hit

    q = (
        db.query(
            FuelPrice.effective_date,
            func.min(FuelPrice.petrol_price),
            func.max(FuelPrice.petrol_price),
            func.round(func.avg(FuelPrice.petrol_price).cast(Numeric), 2),
            func.min(FuelPrice.diesel_price),
            func.max(FuelPrice.diesel_price),
            func.round(func.avg(FuelPrice.diesel_price).cast(Numeric), 2),
            func.min(FuelPrice.kerosene_price),
            func.max(FuelPrice.kerosene_price),
            func.round(func.avg(FuelPrice.kerosene_price).cast(Numeric), 2),
            func.count(FuelPrice.id),
        )
        .group_by(FuelPrice.effective_date)
        .order_by(FuelPrice.effective_date.asc())
    )
    if from_date:
        q = q.filter(FuelPrice.effective_date >= from_date)
    if to_date:
        q = q.filter(FuelPrice.effective_date <= to_date)

    rows = q.all()
    if not rows:
        raise HTTPException(status_code=404, detail="No data in the requested date range.")

    data: List[RegionalGapPoint] = []
    for r in rows:
        eff_date, p_min, p_max, p_avg, d_min, d_max, d_avg, k_min, k_max, k_avg, cnt = r
        p_min, p_max, p_avg = float(p_min or 0), float(p_max or 0), float(p_avg or 0)
        d_min, d_max, d_avg = float(d_min or 0), float(d_max or 0), float(d_avg or 0)
        k_min, k_max, k_avg = float(k_min or 0), float(k_max or 0), float(k_avg or 0)
        p_spread = round(p_max - p_min, 2)
        d_spread = round(d_max - d_min, 2)
        k_spread = round(k_max - k_min, 2)
        data.append(RegionalGapPoint(
            effective_date=eff_date,
            petrol_min=p_min, petrol_max=p_max, petrol_avg=p_avg,
            petrol_spread=p_spread,
            petrol_spread_pct=round(p_spread / p_avg * 100, 2) if p_avg else 0,
            diesel_spread=d_spread,
            diesel_spread_pct=round(d_spread / d_avg * 100, 2) if d_avg else 0,
            kerosene_spread=k_spread,
            kerosene_spread_pct=round(k_spread / k_avg * 100, 2) if k_avg else 0,
            district_count=cnt,
        ))

    spreads = [p.petrol_spread for p in data]
    max_spread_point = max(data, key=lambda p: p.petrol_spread)
    result = RegionalGapResponse(
        from_date=data[0].effective_date,
        to_date=data[-1].effective_date,
        periods=len(data),
        avg_petrol_spread=round(sum(spreads) / len(spreads), 2),
        max_petrol_spread=round(max(spreads), 2),
        max_petrol_spread_date=max_spread_point.effective_date,
        data=data,
    )
    cache_set(cache_key, result)
    return result


@app.get(
    "/api/v1/analytics/peak",
    response_model=PeakRecordsResponse,
    tags=["Analytics — Deep"],
    description=(
        "All-time price records across the full historical dataset (2009–present): "
        "highest and lowest national average per fuel type, all-time single-district "
        "extremes, and the biggest single-period national price jump and drop. "
        "Good for context in news articles and research."
    ),
)
def analytics_peak(
    request: Request = None,
    db: Session = Depends(get_db),
):
    _check_rate_limit(request)
    cache_key = "analytics:peak"
    hit = cache_get(cache_key)
    if hit is not None:
        return hit

    records: List[AllTimeRecord] = []

    # national average per date
    avg_rows = (
        db.query(
            FuelPrice.effective_date,
            func.avg(FuelPrice.petrol_price),
            func.avg(FuelPrice.diesel_price),
            func.avg(FuelPrice.kerosene_price),
        )
        .group_by(FuelPrice.effective_date)
        .order_by(FuelPrice.effective_date.asc())
        .all()
    )

    for fuel_idx, fuel_name in enumerate(["petrol", "diesel", "kerosene"], start=1):
        avgs = [(r[0], float(r[fuel_idx] or 0)) for r in avg_rows]
        hi = max(avgs, key=lambda x: x[1])
        lo = min(avgs, key=lambda x: x[1])
        records.append(AllTimeRecord(
            fuel_type=fuel_name,
            record_type="highest_national_average",
            value=round(hi[1], 2),
            effective_date=hi[0],
            note=f"Highest ever national average {fuel_name} price (TZS/L)",
        ))
        records.append(AllTimeRecord(
            fuel_type=fuel_name,
            record_type="lowest_national_average",
            value=round(lo[1], 2),
            effective_date=lo[0],
            note=f"Lowest ever national average {fuel_name} price (TZS/L)",
        ))

        # biggest single-period jump / drop
        if len(avgs) >= 2:
            changes = [
                (avgs[i][0], avgs[i][1] - avgs[i - 1][1], avgs[i - 1][1])
                for i in range(1, len(avgs))
                if avgs[i - 1][1]
            ]
            if changes:
                biggest_jump = max(changes, key=lambda x: x[1])
                biggest_drop = min(changes, key=lambda x: x[1])
                pct_jump = round(biggest_jump[1] / biggest_jump[2] * 100, 2)
                pct_drop = round(biggest_drop[1] / biggest_drop[2] * 100, 2)
                records.append(AllTimeRecord(
                    fuel_type=fuel_name,
                    record_type="biggest_single_period_increase",
                    value=round(biggest_jump[1], 2),
                    effective_date=biggest_jump[0],
                    note=f"+{pct_jump}% — largest single-period national average rise for {fuel_name}",
                ))
                records.append(AllTimeRecord(
                    fuel_type=fuel_name,
                    record_type="biggest_single_period_decrease",
                    value=round(biggest_drop[1], 2),
                    effective_date=biggest_drop[0],
                    note=f"{pct_drop}% — largest single-period national average drop for {fuel_name}",
                ))

    # all-time single-district extremes
    for fuel_col, fuel_name in [
        (FuelPrice.petrol_price, "petrol"),
        (FuelPrice.diesel_price, "diesel"),
        (FuelPrice.kerosene_price, "kerosene"),
    ]:
        for record_type, order in [("highest_single_district", fuel_col.desc()), ("lowest_single_district", fuel_col.asc())]:
            row = (
                db.query(
                    RegionDistrict.district_name,
                    RegionDistrict.region_name,
                    fuel_col,
                    FuelPrice.effective_date,
                )
                .join(FuelPrice, FuelPrice.district_id == RegionDistrict.id)
                .order_by(order)
                .first()
            )
            if row:
                records.append(AllTimeRecord(
                    fuel_type=fuel_name,
                    record_type=record_type,
                    value=row[2],
                    effective_date=row[3],
                    district_name=row[0],
                    region_name=row[1],
                    note=f"{'Most expensive' if 'highest' in record_type else 'Cheapest'} ever single district {fuel_name} price (TZS/L)",
                ))

    result = PeakRecordsResponse(
        generated_at=datetime.utcnow().isoformat() + "Z",
        records=records,
    )
    cache_set(cache_key, result)
    return result


@app.get(
    "/api/v1/analytics/inflation",
    response_model=InflationResponse,
    tags=["Analytics — Deep"],
    description=(
        "Fuel price inflation index: tracks cumulative price change since a base date, "
        "indexed to 100. Also returns the compound annual growth rate (CAGR) for each "
        "fuel type over the full period. Use this to compare fuel inflation against "
        "other economic indicators."
    ),
)
def analytics_inflation(
    base_date: Optional[date] = Query(None, description="Base date (YYYY-MM-DD). Defaults to earliest available."),
    request: Request = None,
    db: Session = Depends(get_db),
):
    _check_rate_limit(request)
    cache_key = f"analytics:inflation:{base_date}"
    hit = cache_get(cache_key)
    if hit is not None:
        return hit

    avg_rows = (
        db.query(
            FuelPrice.effective_date,
            func.avg(FuelPrice.petrol_price),
            func.avg(FuelPrice.diesel_price),
            func.avg(FuelPrice.kerosene_price),
        )
        .group_by(FuelPrice.effective_date)
        .order_by(FuelPrice.effective_date.asc())
        .all()
    )
    if not avg_rows:
        raise HTTPException(status_code=404, detail="No data available.")

    # locate base row
    if base_date:
        base_row = next((r for r in avg_rows if r[0] >= base_date), None)
        if not base_row:
            raise HTTPException(status_code=404, detail=f"No data on or after {base_date}.")
        avg_rows = [r for r in avg_rows if r[0] >= base_row[0]]
    else:
        base_row = avg_rows[0]

    b_p, b_d, b_k = float(base_row[1] or 0), float(base_row[2] or 0), float(base_row[3] or 0)
    if not all([b_p, b_d, b_k]):
        raise HTTPException(status_code=422, detail="Base date has incomplete price data.")

    data: List[InflationPoint] = []
    for eff_date, p_avg, d_avg, k_avg in avg_rows:
        p_avg, d_avg, k_avg = float(p_avg or 0), float(d_avg or 0), float(k_avg or 0)
        p_idx = round(p_avg / b_p * 100, 2)
        d_idx = round(d_avg / b_d * 100, 2)
        k_idx = round(k_avg / b_k * 100, 2)
        data.append(InflationPoint(
            effective_date=eff_date,
            petrol_avg=round(p_avg, 2),
            diesel_avg=round(d_avg, 2),
            kerosene_avg=round(k_avg, 2),
            petrol_index=p_idx,
            diesel_index=d_idx,
            kerosene_index=k_idx,
            petrol_cumulative_pct=round(p_idx - 100, 2),
            diesel_cumulative_pct=round(d_idx - 100, 2),
            kerosene_cumulative_pct=round(k_idx - 100, 2),
        ))

    last = avg_rows[-1]
    years = (last[0] - base_row[0]).days / 365.25
    result = InflationResponse(
        base_date=base_row[0],
        base_petrol_avg=round(b_p, 2),
        base_diesel_avg=round(b_d, 2),
        base_kerosene_avg=round(b_k, 2),
        total_periods=len(data),
        petrol_annualised_rate=_cagr(b_p, float(last[1] or 0), years),
        diesel_annualised_rate=_cagr(b_d, float(last[2] or 0), years),
        kerosene_annualised_rate=_cagr(b_k, float(last[3] or 0), years),
        data=data,
    )
    cache_set(cache_key, result)
    return result


@app.get(
    "/api/v1/analytics/correlation",
    response_model=CorrelationResponse,
    tags=["Analytics — Deep"],
    description=(
        "Pearson correlation coefficients between petrol, diesel, and kerosene national "
        "average prices over the historical dataset. A high positive correlation means "
        "the fuels move together (usually driven by crude oil price shifts). "
        "Useful for commodity traders, economists, and policy researchers."
    ),
)
def analytics_correlation(
    from_date: Optional[date] = Query(None),
    to_date: Optional[date] = Query(None),
    request: Request = None,
    db: Session = Depends(get_db),
):
    _check_rate_limit(request)
    cache_key = f"analytics:correlation:{from_date}:{to_date}"
    hit = cache_get(cache_key)
    if hit is not None:
        return hit

    q = (
        db.query(
            FuelPrice.effective_date,
            func.avg(FuelPrice.petrol_price),
            func.avg(FuelPrice.diesel_price),
            func.avg(FuelPrice.kerosene_price),
        )
        .group_by(FuelPrice.effective_date)
        .order_by(FuelPrice.effective_date.asc())
    )
    if from_date:
        q = q.filter(FuelPrice.effective_date >= from_date)
    if to_date:
        q = q.filter(FuelPrice.effective_date <= to_date)

    rows = q.all()
    if len(rows) < 3:
        raise HTTPException(status_code=422, detail="Need at least 3 pricing periods for correlation.")

    p_avgs = [float(r[1] or 0) for r in rows]
    d_avgs = [float(r[2] or 0) for r in rows]
    k_avgs = [float(r[3] or 0) for r in rows]

    pd_r = _pearson(p_avgs, d_avgs)
    pk_r = _pearson(p_avgs, k_avgs)
    dk_r = _pearson(d_avgs, k_avgs)

    result = CorrelationResponse(
        periods_analysed=len(rows),
        from_date=rows[0][0],
        to_date=rows[-1][0],
        national_average_correlation=CorrelationMatrix(
            petrol_diesel=pd_r,
            petrol_kerosene=pk_r,
            diesel_kerosene=dk_r,
        ),
        interpretation={
            "petrol_diesel": _interp("Petrol↔Diesel", pd_r),
            "petrol_kerosene": _interp("Petrol↔Kerosene", pk_r),
            "diesel_kerosene": _interp("Diesel↔Kerosene", dk_r),
        },
    )
    cache_set(cache_key, result)
    return result


@app.get(
    "/api/v1/analytics/forecast",
    response_model=ForecastResponse,
    tags=["Analytics — Deep"],
    description=(
        "Projects national average fuel prices using automated model selection: fits ARIMA(1,1,1), "
        "SARIMA(1,1,1)(1,0,0,12), and Holt-Winters ETS, then picks the lowest-AIC model. "
        "Response includes `model_comparison` showing every model's AIC and R² so the selection "
        "is fully transparent. 95% CI from the model's own error estimates. "
        "**Statistical model only — not an official EWURA forecast.**"
    ),
)
def analytics_forecast(
    periods: int = Query(3, ge=1, le=12, description="Number of future periods to project"),
    training_periods: int = Query(36, ge=6, le=80, description="How many recent periods to train on"),
    request: Request = None,
    db: Session = Depends(get_db),
):
    _check_rate_limit(request)
    cache_key = f"analytics:forecast:{periods}:{training_periods}"
    hit = cache_get(cache_key)
    if hit is not None:
        return hit

    avg_rows = (
        db.query(
            FuelPrice.effective_date,
            func.avg(FuelPrice.petrol_price),
            func.avg(FuelPrice.diesel_price),
            func.avg(FuelPrice.kerosene_price),
        )
        .group_by(FuelPrice.effective_date)
        .order_by(FuelPrice.effective_date.desc())
        .limit(training_periods)
        .all()
    )
    if len(avg_rows) < 6:
        raise HTTPException(status_code=422, detail="Need at least 6 pricing periods for forecasting.")

    avg_rows = list(reversed(avg_rows))  # oldest first

    p_ys = [float(r[1] or 0) for r in avg_rows]
    d_ys = [float(r[2] or 0) for r in avg_rows]
    k_ys = [float(r[3] or 0) for r in avg_rows]

    def _nearest_by_ym(d, by_ym: Dict[Tuple[int, int], float]) -> float:
        for delta in range(4):
            y, m = (d.year, d.month - delta) if d.month > delta else (d.year - 1, d.month + 12 - delta)
            if (y, m) in by_ym:
                return by_ym[(y, m)]
        return 0.0

    # Align Brent crude monthly prices to EWURA pricing dates
    brent_exog: Optional[List[float]] = None
    brent_future: Optional[List[float]] = None
    brent_last_known: Optional[float] = None
    try:
        brent_rows = (
            db.query(BrentCrude.price_date, BrentCrude.price_usd)
            .order_by(BrentCrude.price_date)
            .all()
        )
        if brent_rows:
            brent_by_ym: Dict[Tuple[int, int], float] = {
                (r[0].year, r[0].month): float(r[1]) for r in brent_rows
            }
            brent_last_known = float(brent_rows[-1][1])
            aligned = [_nearest_by_ym(r[0], brent_by_ym) for r in avg_rows]
            if sum(1 for v in aligned if v > 0) >= 12:
                brent_exog = aligned
                brent_future = _project_brent([v for v in aligned if v > 0], periods)
    except Exception:
        pass

    # Align TZS/USD exchange rates to EWURA pricing dates
    fx_exog: Optional[List[float]] = None
    fx_future: Optional[List[float]] = None
    fx_last_known: Optional[float] = None
    try:
        fx_rows = (
            db.query(TzsUsdRate.rate_date, TzsUsdRate.rate_tzs_per_usd)
            .order_by(TzsUsdRate.rate_date)
            .all()
        )
        if fx_rows:
            fx_by_ym: Dict[Tuple[int, int], float] = {
                (r[0].year, r[0].month): float(r[1]) for r in fx_rows
            }
            fx_last_known = float(fx_rows[-1][1])
            aligned_fx = [_nearest_by_ym(r[0], fx_by_ym) for r in avg_rows]
            if sum(1 for v in aligned_fx if v > 0) >= 12:
                fx_exog = aligned_fx
                fx_future = _project_fx([v for v in aligned_fx if v > 0], periods)
    except Exception:
        pass

    p_means, p_ci_lows, p_ci_highs, p_r2, model_name, model_comparison = _best_forecast(
        p_ys, periods, brent_exog=brent_exog, brent_future=brent_future,
        fx_exog=fx_exog, fx_future=fx_future,
    )
    d_means, d_ci_lows, d_ci_highs, d_r2, _, _ = _best_forecast(
        d_ys, periods, brent_exog=brent_exog, brent_future=brent_future,
        fx_exog=fx_exog, fx_future=fx_future,
    )
    k_means, k_ci_lows, k_ci_highs, k_r2, _, _ = _best_forecast(
        k_ys, periods, brent_exog=brent_exog, brent_future=brent_future,
        fx_exog=fx_exog, fx_future=fx_future,
    )

    intervals = [
        (avg_rows[i][0] - avg_rows[i - 1][0]).days
        for i in range(1, len(avg_rows))
    ]
    avg_interval = int(sum(intervals) / len(intervals)) if intervals else 30

    def _direction(ys: List[float]) -> str:
        if len(ys) < 2:
            return "stable"
        slope, _, _ = _ols(list(range(len(ys))), ys)
        if abs(slope) < 0.5:
            return "stable"
        return "upward trend" if slope > 0 else "downward trend"

    last_date = avg_rows[-1][0]
    forecast: List[ForecastPoint] = []
    for i in range(periods):
        proj_date = last_date + timedelta(days=avg_interval * (i + 1))
        forecast.append(ForecastPoint(
            projected_date=proj_date,
            petrol_forecast=max(0.0, p_means[i]),
            diesel_forecast=max(0.0, d_means[i]),
            kerosene_forecast=max(0.0, k_means[i]),
            petrol_ci_low=max(0.0, p_ci_lows[i]),
            petrol_ci_high=p_ci_highs[i],
            diesel_ci_low=max(0.0, d_ci_lows[i]),
            diesel_ci_high=d_ci_highs[i],
            kerosene_ci_low=max(0.0, k_ci_lows[i]),
            kerosene_ci_high=k_ci_highs[i],
        ))

    selected_aic = model_comparison.get(model_name, {}).get("aic", None)
    result = ForecastResponse(
        model=model_name,
        model_aic=selected_aic,
        model_comparison=model_comparison,
        periods_used=len(avg_rows),
        from_date=avg_rows[0][0],
        to_date=last_date,
        avg_period_days=avg_interval,
        r_squared={"petrol": p_r2, "diesel": d_r2, "kerosene": k_r2},
        trend_direction={
            "petrol": _direction(p_ys),
            "diesel": _direction(d_ys),
            "kerosene": _direction(k_ys),
        },
        brent_last_known=brent_last_known,
        brent_forecast_inputs=brent_future,
        fx_last_known=fx_last_known,
        fx_forecast_inputs=fx_future,
        forecast=forecast,
        disclaimer=(
            "Forecasts are generated by automated time-series modelling (lowest-AIC model selected from "
            "ARIMA, SARIMA, Holt-Winters ETS, and SARIMAX-X with oil cost in TZS). "
            "They do not reflect EWURA's pricing methodology or regulatory decisions. "
            "Do not use for financial or policy decisions."
        ),
    )
    cache_set(cache_key, result)
    return result


@app.get(
    "/api/v1/analytics/regional-summary",
    response_model=RegionalSummaryResponse,
    tags=["Analytics — Deep"],
    description=(
        "Average petrol, diesel, and kerosene prices grouped by region for the latest "
        "pricing period (or a specific date). Returns one row per region with min/max "
        "and district count. Useful for regional comparison charts and map choropleth layers."
    ),
)
def analytics_regional_summary(
    effective_date_param: Optional[date] = Query(None, alias="date", description="Pricing date (YYYY-MM-DD). Defaults to latest."),
    request: Request = None,
    db: Session = Depends(get_db),
):
    _check_rate_limit(request)
    cache_key = f"analytics:regional-summary:{effective_date_param}"
    hit = cache_get(cache_key)
    if hit is not None:
        return hit

    if effective_date_param is None:
        latest = db.query(func.max(FuelPrice.effective_date)).scalar()
        if not latest:
            raise HTTPException(status_code=404, detail="No price data available.")
        effective_date_param = latest

    rows = (
        db.query(
            RegionDistrict.region_name,
            func.count(FuelPrice.id),
            func.round(func.avg(FuelPrice.petrol_price).cast(Numeric), 2),
            func.round(func.avg(FuelPrice.diesel_price).cast(Numeric), 2),
            func.round(func.avg(FuelPrice.kerosene_price).cast(Numeric), 2),
            func.round(func.min(FuelPrice.petrol_price).cast(Numeric), 2),
            func.round(func.max(FuelPrice.petrol_price).cast(Numeric), 2),
        )
        .join(FuelPrice, FuelPrice.district_id == RegionDistrict.id)
        .filter(FuelPrice.effective_date == effective_date_param)
        .group_by(RegionDistrict.region_name)
        .order_by(func.avg(FuelPrice.petrol_price).asc())
        .all()
    )

    if not rows:
        raise HTTPException(status_code=404, detail=f"No data for date {effective_date_param}.")

    regions = [
        RegionalSummaryEntry(
            region_name=r[0],
            district_count=r[1],
            petrol_avg=float(r[2] or 0),
            diesel_avg=float(r[3] or 0),
            kerosene_avg=float(r[4] or 0),
            petrol_min=float(r[5] or 0),
            petrol_max=float(r[6] or 0),
        )
        for r in rows
    ]
    result = RegionalSummaryResponse(effective_date=effective_date_param, regions=regions)
    cache_set(cache_key, result)
    return result


@app.get(
    "/api/v1/analytics/clusters",
    response_model=ClusterResponse,
    tags=["Analytics — Deep"],
    description=(
        "Clusters all districts by their price trajectory shape using K-means. "
        "Districts with similar price patterns over time end up in the same cluster "
        "regardless of absolute price level (z-score normalised before clustering). "
        "Returns cluster assignments, auto-generated labels, and centroid time series "
        "for chart rendering. Use `fuel_type` to select which fuel to cluster on. "
        "Requires scikit-learn."
    ),
)
def analytics_clusters(
    fuel_type: str = Query("petrol", pattern="^(petrol|diesel|kerosene)$", description="Fuel type to cluster on"),
    n_clusters: int = Query(5, ge=2, le=8, description="Number of clusters (2–8)"),
    min_periods: int = Query(12, ge=6, le=80, description="Minimum pricing periods a district must have to qualify"),
    request: Request = None,
    db: Session = Depends(get_db),
):
    _check_rate_limit(request)
    cache_key = f"analytics:clusters:{fuel_type}:{n_clusters}:{min_periods}"
    hit = cache_get(cache_key)
    if hit is not None:
        return hit

    try:
        import numpy as np
        from sklearn.cluster import KMeans
    except ImportError:
        raise HTTPException(status_code=503, detail="scikit-learn is required. Add scikit-learn to requirements.txt.")

    all_dates = [
        r[0] for r in db.query(FuelPrice.effective_date)
        .distinct()
        .order_by(FuelPrice.effective_date)
        .all()
    ]
    if len(all_dates) < min_periods:
        raise HTTPException(status_code=422, detail=f"Only {len(all_dates)} pricing periods in DB; reduce min_periods.")

    price_col = {
        "petrol": FuelPrice.petrol_price,
        "diesel": FuelPrice.diesel_price,
        "kerosene": FuelPrice.kerosene_price,
    }[fuel_type]

    rows = (
        db.query(
            RegionDistrict.id,
            RegionDistrict.district_name,
            RegionDistrict.region_name,
            FuelPrice.effective_date,
            price_col,
        )
        .join(FuelPrice, FuelPrice.district_id == RegionDistrict.id)
        .filter(price_col.isnot(None))
        .all()
    )

    district_meta: Dict[int, Tuple[str, str]] = {}
    district_prices: Dict[int, Dict] = defaultdict(dict)
    for did, dname, rname, edate, price in rows:
        district_meta[did] = (dname, rname)
        district_prices[did][edate] = float(price)

    date_to_idx = {d: i for i, d in enumerate(all_dates)}
    n_dates = len(all_dates)
    col_sums = [0.0] * n_dates
    col_counts = [0] * n_dates
    for dprices in district_prices.values():
        for d, p in dprices.items():
            if d in date_to_idx:
                col_sums[date_to_idx[d]] += p
                col_counts[date_to_idx[d]] += 1
    col_means = [col_sums[i] / col_counts[i] if col_counts[i] > 0 else 0.0 for i in range(n_dates)]

    qualified_ids: List[int] = []
    price_matrix: List[List[float]] = []
    for did, dprices in district_prices.items():
        if len(dprices) < min_periods:
            continue
        row = [dprices.get(d, col_means[date_to_idx[d]]) for d in all_dates]
        qualified_ids.append(did)
        price_matrix.append(row)

    if len(qualified_ids) < n_clusters:
        raise HTTPException(
            status_code=422,
            detail=f"Only {len(qualified_ids)} districts qualify (need >= {n_clusters}). Reduce min_periods or n_clusters.",
        )

    X_norm: List[List[float]] = []
    for row in price_matrix:
        mean = sum(row) / len(row)
        variance = sum((v - mean) ** 2 for v in row) / len(row)
        std = math.sqrt(variance) if variance > 1e-6 else 1.0
        X_norm.append([(v - mean) / std for v in row])

    X = np.array(X_norm, dtype=float)
    km = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    cluster_labels = km.fit_predict(X).tolist()
    centroids = km.cluster_centers_

    latest_date = all_dates[-1]
    cluster_members: Dict[int, List[Dict]] = defaultdict(list)
    for i, did in enumerate(qualified_ids):
        cid = int(cluster_labels[i])
        dname, rname = district_meta[did]
        dprices = district_prices[did]
        latest_price = dprices.get(latest_date) or dprices[max(dprices)]
        cluster_members[cid].append({
            "district_name": dname,
            "region_name": rname,
            "cluster_id": cid,
            "periods_with_data": len(dprices),
            "latest_price": round(latest_price, 2),
        })

    def _cluster_avg_price(cid: int) -> float:
        ds = cluster_members.get(cid, [])
        return sum(d["latest_price"] for d in ds) / len(ds) if ds else 0.0

    sorted_by_price = sorted(range(n_clusters), key=_cluster_avg_price)
    price_rank = {cid: rank for rank, cid in enumerate(sorted_by_price)}

    _level_map: Dict[int, List[str]] = {
        2: ["Lower", "Higher"],
        3: ["Lowest", "Mid", "Highest"],
        4: ["Lowest", "Low-Mid", "High-Mid", "Highest"],
    }

    def _price_level_label(rank: int, total: int) -> str:
        if total in _level_map:
            return _level_map[total][rank]
        pct = rank / max(total - 1, 1)
        if pct < 0.2:
            return "Lowest"
        if pct < 0.4:
            return "Low"
        if pct < 0.6:
            return "Mid"
        if pct < 0.8:
            return "High"
        return "Highest"

    def _centroid_trend(c) -> str:
        slope, _, _ = _ols(list(range(len(c))), [float(v) for v in c])
        if slope > 0.02:
            return "rising"
        if slope < -0.02:
            return "falling"
        return "stable"

    trend_display = {"rising": "Rising", "falling": "Falling", "stable": "Stable"}

    centroid_list = [
        ClusterCentroid(
            cluster_id=cid,
            label=f"{_price_level_label(price_rank[cid], n_clusters)} Price, {trend_display[_centroid_trend(centroids[cid])]}",
            district_count=len(cluster_members.get(cid, [])),
            avg_latest_price=round(_cluster_avg_price(cid), 2),
            trend=_centroid_trend(centroids[cid]),
            centroid_values=[round(float(v), 4) for v in centroids[cid]],
        )
        for cid in range(n_clusters)
    ]

    all_district_objects = [
        ClusterDistrict(**d)
        for cid in range(n_clusters)
        for d in sorted(cluster_members.get(cid, []), key=lambda x: x["district_name"])
    ]

    result = ClusterResponse(
        fuel_type=fuel_type,
        n_clusters=n_clusters,
        periods_used=n_dates,
        from_date=all_dates[0],
        to_date=all_dates[-1],
        districts_included=len(qualified_ids),
        districts=all_district_objects,
        centroids=centroid_list,
        dates=all_dates,
    )
    cache_set(cache_key, result)
    return result


@app.get(
    "/api/v1/analytics/brent",
    response_model=BrentResponse,
    tags=["Analytics — Deep"],
    description=(
        "Returns the full Brent crude oil price series stored in the database (USD/barrel, monthly). "
        "Used by the forecast model as an exogenous variable. Sync fresh data via "
        "`POST /api/v1/admin/sync-brent` (admin key required). "
        "Empty if no Brent data has been synced yet."
    ),
)
def analytics_brent(
    request: Request = None,
    db: Session = Depends(get_db),
):
    _check_rate_limit(request)
    cache_key = "analytics:brent"
    hit = cache_get(cache_key)
    if hit is not None:
        return hit

    rows = db.query(BrentCrude).order_by(BrentCrude.price_date).all()
    if not rows:
        raise HTTPException(status_code=404, detail="No Brent crude data. Trigger POST /api/v1/admin/sync-brent first.")

    series = [BrentPoint(price_date=r.price_date, price_usd=r.price_usd) for r in rows]
    result = BrentResponse(
        count=len(series),
        from_date=rows[0].price_date,
        to_date=rows[-1].price_date,
        latest_price_usd=rows[-1].price_usd,
        series=series,
    )
    cache_set(cache_key, result)
    return result


@app.get(
    "/api/v1/analytics/exchange-rate",
    response_model=FxResponse,
    tags=["Analytics — Deep"],
    description=(
        "Returns the full TZS/USD exchange rate series stored in the database (monthly). "
        "Annual data comes from the World Bank official rates (PA.NUS.FCRF), interpolated monthly. "
        "The most recent month is supplemented with the spot rate from open.er-api.com. "
        "Used by the forecast model alongside Brent crude to compute 'oil cost in TZS' — "
        "the most direct predictor of EWURA fuel prices. "
        "Sync fresh data via `POST /api/v1/admin/sync-fx` (admin key required)."
    ),
)
def analytics_exchange_rate(
    request: Request = None,
    db: Session = Depends(get_db),
):
    _check_rate_limit(request)
    cache_key = "analytics:exchange_rate"
    hit = cache_get(cache_key)
    if hit is not None:
        return hit

    rows = db.query(TzsUsdRate).order_by(TzsUsdRate.rate_date).all()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail="No exchange rate data. Trigger POST /api/v1/admin/sync-fx first.",
        )

    series = [FxPoint(rate_date=r.rate_date, rate_tzs_per_usd=r.rate_tzs_per_usd) for r in rows]
    result = FxResponse(
        count=len(series),
        from_date=rows[0].rate_date,
        to_date=rows[-1].rate_date,
        latest_rate_tzs_per_usd=rows[-1].rate_tzs_per_usd,
        source="World Bank PA.NUS.FCRF (annual, interpolated monthly) + open.er-api.com (current spot)"
        if not FRED_API_KEY else "FRED DEXTANUS (monthly) + open.er-api.com (current spot)",
        series=series,
    )
    cache_set(cache_key, result)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Public Documents Listing
# ─────────────────────────────────────────────────────────────────────────────


@app.get(
    "/api/v1/documents",
    tags=["Discovery"],
    description=(
        "Lists all EWURA fuel-price PDF bulletins that have been successfully imported. "
        "Returns each bulletin's effective date, PDF URL, number of records imported, "
        "and when it was synced. Newest bulletins first. Deduplicated by effective date."
    ),
)
def list_documents(
    request: Request = None,
    db: Session = Depends(get_db),
):
    _check_rate_limit(request)
    cache_key = "documents:list"
    hit = cache_get(cache_key)
    if hit is not None:
        return hit

    # For each effective_date, keep only the latest successful sync entry
    subq = (
        db.query(
            SyncLog.effective_date,
            func.max(SyncLog.id).label("max_id"),
        )
        .filter(
            SyncLog.status == "success",
            SyncLog.pdf_url.isnot(None),
            SyncLog.effective_date.isnot(None),
        )
        .group_by(SyncLog.effective_date)
        .subquery()
    )

    rows = (
        db.query(SyncLog)
        .join(subq, SyncLog.id == subq.c.max_id)
        .order_by(SyncLog.effective_date.desc())
        .all()
    )

    documents = [
        {
            "effective_date": r.effective_date,
            "pdf_url": r.pdf_url,
            "records_imported": r.records_upserted,
            "synced_at": r.triggered_at,
            "has_local_pdf": os.path.exists(_get_stored_pdf_path(r.effective_date)),
        }
        for r in rows
        if r.effective_date
    ]

    result = {"total": len(documents), "documents": documents}
    cache_set(cache_key, result)
    return result


@app.get(
    "/api/v1/documents/{doc_date}/pdf",
    tags=["Discovery"],
    description=(
        "Serve the stored EWURA bulletin PDF for a given effective date. "
        "If the PDF is not cached locally, proxies from the original EWURA URL. "
        "Returns 404 if neither local file nor EWURA URL is available."
    ),
    response_class=FileResponse,
)
def serve_document_pdf(
    doc_date: str,
    request: Request = None,
    db: Session = Depends(get_db),
):
    _check_rate_limit(request)
    try:
        eff_date = date.fromisoformat(doc_date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format — use YYYY-MM-DD")

    local_path = _get_stored_pdf_path(eff_date)
    if os.path.exists(local_path):
        return FileResponse(
            local_path,
            media_type="application/pdf",
            filename=f"ewura_fuel_prices_{doc_date}.pdf",
        )

    # Fall back to proxying the original EWURA URL
    row = (
        db.query(SyncLog)
        .filter(
            SyncLog.effective_date == eff_date,
            SyncLog.status == "success",
            SyncLog.pdf_url.isnot(None),
        )
        .order_by(SyncLog.id.desc())
        .first()
    )
    if not row:
        raise HTTPException(status_code=404, detail="PDF not found for this date")

    try:
        resp = requests.get(row.pdf_url, headers=_HTTP_HEADERS, timeout=30, stream=True)
        resp.raise_for_status()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Could not fetch PDF from EWURA: {exc}") from exc

    def _stream():
        for chunk in resp.iter_content(chunk_size=65536):
            yield chunk

    return StreamingResponse(
        _stream(),
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="ewura_fuel_prices_{doc_date}.pdf"'},
    )


# ─────────────────────────────────────────────────────────────────────────────
# Export
# ─────────────────────────────────────────────────────────────────────────────


@app.get("/api/v1/export/csv", tags=["Export"])
def export_csv(
    effective_date: Optional[date] = Query(None, description="Defaults to latest"),
    request: Request = None,
    db: Session = Depends(get_db),
):
    """
    Download all prices for a date as a UTF-8 CSV file.
    Perfect for spreadsheet import or offline analysis.
    """
    _check_rate_limit(request)
    eff_date = effective_date or _require_latest_date(db)
    rows = (
        _price_query(db, eff_date)
        .order_by(RegionDistrict.region_name, RegionDistrict.district_name)
        .all()
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data for {eff_date}.")

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Region", "District", "Petrol (TZS/L)", "Diesel (TZS/L)", "Kerosene (TZS/L)", "Effective Date"])
    for r in rows:
        writer.writerow([
            r.region_name, r.district_name,
            r.petrol_price, r.diesel_price, r.kerosene_price,
            r.effective_date.isoformat(),
        ])

    filename = f"tz_fuel_prices_{eff_date}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ─────────────────────────────────────────────────────────────────────────────
# Admin
# ─────────────────────────────────────────────────────────────────────────────


@app.post(
    "/api/v1/admin/trigger-sync",
    status_code=status.HTTP_202_ACCEPTED,
    tags=["Admin"],
    dependencies=[Depends(verify_admin_key)],
)
def trigger_sync(background_tasks: BackgroundTasks):
    """Kick off a background EWURA PDF scrape. Requires X-API-KEY header."""
    background_tasks.add_task(background_scraper_task, SessionLocal)
    return {
        "status": "accepted",
        "message": "Sync job queued. Poll GET /api/v1/admin/sync-logs for progress.",
    }


@app.get(
    "/api/v1/admin/sync-logs",
    response_model=List[SyncStatusResponse],
    tags=["Admin"],
    dependencies=[Depends(verify_admin_key)],
)
def sync_logs(
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Recent sync job history. Requires X-API-KEY header."""
    return db.query(SyncLog).order_by(SyncLog.id.desc()).limit(limit).all()


@app.delete(
    "/api/v1/admin/cache",
    status_code=status.HTTP_200_OK,
    tags=["Admin"],
    dependencies=[Depends(verify_admin_key)],
)
def flush_cache():
    """Flush the in-process response cache. Requires X-API-KEY header."""
    before = len(_CACHE)
    cache_clear()
    return {"cleared_entries": before}


@app.post(
    "/api/v1/admin/backfill",
    status_code=status.HTTP_202_ACCEPTED,
    tags=["Admin"],
    dependencies=[Depends(verify_admin_key)],
)
def trigger_backfill(background_tasks: BackgroundTasks):
    """
    Download and import every historical EWURA PDF available on the pricing page
    (2009–present). Already-imported months are skipped automatically.
    This is a long-running job — poll GET /api/v1/admin/backfill/status for progress.
    Requires X-API-KEY header.
    """
    if _backfill_state.get("status") == "running":
        return {
            "status": "already_running",
            "message": "A backfill job is already in progress.",
            "progress": _backfill_state,
        }
    background_tasks.add_task(background_backfill_task, SessionLocal)
    return {
        "status": "accepted",
        "message": "Historical backfill queued. Poll GET /api/v1/admin/backfill/status for progress.",
    }


@app.get(
    "/api/v1/admin/backfill/status",
    tags=["Admin"],
    dependencies=[Depends(verify_admin_key)],
)
def backfill_status():
    """Live progress of the most recent backfill job. Requires X-API-KEY header."""
    if not _backfill_state:
        return {"status": "never_run", "message": "No backfill has been triggered yet."}
    return _backfill_state


@app.post(
    "/api/v1/admin/sync-brent",
    tags=["Admin"],
    dependencies=[Depends(verify_admin_key)],
    description=(
        "Pulls Brent crude monthly spot prices from the EIA API and upserts them into the "
        "`brent_crude` table. Fetches up to 200 months of history. "
        "Requires `X-API-KEY` header. Set `EIA_API_KEY` env var for higher rate limits "
        "(free registration at eia.gov); the default DEMO_KEY works for ~100 requests/day."
    ),
)
def sync_brent(db: Session = Depends(get_db)):
    """Sync Brent crude prices from EIA. Requires X-API-KEY header."""
    from datetime import date as _date

    url = (
        f"https://api.eia.gov/v2/seriesid/PET.RBRTE.M"
        f"?api_key={EIA_API_KEY}&length=200"
    )
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"EIA API error: {exc}")

    rows = data.get("response", {}).get("data", [])
    if not rows:
        raise HTTPException(status_code=502, detail="EIA returned no data. Check API key or series ID.")

    upserted = 0
    skipped = 0
    for row in rows:
        period = row.get("period", "")  # "YYYY-MM"
        value = row.get("value")
        if not period or value is None:
            continue
        try:
            yr, mo = int(period[:4]), int(period[5:7])
            price_date = _date(yr, mo, 1)
            price_usd = float(value)
        except (ValueError, TypeError):
            continue

        existing = db.query(BrentCrude).filter(BrentCrude.price_date == price_date).first()
        if existing:
            existing.price_usd = price_usd
            skipped += 1
        else:
            db.add(BrentCrude(price_date=price_date, price_usd=price_usd))
            upserted += 1

    db.commit()
    # Bust forecast cache so next call uses the updated Brent data
    cache_clear()

    eia_result = {
        "status": "ok",
        "inserted": upserted,
        "updated": skipped,
        "total_synced": upserted + skipped,
        "latest_period": rows[0].get("period") if rows else None,
    }

    # Also pull recent spot prices from Yahoo Finance to cover the current (incomplete) month
    yf_result = _sync_brent_spot_yf()

    return {"eia": eia_result, "yahoo_finance": yf_result}


@app.post(
    "/api/v1/admin/sync-fx",
    tags=["Admin"],
    dependencies=[Depends(verify_admin_key)],
    description=(
        "Syncs TZS/USD exchange rate history into the `tzs_usd_rate` table. "
        "Primary source: FRED DEXTANUS monthly series (set `FRED_API_KEY` env var — free at fred.stlouisfed.org). "
        "Fallback: World Bank PA.NUS.FCRF annual official rates interpolated monthly. "
        "Current spot rate always fetched from open.er-api.com (no key needed). "
        "Requires `X-API-KEY` header."
    ),
)
def sync_fx():
    """Sync TZS/USD exchange rates. Requires X-API-KEY header."""
    result = _sync_tzs_usd_rates()
    if result.get("status") == "failed":
        raise HTTPException(status_code=502, detail=result.get("error", "FX sync failed"))
    return result
