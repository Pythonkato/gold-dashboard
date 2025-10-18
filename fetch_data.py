#!/usr/bin/env python3
"""Download macro and gold market data and write JSON files under ./data.

This script fetches:
* FRED series: DFII10 (10y breakeven inflation) and DTWEXBGS (trade-weighted USD index)
* Alpha Vantage series: XAUUSD (FX_DAILY), GLD, and IAU (TIME_SERIES_DAILY_ADJUSTED)
* Optional central bank balance sheet CSV if CB_SHEETS_CSV_URL is provided, saved as cb_sheets.json

Environment variables required:
- FRED_API_KEY
- ALPHAVANTAGE_API_KEY
"""

from __future__ import annotations

import csv
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

DATA_DIR = Path(__file__).parent / "data"
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
ALPHA_BASE_URL = "https://www.alphavantage.co/query"


@dataclass(frozen=True)
class SeriesConfig:
    """Configuration to fetch a single time-series and persist it to disk."""

    source: str
    filename: str
    series_id: Optional[str] = None
    from_symbol: Optional[str] = None
    to_symbol: Optional[str] = None
    symbol: Optional[str] = None


SERIES_CONFIG: Dict[str, SeriesConfig] = {
    "dfii10": SeriesConfig(
        source="fred",
        series_id="DFII10",
        filename="dfii10.json",
    ),
    "dtwexbgs": SeriesConfig(
        source="fred",
        series_id="DTWEXBGS",
        filename="dtwexbgs.json",
    ),
    "xauusd": SeriesConfig(
        source="alpha_fx",
        from_symbol="XAU",
        to_symbol="USD",
        filename="xauusd.json",
    ),
    "gld": SeriesConfig(
        source="alpha_equity",
        symbol="GLD",
        filename="gld.json",
    ),
    "iau": SeriesConfig(
        source="alpha_equity",
        symbol="IAU",
        filename="iau.json",
    ),
}


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def float_or_none(value: str) -> Optional[float]:
    try:
        value = value.strip()
    except AttributeError:
        return None
    if value in {"", ".", "NA", "nan", "NaN"}:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def save_json(filename: str, data: Any) -> None:
    path = DATA_DIR / filename
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"Wrote {path}")


def _check_alpha_errors(payload: Dict[str, Any], label: str) -> None:
    """Raise a helpful error message when Alpha Vantage throttles or errors."""

    if "Error Message" in payload:
        raise RuntimeError(f"Alpha Vantage error for {label}: {payload['Error Message']}")
    if "Note" in payload:
        raise RuntimeError(
            "Alpha Vantage request was throttled. Please wait and retry or reduce frequency."
        )


def fetch_fred_series(series_id: str) -> List[Dict[str, Any]]:
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        raise RuntimeError("FRED_API_KEY is not set")

    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": "2000-01-01",
    }
    response = requests.get(FRED_BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()

    observations: Iterable[Dict[str, str]] = payload.get("observations", [])
    series: List[Dict[str, Any]] = []
    for obs in observations:
        value = float_or_none(obs.get("value", ""))
        if value is None:
            continue
        date_str = obs.get("date")
        if not date_str:
            continue
        series.append({
            "date": date_str,
            "value": value,
        })

    series.sort(key=lambda item: datetime.fromisoformat(item["date"]))
    return series


def fetch_alpha_fx(from_symbol: str, to_symbol: str) -> List[Dict[str, Any]]:
    api_key = os.environ.get("ALPHAVANTAGE_API_KEY")
    if not api_key:
        raise RuntimeError("ALPHAVANTAGE_API_KEY is not set")

    params = {
        "function": "FX_DAILY",
        "from_symbol": from_symbol,
        "to_symbol": to_symbol,
        "outputsize": "full",
        "apikey": api_key,
    }
    response = requests.get(ALPHA_BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    _check_alpha_errors(payload, f"{from_symbol}/{to_symbol}")

    time_series = payload.get("Time Series FX (Daily)", {})
    parsed: List[Dict[str, Any]] = []
    for date_str, values in time_series.items():
        close = float_or_none(values.get("4. close", ""))
        if close is None:
            continue
        parsed.append({"date": date_str, "close": close})

    parsed.sort(key=lambda item: datetime.fromisoformat(item["date"]))
    return parsed


def fetch_alpha_equity(symbol: str) -> List[Dict[str, Any]]:
    api_key = os.environ.get("ALPHAVANTAGE_API_KEY")
    if not api_key:
        raise RuntimeError("ALPHAVANTAGE_API_KEY is not set")

    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol,
        "outputsize": "full",
        "apikey": api_key,
    }
    response = requests.get(ALPHA_BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    _check_alpha_errors(payload, symbol or "equity")

    time_series = payload.get("Time Series (Daily)", {})
    parsed: List[Dict[str, Any]] = []
    for date_str, values in time_series.items():
        close = float_or_none(values.get("4. close", ""))
        adjusted_close = float_or_none(values.get("5. adjusted close", ""))
        volume = float_or_none(values.get("6. volume", ""))
        if close is None and adjusted_close is None:
            continue
        parsed.append({
            "date": date_str,
            "close": close,
            "adjusted_close": adjusted_close,
            "volume": volume,
        })

    parsed.sort(key=lambda item: datetime.fromisoformat(item["date"]))
    return parsed


def fetch_cb_sheets(url: str) -> List[Dict[str, Any]]:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    text = response.text.splitlines()
    reader = csv.DictReader(text)

    rows: List[Dict[str, Any]] = []
    for row in reader:
        normalized: Dict[str, Any] = {k: float_or_none(v) if k.lower() != "date" else v for k, v in row.items()}
        rows.append(normalized)

    rows.sort(key=lambda item: datetime.fromisoformat(item["date"]))
    return rows


def main() -> int:
    ensure_data_dir()

    for key, config in SERIES_CONFIG.items():
        print(f"Fetching {key} ({config.source})...")
        if config.source == "fred":
            assert config.series_id is not None
            series = fetch_fred_series(config.series_id)
        elif config.source == "alpha_fx":
            assert config.from_symbol and config.to_symbol
            series = fetch_alpha_fx(config.from_symbol, config.to_symbol)
        elif config.source == "alpha_equity":
            assert config.symbol
            series = fetch_alpha_equity(config.symbol)
        else:
            raise ValueError(f"Unsupported source: {config.source}")
        save_json(config.filename, series)

    cb_url = os.environ.get("CB_SHEETS_CSV_URL")
    if cb_url:
        try:
            print("Fetching central bank balance sheet CSV...")
            rows = fetch_cb_sheets(cb_url)
            save_json("cb_sheets.json", rows)
        except Exception as exc:  # pragma: no cover - optional data
            print(f"Warning: failed to fetch CB sheets: {exc}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
