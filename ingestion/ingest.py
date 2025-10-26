import argparse
import io
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import pandas as pd
import requests
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from google.cloud import storage
from dateutil import parser as dtp

BINANCE_INTERVALS = {"1m","5m","15m","30m","1h","4h","1d"}
UTC = timezone.utc
BINANCE_BASE = "https://api.binance.com"

class IngestionError(Exception): ...

def to_utc(ts):
    """Return a pandas.Timestamp in UTC, regardless of input being naive/aware."""
    p = pd.Timestamp(ts)
    if p.tzinfo is None:
        return p.tz_localize(UTC)
    return p.tz_convert(UTC)

def load_yaml(path: str) -> dict:
    import yaml
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def ensure_gcs(project_id: str | None) -> storage.Client:
    # Uses ADC (run: gcloud auth application-default login)
    return storage.Client(project=project_id) if project_id else storage.Client()

def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()

def gcs_upload_bytes(client: storage.Client, bucket: str, blob: str, data: bytes):
    bkt = client.bucket(bucket)
    obj = bkt.blob(blob)
    obj.upload_from_string(data, content_type="application/octet-stream")

def chunk_window(interval: str) -> timedelta:
    # Conservative chunks to respect Binance limits
    return {
        "1m": timedelta(hours=12),
        "5m": timedelta(days=2),
        "15m": timedelta(days=6),
        "30m": timedelta(days=12),
        "1h": timedelta(days=24),
        "4h": timedelta(days=96),
        "1d": timedelta(days=365 * 3),
    }[interval]

@retry(wait=wait_exponential(multiplier=1, min=1, max=20),
       stop=stop_after_attempt(6),
       retry=retry_if_exception_type((requests.RequestException, IngestionError)))
def binance_fetch(symbol: str, interval: str, start_ms: int, end_ms: int, limit: int = 1000) -> list:
    url = f"{BINANCE_BASE}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "startTime": start_ms, "endTime": end_ms, "limit": limit}
    r = requests.get(url, params=params, timeout=30)
    if r.status_code == 429:  # rate limited
        time.sleep(2.0)
        raise IngestionError("Binance rate limit")
    r.raise_for_status()
    return r.json()

def fetch_ohlcv(symbol: str, interval: str, start: datetime, end: datetime) -> pd.DataFrame:
    if interval not in BINANCE_INTERVALS:
        raise ValueError(f"Unsupported interval: {interval}")
    rows: list[dict] = []
    step = chunk_window(interval)
    cur = start
    while cur < end:
        chunk_end = min(cur + step, end)
        data = binance_fetch(symbol, interval, int(cur.timestamp()*1000), int(chunk_end.timestamp()*1000))
        for k in data:
            # kline: [openTime, open, high, low, close, volume, closeTime, ...]
            rows.append({
                "timestamp": pd.to_datetime(int(k[0]), unit="ms", utc=True),
                "open": float(k[1]),
                "high": float(k[2]),
                "low":  float(k[3]),
                "close":float(k[4]),
                "volume":float(k[5]),
            })
        cur = chunk_end
        time.sleep(0.15)
    if not rows:
        return pd.DataFrame(columns=["timestamp","open","high","low","close","volume"])
    df = pd.DataFrame(rows).drop_duplicates(subset=["timestamp"]).sort_values("timestamp")

    s = to_utc(start)
    e = to_utc(end)
    mask = (df["timestamp"] >= s) & (df["timestamp"] <= e)
    return df.loc[mask].reset_index(drop=True)


def normalize(df: pd.DataFrame, symbol: str, interval: str) -> pd.DataFrame:
    df = df.copy()
    df["symbol"] = symbol
    df["interval"] = interval
    df["source"] = "binance"
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df[["timestamp","open","high","low","close","volume","symbol","interval","source"]]

def resolve_window(start: str | None, end: str | None, days: int | None) -> Tuple[datetime, datetime]:
    if start:
        s = dtp.isoparse(start).astimezone(UTC)
    else:
        s = datetime.now(UTC) - timedelta(days=int(days or 30))
    e = dtp.isoparse(end).astimezone(UTC) if end else datetime.now(UTC)
    if s >= e:
        raise ValueError("start must be before end")
    return s, e

def build_gcs_path(prefix: str, symbol: str, interval: str, run_dt: datetime) -> str:
    # Non-hive layout with a single, fixed filename per day.
    date_part = run_dt.strftime("%Y-%m-%d")
    prefix_clean = (prefix or "").strip("/")
    components = [c for c in [prefix_clean, symbol, interval, date_part] if c]
    return "/".join(components) + "/data.parquet"


def parse_args():
    ap = argparse.ArgumentParser(description="Ingest OHLCV from Binance → GCS (Parquet)")
    ap.add_argument("--config", default=os.path.join(os.path.dirname(__file__), "config.yaml"))
    ap.add_argument("--bucket", help="GCS bucket (name only)")
    ap.add_argument("--project-id", help="Optional GCP project for GCS client")
    ap.add_argument("--symbols", nargs="*", help="Binance symbols (e.g., BTCUSDT ETHUSDT)")
    ap.add_argument("--interval", choices=list(BINANCE_INTERVALS), help="Candle interval")
    ap.add_argument("--days", type=int, help="Lookback if no start/end")
    ap.add_argument("--start", help="UTC start, e.g. 2024-01-01T00:00:00Z")
    ap.add_argument("--end", help="UTC end (default now)")
    ap.add_argument("--prefix", default=None, help="GCS object prefix (e.g., ohlcv)")
    return ap.parse_args()

def main():
    args = parse_args()
    cfg = load_yaml(args.config) if os.path.exists(args.config) else {}

    bucket = args.bucket or cfg.get("bucket")
    if not bucket:
        print("ERROR: --bucket or config.yaml bucket is required", file=sys.stderr); sys.exit(2)

    project_id = args.project_id or cfg.get("project_id") or None
    symbols = args.symbols or cfg.get("symbols", ["BTCUSDT"])
    interval = args.interval or cfg.get("interval", "1h")
    days = args.days or cfg.get("days", 30)
    prefix = args.prefix if args.prefix is not None else cfg.get("partitioning", {}).get("prefix", "ohlcv")

    start, end = resolve_window(args.start, args.end, days)
    client = ensure_gcs(project_id)

    for sym in symbols:
        df = fetch_ohlcv(sym, interval, start, end)
        if df.empty:
            print(f"[{sym}] No data returned."); continue
        df = normalize(df, sym, interval)
        run_dt = datetime.now(UTC)
        blob = build_gcs_path(prefix, sym, interval, run_dt)
        gcs_upload_bytes(client, bucket, blob, to_parquet_bytes(df))
        print(f"[{sym}] Uploaded {len(df):,} rows → gs://{bucket}/{blob}")

if __name__ == "__main__":
    main()
