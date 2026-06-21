import requests
import psycopg2
import psycopg2.extras
import os
import time
import json
import uuid
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("alpha_vantage_extractor")

API_KEY = os.environ["ALPHA_VANTAGE_API_KEY"]
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "JPM", "V", "JNJ"]
BASE_URL = "https://www.alphavantage.co/query"
SECONDS_BETWEEN_CALLS = 13  # keeps under 5 calls/min with safety margin


class AlphaVantageError(Exception):
    """Raised when the API returns a non-data response (rate limit, bad symbol, etc.)"""


def fetch_daily_prices(ticker: str) -> dict:
    """
    Fetch daily OHLCV data for a single ticker.
    Returns the full parsed JSON payload dict.
    Raises AlphaVantageError on rate limits, bad symbols, or unexpected shapes.
    """
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": ticker,
        "apikey": API_KEY,
        "outputsize": "compact",  # last ~100 trading days
    }
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()  
    payload = resp.json()

    if "Note" in payload or "Information" in payload:
        raise AlphaVantageError(payload.get("Note") or payload.get("Information"))
    if "Error Message" in payload:
        raise AlphaVantageError(f"Invalid symbol or request: {payload['Error Message']}")

    series = payload.get("Time Series (Daily)")
    if not series:
        raise AlphaVantageError(f"Unexpected response shape for {ticker}: {list(payload.keys())}")

    return payload


def land_raw_response(ticker: str, dag_run_id: str, payload: dict, url_without_key: str, conn) -> str:
    """Insert the raw JSONB payload into raw_api_responses. Returns the response_id."""
    cursor = conn.cursor()
    response_id = str(uuid.uuid4())
    cursor.execute(
        """
        INSERT INTO raw.raw_api_responses
            (response_id, ticker, api_function, raw_payload, dag_run_id, source_request_url)
        VALUES (%s, %s, 'TIME_SERIES_DAILY', %s, %s, %s)
        """,
        (response_id, ticker, json.dumps(payload), dag_run_id, url_without_key),
    )
    cursor.close()
    return response_id


def upsert_daily_prices(ticker: str, payload: dict, response_id: str, conn) -> int:
    """Parse the Time Series (Daily) block and upsert into raw.daily_prices. Returns row count."""
    series = payload.get("Time Series (Daily)", {})
    if not series:
        return 0

    cursor = conn.cursor()
    rows_loaded = 0
    for date, values in series.items():
        cursor.execute(
            """
            INSERT INTO raw.daily_prices
                (ticker, trade_date, open_price, high_price, low_price, close_price, volume, source_response_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, trade_date) DO NOTHING
            """,
            (
                ticker,
                date,
                float(values["1. open"]),
                float(values["2. high"]),
                float(values["3. low"]),
                float(values["4. close"]),
                int(values["5. volume"]),
                response_id,
            ),
        )
        rows_loaded += 1
    conn.commit()
    cursor.close()
    return rows_loaded


def log_ingestion(dag_run_id: str, ticker: str, status: str, rows_loaded: int,
                  error_message: str | None, started_at: datetime, conn):
    """Write a row to the ingestion_log."""
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO raw.ingestion_log
            (dag_run_id, ticker, status, rows_loaded, error_message, started_at, finished_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (dag_run_id, ticker, status, rows_loaded, error_message, started_at, datetime.now(timezone.utc)),
    )
    conn.commit()
    cursor.close()


def fetch_and_land(ticker: str, dag_run_id: str, conn) -> dict:
    """
    Full extraction cycle for one ticker:
    fetch → land raw JSON → upsert typed rows → log result.
    Returns a result dict for the Airflow task to consume.
    """
    started_at = datetime.now(timezone.utc)
    status = "success"
    rows_loaded = 0
    error_message = None
    response_id = None

    try:
        # 1. Fetch from API
        url_without_key = f"{BASE_URL}?function=TIME_SERIES_DAILY&symbol={ticker}&outputsize=compact"
        payload = fetch_daily_prices(ticker)

        # 2. Land raw JSON unconditionally (even error payloads get stored)
        response_id = land_raw_response(ticker, dag_run_id, payload, url_without_key, conn)

        # 3. Parse and upsert typed rows
        rows_loaded = upsert_daily_prices(ticker, payload, response_id, conn)

    except AlphaVantageError as e:
        status = "rate_limited" if "rate limit" in str(e).lower() else "failed"
        error_message = str(e)
        logger.error(f"{ticker}: API error — {e}")

    except Exception as e:
        status = "failed"
        error_message = str(e)
        logger.exception(f"{ticker}: unexpected failure")

    finally:
        log_ingestion(dag_run_id, ticker, status, rows_loaded, error_message, started_at, conn)

    return {"ticker": ticker, "status": status, "rows_loaded": rows_loaded}


def run(dag_run_id: str = "manual") -> list[dict]:
    """
    Run extraction for all tickers, one at a time with pacing.
    Called by Airflow's PythonOperator.
    Returns list of per-ticker result dicts.
    """
    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

    results: list[dict] = []
    try:
        for i, ticker in enumerate(TICKERS):
            result = fetch_and_land(ticker, dag_run_id, conn)
            results.append(result)
            logger.info(f"{ticker}: status={result['status']}, rows={result['rows_loaded']}")

            # Pace requests to stay under 5 calls/min
            if i < len(TICKERS) - 1:
                time.sleep(SECONDS_BETWEEN_CALLS)
    finally:
        conn.close()

    succeeded = [r for r in results if r["status"] == "success"]
    failed = [r for r in results if r["status"] != "success"]
    logger.info(f"Extraction complete: {len(succeeded)} succeeded, {len(failed)} failed")

    # Fail the task loudly if more than 30% of tickers failed
    if len(failed) > len(TICKERS) * 0.3:
        raise RuntimeError(f"Too many extraction failures: {[(f['ticker'], f.get('error_message')) for f in failed]}")

    return results


if __name__ == "__main__":
    run()