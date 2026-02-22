"""
Explore FRED to find ALL daily-frequency series with recent observations,
using multiple search strategies to get past the 5000-match API limit.
Output: all_daily_series.json (no pickle used)
"""
import os
import requests
import json
import time
import atexit
from pathlib import Path

API_KEY = os.environ.get('FRED_API_KEY')
if not API_KEY:
    print('Error: FRED_API_KEY environment variable not set.')
    raise SystemExit(1)
BASE = "https://api.stlouisfed.org/fred"
CUTOFF = "2026-01-30"
PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_FILE = PROJECT_ROOT / "all_daily_series.json"
MIN_REQUEST_INTERVAL_S = float(os.environ.get("FRED_MIN_REQUEST_INTERVAL_S", "1.0"))
MAX_RETRIES = int(os.environ.get("FRED_MAX_RETRIES", "6"))
MAX_BACKOFF_S = float(os.environ.get("FRED_MAX_BACKOFF_S", "60"))
_last_request_ts = 0.0
_api_attempts = 0
_api_successes = 0
_api_retries = 0


def throttle_fred_requests():
    """Ensure at least MIN_REQUEST_INTERVAL_S between FRED API request start times."""
    global _last_request_ts
    now = time.monotonic()
    wait = MIN_REQUEST_INTERVAL_S - (now - _last_request_ts)
    if wait > 0:
        time.sleep(wait)
    _last_request_ts = time.monotonic()


def print_api_summary():
    print(
        f"[API] FRED request attempts={_api_attempts} successes={_api_successes} retries={_api_retries}"
    )


atexit.register(print_api_summary)

def fred_get(endpoint, params, retries=MAX_RETRIES):
    global _api_attempts, _api_successes, _api_retries
    params = dict(params)
    params["api_key"] = API_KEY
    params["file_type"] = "json"

    for attempt in range(retries):
        try:
            throttle_fred_requests()
            _api_attempts += 1
            resp = requests.get(f"{BASE}/{endpoint}", params=params, timeout=30)
            if resp.status_code == 429:
                wait = min(MAX_BACKOFF_S, max(5.0, MIN_REQUEST_INTERVAL_S * (2 ** (attempt + 1))))
                print(f"  [RATE LIMITED] {endpoint} retry {attempt + 1}/{retries} after {wait:.1f}s")
                _api_retries += 1
                time.sleep(wait)
                continue
            if 500 <= resp.status_code < 600:
                wait = min(MAX_BACKOFF_S, 2.0 * (attempt + 1))
                print(f"  [SERVER {resp.status_code}] {endpoint} retry {attempt + 1}/{retries} after {wait:.1f}s")
                _api_retries += 1
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                raise RuntimeError(f"FRED {endpoint} returned {resp.status_code}: {resp.text[:200]}")
            data = resp.json()
            if data.get("error_code"):
                if str(data["error_code"]) == "429":
                    wait = min(MAX_BACKOFF_S, max(5.0, MIN_REQUEST_INTERVAL_S * (2 ** (attempt + 1))))
                    print(f"  [RATE LIMITED payload] {endpoint} retry {attempt + 1}/{retries} after {wait:.1f}s")
                    _api_retries += 1
                    time.sleep(wait)
                    continue
                raise RuntimeError(f"FRED {endpoint} error {data['error_code']}: {data.get('error_message', '')}")
            _api_successes += 1
            return data
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as err:
            wait = min(MAX_BACKOFF_S, 2.0 * (attempt + 1))
            print(f"  [NETWORK] {endpoint} retry {attempt + 1}/{retries} after {wait:.1f}s: {err}")
            _api_retries += 1
            time.sleep(wait)

    raise RuntimeError(f"FRED {endpoint} failed after {retries} retries")

def get_excluded_series():
    """Get all series in category 32255 (stock market indexes)."""
    excluded = set()
    offset = 0
    while True:
        data = fred_get("category/series", {"category_id": 32255, "limit": 1000, "offset": offset})
        series = data.get("seriess", [])
        print(f"  Category 32255 offset={offset}: {len(series)} series")
        for s in series:
            excluded.add(s["id"])
        if len(series) < 1000:
            break
        offset += 1000
    print(f"  Total excluded: {len(excluded)}")
    return excluded

def search_daily_series_by_text(search_terms):
    """Search for daily series using various search terms."""
    all_series = {}
    for term in search_terms:
        offset = 0
        term_count = 0
        while True:
            data = fred_get("series/search", {
                "search_text": term,
                "filter_variable": "frequency",
                "filter_value": "Daily",
                "order_by": "observation_end",
                "sort_order": "desc",
                "limit": 1000,
                "offset": offset,
            })
            series = data.get("seriess", [])
            count = data.get("count", 0)
            if offset == 0:
                print(f"  Search '{term}': {count} total matches")
                if count >= 5000:
                    print(f"    WARNING: >= 5000 matches, results truncated!")

            for s in series:
                if s["observation_end"] < CUTOFF:
                    break
                if s["id"] not in all_series:
                    all_series[s["id"]] = {
                        "id": s["id"],
                        "title": s["title"],
                        "observation_end": s["observation_end"],
                        "frequency": s.get("frequency_short", ""),
                        "units": s.get("units", ""),
                        "popularity": s.get("popularity", 0),
                    }
                    term_count += 1

            if series and series[-1]["observation_end"] < CUTOFF:
                break
            if len(series) < 1000:
                break
            offset += 1000

        if term_count > 0:
            print(f"    New qualifying series from '{term}': {term_count}")

    return all_series

def search_by_tags(tag_names_list):
    """Search using various tag combinations."""
    all_series = {}
    for tags in tag_names_list:
        offset = 0
        tag_count = 0
        while True:
            data = fred_get("tags/series", {
                "tag_names": tags,
                "order_by": "observation_end",
                "sort_order": "desc",
                "limit": 1000,
                "offset": offset,
            })
            series = data.get("seriess", [])
            if offset == 0:
                count = data.get("count", 0)
                print(f"  Tag '{tags}': ~{count} total")

            for s in series:
                if s["observation_end"] < CUTOFF:
                    break
                if s.get("frequency_short") == "D" and s["id"] not in all_series:
                    all_series[s["id"]] = {
                        "id": s["id"],
                        "title": s["title"],
                        "observation_end": s["observation_end"],
                        "frequency": s.get("frequency_short", ""),
                        "units": s.get("units", ""),
                        "popularity": s.get("popularity", 0),
                    }
                    tag_count += 1

            if series and series[-1]["observation_end"] < CUTOFF:
                break
            if len(series) < 1000:
                break
            offset += 1000

        if tag_count > 0:
            print(f"    New daily series from tag '{tags}': {tag_count}")

    return all_series

def search_categories(category_ids):
    """Search specific FRED categories for daily series."""
    all_series = {}
    for cat_id, cat_name in category_ids:
        offset = 0
        cat_count = 0
        while True:
            data = fred_get("category/series", {
                "category_id": cat_id,
                "filter_variable": "frequency",
                "filter_value": "Daily",
                "order_by": "observation_end",
                "sort_order": "desc",
                "limit": 1000,
                "offset": offset,
            })
            series = data.get("seriess", [])
            if offset == 0:
                print(f"  Category {cat_id} ({cat_name}): {len(series)} daily series")

            for s in series:
                if s["observation_end"] < CUTOFF:
                    break
                if s["id"] not in all_series:
                    all_series[s["id"]] = {
                        "id": s["id"],
                        "title": s["title"],
                        "observation_end": s["observation_end"],
                        "frequency": s.get("frequency_short", ""),
                        "units": s.get("units", ""),
                        "popularity": s.get("popularity", 0),
                    }
                    cat_count += 1

            if series and series[-1]["observation_end"] < CUTOFF:
                break
            if len(series) < 1000:
                break
            offset += 1000

        if cat_count > 0:
            print(f"    New from cat {cat_id}: {cat_count}")

    return all_series


if __name__ == "__main__":
    print("=" * 60)
    print("FRED Daily Series Discovery - Comprehensive Search")
    print(f"Throttle: {MIN_REQUEST_INTERVAL_S:.1f}s between requests; retries={MAX_RETRIES}")
    print("=" * 60)

    # 1. Build exclusion list
    print("\n--- Step 1: Building exclusion list (category 32255) ---")
    excluded = get_excluded_series()

    # 2. Tag-based search (what we already have, plus more tags)
    print("\n--- Step 2: Tag-based search ---")
    tag_series = search_by_tags([
        "daily",
        "daily;rate",
        "daily;index",
        "daily;price",
        "daily;exchange rate",
        "daily;interest rate",
        "daily;spread",
        "daily;yield",
        "daily;commodity",
        "daily;currency",
        "daily;treasury",
        "daily;bond",
        "daily;oil",
        "daily;gold",
        "daily;bitcoin",
        "daily;mortgage",
    ])

    # 3. Text-based search with frequency filter - cast a wide net
    print("\n--- Step 3: Text search with daily frequency filter ---")
    search_terms = [
        # Rates and yields
        "interest rate", "treasury yield", "bond yield", "mortgage rate",
        "federal funds", "LIBOR", "SOFR", "prime rate",
        # Commodities and prices
        "oil price", "gold price", "silver price", "copper price",
        "natural gas price", "wheat price", "corn price", "soybean",
        "commodity price", "gasoline price", "diesel",
        # Currencies
        "exchange rate", "dollar", "euro", "yen", "pound sterling",
        "yuan", "canadian dollar", "swiss franc", "peso",
        "australian dollar", "brazilian real",
        # Credit and spreads
        "credit spread", "yield spread", "TED spread", "swap rate",
        "corporate bond", "high yield", "investment grade",
        "breakeven inflation",
        # Fun/unusual
        "bitcoin", "ethereum", "crypto",
        "volatility", "VIX",
        "unemployment claims", "initial claims",
        "consumer", "retail",
        "shipping", "freight", "Baltic",
        "electricity", "coal",
        "lumber", "cotton", "coffee", "cocoa", "sugar",
        "cattle", "hog", "pork",
        "platinum", "palladium",
        "nickel", "zinc", "aluminum",
        "fertilizer",
        "egg", "milk", "cheese", "butter",
        "ice cream",
        "chicken", "turkey",
        "avocado",
        "banana",
    ]
    text_series = search_daily_series_by_text(search_terms)

    # 4. Category-based search - key FRED categories
    print("\n--- Step 4: Category-based search ---")
    categories = [
        (15, "Exchange Rates"),
        (22, "Interest Rates"),
        (32, "Money, Banking, & Finance"),
        (94, "Housing"),
        (3000, "Prices"),
        (32991, "Commodities"),
        (33705, "International"),
        (32413, "Academic Data"),
        (32263, "Treasury Bills"),
        (32264, "Treasury Bonds"),
        (32265, "Treasury Notes"),
        (32266, "TIPS"),
        (33446, "Commercial Paper"),
        (32345, "Corporate Bonds"),
        (32348, "Swaps"),
        (33060, "Money Market"),
        (32216, "Daily Rates"),
    ]
    cat_series = search_categories(categories)

    # 5. Merge all results
    print("\n--- Step 5: Merging and filtering ---")
    all_series = {}
    for source_name, source_dict in [("tags", tag_series), ("text", text_series), ("categories", cat_series)]:
        for sid, s in source_dict.items():
            if sid not in all_series:
                all_series[sid] = s
        print(f"  After {source_name}: {len(all_series)} unique series")

    # Remove excluded
    before = len(all_series)
    all_series = {k: v for k, v in all_series.items() if k not in excluded}
    print(f"  After excluding cat 32255: {len(all_series)} series ({before - len(all_series)} removed)")

    # 6. Save
    series_list = list(all_series.values())
    series_list.sort(key=lambda s: s.get("popularity", 0), reverse=True)

    with open(OUTPUT_FILE, "w") as f:
        json.dump(series_list, f, indent=2)

    print(f"\n--- TOTAL: {len(series_list)} daily series saved to {OUTPUT_FILE} ---")
    print(f"Top 20 by popularity:")
    for s in series_list[:20]:
        print(f"  {s['id']:20s} pop={s['popularity']:3d}  {s['title'][:70]}")
