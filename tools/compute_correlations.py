"""
compute_correlations.py — Select FRED daily series that correlate with the S&P 500.

Filtering rules:
1. Exclude all securities, commodities, indexes, crypto, VIX, Nikkei, etc.
2. Use a 60-calendar-day rolling window ending today.
3. Require >= 95% date overlap with S&P 500 trading dates in the window.
   Missing dates are filled via last-observation-carried-forward (LOCF).
4. Keep only non-market series (rates, spreads, job postings, economic indicators).

Outputs:
  strict_correlations.json  -- full ranked results with correlation statistics
  strict_curated.json       -- [{id, title}] list used to regenerate src/curated.js

Usage:
  FRED_API_KEY=<key> python3 compute_correlations.py

Run this when:
  - The FRED series pool changes (after running tools/discover_all_fred.py)
  - You want to refresh correlation rankings with a longer historical window
"""
import os
import requests
import json
import time
import atexit
import numpy as np
from datetime import date, timedelta

WINDOW_DAYS = 60
MIN_OVERLAP_RATIO = 0.95

API_KEY = os.environ.get('FRED_API_KEY')
if not API_KEY:
    print('Error: FRED_API_KEY environment variable not set.')
    print('  export FRED_API_KEY=<your_key>')
    raise SystemExit(1)

BASE = "https://api.stlouisfed.org/fred"
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

# Series to EXCLUDE: anything that is directly a price of a traded security,
# commodity, commodity index, security index, or crypto
EXCLUDE_KEYWORDS = [
    # Stock/security indexes and volatility
    "vix", "volatility index", "nikkei", "nasdaq", "dow jones",
    "russell 2000", "s&p 500", "s&p500",
    # Crypto
    "bitcoin", "ethereum", "coinbase", "litecoin", "bitcoin cash",
    "crypto",
    # Commodities - direct prices
    "gold price", "silver price", "copper price", "oil price",
    "platinum price", "palladium price",
    "wheat price", "corn price", "soybean price",
    "lumber price", "cotton price", "coffee price",
    "cocoa price", "sugar price", "cattle price",
    "natural gas price", "gasoline price", "diesel price",
    "wti", "brent",
    # Commodity indexes
    "commodity index",
    # Direct equity/security prices
    "stock price", "share price", "equity price",
    "total return index value",
]

# Series IDs to explicitly exclude
EXCLUDE_IDS = {
    # VIX and variants
    "VIXCLS", "VXNCLS", "VXDCLS", "VXVCLS", "RVXCLS",
    "VXGSCLS", "VXAPLCLS", "VXAZNCLS", "VXGOGCLS", "VXIBMCLS",
    "VXSLVCLS", "VXEWZCLS", "VXFXICLS", "VXEEMCLS", "VXGDXCLS",
    "VXUSCLS",
    # Nikkei
    "NIKKEI225",
    # Crypto
    "CBBTCUSD", "CBETHUSD", "CBLTCUSD", "CBBCHUSD",
    # Gasoline/energy spot prices
    "DGASNYH", "DGASRGCG", "DCOILWTICO", "DCOILBRENTEU",
    "DHHNGSP", "DPROPANEMBTX",
    # Gold/Silver/Commodity spot prices
    "GOLDAMGBD228NLBM", "GOLDPMGBD228NLBM",
    "SLVPRUSD",
    # Total return indexes (these are securities)
    "BAMLHYH0A3CMTRIV", "BAMLHYH0A0HYM2TRIV",
    "BAMLCC0A1AAATRIV", "BAMLCC0A0CMTRIV",
}
EXCLUDE_CATEGORY_ID = 32255  # Stock Market Indexes

def fred_get(endpoint, params, retries=MAX_RETRIES):
    global _api_attempts, _api_successes, _api_retries
    params["api_key"] = API_KEY
    params["file_type"] = "json"
    for attempt in range(retries):
        try:
            throttle_fred_requests()
            _api_attempts += 1
            resp = requests.get(f"{BASE}/{endpoint}", params=params, timeout=30)
            if resp.status_code == 429:
                wait = min(MAX_BACKOFF_S, max(5.0, MIN_REQUEST_INTERVAL_S * (2 ** (attempt + 1))))
                print(f"  [RATE LIMITED] {endpoint} retry {attempt+1}/{retries} after {wait:.1f}s")
                _api_retries += 1
                time.sleep(wait)
                continue
            if 500 <= resp.status_code < 600:
                wait = min(MAX_BACKOFF_S, 2.0 * (attempt + 1))
                print(f"  [SERVER {resp.status_code}] {endpoint} retry {attempt+1}/{retries} after {wait:.1f}s")
                _api_retries += 1
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                raise RuntimeError(f"FRED {endpoint} returned {resp.status_code}: {resp.text[:200]}")
            data = resp.json()
            if data.get("error_code"):
                if str(data["error_code"]) == "429":
                    wait = min(MAX_BACKOFF_S, max(5.0, MIN_REQUEST_INTERVAL_S * (2 ** (attempt + 1))))
                    print(f"  [RATE LIMITED payload] {endpoint} retry {attempt+1}/{retries} after {wait:.1f}s")
                    _api_retries += 1
                    time.sleep(wait)
                    continue
                raise RuntimeError(f"FRED {endpoint} error {data['error_code']}: {data.get('error_message', '')}")
            _api_successes += 1
            return data
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as err:
            wait = min(MAX_BACKOFF_S, 2.0 * (attempt + 1))
            print(f"  [NETWORK] {endpoint} retry {attempt+1}/{retries} after {wait:.1f}s: {err}")
            _api_retries += 1
            time.sleep(wait)
    raise RuntimeError(f"FRED {endpoint} failed after {retries} retries")

def fetch_category_series_ids(category_id):
    """Fetch all series IDs in a FRED category."""
    ids = set()
    offset = 0
    while True:
        data = fred_get("category/series", {
            "category_id": category_id,
            "limit": 1000,
            "offset": offset,
            "sort_order": "asc",
        })
        batch = data.get("seriess", [])
        for s in batch:
            if "id" in s:
                ids.add(s["id"])
        if len(batch) < 1000:
            break
        offset += 1000
    return ids

def fetch_observations(series_id, start_date):
    data = fred_get("series/observations", {
        "series_id": series_id,
        "observation_start": start_date,
        "limit": 100000,
        "sort_order": "asc",
    })
    return [
        {"date": o["date"], "value": float(o["value"])}
        for o in data.get("observations", [])
        if o["value"] != "."
    ]

def get_window_start(today=None):
    if today is None:
        today = date.today()
    return today - timedelta(days=WINDOW_DAYS)

def is_excluded(series):
    """Check if a series is a traded security, commodity, or index."""
    sid = series["id"]
    title = series["title"].lower()

    if sid in EXCLUDE_IDS:
        return True

    for kw in EXCLUDE_KEYWORDS:
        if kw in title:
            return True

    return False

def forward_fill(obs, target_dates):
    """
    Fill missing target_dates using last-observation-carried-forward (LOCF).
    Returns augmented obs list sorted by date, or None if a date cannot be filled
    (i.e., the missing date is earlier than all available observations).
    """
    obs_sorted = sorted(obs, key=lambda x: x['date'])
    obs_map = {o['date']: o['value'] for o in obs_sorted}
    extra = []
    for d in sorted(target_dates):
        if d in obs_map:
            continue
        # Find the most recent obs on or before d
        prior_value = None
        for o in obs_sorted:
            if o['date'] <= d:
                prior_value = o['value']
            else:
                break
        if prior_value is None:
            return None  # Missing date precedes all data
        obs_map[d] = prior_value
        extra.append({'date': d, 'value': prior_value})
    return obs_sorted + extra if extra else obs_sorted


def pearson_r(xs, ys):
    xs = np.array(xs, dtype=float)
    ys = np.array(ys, dtype=float)
    if len(xs) < 10:
        return float('nan')
    if np.std(xs) == 0 or np.std(ys) == 0:
        return float('nan')
    return float(np.corrcoef(xs, ys)[0, 1])


if __name__ == "__main__":
    print("=" * 70)
    print("Daily Chart Crime — Correlation Analysis")
    print("Excluding: securities, commodities, indexes, crypto")
    print(f"Window: trailing {WINDOW_DAYS} calendar days")
    print(f"Requiring: >= {int(MIN_OVERLAP_RATIO*100)}% date overlap (LOCF fill-forward for missing dates)")
    print(f"Throttle: {MIN_REQUEST_INTERVAL_S:.1f}s between requests; retries={MAX_RETRIES}")
    print("=" * 70)

    print(f"Fetching explicit exclusion IDs from FRED category {EXCLUDE_CATEGORY_ID}...")
    exclude_from_category = fetch_category_series_ids(EXCLUDE_CATEGORY_ID)
    effective_excluded_ids = EXCLUDE_IDS | exclude_from_category
    print(f"  Exclusion set size: {len(effective_excluded_ids)} (category added {len(exclude_from_category)})")

    # Load all series — prefer the full category-tree file (tools/ or root)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(script_dir, "tools", "all_daily_series_full.json"),
        os.path.join(script_dir, "all_daily_series_full.json"),
        os.path.join(script_dir, "all_daily_series.json"),
    ]
    series_file = None
    all_series = []
    for path in candidates:
        if not os.path.exists(path):
            continue
        with open(path) as f:
            loaded = json.load(f)
        if loaded:
            series_file = path
            all_series = loaded
            break
    if not series_file:
        # Fall back to the last existing candidate (even if empty) for explicit failure context.
        for path in candidates:
            if os.path.exists(path):
                series_file = path
                with open(path) as f:
                    all_series = json.load(f)
                break
    if not series_file:
        print("Error: no discovery series file found.")
        raise SystemExit(1)
    print(f"Total series from discovery ({os.path.basename(series_file)}): {len(all_series)}")
    if not all_series:
        raise RuntimeError(f"Discovery file is empty: {series_file}")

    # Filter out excluded series
    filtered = [
        s for s in all_series
        if s["id"] not in effective_excluded_ids and not is_excluded(s)
    ]
    excluded_count = len(all_series) - len(filtered)
    print(f"After removing securities/commodities/indexes: {len(filtered)} ({excluded_count} removed)")

    # Show what was excluded
    excluded_examples = [s for s in all_series if is_excluded(s)][:20]
    print(f"\nExcluded examples:")
    for s in excluded_examples:
        print(f"  {s['id']:20s}  {s['title'][:60]}")

    # Correlation window
    window_start = get_window_start().isoformat()
    print(f"\nCorrelation window start: {window_start}")

    # Fetch S&P 500
    print("Fetching S&P 500...")
    sp500 = fetch_observations("SP500", window_start)
    if not sp500:
        raise RuntimeError("No S&P 500 observations returned for the selected window")
    sp500_dates = set(o["date"] for o in sp500)
    sp500_map = {o["date"]: o["value"] for o in sp500}
    print(f"  S&P 500: {len(sp500)} observations ({sp500[0]['date']} to {sp500[-1]['date']})")
    print(f"  S&P 500 dates: {sorted(sp500_dates)[:5]}...{sorted(sp500_dates)[-3:]}")
    max_missing = int((1.0 - MIN_OVERLAP_RATIO) * len(sp500_dates))
    print(f"  Overlap rule: allow up to {max_missing} missing S&P dates out of {len(sp500_dates)}")

    # Compute correlations with STRICT date overlap
    results = []
    perfect_overlap = 0
    partial_overlap = 0
    no_overlap = 0

    for i, series in enumerate(filtered):
        sid = series["id"]
        title = series["title"]

        if (i + 1) % 50 == 0:
            print(f"  Progress: {i+1}/{len(filtered)} "
                  f"(perfect={perfect_overlap}, partial={partial_overlap}, "
                  f"none={no_overlap})")

        try:
            obs = fetch_observations(sid, window_start)
            obs_dates = set(o["date"] for o in obs)

            # Allow up to max_missing missing S&P 500 dates, filled by LOCF
            missing_dates = sp500_dates - obs_dates
            if len(missing_dates) > max_missing:
                partial_overlap += 1
                continue

            if missing_dates:
                obs = forward_fill(obs, missing_dates)
                if obs is None:
                    partial_overlap += 1
                    continue

            obs_map = {o["date"]: o["value"] for o in obs}

            # Align on S&P 500 dates
            sp_vals = []
            other_vals = []
            dates = sorted(sp500_dates)
            for d in dates:
                sp_vals.append(sp500_map[d])
                other_vals.append(obs_map[d])

            r = pearson_r(sp_vals, other_vals)
            if not np.isnan(r):
                perfect_overlap += 1
                results.append({
                    "id": sid,
                    "title": title,
                    "r": round(r, 6),
                    "abs_r": round(abs(r), 6),
                    "n_dates": len(dates),
                    "filled_dates": sorted(missing_dates),
                    "units": series.get("units", ""),
                    "popularity": series.get("popularity", 0),
                })
            else:
                no_overlap += 1

        except Exception as e:
            raise RuntimeError(f"Failed while processing series {sid}: {e}") from e

    print(f"\n{'=' * 70}")
    print(f"RESULTS: {len(results)} series with perfect date overlap")
    print(f"Partial overlap (rejected): {partial_overlap}")
    print(f"No data/zero variance: {no_overlap}")
    print(f"{'=' * 70}")

    results.sort(key=lambda x: x["abs_r"], reverse=True)

    # Save full results
    out_full = os.path.join(script_dir, "strict_correlations.json")
    with open(out_full, "w") as f:
        json.dump(results, f, indent=2)

    # Print all results
    print(f"\nALL {len(results)} series with sufficient S&P 500 date overlap:")
    print(f"{'Rank':>4} {'r':>8} {'N':>3} {'Pop':>3} {'ID':>24}  {'Title'}")
    print("-" * 110)
    for i, r in enumerate(results):
        print(f"{i+1:4d} {r['r']:+.4f} {r['n_dates']:3d} {r['popularity']:3d} {r['id']:>24s}  {r['title'][:60]}")

    # Save curated list used to regenerate src/curated.js
    curated = [{"id": r["id"], "title": r["title"]} for r in results]
    out_curated = os.path.join(script_dir, "strict_curated.json")
    with open(out_curated, "w") as f:
        json.dump(curated, f, indent=2)
    print(f"\nSaved {len(curated)} series to strict_curated.json")
