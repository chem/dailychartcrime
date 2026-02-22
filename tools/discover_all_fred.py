#!/usr/bin/env python3
"""
discover_all_fred.py — Discover daily FRED series efficiently.

Default mode uses fred/series/updates for a narrow time window (today+yesterday),
which is much faster than category-tree traversal when you only care about
recently-updated daily series.

Modes:
  - fast (default): recent updates endpoint
  - full-tree: recursive category walk (fallback/completeness mode)

Outputs:
  tools/all_daily_series_full.json
  all_daily_series.json

Usage:
  FRED_API_KEY=<key> python3 tools/discover_all_fred.py
  FRED_API_KEY=<key> python3 tools/discover_all_fred.py --mode full-tree
"""

import argparse
import json
import os
import sys
import time
import atexit
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print('Error: pip install requests')
    sys.exit(1)

API_KEY = os.environ.get('FRED_API_KEY')
if not API_KEY:
    print('Error: FRED_API_KEY environment variable not set.')
    sys.exit(1)
BASE = 'https://api.stlouisfed.org/fred'
MIN_REQUEST_INTERVAL_S = float(os.environ.get('FRED_MIN_REQUEST_INTERVAL_S', '1.0'))
MAX_RETRIES = int(os.environ.get('FRED_MAX_RETRIES', '6'))
MAX_BACKOFF_S = float(os.environ.get('FRED_MAX_BACKOFF_S', '60'))
RECENT_WINDOW_DAYS = int(os.environ.get('FRED_RECENT_WINDOW_DAYS', '2'))
TOOLS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TOOLS_DIR.parent
_last_request_ts = 0.0
_api_attempts = 0
_api_successes = 0
_api_retries = 0

# Categories known to contain only securities/indexes — skip entirely
# to avoid fetching thousands of series we'll discard anyway.
SKIP_CATEGORY_IDS = {
    32255,   # Stock Market Indexes
    32356,   # Cryptocurrencies
    33913,   # Equity Premium
}

# -----------------------------------------------------------------------

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
        f'[API] FRED request attempts={_api_attempts} successes={_api_successes} retries={_api_retries}'
    )


atexit.register(print_api_summary)

def fred_get(endpoint, params, retries=MAX_RETRIES):
    global _api_attempts, _api_successes, _api_retries
    params = dict(params)
    params['api_key'] = API_KEY
    params['file_type'] = 'json'
    for attempt in range(retries):
        try:
            throttle_fred_requests()
            _api_attempts += 1
            resp = requests.get(f'{BASE}/{endpoint}', params=params, timeout=30)
            if resp.status_code == 429:
                wait = min(MAX_BACKOFF_S, max(5.0, MIN_REQUEST_INTERVAL_S * (2 ** (attempt + 1))))
                print(f'  [RATE LIMITED] {endpoint} retry {attempt + 1}/{retries} after {wait:.1f}s')
                _api_retries += 1
                time.sleep(wait)
                continue
            if 500 <= resp.status_code < 600:
                wait = min(MAX_BACKOFF_S, 2.0 * (attempt + 1))
                print(f'  [SERVER {resp.status_code}] {endpoint} retry {attempt + 1}/{retries} after {wait:.1f}s')
                _api_retries += 1
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                raise RuntimeError(f'FRED {endpoint} returned {resp.status_code}: {resp.text[:200]}')
            data = resp.json()
            if data.get('error_code'):
                if str(data['error_code']) == '429':
                    wait = min(MAX_BACKOFF_S, max(5.0, MIN_REQUEST_INTERVAL_S * (2 ** (attempt + 1))))
                    print(f'  [RATE LIMITED payload] {endpoint} retry {attempt + 1}/{retries} after {wait:.1f}s')
                    _api_retries += 1
                    time.sleep(wait)
                    continue
                raise RuntimeError(f"FRED {endpoint} error {data['error_code']}: {data.get('error_message', '')}")
            _api_successes += 1
            return data
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            wait = min(MAX_BACKOFF_S, 2.0 * (attempt + 1))
            _api_retries += 1
            time.sleep(wait)
    raise RuntimeError(f'FRED {endpoint} failed after {retries} retries')


def ts(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def discover_recent_daily_series():
    """
    Fast mode: pull recently updated series from fred/series/updates and keep
    only daily-frequency series with observation_end >= yesterday (UTC date).
    """
    now = datetime.now(timezone.utc)
    start_dt = (now - timedelta(days=RECENT_WINDOW_DAYS - 1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    end_dt = now.replace(hour=23, minute=59, second=59, microsecond=0)
    yesterday = (now - timedelta(days=1)).date().isoformat()

    print(f'Recent window: {ts(start_dt)} to {ts(end_dt)} (UTC)')
    print(f'Keeping daily series with observation_end >= {yesterday}')

    all_series = {}
    offset = 0
    total_seen = 0
    while True:
        data = fred_get('series/updates', {
            'start_time': ts(start_dt),
            'end_time': ts(end_dt),
            'limit': 1000,
            'offset': offset,
            'sort_order': 'desc',
        })

        batch = data.get('seriess', [])
        if offset == 0:
            print(f"series/updates returned {data.get('count', 0)} records in window")

        if not batch:
            break

        for s in batch:
            total_seen += 1
            if s.get('frequency_short') != 'D':
                continue
            if s.get('observation_end', '') < yesterday:
                continue
            sid = s['id']
            if sid in all_series:
                continue
            all_series[sid] = {
                'id': sid,
                'title': s.get('title', ''),
                'observation_end': s.get('observation_end', ''),
                'frequency': s.get('frequency_short', ''),
                'units': s.get('units', ''),
                'popularity': s.get('popularity', 0),
                'last_updated': s.get('last_updated', ''),
            }

        if len(batch) < 1000:
            break
        offset += 1000

    print(f'Processed {total_seen} recent update records')
    return all_series


def get_child_categories(cat_id):
    data = fred_get('category/children', {'category_id': cat_id})
    return data.get('categories', [])


def get_series_in_category(cat_id):
    """Fetch all daily-frequency series in a category."""
    series = []
    offset = 0
    while True:
        data = fred_get('category/series', {
            'category_id': cat_id,
            'filter_variable': 'frequency',
            'filter_value': 'Daily',
            'order_by': 'observation_end',
            'sort_order': 'desc',
            'limit': 1000,
            'offset': offset,
        })
        batch = data.get('seriess', [])
        for s in batch:
            series.append({
                'id': s['id'],
                'title': s['title'],
                'observation_end': s['observation_end'],
                'frequency': s.get('frequency_short', ''),
                'units': s.get('units', ''),
                'popularity': s.get('popularity', 0),
            })
        if len(batch) < 1000:
            break
        offset += 1000
    return series


def walk_category_tree(root_id, all_series, visited_cats, depth=0):
    """Recursively walk category tree, collecting all daily series."""
    if root_id in visited_cats or root_id in SKIP_CATEGORY_IDS:
        return
    visited_cats.add(root_id)

    children = get_child_categories(root_id)

    if children:
        # Branch node — recurse into children
        for child in children:
            walk_category_tree(child['id'], all_series, visited_cats, depth + 1)
    else:
        # Leaf node — fetch series
        series = get_series_in_category(root_id)
        new = 0
        for s in series:
            if s['id'] not in all_series:
                all_series[s['id']] = s
                new += 1
        if new:
            indent = '  ' * depth
            print(f'{indent}Cat {root_id}: +{new} new series (total {len(all_series)})')


def run_full_tree_mode():
    print('=' * 70)
    print('FRED Full Category Tree Discovery')
    print('Collecting all daily-frequency series via category traversal')
    print(f'Throttle: {MIN_REQUEST_INTERVAL_S:.1f}s between requests; retries={MAX_RETRIES}')
    print('=' * 70)

    # Load existing series as starting point
    existing_file = PROJECT_ROOT / 'all_daily_series.json'
    try:
        with open(existing_file) as f:
            existing = json.load(f)
        all_series = {s['id']: s for s in existing}
        print(f'Loaded {len(all_series)} existing series from {existing_file}')
    except FileNotFoundError:
        all_series = {}
        print('No existing series file found — starting fresh')

    print(f'\nWalking FRED category tree from root (category 0)...')
    print('This will take a while.\n')

    visited_cats = set()
    walk_category_tree(0, all_series, visited_cats)

    series_list = list(all_series.values())
    series_list.sort(key=lambda s: s.get('popularity', 0), reverse=True)

    output_file = TOOLS_DIR / 'all_daily_series_full.json'
    with open(output_file, 'w') as f:
        json.dump(series_list, f, indent=2)

    print(f'\n{"=" * 70}')
    print(f'Done. Visited {len(visited_cats)} categories.')
    print(f'Total daily series: {len(series_list)}')
    print(f'Saved to {output_file}')
    print('\nNext: run compute_correlations.py')


def run_fast_mode():
    print('=' * 70)
    print('FRED Fast Daily Discovery (series/updates)')
    print(f'Throttle: {MIN_REQUEST_INTERVAL_S:.1f}s between requests; retries={MAX_RETRIES}')
    print('=' * 70)

    all_series = discover_recent_daily_series()
    series_list = list(all_series.values())
    series_list.sort(key=lambda s: s.get('popularity', 0), reverse=True)

    if not series_list:
        raise RuntimeError('Fast discovery produced zero qualifying series; refusing to overwrite outputs')

    out_full = TOOLS_DIR / 'all_daily_series_full.json'
    out_base = PROJECT_ROOT / 'all_daily_series.json'
    with open(out_full, 'w') as f:
        json.dump(series_list, f, indent=2)
    with open(out_base, 'w') as f:
        json.dump(series_list, f, indent=2)

    print(f'\n{"=" * 70}')
    print(f'Total qualifying recent daily series: {len(series_list)}')
    print(f'Saved to {out_full}')
    print(f'Saved to {out_base}')
    print('\nNext: run compute_correlations.py')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Discover FRED daily series')
    parser.add_argument(
        '--mode',
        choices=['fast', 'full-tree'],
        default='fast',
        help='Discovery mode (default: fast)',
    )
    args = parser.parse_args()

    if args.mode == 'full-tree':
        run_full_tree_mode()
    else:
        try:
            run_fast_mode()
        except Exception as err:
            print(f'[WARN] Fast discovery failed: {err}')
            print('[WARN] Falling back to full-tree mode for completeness')
            run_full_tree_mode()
