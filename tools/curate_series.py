"""
Curate the best series for Daily Chart Crime rotation.
Filters for reliability, deduplicates boring series, prioritizes funny ones.
"""
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
INPUT_CANDIDATES = [
    PROJECT_ROOT / "strict_correlations.json",
    PROJECT_ROOT / "all_correlations.json",  # legacy name
]
OUTPUT_FILE = PROJECT_ROOT / "strict_curated.json"

input_file = next((p for p in INPUT_CANDIDATES if p.exists()), None)
if not input_file:
    print("Error: No correlation input found.")
    print("Expected one of:")
    for p in INPUT_CANDIDATES:
        print(f"  - {p}")
    print("Run: FRED_API_KEY=<key> python3 compute_correlations.py")
    raise SystemExit(1)

with open(input_file) as f:
    all_corr = json.load(f)

def aligned_count(row):
    # Support both current (n_dates) and legacy (n_aligned) schemas.
    return row.get("n_dates", row.get("n_aligned", 0))

print(f"Loaded: {input_file.name}")
print(f"Total correlations: {len(all_corr)}")

# 1. Filter for reliability: at least 20 aligned dates
reliable = [c for c in all_corr if aligned_count(c) >= 20]
print(f"With >= 20 aligned dates: {len(reliable)}")

# 2. Remove duplicate series (same title = same data under different ID)
seen_titles = set()
deduped = []
for c in reliable:
    if c["title"] not in seen_titles:
        seen_titles.add(c["title"])
        deduped.append(c)
print(f"After deduplication: {len(deduped)}")

# 3. Categorize series for smart rotation
# We want a mix of: high-corr financial, funny categories, interesting indicators

def is_funny(c):
    """Series that are inherently funny when correlated with S&P 500."""
    t = c["title"].lower()
    funny_keywords = [
        "indeed", "job posting", "beauty", "wellness", "therapy",
        "nursing", "pharmacy", "coffee", "bitcoin", "crypto",
        "loading", "stocking", "installation", "maintenance",
        "scientific research", "real estate", "software development",
        "marketing", "insurance", "media", "human resources",
        "oregon", "west virginia", "tennessee", "maine",
        "australia", "germany", "france", "united kingdom", "canada",
    ]
    return any(kw in t for kw in funny_keywords)

def is_interesting_financial(c):
    """Financially meaningful but not boring duplicates."""
    t = c["title"]
    interesting = [
        "VIX",  # THE VIX, not variants
        "High Yield Index Option-Adjusted Spread",  # THE HY spread
        "Policy Rate Uncertainty",
        "Breakeven Inflation",
        "10-Year Treasury",
        "Dollar Index",
        "Gold",
        "Nikkei",
        "SOFR",
        "Federal Funds",
        "Mortgage",
        "Swap",
    ]
    return any(kw in t for kw in interesting)

# Classify
funny_series = []
financial_series = []
other_series = []

for c in deduped:
    if is_funny(c) and c["abs_r"] > 0.15:
        funny_series.append(c)
    elif is_interesting_financial(c) and c["abs_r"] > 0.1:
        financial_series.append(c)
    elif c["abs_r"] > 0.3:  # High correlation from any category
        other_series.append(c)

# Sort each by abs_r descending
funny_series.sort(key=lambda x: x["abs_r"], reverse=True)
financial_series.sort(key=lambda x: x["abs_r"], reverse=True)
other_series.sort(key=lambda x: x["abs_r"], reverse=True)

print(f"\nFunny series (|r| > 0.15): {len(funny_series)}")
print(f"Interesting financial (|r| > 0.1): {len(financial_series)}")
print(f"Other high-corr (|r| > 0.3): {len(other_series)}")

# 4. Build curated rotation list
# Interleave: funny, financial, funny, financial, other...
# This ensures every day has something interesting
curated = []
fi, ff, fo = 0, 0, 0

# First: add ALL funny series (these are the gold)
for c in funny_series:
    curated.append({**c, "category": "funny"})

# Then: add top financial series (limit duplicates)
baml_count = 0
vix_count = 0
for c in financial_series:
    t = c["title"]
    if "ICE BofA" in t:
        baml_count += 1
        if baml_count > 5:
            continue
    if "CBOE" in t or "VIX" in t:
        vix_count += 1
        if vix_count > 3:
            continue
    curated.append({**c, "category": "financial"})

# Then: add other high-correlation series (limit BAML/VIX further)
for c in other_series:
    t = c["title"]
    if "ICE BofA" in t or "CBOE" in t:
        continue  # Already represented
    curated.append({**c, "category": "other"})

# Sort final list by abs_r for the rotation
curated.sort(key=lambda x: x["abs_r"], reverse=True)

print(f"\nFinal curated list: {len(curated)} series")
print(f"\n{'Rank':>4} {'Cat':>10} {'r':>8} {'N':>3} {'ID':>24}  {'Title'}")
print("-" * 120)
for i, c in enumerate(curated):
    print(f"{i+1:4d} {c['category']:>10} {c['r']:+.4f} {aligned_count(c):3d} {c['id']:>24s}  {c['title'][:65]}")

# 5. Save curated list (just id + title for the worker)
curated_for_worker = [{"id": c["id"], "title": c["title"]} for c in curated]
with open(OUTPUT_FILE, "w") as f:
    json.dump(curated_for_worker, f, indent=2)

print(f"\nSaved {len(curated_for_worker)} curated series to {OUTPUT_FILE.name}")
