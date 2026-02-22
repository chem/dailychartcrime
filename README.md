# Daily Chart Crime

A parody of institutional financial research. Every weekday, an algorithm finds the
FRED economic series most correlated with the S&P 500 over the past ~6 weeks and
presents it with the deadpan gravity of a Goldman Sachs morning note.

Runs on GitHub Actions + Cloudflare Pages. No server, no database, no subscription required.

---

## Architecture

```
GitHub Actions (cron: 12:30 PM ET weekdays)
  └─ node scripts/generate.mjs
       ├─ Fetch SP500 + all 351 curated FRED series (incremental, default 1000ms FRED throttle)
       ├─ Fetch 3 external series (USGS, Wikipedia)
       ├─ Compute Pearson r for each series vs S&P 500
       ├─ Pick highest-correlation series not shown in past 7 days
       ├─ Write observation cache → data/cache/observations/
       ├─ Render static HTML shell → data/site/index.html
       └─ Write frontend assets/data → data/site/app.js + data/site/chart-data.json

Cloudflare Pages
  └─ Serves data/site/index.html at the edge
```

Correlation window: prior month's options expiration (3rd Friday) → today
→ typically 4–6 weeks of daily data, ~20–25 S&P 500 trading days

---

## First-Time Setup

### Prerequisites

- Node.js >= 20
- Python >= 3.10 + pip (maintenance scripts only)
- A FRED API key: https://fred.stlouisfed.org/docs/api/api_key.html
- A Cloudflare account (free tier)
- A GitHub account

### Step 1: Seed the local cache

Fetches all series from FRED and generates the initial `data/site/index.html`.
The first run usually takes ~6-10 minutes with default settings
(`FRED_MIN_REQUEST_INTERVAL_MS=1000`, plus any retry/backoff delay).

```bash
FRED_API_KEY=<your_key> node scripts/generate.mjs
```

### Step 2: Create Cloudflare Pages project

```bash
npx wrangler pages project create daily-chart-crime
```

### Step 3: Push to GitHub

```bash
git remote add origin https://github.com/<you>/dailychartcrime.git
git push -u origin main
```

### Step 4: Set GitHub Actions secrets

In your repo: Settings → Secrets and variables → Actions → New repository secret

| Secret | Value |
|--------|-------|
| `FRED_API_KEY` | Your FRED API key |
| `CLOUDFLARE_API_TOKEN` | Cloudflare API token (Pages:Edit permission) |
| `CLOUDFLARE_ACCOUNT_ID` | Found in Cloudflare dashboard sidebar |

### Step 5: Deploy

Trigger the workflow manually from Actions → Daily Chart Crime Update → Run workflow,
or wait for the next scheduled run at 12:30 PM ET on a weekday.

---

## Local Development

```bash
# Run the full pipeline locally (generates data/site/index.html)
FRED_API_KEY=<your_key> node scripts/generate.mjs

# Or using npm script (reads key from .dev.vars)
npm run dev
```

Copy `.dev.vars.example` to `.dev.vars` and fill in your key:
```bash
cp .dev.vars.example .dev.vars
# then edit .dev.vars
```

---

## Python Maintenance Tooling

Python is only needed for the maintenance scripts under `tools/`. Dependencies are
`numpy` and `requests` (see `requirements.txt`).

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

| Script | Purpose |
|--------|---------|
| `tools/discover_all_fred.py` | Discover all daily FRED series (fast mode uses recent-updates endpoint; full mode walks the category tree) |
| `tools/compute_correlations.py` | Fetch each discovered series and compute correlation with S&P 500; outputs `strict_curated.json` and `strict_correlations.json` |
| `tools/curate_series.py` | Optional extra filtering — deduplicates boring series, applies category exclusions |
| `tools/explore_fred.py` | Ad-hoc FRED search and analysis; not part of any required workflow |

---

## Project Structure

```
.github/workflows/
  daily-update.yml      — GitHub Actions cron (12:30 PM ET weekdays, DST-aware)

scripts/
  generate.mjs          — Daily pipeline: fetch → correlate → render HTML
  file-cache.mjs        — File-based observation cache (data/cache/)
  dev.mjs               — Local dev wrapper: loads .dev.vars then runs generate.mjs

src/
  correlation.js        — Pearson r, date alignment, series picker, options expiration
  fred.js               — FRED API client (throttled, retrying)
  external.js           — External data source fetchers (USGS, Wikipedia)
  app.js                — Client-side chart rendering + UI behavior
  html.js               — Static HTML shell (Chart.js, parody styling)
  curated.js            — 351 curated FRED series (id + title); generated from strict_curated.json

data/
  cache/
    observations/       — Per-series JSON cache (incremental FRED fetches)
    chart_YYYY-MM-DD.json — Daily chart payload (kept 30 days)
    usage_history.json  — 30-day series rotation tracker
    excluded_category_32255.json — Cached list of stock-index series to exclude (refreshed daily)
  site/
    index.html          — Generated static site (deployed to Cloudflare Pages)
    app.js              — Copied from src/app.js during generation
    chart-data.json     — Current chart payload consumed by app.js
    _headers            — Cloudflare Pages security headers

tools/  (maintenance scripts — not part of daily pipeline)
  discover_all_fred.py     — Discover daily FRED series (fast updates mode + full-tree fallback)
  compute_correlations.py  — Compute correlations vs S&P 500; outputs strict_curated.json
  curate_series.py         — Optional extra filtering over correlation results
  explore_fred.py          — Ad-hoc FRED search and analysis

strict_curated.json      — Source data for src/curated.js (committed)
strict_correlations.json — Full correlation results with r-values (gitignored)
.dev.vars.example        — Template for local dev credentials
```

---

## External Data Sources

In addition to FRED, three external daily series are included:

| ID | Source | Description |
|----|--------|-------------|
| `ext:usgs-earthquakes` | USGS | Global earthquake count (M2.5+) |
| `ext:wiki-recession` | Wikimedia | Wikipedia pageviews: "Recession" |
| `ext:usgs-mississippi` | USGS | Mississippi River level at St. Louis |

---

## Updating the Curated Series List

The current repo snapshot includes 351 FRED series in `src/curated.js`.
To refresh:

```bash
# 1. Re-discover all FRED daily series (optional, slow)
FRED_API_KEY=<key> python3 tools/discover_all_fred.py

# 2. Recompute correlations
FRED_API_KEY=<key> python3 tools/compute_correlations.py
# outputs: strict_curated.json, strict_correlations.json

# 3. Regenerate src/curated.js
python3 -c "
import json
with open('strict_curated.json') as f:
    series = json.load(f)
lines = ['export const CURATED_SERIES = [']
for s in series:
    t = s['title'].replace('\"', '\\\\\"')
    lines.append(f'  {{ id: \"{s[\"id\"]}\", title: \"{t}\" }},')
lines.append('];')
with open('src/curated.js', 'w') as f:
    f.write('\n'.join(lines) + '\n')
print(f'Wrote {len(series)} series to src/curated.js')
"
```
