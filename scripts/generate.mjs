/**
 * Daily Chart Crime — main pipeline script.
 *
 * Fetches today's S&P 500 and all curated FRED/external series (incrementally,
 * from cache), computes Pearson correlations, picks the highest-correlation
 * series not shown in the past 7 days, and renders a static HTML page.
 *
 * Outputs:
 *   data/cache/observations/  — per-series observation JSON (committed to repo)
 *   data/cache/chart_YYYY-MM-DD.json — raw chart payload
 *   data/site/index.html      — generated static site for Cloudflare Pages
 *
 * Usage:
 *   FRED_API_KEY=<key> node scripts/generate.mjs
 *   DEBUG=1 FRED_API_KEY=<key> node scripts/generate.mjs  # verbose cache logging
 */

import { readFile, writeFile, mkdir, copyFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { join, dirname } from 'node:path';

import { fetchObservations, fetchCategorySeriesIds } from '../src/fred.js';
import {
  getPriorMonthlyExpiration, alignSeries, pearsonCorrelation,
  pickTodaysSeries, toDateString,
} from '../src/correlation.js';
import {
  getCachedObservations, appendObservations,
  setChartData, pruneOldChartData,
  getRecentlyUsedIds, recordSeriesUsed,
} from './file-cache.mjs';
import { CURATED_SERIES } from '../src/curated.js';
import { EXTERNAL_SERIES, fetchExternalObservations, getSeriesSource } from '../src/external.js';
import { renderPage } from '../src/html.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = join(__dirname, '..');
const CACHE_PATH = join(PROJECT_ROOT, 'data', 'cache');
const SITE_PATH = join(PROJECT_ROOT, 'data', 'site');
const APP_JS_SOURCE = join(PROJECT_ROOT, 'src', 'app.js');

const SP500_ID = 'SP500';
const MIN_OVERLAP_RATIO = 0.95;
const EXCLUDE_CATEGORY_ID = 32255; // FRED category: Stock Market Indexes
const MIN_SERIES_SUCCESS_RATIO = Number(process.env.MIN_SERIES_SUCCESS_RATIO ?? '0.9');

/**
 * Fill missing target dates using last-observation-carried-forward.
 * Returns the augmented list, or null if a missing date precedes all data.
 */
function forwardFill(obs, missingDates) {
  const obsSorted = [...obs].sort((a, b) => a.date.localeCompare(b.date));
  const sortedMissing = [...missingDates].sort();
  const extra = [];
  let ptr = 0;
  let priorValue = null;
  for (const d of sortedMissing) {
    // Advance pointer through obsSorted up to and including date d.
    while (ptr < obsSorted.length && obsSorted[ptr].date <= d) {
      priorValue = obsSorted[ptr].value;
      ptr++;
    }
    if (priorValue === null) return null;
    extra.push({ date: d, value: priorValue });
  }
  return extra.length > 0 ? [...obsSorted, ...extra] : obsSorted;
}

/**
 * Ensure a series has acceptable overlap with S&P 500 dates, with optional LOCF.
 * @returns {{
 *   ok: boolean,
 *   data?: {date: string, value: number}[],
 *   missingCount: number,
 *   reason?: 'too_many_missing' | 'cannot_forward_fill'
 * }}
 */
function prepareSeriesForCorrelation(obsData, sp500DateSet) {
  const obsDates = new Set(obsData.map(o => o.date));
  const missingDates = [...sp500DateSet].filter(d => !obsDates.has(d));
  const maxMissing = Math.floor((1 - MIN_OVERLAP_RATIO) * sp500DateSet.size);

  if (missingDates.length > maxMissing) {
    return { ok: false, missingCount: missingDates.length, reason: 'too_many_missing' };
  }

  if (missingDates.length === 0) {
    return { ok: true, data: obsData, missingCount: 0 };
  }

  const filledData = forwardFill(obsData, missingDates);
  if (!filledData) {
    return { ok: false, missingCount: missingDates.length, reason: 'cannot_forward_fill' };
  }

  return { ok: true, data: filledData, missingCount: missingDates.length };
}

/**
 * Fetch and cache FRED observations for a series.
 * Only applies the rate-limit delay when an actual API call is needed.
 * Incrementally fetches only new dates since the last cache update.
 */
async function fetchFredWithCache(apiKey, seriesId, windowStart, today) {
  const cached = await getCachedObservations(CACHE_PATH, seriesId);
  const earliestCached = cached?.data?.[0]?.date || null;
  const needsBackfill = Boolean(earliestCached && earliestCached > windowStart);

  if (cached && cached.lastFetched === today && !needsBackfill) {
    return cached.data.filter(o => o.date >= windowStart);
  }

  let fetchStart;
  let existingData = [];

  if (cached) {
    existingData = cached.data;
    if (needsBackfill) {
      // Window moved earlier than current cache coverage: backfill from window start.
      fetchStart = windowStart;
    } else {
      const lastDate = new Date(cached.lastFetched + 'T00:00:00Z');
      lastDate.setUTCDate(lastDate.getUTCDate() + 1);
      fetchStart = toDateString(lastDate);
    }
  } else {
    fetchStart = windowStart;
  }

  const newObs = await fetchObservations(apiKey, seriesId, fetchStart);
  const merged = await appendObservations(CACHE_PATH, seriesId, existingData, newObs, today);
  return merged.filter(o => o.date >= windowStart);
}

/**
 * Fetch and cache external (non-FRED) observations for a series.
 * Re-fetches the full window each day (datasets are small).
 */
async function fetchExternalWithCache(seriesId, windowStart, today) {
  const cached = await getCachedObservations(CACHE_PATH, seriesId);
  if (cached && cached.lastFetched === today) {
    return cached.data.filter(o => o.date >= windowStart);
  }

  const obs = await fetchExternalObservations(seriesId, windowStart, today);
  await appendObservations(CACHE_PATH, seriesId, [], obs, today);
  return obs.filter(o => o.date >= windowStart);
}

/**
 * Run the full correlation analysis for all curated + external series.
 */
async function computeAllCorrelations(apiKey, sp500Data, sp500DateSet, windowStart, today, excludedIds) {
  const corrResults = [];
  let skippedMissing = 0;
  let skippedVariance = 0;
  let fredConsidered = 0;
  let fredErrors = 0;
  let externalConsidered = 0;
  let externalErrors = 0;

  // FRED series
  for (const series of CURATED_SERIES) {
    if (excludedIds.has(series.id)) {
      continue;
    }
    fredConsidered++;
    try {
      const obsData = await fetchFredWithCache(apiKey, series.id, windowStart, today);
      const prepared = prepareSeriesForCorrelation(obsData, sp500DateSet);
      if (!prepared.ok) {
        skippedMissing++;
        continue;
      }

      const aligned = alignSeries(sp500Data, prepared.data);
      const r = pearsonCorrelation(aligned.valuesA, aligned.valuesB);
      if (!isNaN(r)) {
        corrResults.push({ id: series.id, title: series.title, correlation: r });
      } else {
        skippedVariance++;
      }
    } catch (err) {
      fredErrors++;
      console.error(`[GEN] Error processing ${series.id}: ${err.message}`);
    }
  }

  const fredCount = corrResults.length;

  // External series
  for (const series of EXTERNAL_SERIES) {
    externalConsidered++;
    try {
      const obsData = await fetchExternalWithCache(series.id, windowStart, today);
      const prepared = prepareSeriesForCorrelation(obsData, sp500DateSet);
      if (!prepared.ok) {
        if (prepared.reason === 'too_many_missing') {
          console.log(`[GEN] ${series.id}: missing ${prepared.missingCount} S&P 500 dates — skipping`);
        } else {
          console.log(`[GEN] ${series.id}: LOCF failed (missing date before all data) — skipping`);
        }
        continue;
      }

      const aligned = alignSeries(sp500Data, prepared.data);
      const r = pearsonCorrelation(aligned.valuesA, aligned.valuesB);
      if (!isNaN(r)) {
        corrResults.push({ id: series.id, title: series.title, correlation: r });
        console.log(`[GEN] External ${series.id}: r=${r.toFixed(4)}`);
      }
    } catch (err) {
      externalErrors++;
      console.error(`[GEN] Error processing external ${series.id}: ${err.message}`);
    }
  }

  const considered = fredConsidered + externalConsidered;
  const errors = fredErrors + externalErrors;
  const successRatio = considered > 0 ? (considered - errors) / considered : 0;

  console.log(`[GEN] Correlations: ${corrResults.length} total (${fredCount} FRED, ${corrResults.length - fredCount} external), ${skippedMissing} skipped (missing dates), ${skippedVariance} skipped (zero variance)`);
  console.log(
    `[GEN] Processing health: ${considered - errors}/${considered} series processed without hard errors (${(successRatio * 100).toFixed(1)}%)`
  );
  return {
    corrResults,
    stats: {
      considered,
      errors,
      successRatio,
      fredConsidered,
      fredErrors,
      externalConsidered,
      externalErrors,
      skippedMissing,
      skippedVariance,
    },
  };
}

async function getExcludedIdsForToday(apiKey, cachePath, categoryId, today) {
  const filePath = join(cachePath, `excluded_category_${categoryId}.json`);
  try {
    const raw = await readFile(filePath, 'utf8');
    const parsed = JSON.parse(raw);
    if (parsed.lastFetched === today && Array.isArray(parsed.ids)) {
      return new Set(parsed.ids);
    }
  } catch {
    // Cache miss/corrupt file: refresh from FRED.
  }

  const ids = await fetchCategorySeriesIds(apiKey, categoryId);
  await writeFile(
    filePath,
    JSON.stringify({ lastFetched: today, ids: Array.from(ids) }),
    'utf8'
  );
  console.log(`[GEN] Cached ${ids.size} excluded IDs for category ${categoryId}`);
  return ids;
}

async function main() {
  const startTime = Date.now();
  const apiKey = process.env.FRED_API_KEY;
  const today = toDateString(new Date());

  if (!apiKey) {
    console.error('[GEN] FRED_API_KEY not set. Set it via environment variable.');
    process.exit(1);
  }
  if (!Number.isFinite(MIN_SERIES_SUCCESS_RATIO) || MIN_SERIES_SUCCESS_RATIO <= 0 || MIN_SERIES_SUCCESS_RATIO > 1) {
    console.error('[GEN] MIN_SERIES_SUCCESS_RATIO must be a number in (0, 1].');
    process.exit(1);
  }

  console.log(`[GEN] Starting daily update for ${today}...`);

  // Ensure output directories exist
  await mkdir(join(CACHE_PATH, 'observations'), { recursive: true });
  await mkdir(SITE_PATH, { recursive: true });

  // 1. Calculate correlation window (prior monthly options expiration to today)
  const windowStart = toDateString(getPriorMonthlyExpiration(new Date()));
  console.log(`[GEN] Correlation window starts: ${windowStart}`);

  // 2. Fetch S&P 500
  const sp500Data = await fetchFredWithCache(apiKey, SP500_ID, windowStart, today);
  if (sp500Data.length === 0) {
    console.error('[GEN] No S&P 500 data available');
    process.exit(1);
  }
  const sp500DateSet = new Set(sp500Data.map(o => o.date));
  console.log(`[GEN] S&P 500: ${sp500Data.length} observations in window`);
  const maxMissing = Math.floor((1 - MIN_OVERLAP_RATIO) * sp500DateSet.size);
  console.log(`[GEN] Overlap rule: >=${Math.round(MIN_OVERLAP_RATIO * 100)}% (${maxMissing} missing S&P dates allowed)`);

  // 2b. Explicitly exclude stock-market-index category IDs from ranking pool.
  const excludedIds = await getExcludedIdsForToday(apiKey, CACHE_PATH, EXCLUDE_CATEGORY_ID, today);

  // 3. Fetch all series and compute correlations fresh every day
  console.log(`[GEN] Computing correlations for all ${CURATED_SERIES.length + EXTERNAL_SERIES.length} series...`);
  const { corrResults, stats } = await computeAllCorrelations(apiKey, sp500Data, sp500DateSet, windowStart, today, excludedIds);

  if (stats.successRatio < MIN_SERIES_SUCCESS_RATIO) {
    console.error(
      `[GEN] Aborting publish: only ${(stats.successRatio * 100).toFixed(1)}% of series processed without hard errors; minimum is ${(MIN_SERIES_SUCCESS_RATIO * 100).toFixed(1)}%`
    );
    process.exit(1);
  }

  if (corrResults.length === 0) {
    console.error('[GEN] No valid correlations found');
    process.exit(1);
  }

  console.log(`[GEN] Total valid correlations: ${corrResults.length}`);

  // 4. Pick today's series (highest correlation not shown in past 7 days)
  const recentlyUsed = await getRecentlyUsedIds(CACHE_PATH, 7);
  const todayPick = pickTodaysSeries(corrResults, recentlyUsed);

  // 5. Fetch aligned data for the selected series to build the chart
  const isExternal = todayPick.id.startsWith('ext:');
  const selectedObs = isExternal
    ? await fetchExternalWithCache(todayPick.id, windowStart, today)
    : await fetchFredWithCache(apiKey, todayPick.id, windowStart, today);
  const preparedSelected = prepareSeriesForCorrelation(selectedObs, sp500DateSet);
  if (!preparedSelected.ok) {
    throw new Error(
      `Selected series ${todayPick.id} failed overlap checks during render (reason=${preparedSelected.reason}, missing=${preparedSelected.missingCount})`
    );
  }
  const aligned = alignSeries(sp500Data, preparedSelected.data);

  const chartData = {
    sp500: {
      title: 'S&P 500',
      dates: aligned.dates,
      values: aligned.valuesA,
    },
    series: {
      id: todayPick.id,
      title: todayPick.title,
      source: getSeriesSource(todayPick.id),
      dates: aligned.dates,
      values: aligned.valuesB,
    },
    correlation: todayPick.correlation,
    inverted: todayPick.correlation < 0,
    windowStart,
    windowEnd: aligned.dates[aligned.dates.length - 1],
    rank: todayPick.rank,
    totalSeries: todayPick.totalSeries,
    generatedAt: new Date().toISOString(),
    generatedDate: new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' }),
  };

  // 6. Write chart data to file cache and record usage
  await setChartData(CACHE_PATH, today, chartData);
  await pruneOldChartData(CACHE_PATH, 30);
  await recordSeriesUsed(CACHE_PATH, today, todayPick.id);

  // 7. Render and write the static HTML page
  await writeFile(join(SITE_PATH, 'chart-data.json'), JSON.stringify(chartData), 'utf8');
  await copyFile(APP_JS_SOURCE, join(SITE_PATH, 'app.js'));
  const html = renderPage();
  const indexPath = join(SITE_PATH, 'index.html');
  await writeFile(indexPath, html, 'utf8');
  console.log(`[GEN] Wrote ${indexPath}, chart-data.json, and app.js`);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(
    `[GEN] Done in ${elapsed}s. Today: ${todayPick.title} (r=${todayPick.correlation.toFixed(4)}, rank #${todayPick.rank}/${todayPick.totalSeries})`
  );
}

main().catch(err => {
  console.error('[GEN] Fatal error:', err);
  process.exit(1);
});
