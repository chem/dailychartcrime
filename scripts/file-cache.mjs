/**
 * File-based cache layer for the GitHub Actions pipeline.
 *
 * Persists observation data, chart outputs, and series usage history as JSON
 * files under the cache directory. Designed to be committed to the repo so
 * that incremental FRED fetches work correctly across GitHub Actions runs.
 *
 * Key mapping:
 *   obs:{seriesId}        → {cachePath}/observations/obs_{seriesId}.json
 *   chart:{YYYY-MM-DD}    → {cachePath}/chart_{YYYY-MM-DD}.json
 *   usage_history         → {cachePath}/usage_history.json
 *
 * Set DEBUG=1 to enable per-series cache hit/miss logging.
 */

import { readFile, writeFile, mkdir, readdir, unlink } from 'node:fs/promises';
import { join } from 'node:path';

const DEBUG = process.env.DEBUG === '1';

function obsFile(cachePath, seriesId) {
  // Replace characters invalid in filenames (colons from ext: prefixes)
  const safe = seriesId.replace(/:/g, '_');
  return join(cachePath, 'observations', `obs_${safe}.json`);
}

function chartFile(cachePath, dateStr) {
  return join(cachePath, `chart_${dateStr}.json`);
}

function usageFile(cachePath) {
  return join(cachePath, 'usage_history.json');
}

async function readJson(filePath) {
  try {
    const raw = await readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch (err) {
    if (err.code !== 'ENOENT') {
      // Unexpected error (e.g. corrupt JSON, permission denied) — log as warning
      console.warn(`[CACHE] Warning: could not read ${filePath}: ${err.message}`);
    }
    return null;
  }
}

async function writeJson(filePath, data) {
  await writeFile(filePath, JSON.stringify(data), 'utf8');
}

/**
 * Get cached observations for a series.
 * @returns {{ lastFetched: string, data: {date: string, value: number}[] } | null}
 */
export async function getCachedObservations(cachePath, seriesId) {
  const raw = await readJson(obsFile(cachePath, seriesId));
  if (!raw) {
    if (DEBUG) console.log(`[CACHE] Miss: ${seriesId}`);
    return null;
  }
  if (DEBUG) console.log(`[CACHE] Hit: ${seriesId} (${raw.data.length} obs, last fetched ${raw.lastFetched})`);
  return raw;
}

/**
 * Merge new observations into the cache for a series and write to disk.
 * Returns the merged observation array.
 * @returns {{date: string, value: number}[]} merged observations sorted ascending
 */
export async function appendObservations(cachePath, seriesId, existingData, newObs, lastFetchedDate) {
  // Merge: existing + new, deduplicate by date, sort ascending
  const byDate = new Map();
  for (const o of existingData) byDate.set(o.date, o.value);
  for (const o of newObs) byDate.set(o.date, o.value);

  const merged = Array.from(byDate.entries())
    .map(([date, value]) => ({ date, value }))
    .sort((a, b) => a.date.localeCompare(b.date));

  await mkdir(join(cachePath, 'observations'), { recursive: true });
  await writeJson(obsFile(cachePath, seriesId), {
    lastFetched: lastFetchedDate,
    data: merged,
  });

  if (DEBUG) console.log(`[CACHE] Updated ${seriesId}: ${merged.length} total observations`);
  return merged;
}

/**
 * Store chart data for a specific date.
 */
export async function setChartData(cachePath, dateStr, data) {
  await writeJson(chartFile(cachePath, dateStr), data);
  console.log(`[CACHE] Stored chart data for ${dateStr}`);
}

/**
 * Prune chart_YYYY-MM-DD.json files older than N days.
 * @returns {number} number of chart files deleted
 */
export async function pruneOldChartData(cachePath, days = 30) {
  let entries;
  try {
    entries = await readdir(cachePath, { withFileTypes: true });
  } catch (err) {
    if (err.code !== 'ENOENT') {
      console.warn(`[CACHE] Warning: could not list ${cachePath}: ${err.message}`);
    }
    return 0;
  }

  const cutoff = new Date();
  cutoff.setUTCHours(0, 0, 0, 0);
  cutoff.setUTCDate(cutoff.getUTCDate() - days);
  const cutoffStr = cutoff.toISOString().slice(0, 10);

  let pruned = 0;
  for (const entry of entries) {
    if (!entry.isFile()) continue;
    const match = /^chart_(\d{4}-\d{2}-\d{2})\.json$/.exec(entry.name);
    if (!match) continue;

    const dateStr = match[1];
    if (dateStr >= cutoffStr) continue;

    const target = join(cachePath, entry.name);
    try {
      await unlink(target);
      pruned++;
    } catch (err) {
      console.warn(`[CACHE] Warning: could not delete ${target}: ${err.message}`);
    }
  }

  if (pruned > 0 || DEBUG) {
    console.log(
      `[CACHE] Pruned ${pruned} chart file${pruned === 1 ? '' : 's'} older than ${days} days (cutoff ${cutoffStr})`
    );
  }

  return pruned;
}

/**
 * Get series IDs that were shown within the last N days.
 * @returns {string[]}
 */
export async function getRecentlyUsedIds(cachePath, days = 7) {
  const history = await readJson(usageFile(cachePath));
  if (!history) return [];

  const cutoff = new Date();
  cutoff.setUTCDate(cutoff.getUTCDate() - days);
  const cutoffStr = cutoff.toISOString().slice(0, 10);

  return Array.from(new Set(history
    .filter(entry => entry.date >= cutoffStr)
    .map(entry => entry.id)));
}

/**
 * Record that a series was shown on a specific date.
 * Multiple series can be recorded per date (e.g. seeding runs or reruns).
 * Keeps 30 days of history; older entries are pruned automatically.
 */
export async function recordSeriesUsed(cachePath, dateStr, seriesId) {
  const history = (await readJson(usageFile(cachePath))) || [];
  // Use composite key so multiple series per day are preserved (idempotent per {date,id} pair).
  const byKey = new Map();
  for (const entry of history) {
    if (entry?.date && entry?.id) {
      byKey.set(`${entry.date}:${entry.id}`, entry);
    }
  }
  byKey.set(`${dateStr}:${seriesId}`, { date: dateStr, id: seriesId });

  const cutoff = new Date();
  cutoff.setUTCDate(cutoff.getUTCDate() - 30);
  const cutoffStr = cutoff.toISOString().slice(0, 10);
  const trimmed = Array.from(byKey.values())
    .filter(entry => entry.date >= cutoffStr)
    .sort((a, b) => a.date.localeCompare(b.date) || a.id.localeCompare(b.id));

  await writeJson(usageFile(cachePath), trimmed);
  console.log(`[CACHE] Recorded usage: ${seriesId} on ${dateStr} (${trimmed.length} history entries)`);
}
