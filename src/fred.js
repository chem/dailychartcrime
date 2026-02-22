const BASE_URL = 'https://api.stlouisfed.org/fred';
const DEBUG = process.env.DEBUG === '1';
const MIN_REQUEST_INTERVAL_MS = Number(process.env.FRED_MIN_REQUEST_INTERVAL_MS ?? '1000');
const MAX_RETRIES = Number(process.env.FRED_MAX_RETRIES ?? '6');
const MAX_BACKOFF_MS = Number(process.env.FRED_MAX_BACKOFF_MS ?? '60000');
const REQUEST_TIMEOUT_MS = Number(process.env.FRED_REQUEST_TIMEOUT_MS ?? '30000');
let lastRequestAtMs = 0;
console.log(
  `[FRED] throttle=${MIN_REQUEST_INTERVAL_MS}ms retries=${MAX_RETRIES} maxBackoff=${MAX_BACKOFF_MS}ms timeout=${REQUEST_TIMEOUT_MS}ms`
);

class FredPermanentError extends Error {
  constructor(message) {
    super(message);
    this.name = 'FredPermanentError';
  }
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function throttleFredRequests() {
  const now = Date.now();
  const wait = MIN_REQUEST_INTERVAL_MS - (now - lastRequestAtMs);
  if (wait > 0) {
    await delay(wait);
  }
  lastRequestAtMs = Date.now();
}

function getRetryAfterMs(resp) {
  const retryAfter = resp.headers.get('retry-after');
  if (!retryAfter) return null;
  const asNumber = Number(retryAfter);
  if (!isNaN(asNumber)) return Math.max(0, asNumber * 1000);
  const asDate = Date.parse(retryAfter);
  if (!isNaN(asDate)) return Math.max(0, asDate - Date.now());
  return null;
}

async function fetchWithTimeout(url, timeoutMs) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { signal: controller.signal });
  } catch (err) {
    if (err?.name === 'AbortError') {
      throw new Error(`request timeout after ${timeoutMs}ms`);
    }
    throw err;
  } finally {
    clearTimeout(timeout);
  }
}

async function fredGet(endpoint, params, apiKey, retries = MAX_RETRIES) {
  const url = new URL(`${BASE_URL}/${endpoint}`);
  url.searchParams.set('api_key', apiKey);
  url.searchParams.set('file_type', 'json');
  for (const [key, value] of Object.entries(params)) {
    url.searchParams.set(key, String(value));
  }

  if (DEBUG) console.log(`[FRED] GET ${endpoint} params=${JSON.stringify(params)}`);
  for (let attempt = 0; attempt < retries; attempt++) {
    await throttleFredRequests();
    try {
      const resp = await fetchWithTimeout(url.toString(), REQUEST_TIMEOUT_MS);
      if (resp.status === 429) {
        const retryAfterMs = getRetryAfterMs(resp);
        const expBackoffMs = MIN_REQUEST_INTERVAL_MS * (2 ** (attempt + 1));
        const waitMs = Math.min(MAX_BACKOFF_MS, Math.max(5000, retryAfterMs ?? expBackoffMs));
        console.warn(`[FRED] 429 ${endpoint}; retry ${attempt + 1}/${retries} in ${Math.round(waitMs / 1000)}s`);
        await delay(waitMs);
        continue;
      }
      if (resp.status >= 500 && resp.status < 600) {
        const waitMs = Math.min(MAX_BACKOFF_MS, 2000 * (attempt + 1));
        console.warn(`[FRED] ${resp.status} ${endpoint}; retry ${attempt + 1}/${retries} in ${Math.round(waitMs / 1000)}s`);
        await delay(waitMs);
        continue;
      }
      if (!resp.ok) {
        const text = await resp.text();
        if (resp.status === 408) {
          const waitMs = Math.min(MAX_BACKOFF_MS, 2000 * (attempt + 1));
          console.warn(`[FRED] 408 ${endpoint}; retry ${attempt + 1}/${retries} in ${Math.round(waitMs / 1000)}s`);
          await delay(waitMs);
          continue;
        }
        throw new FredPermanentError(`FRED API ${endpoint} returned ${resp.status}: ${text}`);
      }

      const data = await resp.json();
      // FRED sometimes returns HTTP 200 with an error payload.
      if (data.error_code) {
        const shouldRetry = String(data.error_code) === '429';
        if (shouldRetry) {
          const waitMs = Math.min(MAX_BACKOFF_MS, Math.max(5000, MIN_REQUEST_INTERVAL_MS * (2 ** (attempt + 1))));
          console.warn(`[FRED] payload 429 ${endpoint}; retry ${attempt + 1}/${retries} in ${Math.round(waitMs / 1000)}s`);
          await delay(waitMs);
          continue;
        }
        throw new FredPermanentError(`FRED API ${endpoint} error ${data.error_code}: ${data.error_message}`);
      }
      return data;
    } catch (err) {
      if (err instanceof FredPermanentError) {
        throw err;
      }
      const isLastAttempt = attempt === retries - 1;
      if (isLastAttempt) throw err;
      const waitMs = Math.min(MAX_BACKOFF_MS, 2000 * (attempt + 1));
      console.warn(`[FRED] network/error ${endpoint}; retry ${attempt + 1}/${retries} in ${Math.round(waitMs / 1000)}s (${err.message})`);
      await delay(waitMs);
    }
  }
  throw new Error(`FRED API ${endpoint} failed after ${retries} retries`);
}

/**
 * Fetch observations for a series, optionally starting from a date.
 * Returns array of {date: "YYYY-MM-DD", value: number}.
 */
export async function fetchObservations(apiKey, seriesId, startDate = null) {
  const params = { series_id: seriesId, limit: 100000, sort_order: 'asc' };
  if (startDate) {
    params.observation_start = startDate;
  }

  const data = await fredGet('series/observations', params, apiKey);
  const observations = (data.observations || [])
    .filter(o => o.value !== '.')  // FRED uses '.' for missing values
    .map(o => ({ date: o.date, value: parseFloat(o.value) }));

  console.log(`[FRED] ${seriesId}: ${observations.length} observations` +
    (startDate ? ` from ${startDate}` : '') +
    (observations.length > 0 ? ` (${observations[0].date} to ${observations[observations.length - 1].date})` : ''));

  return observations;
}

/**
 * Fetch all series IDs in a FRED category.
 * Used to enforce explicit category exclusions in daily generation.
 * @returns {Promise<Set<string>>}
 */
export async function fetchCategorySeriesIds(apiKey, categoryId) {
  const ids = new Set();
  let offset = 0;

  while (true) {
    const data = await fredGet('category/series', {
      category_id: categoryId,
      limit: 1000,
      offset,
      sort_order: 'asc',
    }, apiKey);

    const series = data.seriess || [];
    for (const s of series) {
      if (s.id) ids.add(s.id);
    }

    if (series.length < 1000) break;
    offset += 1000;
  }

  console.log(`[FRED] category ${categoryId}: ${ids.size} excluded ids`);
  return ids;
}
