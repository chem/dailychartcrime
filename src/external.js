/**
 * External data sources for Daily Chart Crime.
 * Fetches publicly available daily datasets from USGS, Wikipedia, etc.
 * All fetch functions return {date: string (YYYY-MM-DD), value: number}[] sorted ascending.
 */

export const EXTERNAL_SERIES = [
  {
    id: 'ext:usgs-earthquakes',
    title: 'Global Daily Earthquake Count (Magnitude 2.5+)',
    source: 'USGS Earthquake Hazards Program',
  },
  {
    id: 'ext:wiki-recession',
    title: 'Wikipedia Pageviews: "Recession"',
    source: 'Wikimedia Analytics',
  },
  {
    id: 'ext:usgs-mississippi',
    title: 'Mississippi River Level at St. Louis, MO (ft)',
    source: 'USGS National Water Information System',
  },
];

const EXTERNAL_FETCH_TIMEOUT_MS = Number(process.env.EXTERNAL_FETCH_TIMEOUT_MS ?? '30000');

async function safeFetch(url, headers = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), EXTERNAL_FETCH_TIMEOUT_MS);
  let resp;
  try {
    resp = await fetch(url, { headers, signal: controller.signal });
  } catch (err) {
    if (err?.name === 'AbortError') {
      throw new Error(`HTTP timeout after ${EXTERNAL_FETCH_TIMEOUT_MS}ms: ${url}`);
    }
    throw err;
  } finally {
    clearTimeout(timeout);
  }
  if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${url}`);
  return resp.json();
}

async function fetchEarthquakes(startDate, endDate) {
  // Include one extra day buffer since USGS endtime is exclusive
  const nextDay = new Date(endDate);
  nextDay.setDate(nextDay.getDate() + 1);
  const end = nextDay.toISOString().slice(0, 10);

  const url =
    `https://earthquake.usgs.gov/fdsnws/event/1/query` +
    `?format=geojson&starttime=${startDate}&endtime=${end}&minmagnitude=2.5`;
  const data = await safeFetch(url);

  const byDay = new Map();
  for (const feature of data.features || []) {
    const date = new Date(feature.properties.time).toISOString().slice(0, 10);
    if (date >= startDate && date <= endDate) {
      byDay.set(date, (byDay.get(date) || 0) + 1);
    }
  }
  return Array.from(byDay.entries())
    .map(([date, value]) => ({ date, value }))
    .sort((a, b) => a.date.localeCompare(b.date));
}

async function fetchWikiViews(startDate, endDate) {
  const start = startDate.replace(/-/g, '');
  const end = endDate.replace(/-/g, '');
  const url =
    `https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article` +
    `/en.wikipedia.org/all-access/user/Recession/daily/${start}/${end}`;
  const data = await safeFetch(url, {
    'User-Agent': 'DailyChartCrime/1.0 (financial satire; open source)',
  });
  if (!data.items) return [];
  return data.items.map(item => {
    const ts = item.timestamp;
    return {
      date: `${ts.slice(0, 4)}-${ts.slice(4, 6)}-${ts.slice(6, 8)}`,
      value: item.views,
    };
  });
}

async function fetchMississippi(startDate, endDate) {
  const url =
    `https://waterservices.usgs.gov/nwis/dv/?format=json` +
    `&sites=07010000&startDT=${startDate}&endDT=${endDate}&parameterCd=00065`;
  const data = await safeFetch(url);
  const ts = data?.value?.timeSeries?.[0];
  if (!ts) return [];
  return (ts.values?.[0]?.value || [])
    .filter(v => v.value !== '-999999' && v.value !== '')
    .map(v => ({
      date: v.dateTime.slice(0, 10),
      value: parseFloat(v.value),
    }))
    .filter(v => !isNaN(v.value));
}

/**
 * Fetch observations for an external series over a date range.
 * @param {string} seriesId - External series ID (must start with 'ext:')
 * @param {string} startDate - YYYY-MM-DD
 * @param {string} endDate - YYYY-MM-DD
 * @returns {{date: string, value: number}[]} sorted ascending
 */
export async function fetchExternalObservations(seriesId, startDate, endDate) {
  switch (seriesId) {
    case 'ext:usgs-earthquakes': return fetchEarthquakes(startDate, endDate);
    case 'ext:wiki-recession': return fetchWikiViews(startDate, endDate);
    case 'ext:usgs-mississippi': return fetchMississippi(startDate, endDate);
    default: throw new Error(`Unknown external series: ${seriesId}`);
  }
}

/**
 * Get the display source attribution for a series.
 * @param {string} seriesId
 * @returns {string}
 */
export function getSeriesSource(seriesId) {
  const ext = EXTERNAL_SERIES.find(s => s.id === seriesId);
  if (ext) return ext.source;
  return 'Federal Reserve Economic Data (FRED)';
}
