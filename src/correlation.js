/**
 * Get the third Friday of a month.
 * @param {number} year
 * @param {number} month - 0-indexed
 * @returns {Date}
 */
export function getThirdFriday(year, month) {
  const first = new Date(Date.UTC(year, month, 1));
  const firstDow = first.getUTCDay();
  const daysUntilFriday = (5 - firstDow + 7) % 7;
  return new Date(Date.UTC(year, month, 1 + daysUntilFriday + 14));
}

/**
 * Prior monthly options expiration (third Friday) relative to today.
 * Interpreted as the previous month's expiration.
 * @param {Date} [today]
 * @returns {Date}
 */
export function getPriorMonthlyExpiration(today = new Date()) {
  const y = today.getUTCFullYear();
  const m = today.getUTCMonth();
  const prevMonth = m === 0 ? 11 : m - 1;
  const prevYear = m === 0 ? y - 1 : y;
  const prevExp = getThirdFriday(prevYear, prevMonth);
  console.log(`[CORR] Prior monthly expiration: ${toDateString(prevExp)}`);
  return prevExp;
}

/**
 * Align two observation arrays by date (inner join).
 * Both arrays must be sorted by date ascending.
 * @param {{date: string, value: number}[]} seriesA
 * @param {{date: string, value: number}[]} seriesB
 * @returns {{dates: string[], valuesA: number[], valuesB: number[]}}
 */
export function alignSeries(seriesA, seriesB) {
  const mapB = new Map(seriesB.map(o => [o.date, o.value]));
  const dates = [];
  const valuesA = [];
  const valuesB = [];

  for (const a of seriesA) {
    if (mapB.has(a.date)) {
      dates.push(a.date);
      valuesA.push(a.value);
      valuesB.push(mapB.get(a.date));
    }
  }

  return { dates, valuesA, valuesB };
}

/**
 * Compute Pearson correlation coefficient between two arrays.
 * @param {number[]} xs
 * @param {number[]} ys
 * @returns {number} correlation coefficient (-1 to 1), or NaN if insufficient data
 */
export function pearsonCorrelation(xs, ys) {
  const n = xs.length;
  if (n < 3) return NaN;

  let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0, sumY2 = 0;
  for (let i = 0; i < n; i++) {
    sumX += xs[i];
    sumY += ys[i];
    sumXY += xs[i] * ys[i];
    sumX2 += xs[i] * xs[i];
    sumY2 += ys[i] * ys[i];
  }

  const numerator = n * sumXY - sumX * sumY;
  const denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY));

  if (denominator === 0) return NaN;
  return numerator / denominator;
}

/**
 * Pick today's series from ranked correlations.
 * Prefers the highest-correlation series not shown in the last 7 days.
 * Falls back to rank #1 if all series were recently shown.
 * @param {{id: string, title: string, correlation: number}[]} correlations - must have at least 1 entry
 * @param {string[]} recentlyUsed - series IDs shown in the past 7 days
 * @returns {{id: string, title: string, correlation: number, rank: number, totalSeries: number}}
 */
export function pickTodaysSeries(correlations, recentlyUsed = []) {
  const ranked = [...correlations]
    .filter(c => !isNaN(c.correlation))
    .sort((a, b) => Math.abs(b.correlation) - Math.abs(a.correlation));

  if (ranked.length === 0) {
    throw new Error('No valid correlations to pick from');
  }

  const usedSet = new Set(recentlyUsed);
  // Pick the highest-ranked series not used in the past 7 days
  const pick = ranked.find(c => !usedSet.has(c.id)) || ranked[0];
  const rank = ranked.indexOf(pick) + 1;

  console.log(`[CORR] ${usedSet.size} series used recently; picking rank #${rank} of ${ranked.length}`);
  console.log(`[CORR] Top 5:`);
  ranked.slice(0, 5).forEach((c, i) => {
    const marker = c.id === pick.id ? ' <-- TODAY' : (usedSet.has(c.id) ? ' (skip: recent)' : '');
    console.log(`  #${i + 1}: ${c.id} r=${c.correlation.toFixed(4)}${marker}`);
  });

  return { ...pick, rank, totalSeries: ranked.length };
}

/**
 * Format a Date as YYYY-MM-DD string.
 */
export function toDateString(d) {
  return d.toISOString().slice(0, 10);
}
