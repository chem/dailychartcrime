function fmtDate(dateStr) {
  const d = new Date(`${dateStr}T00:00:00`);
  return d.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
}

function fmtDateShort(dateStr) {
  const d = new Date(`${dateStr}T00:00:00`);
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

function wrapText(str, maxPerLine) {
  if (str.length <= maxPerLine) return str;
  const words = str.split(' ');
  const lines = [];
  let current = '';
  for (const word of words) {
    if (!current) {
      current = word;
    } else if (current.length + 1 + word.length <= maxPerLine) {
      current += ` ${word}`;
    } else {
      lines.push(current);
      current = word;
    }
  }
  if (current) lines.push(current);
  return lines.length === 1 ? lines[0] : lines;
}

function setText(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

function generateEditorial(data) {
  const r = data.correlation;
  const absR = Math.abs(r);
  const rStr = r.toFixed(2);
  const id = data.series.id;

  const specials = {
    T10YIE: `The 10-year breakeven inflation rate tracks large-cap equities with an r of ${rStr}. This means the bond market's inflation expectations move in alignment with stock prices-a relationship economists consider "interesting," which in this field means they have no consensus on why it happens.`,
    T5YIE: `The 5-year breakeven inflation rate shows a ${r >= 0 ? 'positive' : 'negative'} correlation with the S&P 500 (r = ${rStr}). Inflation expectations and equity valuations, moving together. We have alerted the relevant authorities.`,
    T10Y3M: `The 10-year minus 3-month yield spread (r = ${rStr}) is the yield curve spread that Wall Street watches obsessively. Its relationship to equities is well-documented, which makes today's chart either sophisticated or trivially obvious depending on your background.`,
    T10Y2Y: `The 2s10s yield spread correlates with equity returns at r = ${rStr}. This particular spread has predicted nearly every recession since 1978 when it inverts. Today it is correlated with stocks. Draw your own conclusions, then discard them.`,
    SOFRINDEX: `SOFR-the Secured Overnight Financing Rate-is the boring risk-free rate that replaced the scandalous LIBOR. It appears to move ${r >= 0 ? 'with' : 'against'} the S&P 500 (r = ${rStr}). SOFR has never been involved in a banking scandal. We find this suspicious.`,
    IUDZOS2: `SONIA-the Sterling Overnight Index Average-is Britain's answer to SOFR, and it tracks American equities with an r of ${rStr}. The British invented the concept of overnight lending. They also invented queuing, which has a better track record.`,
    SOFR180DAYAVG: `The 180-day SOFR average (r = ${rStr}) suggests that six-month overnight lending rates are correlated with large-cap equity performance. The Federal Reserve has built a career on these being independent. Today they are not.`,
    SOFR90DAYAVG: `The 90-day SOFR average correlates with the S&P 500 at r = ${rStr}. Our quant team notes that this is "statistically interesting." Our compliance team has asked us not to speculate further.`,
    SOFR30DAYAVG: `Monthly SOFR average versus large-cap equities (r = ${rStr}). In an efficient market, this relationship should not persist. Markets are famously efficient. Today they are efficiently correlated.`,
    T5YIFR: `The 5-year, 5-year forward inflation expectation rate (r = ${rStr}) represents what the bond market expects inflation to average in years 6 through 10. It moves with stocks. Decades from now, we will know if it was right.`,
    IORB: `The Interest Rate on Reserve Balances is set by the Federal Reserve and changes infrequently. Yet it shows an r of ${rStr} with daily S&P 500 moves, which is remarkable given it barely moved during this period. Statistics, everyone.`,
    ECBMRRFR: `The ECB's main refinancing rate (r = ${rStr}) barely moved during this window-it is a policy rate, not a market rate. Any correlation with daily equity returns is therefore either a profound insight about forward guidance or a rounding error. Probably both.`,
    ECBMLFR: `The ECB marginal lending facility rate correlates with American equities at r = ${rStr}. Frankfurt and Wall Street are 5,569 miles apart. Financial markets have apparently bridged this distance.`,
    'ext:usgs-earthquakes': `Global earthquake frequency tracks the S&P 500 with an r of ${rStr}. Our geophysics correspondent describes this as "geologically implausible." Our quant team describes it as "beautifully backtested." We will let you decide who to believe.`,
    'ext:wiki-recession': `Wikipedia searches for "Recession" correlate with S&P 500 returns at r = ${rStr}. Whether retail investors Googling "recession" cause market moves, or merely reflect them, remains an open question. The algorithm does not take a position.`,
    'ext:usgs-mississippi': `The Mississippi River level at St. Louis (r = ${rStr}) is a genuine proxy for Midwestern agricultural and industrial activity-river levels affect barge traffic, grain transport, and supply chains. This correlation is therefore either deeply meaningful or a reminder that everything correlates with everything else over a few weeks of data.`,
  };

  if (specials[id]) return specials[id];

  if (id.startsWith('DTP')) {
    return `Today's chart features a Treasury Inflation-Protected Security (TIPS) versus the S&P 500 (r = ${rStr}). TIPS prices reflect real yield expectations, which in turn reflect market views on growth and inflation. In other words, this correlation has a theoretical basis. We are as surprised as you are.`;
  }
  if (/^R(P|RP)(ON|AG|TS|MB)/.test(id)) {
    return `Federal Reserve repo and reverse repo operations-the overnight plumbing of the financial system, conducted at 3am by serious people in dark rooms-show a ${r >= 0 ? 'positive' : 'negative'} relationship with equity returns (r = ${rStr}). Liquidity, it turns out, is everywhere, including this chart.`;
  }
  if (id.startsWith('ECB')) {
    return `European Central Bank rate metrics appear to track U.S. equity performance (r = ${rStr}). In a globally integrated financial system, cross-border transmission of monetary policy signals is expected. What is less expected is that we found this at scale, using daily data, from our living room.`;
  }

  const dir = r >= 0 ? 'moves with' : 'moves against';
  const strength = absR > 0.5 ? 'notably' : absR > 0.3 ? 'meaningfully' : 'mildly';
  return `${data.series.title} ${strength} ${dir} the S&P 500 (r = ${rStr}). We present this correlation without editorial comment, except to note that our algorithm identified it within the past 24 hours using publicly available data and a worrying lack of skepticism.`;
}

function renderChart(data) {
  if (!data) {
    setText('editorial', 'Chart data is unavailable. Check the generator logs and try again.');
    const chartWrap = document.getElementById('chart-wrap');
    const downloadLink = document.getElementById('download-link');
    if (chartWrap) chartWrap.style.display = 'none';
    if (downloadLink) downloadLink.style.display = 'none';
    return;
  }
  if (typeof Chart === 'undefined') {
    const chartWrap = document.getElementById('chart-wrap');
    const downloadLink = document.getElementById('download-link');
    if (chartWrap) chartWrap.style.display = 'none';
    if (downloadLink) downloadLink.style.display = 'none';
    throw new Error('Chart.js did not load — check your network connection or ad-blocker');
  }

  const r = data.correlation;
  const corrSign = r >= 0 ? '+' : '';
  const absR = Math.abs(r);
  let strength = 'weakly';
  if (absR > 0.7) strength = 'strongly';
  else if (absR > 0.4) strength = 'moderately';

  const title = data.series.title;
  const postDateStr = data.generatedDate
    || (data.generatedAt ? new Date(data.generatedAt).toISOString().slice(0, 10) : new Date().toISOString().slice(0, 10));

  setText('post-date', fmtDate(postDateStr).toUpperCase());
  setText('post-headline', `${title} is ${strength} correlated with the S&P 500`);
  setText('editorial', generateEditorial(data));

  const seriesSource = data.series.source || 'Federal Reserve Economic Data (FRED)';
  const isFredSource = seriesSource === 'Federal Reserve Economic Data (FRED)';
  const sourceText = `Sources: ${seriesSource}${isFredSource ? '' : '; Federal Reserve Economic Data (FRED)'} - ${fmtDateShort(data.windowStart)}-${fmtDateShort(data.windowEnd)}`;
  setText('chart-source', sourceText);

  setText('download-link', `Download chart (PNG) (${data.rank} of ${data.totalSeries})`);

  const badge = document.getElementById('corr-badge');
  if (badge) {
    badge.className = `corr-badge ${r >= 0 ? 'corr-positive' : 'corr-negative'}`;
    badge.textContent = `r = ${corrSign}${r.toFixed(4)}`;
  }

  setText('corr-rank', `Rank #${data.rank} of ${data.totalSeries} series with complete daily overlap`);

  const ctx = document.getElementById('chart')?.getContext('2d');
  if (!ctx) {
    throw new Error('Chart canvas context is unavailable');
  }

  const legendEl = document.getElementById('chart-legend');
  if (legendEl) {
    legendEl.textContent = '';
    for (const item of [
      { color: '#000', label: 'S&P 500' },
      { color: '#007d55', label: title },
    ]) {
      const wrap = document.createElement('span');
      wrap.className = 'chart-legend-item';
      const swatch = document.createElement('span');
      swatch.className = 'chart-legend-swatch';
      swatch.style.background = item.color;
      wrap.appendChild(swatch);
      wrap.appendChild(document.createTextNode(item.label));
      legendEl.appendChild(wrap);
    }
  }

  new Chart(ctx, {
    type: 'line',
    data: {
      labels: data.sp500.dates,
      datasets: [
        {
          label: 'S&P 500',
          data: data.sp500.values,
          borderColor: '#000',
          borderWidth: 2,
          pointRadius: 0,
          pointHoverRadius: 5,
          tension: 0.1,
          yAxisID: 'y',
          fill: false,
        },
        {
          label: wrapText(title, 50),
          data: data.series.values,
          borderColor: '#007d55',
          borderWidth: 2,
          pointRadius: 0,
          pointHoverRadius: 5,
          tension: 0.1,
          yAxisID: 'y1',
          fill: false,
        },
      ],
    },
    options: {
      animation: {
        onComplete() { setupDownload(data); },
      },
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#fff',
          titleColor: '#000',
          bodyColor: '#333',
          borderColor: '#e7e8ea',
          borderWidth: 1,
          padding: 12,
          titleFont: { family: "'Nunito Sans'", size: 13, weight: '700' },
          bodyFont: { family: "'Nunito Sans'", size: 12 },
          callbacks: {
            title(items) { return fmtDateShort(items[0].label); },
          },
        },
      },
      scales: {
        x: {
          type: 'category',
          ticks: {
            font: { family: "'Nunito Sans'", size: 11 },
            color: '#888',
            maxTicksLimit: 8,
            callback(value) { return fmtDateShort(this.getLabelForValue(value)); },
          },
          grid: { display: false },
          border: { color: '#e7e8ea' },
        },
        y: {
          type: 'linear',
          position: 'left',
          title: {
            display: true,
            text: 'S&P 500',
            font: { family: "'Nunito Sans'", size: 12, weight: '600' },
            color: '#000',
          },
          ticks: {
            font: { family: "'Nunito Sans'", size: 11 },
            color: '#555',
            callback(v) { return v.toLocaleString(); },
          },
          grid: { color: '#f0f0f0' },
          border: { color: '#e7e8ea' },
        },
        y1: {
          type: 'linear',
          position: 'right',
          reverse: data.inverted,
          title: {
            display: true,
            text: (() => {
              const wrapped = wrapText(title, 30);
              const suffix = data.inverted ? ' ^ inverted' : '';
              if (!suffix) return wrapped;
              if (Array.isArray(wrapped)) {
                return [...wrapped.slice(0, -1), wrapped[wrapped.length - 1] + suffix];
              }
              return wrapped + suffix;
            })(),
            font: { family: "'Nunito Sans'", size: 12, weight: '600' },
            color: '#007d55',
          },
          ticks: {
            font: { family: "'Nunito Sans'", size: 11 },
            color: '#007d55',
          },
          grid: { display: false },
          border: { color: '#e7e8ea' },
        },
      },
    },
  });
}

function setupDownload(data) {
  const canvas = document.getElementById('chart');
  const dlLink = document.getElementById('download-link');
  if (!canvas || !dlLink || !data) return;
  if (dlLink.dataset.downloadReady) return;
  dlLink.href = canvas.toDataURL('image/png');
  dlLink.download = 'daily-chart-crime.png';
  dlLink.dataset.downloadReady = 'true';
}

function showStaleBanner(data) {
  if (!data?.generatedAt) return;
  const generated = new Date(data.generatedAt);
  const ageMs = Date.now() - generated.getTime();
  if (ageMs <= 24 * 60 * 60 * 1000) return;
  const banner = document.getElementById('stale-banner');
  if (!banner) return;
  const daysOld = Math.floor(ageMs / (24 * 60 * 60 * 1000));
  const genDate = generated.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
  banner.textContent = `Data not updated today - last generated on ${genDate} (${daysOld} day${daysOld > 1 ? 's' : ''} ago)`;
  banner.classList.add('visible');
}

function showRigged() {
  document.getElementById('rigged-overlay')?.classList.add('open');
}

function closeRigged() {
  document.getElementById('rigged-overlay')?.classList.remove('open');
}

function wireRiggedUi() {
  for (const link of document.querySelectorAll('.rigged-trigger')) {
    link.addEventListener('click', event => {
      event.preventDefault();
      showRigged();
    });
  }
  const overlay = document.getElementById('rigged-overlay');
  if (overlay) {
    overlay.addEventListener('click', closeRigged);
  }
  const riggedBox = overlay?.querySelector('.rigged-box');
  if (riggedBox) {
    riggedBox.addEventListener('click', event => event.stopPropagation());
  }
  document.addEventListener('keydown', event => {
    if (event.key === 'Escape') closeRigged();
  });
}

async function loadChartData() {
  const chartUrl = document.body?.dataset?.chartUrl || './chart-data.json';
  const response = await fetch(chartUrl, { cache: 'no-store' });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} from ${chartUrl}`);
  }
  return response.json();
}

async function init() {
  wireRiggedUi();
  let data = null;
  try {
    data = await loadChartData();
  } catch (err) {
    console.error(`[UI] Failed to load chart data: ${err.message}`);
  }
  renderChart(data);
  showStaleBanner(data);
}

document.addEventListener('DOMContentLoaded', () => {
  init().catch(err => {
    console.error(`[UI] Fatal frontend error: ${err.message}`);
    setText('editorial', 'Chart rendering failed. Check console logs.');
  });
});
