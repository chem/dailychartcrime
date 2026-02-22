import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

const DEV_VARS_PATH = join(process.cwd(), '.dev.vars');

function loadDevVars(raw) {
  for (const lineRaw of raw.split(/\r?\n/)) {
    const line = lineRaw.trim();
    if (!line || line.startsWith('#')) continue;
    const eq = line.indexOf('=');
    if (eq <= 0) continue;
    const key = line.slice(0, eq).trim();
    const value = line.slice(eq + 1).trim();
    if (key && process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}

try {
  const raw = await readFile(DEV_VARS_PATH, 'utf8');
  loadDevVars(raw);
} catch (err) {
  if (err.code !== 'ENOENT') {
    throw err;
  }
}

if (!process.env.FRED_API_KEY) {
  console.error('[DEV] FRED_API_KEY is not set. Put it in .dev.vars or export it in your shell.');
  process.exit(1);
}

await import('./generate.mjs');
