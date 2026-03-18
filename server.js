const express = require('express');
const fetch   = require('node-fetch');
const cors    = require('cors');
const path    = require('path');
const fs      = require('fs');

const app  = express();
const PORT = process.env.PORT || 3000;
app.use(cors());
app.use(express.static(path.join(__dirname, 'public')));

// ── NSE session ───────────────────────────────────────────────────────────────
let cachedCookies = '', cookieExpiry = 0;
async function getNSECookies() {
  if (Date.now() < cookieExpiry && cachedCookies) return cachedCookies;
  const res = await fetch('https://www.nseindia.com/', {
    headers: {
      'User-Agent'     : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36',
      'Accept'         : 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.5',
      'Connection'     : 'keep-alive',
    },
    redirect: 'follow',
  });
  cachedCookies = (res.headers.raw()['set-cookie'] || []).map(c => c.split(';')[0]).join('; ');
  cookieExpiry  = Date.now() + 5 * 60 * 1000;
  return cachedCookies;
}

async function nseGet(url) {
  const cookies = await getNSECookies();
  const res = await fetch(url, {
    headers: {
      'User-Agent'      : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36',
      'Accept'          : 'application/json, text/plain, */*',
      'Accept-Language' : 'en-US,en;q=0.5',
      'Referer'         : 'https://www.nseindia.com/',
      'Cookie'          : cookies,
      'X-Requested-With': 'XMLHttpRequest',
      'Connection'      : 'keep-alive',
    },
  });
  if (!res.ok) throw new Error(`NSE API error: ${res.status}`);
  return res.json();
}

// ── F&O symbols ───────────────────────────────────────────────────────────────
let fnoCache = null, fnoCacheExpiry = 0;
async function getFNOSymbols() {
  if (fnoCache && Date.now() < fnoCacheExpiry) return fnoCache;
  const data = await nseGet('https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O');
  fnoCache       = new Set((data.data || []).map(s => s.symbol));
  fnoCacheExpiry = Date.now() + 10 * 60 * 1000;
  return fnoCache;
}

// ── Sectors ───────────────────────────────────────────────────────────────────
const SECTORS = [
  'NIFTY BANK','NIFTY IT','NIFTY PHARMA','NIFTY AUTO',
  'NIFTY FMCG','NIFTY METAL','NIFTY REALTY','NIFTY ENERGY',
  'NIFTY MEDIA','NIFTY PSU BANK','NIFTY PRIVATE BANK','NIFTY FIN SERVICE',
];

// ── Pivot formula ─────────────────────────────────────────────────────────────
function computePivots(high, low, close) {
  const pp = (high + low + close) / 3;
  return {
    pp: +pp.toFixed(2),
    r1: +(2 * pp - low).toFixed(2),
    r2: +(pp + high - low).toFixed(2),
    s1: +(2 * pp - high).toFixed(2),
    s2: +(pp - high + low).toFixed(2),
  };
}

// ── Date helper ───────────────────────────────────────────────────────────────
function todayKey() {
  return new Date().toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' });
}

function nowIST() {
  return new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
}

function isMarketHours() {
  const ist = nowIST();
  const h = ist.getHours(), m = ist.getMinutes();
  return (h > 9 || (h === 9 && m >= 15)) && (h < 15 || (h === 15 && m <= 30));
}

// ── 5-min candle close time ───────────────────────────────────────────────────
function candleCloseTime() {
  const ist  = nowIST();
  const mins = ist.getMinutes();
  const closeMins = Math.ceil((mins + 1) / 5) * 5;
  const d = new Date(ist);
  d.setMinutes(closeMins, 0, 0);
  return d.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', hour12: false });
}

// ── Break time tracker ────────────────────────────────────────────────────────
const breakTimes     = {};
const prevBreakState = {};

function setBreakTime(symbol, type, time) {
  breakTimes[symbol + '_' + type] = { date: todayKey(), time };
}

function getBreakTime(symbol, type) {
  const entry = breakTimes[symbol + '_' + type];
  if (!entry || entry.date !== todayKey()) return null;
  return entry.time;
}

// ── Fetch exact break time from NSE chart API ─────────────────────────────────
// Scans 5-min candles from 9:15 and finds FIRST candle close above R1 / below S1
async function getExactBreakTime(symbol, level, direction) {
  try {
    const ist      = nowIST();
    const toTime   = Math.floor(ist.getTime() / 1000);
    const fromIST  = new Date(ist);
    fromIST.setHours(9, 15, 0, 0);
    const fromTime = Math.floor(fromIST.getTime() / 1000);

    const url  = `https://charting.nseindia.com/Charts/symbolhistoricaldata/${encodeURIComponent(symbol)}` +
                 `?Fromdate=${fromTime}&Todate=${toTime}&interval=5&exchange=NSE&type=EQ`;
    const data = await nseGet(url);
    const candles = data?.grapthData || data?.graphData || data?.data || [];
    if (!candles.length) return null;

    for (const candle of candles) {
      const [ts, , , , close] = candle;
      const triggered = direction === 'above' ? close > level : close < level;
      if (triggered) {
        const d = new Date(ts);
        const breakIST = new Date(d.toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
        breakIST.setMinutes(breakIST.getMinutes() + 5);
        return breakIST.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', hour12: false });
      }
    }
    return null;
  } catch(e) { return null; }
}

// ── Candle scanner (runs every 5 mins during market hours) ────────────────────
async function runCandleScanner() {
  if (!isMarketHours()) return;
  const closeTime = candleCloseTime();
  console.log('[SCANNER] 5-min scan at', closeTime);
  try {
    const fnoSyms   = await getFNOSymbols();
    const results   = await Promise.allSettled(
      SECTORS.map(sector =>
        nseGet(`https://www.nseindia.com/api/equity-stockIndices?index=${encodeURIComponent(sector)}`)
          .then(data => {
            const meta = data.metadata?.indexName || sector;
            return (data.data || []).filter(s => s.symbol !== meta && fnoSyms.has(s.symbol) && s.lastPrice && s.dayHigh && s.dayLow);
          })
      )
    );
    const seen = new Set();
    results.forEach(r => {
      if (r.status !== 'fulfilled') return;
      r.value.forEach(s => {
        if (seen.has(s.symbol)) return;
        seen.add(s.symbol);
        const ltp    = s.lastPrice;
        const todayP = computePivots(s.dayHigh, s.dayLow, s.previousClose);
        const prevH  = Math.max(s.open || s.previousClose, s.previousClose);
        const prevL  = Math.min(s.open || s.previousClose, s.previousClose);
        const prevP  = computePivots(prevH, prevL, s.previousClose);

        const brokeR1 = ltp > todayP.r1 && todayP.r1 > prevP.r1;
        const brokeS1 = ltp < todayP.s1 && todayP.s1 < prevP.s1;

        // R1 — record on transition only
        const wasR1 = prevBreakState[s.symbol + '_r1'] ?? false;
        if (brokeR1 && !wasR1 && !getBreakTime(s.symbol, 'r1')) {
          setBreakTime(s.symbol, 'r1', closeTime);
          console.log(`[BREAK] ${s.symbol} R1 at ${closeTime}`);
        }
        prevBreakState[s.symbol + '_r1'] = brokeR1;

        // S1 — record on transition only
        const wasS1 = prevBreakState[s.symbol + '_s1'] ?? false;
        if (brokeS1 && !wasS1 && !getBreakTime(s.symbol, 's1')) {
          setBreakTime(s.symbol, 's1', closeTime);
          console.log(`[BREAK] ${s.symbol} S1 at ${closeTime}`);
        }
        prevBreakState[s.symbol + '_s1'] = brokeS1;
      });
    });
    console.log(`[SCANNER] Done — ${seen.size} stocks scanned`);
  } catch(e) { console.error('[SCANNER] Error:', e.message); }
}

// ── Backfill on startup ───────────────────────────────────────────────────────
// If server starts mid-day, fetch exact break time from chart candles
async function backfillBreaks() {
  if (!isMarketHours()) { console.log('[BACKFILL] Outside market hours — skipping'); return; }
  const startTime = nowIST().toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit', hour12: false });
  console.log('[BACKFILL] Server started at', startTime, '— finding existing breaks...');
  try {
    const fnoSyms = await getFNOSymbols();
    const results = await Promise.allSettled(
      SECTORS.map(sector =>
        nseGet(`https://www.nseindia.com/api/equity-stockIndices?index=${encodeURIComponent(sector)}`)
          .then(data => {
            const meta = data.metadata?.indexName || sector;
            return (data.data || []).filter(s => s.symbol !== meta && fnoSyms.has(s.symbol) && s.lastPrice && s.dayHigh && s.dayLow);
          })
      )
    );
    const seen = new Set();
    const promises = [];
    results.forEach(r => {
      if (r.status !== 'fulfilled') return;
      r.value.forEach(s => {
        if (seen.has(s.symbol)) return;
        seen.add(s.symbol);
        const ltp    = s.lastPrice;
        const todayP = computePivots(s.dayHigh, s.dayLow, s.previousClose);
        const prevH  = Math.max(s.open || s.previousClose, s.previousClose);
        const prevL  = Math.min(s.open || s.previousClose, s.previousClose);
        const prevP  = computePivots(prevH, prevL, s.previousClose);

        const brokeR1 = ltp > todayP.r1 && todayP.r1 > prevP.r1;
        const brokeS1 = ltp < todayP.s1 && todayP.s1 < prevP.s1;

        // Set prev state so scanner knows it was already breaking
        prevBreakState[s.symbol + '_r1'] = brokeR1;
        prevBreakState[s.symbol + '_s1'] = brokeS1;

        if (brokeR1) {
          promises.push(
            getExactBreakTime(s.symbol, todayP.r1, 'above').then(t => {
              const time = t || ('~' + startTime);
              setBreakTime(s.symbol, 'r1', time);
              console.log(`[BACKFILL] ${s.symbol} R1 → ${time}`);
            })
          );
        }
        if (brokeS1) {
          promises.push(
            getExactBreakTime(s.symbol, todayP.s1, 'below').then(t => {
              const time = t || ('~' + startTime);
              setBreakTime(s.symbol, 's1', time);
              console.log(`[BACKFILL] ${s.symbol} S1 → ${time}`);
            })
          );
        }
      });
    });
    await Promise.allSettled(promises);
    console.log('[BACKFILL] Complete');
  } catch(e) { console.error('[BACKFILL] Error:', e.message); }
}

// Align scanner to next 5-min boundary
function startCandleScanner() {
  const ist      = nowIST();
  const secs     = ist.getSeconds();
  const ms       = ist.getMilliseconds();
  const minsRem  = 5 - (ist.getMinutes() % 5);
  const msUntil  = (minsRem * 60 - secs) * 1000 - ms;
  console.log(`[SCANNER] Next scan in ${Math.round(msUntil / 1000)}s`);
  setTimeout(() => {
    runCandleScanner();
    setInterval(runCandleScanner, 5 * 60 * 1000);
  }, msUntil);
}

// ── Money Flow history ────────────────────────────────────────────────────────
const MF_FILE = path.join(__dirname, 'mf_history.json');
let mfHistory = {};
try { if (fs.existsSync(MF_FILE)) mfHistory = JSON.parse(fs.readFileSync(MF_FILE, 'utf8')); } catch(e) {}
function saveMFHistory() {
  try { fs.writeFileSync(MF_FILE, JSON.stringify(mfHistory)); } catch(e) {}
}
function recordMF(symbol, mf) {
  if (!mfHistory[symbol]) mfHistory[symbol] = {};
  mfHistory[symbol][todayKey()] = Math.abs(mf);
  const keys = Object.keys(mfHistory[symbol]).sort();
  if (keys.length > 10) keys.slice(0, keys.length - 10).forEach(k => delete mfHistory[symbol][k]);
  if (!recordMF._t) recordMF._t = setTimeout(() => { saveMFHistory(); recordMF._t = null; }, 30000);
}
function getMFMultiplier(symbol, todayMF) {
  const days     = mfHistory[symbol] || {};
  const today    = todayKey();
  const prevKeys = Object.keys(days).filter(k => k !== today).sort();
  if (!prevKeys.length) return { multiplier: null, periods: 0, avgMF: null };
  const avg = prevKeys.reduce((s, k) => s + days[k], 0) / prevKeys.length;
  return { multiplier: avg > 0 ? +(Math.abs(todayMF) / avg).toFixed(2) : null, periods: prevKeys.length, avgMF: +avg.toFixed(0) };
}

// ── Core pivot calc ───────────────────────────────────────────────────────────
function calcPivotSignals(s) {
  const ltp = s.lastPrice;
  if (!ltp || !s.dayHigh || !s.dayLow || !s.previousClose) return null;
  const todayP   = computePivots(s.dayHigh, s.dayLow, s.previousClose);
  const prevHigh = Math.max(s.open || s.previousClose, s.previousClose);
  const prevLow  = Math.min(s.open || s.previousClose, s.previousClose);
  const prevP    = computePivots(prevHigh, prevLow, s.previousClose);

  const ltpAboveTodayR1 = ltp > todayP.r1;
  const ltpBelowTodayS1 = ltp < todayP.s1;
  const r1Rising        = todayP.r1 > prevP.r1;
  const s1Falling       = todayP.s1 < prevP.s1;
  const strongBuy       = ltpAboveTodayR1 && r1Rising;
  const strongSell      = ltpBelowTodayS1 && s1Falling;

  const absMF  = Math.abs((s.dayHigh + s.dayLow + ltp) / 3 * (s.totalTradedVolume || 0));
  recordMF(s.symbol, absMF);
  const mfData = getMFMultiplier(s.symbol, absMF);

  return {
    symbol: s.symbol, lastPrice: ltp, change: s.change, pChange: s.pChange,
    dayHigh: s.dayHigh, dayLow: s.dayLow, previousClose: s.previousClose,
    totalTradedVolume: s.totalTradedVolume, open: s.open,
    today       : { ...todayP, brokeR1: ltpAboveTodayR1, brokeS1: ltpBelowTodayS1 },
    prev        : { ...prevP },
    r1Rising, s1Falling,
    brokeBothR1 : strongBuy,
    brokeBothS1 : strongSell,
    breakTime   : { r1: getBreakTime(s.symbol, 'r1'), s1: getBreakTime(s.symbol, 's1') },
    mf: { today: absMF, todayAbs: absMF, dir: s.pChange >= 0 ? 'buy' : 'sell', multiplier: mfData.multiplier, periods: mfData.periods, avgMF: mfData.avgMF },
  };
}

// ── Routes ────────────────────────────────────────────────────────────────────
app.get('/api/indices', async (req, res) => {
  try { res.json(await nseGet('https://www.nseindia.com/api/allIndices')); }
  catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/heatmap', async (req, res) => {
  const index = req.query.index || 'NIFTY 50';
  try { res.json(await nseGet(`https://www.nseindia.com/api/equity-stockIndices?index=${encodeURIComponent(index)}`)); }
  catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/gainers-losers', async (req, res) => {
  try {
    const [gainersData, losersData] = await Promise.all([
      nseGet('https://www.nseindia.com/api/live-analysis-variations?index=gainers'),
      nseGet('https://www.nseindia.com/api/live-analysis-variations?index=loosers'),
    ]);
    const extract = (data) => {
      const all = [];
      if (!data) return all;
      Object.values(data).forEach(group => {
        if (Array.isArray(group?.data)) {
          group.data.forEach(s => all.push({ symbol: s.symbol, lastPrice: s.ltp, pChange: s.perChange, change: s.netPrice, totalTradedVolume: s.tradedVolume }));
        }
      });
      return all;
    };
    const gainers = extract(gainersData).sort((a,b) => b.pChange - a.pChange).slice(0, 10);
    const losers  = extract(losersData).sort((a,b) => a.pChange - b.pChange).slice(0, 10);
    if (!gainers.length && !losers.length) throw new Error('empty');
    res.json({ gainers, losers });
  } catch(e) {
    try {
      const data   = await nseGet('https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500');
      const stocks = (data.data || []).filter(s => s.symbol !== data.metadata?.indexName);
      res.json({
        gainers: stocks.filter(s => s.pChange > 0).sort((a,b) => b.pChange - a.pChange).slice(0, 10),
        losers : stocks.filter(s => s.pChange < 0).sort((a,b) => a.pChange - b.pChange).slice(0, 10),
      });
    } catch(e2) { res.status(500).json({ error: e2.message }); }
  }
});

app.get('/api/sectors', async (req, res) => {
  try {
    const results = await Promise.allSettled(
      SECTORS.map(s => nseGet(`https://www.nseindia.com/api/equity-stockIndices?index=${encodeURIComponent(s)}`)
        .then(d => ({ name: s.replace('NIFTY ', ''), fullName: s, pChange: d.metadata?.percChange ?? 0, last: d.metadata?.last ?? 0, change: d.metadata?.change ?? 0 })))
    );
    res.json({ sectors: results.filter(r => r.status === 'fulfilled').map(r => r.value).sort((a,b) => b.pChange - a.pChange) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/sector-stocks', async (req, res) => {
  const index = req.query.index;
  if (!index) return res.status(400).json({ error: 'index required' });
  try {
    const [sectorData, fnoSymbols] = await Promise.all([
      nseGet(`https://www.nseindia.com/api/equity-stockIndices?index=${encodeURIComponent(index)}`),
      getFNOSymbols(),
    ]);
    const meta   = sectorData.metadata?.indexName || index;
    const stocks = (sectorData.data || [])
      .filter(s => s.symbol !== meta && fnoSymbols.has(s.symbol))
      .map(s => ({ symbol: s.symbol, lastPrice: s.lastPrice, change: s.change, pChange: s.pChange, open: s.open, dayHigh: s.dayHigh, dayLow: s.dayLow, previousClose: s.previousClose, totalTradedVolume: s.totalTradedVolume }))
      .sort((a,b) => b.pChange - a.pChange);
    res.json({ index, stocks });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/pivot-stocks', async (req, res) => {
  const index = req.query.index;
  if (!index) return res.status(400).json({ error: 'index required' });
  try {
    const [sectorData, fnoSymbols] = await Promise.all([
      nseGet(`https://www.nseindia.com/api/equity-stockIndices?index=${encodeURIComponent(index)}`),
      getFNOSymbols(),
    ]);
    const meta      = sectorData.metadata?.indexName || index;
    const rawStocks = (sectorData.data || []).filter(s => s.symbol !== meta && fnoSymbols.has(s.symbol));
    const result    = rawStocks.map(calcPivotSignals).filter(Boolean).sort((a,b) => b.pChange - a.pChange);
    res.json({ index, stocks: result, todayDate: 'live', prevDate: 'approx' });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/pivot-all', async (req, res) => {
  try {
    const fnoSymbols = await getFNOSymbols();
    const sectorResults = await Promise.allSettled(
      SECTORS.map(sector =>
        nseGet(`https://www.nseindia.com/api/equity-stockIndices?index=${encodeURIComponent(sector)}`)
          .then(data => {
            const meta = data.metadata?.indexName || sector;
            return (data.data || []).filter(s => s.symbol !== meta && fnoSymbols.has(s.symbol) && s.lastPrice && s.dayHigh && s.dayLow);
          })
      )
    );
    const seen = new Set(), allStocks = [];
    sectorResults.forEach(r => {
      if (r.status === 'fulfilled') r.value.forEach(s => { if (!seen.has(s.symbol)) { seen.add(s.symbol); allStocks.push(s); } });
    });
    const result = allStocks.map(calcPivotSignals).filter(Boolean).sort((a,b) => b.pChange - a.pChange);
    res.json({ stocks: result, todayDate: 'live', prevDate: 'approx', total: result.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/money-flow', async (req, res) => {
  try {
    const fnoSymbols = await getFNOSymbols();
    const results = await Promise.allSettled(
      SECTORS.map(async sector => {
        const data   = await nseGet(`https://www.nseindia.com/api/equity-stockIndices?index=${encodeURIComponent(sector)}`);
        const meta   = data.metadata?.indexName || sector;
        const stocks = (data.data || []).filter(s => s.symbol !== meta && fnoSymbols.has(s.symbol));
        let posMF = 0, negMF = 0, totalMF = 0;
        const stockFlows = stocks.map(s => {
          const tp  = (s.dayHigh + s.dayLow + s.lastPrice) / 3;
          const mf  = tp * (s.totalTradedVolume || 0);
          const dir = s.pChange >= 0 ? 'buy' : 'sell';
          if (dir === 'buy') posMF += mf; else negMF += mf;
          totalMF += mf;
          return { symbol: s.symbol, lastPrice: s.lastPrice, pChange: s.pChange, tp: +tp.toFixed(2), volume: s.totalTradedVolume, mf: +mf.toFixed(0), dir };
        }).sort((a,b) => b.mf - a.mf);
        const mfRatio = negMF > 0 ? posMF / negMF : posMF > 0 ? 999 : 1;
        const mfi     = +(100 - (100 / (1 + mfRatio))).toFixed(2);
        const netMF   = posMF - negMF;
        const mfPct   = totalMF > 0 ? +((netMF / totalMF) * 100).toFixed(2) : 0;
        return { sector, name: sector.replace('NIFTY ', ''), mfi, mfPct, posMF: +posMF.toFixed(0), negMF: +negMF.toFixed(0), netMF: +netMF.toFixed(0), totalMF: +totalMF.toFixed(0), stockCount: stocks.length, buyCount: stockFlows.filter(s => s.dir === 'buy').length, sellCount: stockFlows.filter(s => s.dir === 'sell').length, stocks: stockFlows, sectorPct: data.metadata?.percChange ?? 0 };
      })
    );
    res.json({ sectors: results.filter(r => r.status === 'fulfilled').map(r => r.value).sort((a,b) => b.mfi - a.mfi) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Start ─────────────────────────────────────────────────────────────────────
app.listen(PORT, '0.0.0.0', () => {
  console.log(`NSE Heatmap server running → http://localhost:${PORT}`);
  backfillBreaks().then(() => startCandleScanner());
});

// ── OPTION CHAIN ──────────────────────────────────────────────────────────────
// Fetches option chain and returns 5 ITM + ATM + 5 OTM strikes
// with change in OI for both CE and PE

app.get('/api/option-chain', async (req, res) => {
  const symbol = (req.query.symbol || 'NIFTY').toUpperCase();
  const isIndex = ['NIFTY','BANKNIFTY','FINNIFTY','MIDCPNIFTY'].includes(symbol);

  try {
    const url  = isIndex
      ? `https://www.nseindia.com/api/option-chain-indices?symbol=${encodeURIComponent(symbol)}`
      : `https://www.nseindia.com/api/option-chain-equities?symbol=${encodeURIComponent(symbol)}`;
    const data = await nseGet(url);

    const records  = data.records  || {};
    const filtered = data.filtered || {};
    const spot     = filtered.underlyingValue || records.underlyingValue || 0;
    const expiries = records.expiryDates || [];
    const expiry   = req.query.expiry || expiries[0]; // nearest by default

    // Filter by selected expiry
    const allStrikes = (records.data || []).filter(r => r.expiryDate === expiry);

    // Get all strike prices sorted
    const strikes = [...new Set(allStrikes.map(r => r.strikePrice))].sort((a,b) => a - b);

    // Find ATM — closest strike to spot
    const atm = strikes.reduce((prev, curr) =>
      Math.abs(curr - spot) < Math.abs(prev - spot) ? curr : prev, strikes[0]
    );
    const atmIdx = strikes.indexOf(atm);

    // 5 ITM + ATM + 5 OTM
    const selectedStrikes = strikes.slice(
      Math.max(0, atmIdx - 5),
      Math.min(strikes.length, atmIdx + 6)
    );

    // Build strike rows
    const rows = selectedStrikes.map(strike => {
      const row    = allStrikes.find(r => r.strikePrice === strike) || {};
      const ce     = row.CE || {};
      const pe     = row.PE || {};
      const isATM  = strike === atm;
      const isITM_CE = strike < atm;
      const isITM_PE = strike > atm;

      return {
        strike,
        isATM,
        label: isATM ? 'ATM' : strike < atm ? 'ITM' : 'OTM',
        CE: {
          oi        : ce.openInterest        || 0,
          oiChange  : ce.changeinOpenInterest || 0,
          oiChangePct: ce.pchangeinOpenInterest || 0,
          ltp       : ce.lastPrice           || 0,
          volume    : ce.totalTradedVolume   || 0,
          iv        : ce.impliedVolatility   || 0,
          bid       : ce.bidprice            || 0,
          ask       : ce.askPrice            || 0,
        },
        PE: {
          oi        : pe.openInterest        || 0,
          oiChange  : pe.changeinOpenInterest || 0,
          oiChangePct: pe.pchangeinOpenInterest || 0,
          ltp       : pe.lastPrice           || 0,
          volume    : pe.totalTradedVolume   || 0,
          iv        : pe.impliedVolatility   || 0,
          bid       : pe.bidprice            || 0,
          ask       : pe.askPrice            || 0,
        },
      };
    });

    // PCR — Put/Call Ratio based on OI
    const totalCEoi = rows.reduce((s,r) => s + r.CE.oi, 0);
    const totalPEoi = rows.reduce((s,r) => s + r.PE.oi, 0);
    const pcr       = totalCEoi > 0 ? +(totalPEoi / totalCEoi).toFixed(2) : 0;

    // Max Pain — strike where total OI loss is minimum
    let maxPainStrike = atm, minLoss = Infinity;
    selectedStrikes.forEach(testStrike => {
      const loss = rows.reduce((sum, r) => {
        const ceLoss = r.CE.oi * Math.max(0, r.strike - testStrike);
        const peLoss = r.PE.oi * Math.max(0, testStrike - r.strike);
        return sum + ceLoss + peLoss;
      }, 0);
      if (loss < minLoss) { minLoss = loss; maxPainStrike = testStrike; }
    });

    res.json({ symbol, spot, expiry, expiries, atm, pcr, maxPain: maxPainStrike, rows });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// F&O stocks list for option view dropdown
app.get('/api/fno-list', async (req, res) => {
  try {
    const syms = await getFNOSymbols();
    res.json({ symbols: [...syms].sort() });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── BATCH PCR for multiple symbols ───────────────────────────────────────────
app.get('/api/pcr-batch', async (req, res) => {
  const symbols = (req.query.symbols || '').split(',').filter(Boolean).slice(0, 30);
  if (!symbols.length) return res.json({ pcr: {} });

  const results = await Promise.allSettled(
    symbols.map(async sym => {
      const isIndex = ['NIFTY','BANKNIFTY','FINNIFTY','MIDCPNIFTY'].includes(sym);
      const url = isIndex
        ? `https://www.nseindia.com/api/option-chain-indices?symbol=${encodeURIComponent(sym)}`
        : `https://www.nseindia.com/api/option-chain-equities?symbol=${encodeURIComponent(sym)}`;
      const data    = await nseGet(url);
      const records = data.records || {};
      const expiry  = (records.expiryDates || [])[0];
      const rows    = (records.data || []).filter(r => r.expiryDate === expiry);
      const totalCE = rows.reduce((s, r) => s + (r.CE?.openInterest || 0), 0);
      const totalPE = rows.reduce((s, r) => s + (r.PE?.openInterest || 0), 0);
      const pcr     = totalCE > 0 ? +(totalPE / totalCE).toFixed(2) : 0;
      return { sym, pcr };
    })
  );

  const pcr = {};
  results.forEach(r => { if (r.status === 'fulfilled') pcr[r.value.sym] = r.value.pcr; });
  res.json({ pcr });
});

// ── BATCH PCR for multiple symbols ───────────────────────────────────────────
app.get('/api/pcr-batch', async (req, res) => {
  const symbols = (req.query.symbols || '').split(',').filter(Boolean).slice(0, 30);
  if (!symbols.length) return res.json({ pcr: {} });

  const results = await Promise.allSettled(
    symbols.map(async sym => {
      const isIndex = ['NIFTY','BANKNIFTY','FINNIFTY','MIDCPNIFTY'].includes(sym);
      const url = isIndex
        ? `https://www.nseindia.com/api/option-chain-indices?symbol=${encodeURIComponent(sym)}`
        : `https://www.nseindia.com/api/option-chain-equities?symbol=${encodeURIComponent(sym)}`;
      const data    = await nseGet(url);
      const records = data.records || {};
      const expiry  = (records.expiryDates || [])[0];
      const rows    = (records.data || []).filter(r => r.expiryDate === expiry);
      const totalCE = rows.reduce((s, r) => s + (r.CE?.openInterest || 0), 0);
      const totalPE = rows.reduce((s, r) => s + (r.PE?.openInterest || 0), 0);
      const pcr     = totalCE > 0 ? +(totalPE / totalCE).toFixed(2) : 0;
      return { sym, pcr };
    })
  );

  const pcr = {};
  results.forEach(r => { if (r.status === 'fulfilled') pcr[r.value.sym] = r.value.pcr; });
  res.json({ pcr });
});
