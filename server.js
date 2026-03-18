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
    headers: { 'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36', 'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', 'Accept-Language':'en-US,en;q=0.5', 'Connection':'keep-alive' },
    redirect: 'follow',
  });
  cachedCookies = (res.headers.raw()['set-cookie']||[]).map(c=>c.split(';')[0]).join('; ');
  cookieExpiry  = Date.now() + 5*60*1000;
  return cachedCookies;
}
async function nseGet(url) {
  const cookies = await getNSECookies();
  const res = await fetch(url, { headers: { 'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36', 'Accept':'application/json, text/plain, */*', 'Accept-Language':'en-US,en;q=0.5', 'Referer':'https://www.nseindia.com/', 'Cookie':cookies, 'X-Requested-With':'XMLHttpRequest', 'Connection':'keep-alive' } });
  if (!res.ok) throw new Error(`NSE API error: ${res.status}`);
  return res.json();
}

// ── F&O symbol list ───────────────────────────────────────────────────────────
let fnoCache = null, fnoCacheExpiry = 0;
async function getFNOSymbols() {
  if (fnoCache && Date.now() < fnoCacheExpiry) return fnoCache;
  const data = await nseGet('https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O');
  fnoCache = new Set((data.data||[]).map(s=>s.symbol));
  fnoCacheExpiry = Date.now() + 10*60*1000;
  return fnoCache;
}

// ── Sectors list ──────────────────────────────────────────────────────────────
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
    r1: +(2*pp - low).toFixed(2),
    r2: +(pp + high - low).toFixed(2),
    s1: +(2*pp - high).toFixed(2),
    s2: +(pp - high + low).toFixed(2),
  };
}

// ── Date helper ───────────────────────────────────────────────────────────────
function todayKey() {
  return new Date().toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' });
}

// ── Break time tracker (5-min candle close based) ────────────────────────────
// A background job runs every 5 minutes aligned to candle close times
// (9:20, 9:25, 9:30 ... 3:30 IST)
// Break time = the 5-min candle close at which stock FIRST closed above R1 / below S1
const breakTimes     = {};  // { 'SYMBOL_r1': { date, time } }
const prevBreakState = {};  // { 'SYMBOL_r1': bool } tracks last candle state

// Get current 5-min candle close time string (e.g. "09:50" if time is 9:47)
function candleCloseTime() {
  const now = new Date();
  const ist = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
  const mins = ist.getMinutes();
  // Round up to next 5-min boundary
  const closeMins = Math.ceil((mins + 1) / 5) * 5;
  const closeDate = new Date(ist);
  closeDate.setMinutes(closeMins, 0, 0);
  return closeDate.toLocaleTimeString('en-IN', { hour:'2-digit', minute:'2-digit', hour12:false });
}

// Called by background job only — not by API requests
function recordBreakCandle(symbol, type, isBreaking) {
  const key         = symbol + '_' + type;
  const today       = todayKey();
  const wasBreaking = prevBreakState[key] ?? false;

  // Reset on new day
  if (breakTimes[key] && breakTimes[key].date !== today) {
    delete breakTimes[key];
    prevBreakState[key] = false;
  }

  // Record ONLY on transition: was NOT breaking → now IS breaking
  // Time = current 5-min candle close time
  if (isBreaking && !wasBreaking && !breakTimes[key]) {
    breakTimes[key] = { date: today, time: candleCloseTime() };
    console.log(`[BREAK] ${symbol} crossed ${type.toUpperCase()} at candle close ${breakTimes[key].time}`);
  }

  prevBreakState[key] = isBreaking;
}

function getBreakTime(symbol, type) {
  const entry = breakTimes[symbol + '_' + type];
  if (!entry || entry.date !== todayKey()) return null;
  return entry.time;
}

// ── Background 5-min candle scanner ──────────────────────────────────────────
// Runs at every 5-min candle close during market hours (9:15–3:30 IST)
async function runCandleScanner() {
  const now = new Date();
  const ist = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
  const h   = ist.getHours(), m = ist.getMinutes();

  // Only run during market hours: 9:15 AM to 3:30 PM IST
  const afterOpen  = h > 9  || (h === 9  && m >= 15);
  const beforeClose= h < 15 || (h === 15 && m <= 30);
  if (!afterOpen || !beforeClose) return;

  console.log('[SCANNER] Running 5-min candle pivot scan at', candleCloseTime());
  try {
    const fnoSyms = await getFNOSymbols();
    // Scan all sectors
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
        const r1Rising  = todayP.r1 > prevP.r1;
        const s1Falling = todayP.s1 < prevP.s1;
        // Record candle-close breaks
        recordBreakCandle(s.symbol, 'r1', ltp > todayP.r1 && r1Rising);
        recordBreakCandle(s.symbol, 's1', ltp < todayP.s1 && s1Falling);
      });
    });
    console.log('[SCANNER] Done. Tracked', seen.size, 'stocks.');
  } catch(e) { console.error('[SCANNER] Error:', e.message); }
}

// Align scanner to next 5-min boundary, then run every 5 mins
function startCandleScanner() {
  const now  = new Date();
  const ist  = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
  const secs = ist.getSeconds();
  const ms   = ist.getMilliseconds();
  const minsRem = 5 - (ist.getMinutes() % 5);
  const msUntilNext = (minsRem * 60 - secs) * 1000 - ms;
  console.log(`[SCANNER] Starting in ${Math.round(msUntilNext/1000)}s (next 5-min boundary)`);
  setTimeout(() => {
    runCandleScanner(); // run immediately at boundary
    setInterval(runCandleScanner, 5 * 60 * 1000); // then every 5 mins
  }, msUntilNext);
}

startCandleScanner();

// ── Money flow rolling history ────────────────────────────────────────────────
const MF_FILE = path.join(__dirname, 'mf_history.json');
let mfHistory = {};
try { if (fs.existsSync(MF_FILE)) mfHistory = JSON.parse(fs.readFileSync(MF_FILE,'utf8')); } catch(e) {}
function saveMFHistory() {
  try { fs.writeFileSync(MF_FILE, JSON.stringify(mfHistory)); } catch(e) {}
}
function recordMF(symbol, mf) {
  if (!mfHistory[symbol]) mfHistory[symbol] = {};
  mfHistory[symbol][todayKey()] = Math.abs(mf);
  const keys = Object.keys(mfHistory[symbol]).sort();
  if (keys.length > 10) keys.slice(0, keys.length-10).forEach(k=>delete mfHistory[symbol][k]);
  if (!recordMF._t) recordMF._t = setTimeout(()=>{ saveMFHistory(); recordMF._t=null; }, 30000);
}
function getMFMultiplier(symbol, todayMF) {
  const days    = mfHistory[symbol] || {};
  const today   = todayKey();
  const prevKeys = Object.keys(days).filter(k=>k!==today).sort();
  if (!prevKeys.length) return { multiplier:null, periods:0, avgMF:null };
  const avg = prevKeys.reduce((s,k)=>s+days[k],0) / prevKeys.length;
  return { multiplier: avg>0 ? +(Math.abs(todayMF)/avg).toFixed(2) : null, periods:prevKeys.length, avgMF:+avg.toFixed(0) };
}

// ── Core pivot calc for one stock row ─────────────────────────────────────────
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

  if (ltpAboveTodayR1) recordBreak(s.symbol, 'r1');
  if (ltpBelowTodayS1) recordBreak(s.symbol, 's1');

  const absMF  = Math.abs((s.dayHigh + s.dayLow + ltp) / 3 * (s.totalTradedVolume||0));
  recordMF(s.symbol, absMF);
  const mfData = getMFMultiplier(s.symbol, absMF);

  return {
    symbol    : s.symbol,
    lastPrice : ltp,
    pChange   : s.pChange,
    today     : { ...todayP, brokeR1: ltpAboveTodayR1, brokeS1: ltpBelowTodayS1 },
    prev      : { ...prevP },
    r1Rising, s1Falling,
    brokeBothR1 : strongBuy,
    brokeBothS1 : strongSell,
    breakTime : { r1: getBreakTime(s.symbol,'r1'), s1: getBreakTime(s.symbol,'s1') },
    mf: { today:absMF, todayAbs:absMF, dir:s.pChange>=0?'buy':'sell', multiplier:mfData.multiplier, periods:mfData.periods, avgMF:mfData.avgMF },
  };
}


// ── Intraday candle data from NSE chart API ───────────────────────────────────
// Fetches today's 5-min candles and finds FIRST candle that closed above R1 / below S1
async function getExactBreakTime(symbol, level, direction) {
  // direction = 'above' for R1, 'below' for S1
  try {
    const now     = new Date();
    const ist     = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
    const toTime  = Math.floor(ist.getTime() / 1000);
    // From 9:15 AM today IST
    const fromIST = new Date(ist);
    fromIST.setHours(9, 15, 0, 0);
    const fromTime = Math.floor(fromIST.getTime() / 1000);

    const url  = `https://charting.nseindia.com/Charts/symbolhistoricaldata/${encodeURIComponent(symbol)}` +
                 `?Fromdate=${fromTime}&Todate=${toTime}&interval=5&exchange=NSE&type=EQ`;
    const data = await nseGet(url);
    const candles = data?.grapthData || data?.graphData || data?.data || [];

    if (!candles.length) return null;

    // Each candle: [timestamp, open, high, low, close, volume]
    for (const candle of candles) {
      const [ts, , , , close] = candle;
      const closeAbove = direction === 'above' ? close > level : close < level;
      if (closeAbove) {
        // ts is in milliseconds
        const d = new Date(ts);
        const breakIST = new Date(d.toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
        // Add 5 mins to get candle CLOSE time
        breakIST.setMinutes(breakIST.getMinutes() + 5);
        return breakIST.toLocaleTimeString('en-IN', {
          hour: '2-digit', minute: '2-digit', hour12: false
        });
      }
    }
    return null;
  } catch(e) {
    return null;
  }
}

// ── Standard routes ───────────────────────────────────────────────────────────
app.get('/api/indices', async (req, res) => {
  try { res.json(await nseGet('https://www.nseindia.com/api/allIndices')); }
  catch(e) { res.status(500).json({ error:e.message }); }
});

app.get('/api/heatmap', async (req, res) => {
  const index = req.query.index || 'NIFTY 50';
  try { res.json(await nseGet(`https://www.nseindia.com/api/equity-stockIndices?index=${encodeURIComponent(index)}`)); }
  catch(e) { res.status(500).json({ error:e.message }); }
});

app.get('/api/gainers-losers', async (req, res) => {
  const index = req.query.index || 'NIFTY 50';
  try {
    const data   = await nseGet(`https://www.nseindia.com/api/equity-stockIndices?index=${encodeURIComponent(index)}`);
    const stocks = (data.data||[]).filter(s=>s.symbol!==data.metadata?.indexName);
    res.json({
      gainers: stocks.filter(s=>s.pChange>0).sort((a,b)=>b.pChange-a.pChange).slice(0,5),
      losers : stocks.filter(s=>s.pChange<0).sort((a,b)=>a.pChange-b.pChange).slice(0,5),
    });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

app.get('/api/sectors', async (req, res) => {
  try {
    const results = await Promise.allSettled(
      SECTORS.map(s => nseGet(`https://www.nseindia.com/api/equity-stockIndices?index=${encodeURIComponent(s)}`)
        .then(d => ({ name:s.replace('NIFTY ',''), fullName:s, pChange:d.metadata?.percChange??0, last:d.metadata?.last??0, change:d.metadata?.change??0 })))
    );
    res.json({ sectors: results.filter(r=>r.status==='fulfilled').map(r=>r.value).sort((a,b)=>b.pChange-a.pChange) });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

app.get('/api/sector-stocks', async (req, res) => {
  const index = req.query.index;
  if (!index) return res.status(400).json({ error:'index required' });
  try {
    const [sectorData, fnoSymbols] = await Promise.all([
      nseGet(`https://www.nseindia.com/api/equity-stockIndices?index=${encodeURIComponent(index)}`),
      getFNOSymbols(),
    ]);
    const meta   = sectorData.metadata?.indexName || index;
    const stocks = (sectorData.data||[])
      .filter(s=>s.symbol!==meta && fnoSymbols.has(s.symbol))
      .map(s=>({ symbol:s.symbol, lastPrice:s.lastPrice, change:s.change, pChange:s.pChange, open:s.open, dayHigh:s.dayHigh, dayLow:s.dayLow, previousClose:s.previousClose, totalTradedVolume:s.totalTradedVolume }))
      .sort((a,b)=>b.pChange-a.pChange);
    res.json({ index, stocks });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ── Pivot stocks for a single sector ─────────────────────────────────────────
app.get('/api/pivot-stocks', async (req, res) => {
  const index = req.query.index;
  if (!index) return res.status(400).json({ error:'index required' });
  try {
    const [sectorData, fnoSymbols] = await Promise.all([
      nseGet(`https://www.nseindia.com/api/equity-stockIndices?index=${encodeURIComponent(index)}`),
      getFNOSymbols(),
    ]);
    const meta      = sectorData.metadata?.indexName || index;
    const rawStocks = (sectorData.data||[]).filter(s=>s.symbol!==meta && fnoSymbols.has(s.symbol));
    const result    = rawStocks.map(calcPivotSignals).filter(Boolean).sort((a,b)=>b.pChange-a.pChange);
    res.json({ index, stocks:result, todayDate:'live', prevDate:'approx' });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ── Pivot stocks for ALL F&O — fetches each sector, merges & deduplicates ─────
// NOTE: SECURITIES IN F&O index does NOT return dayHigh/dayLow/open fields
// so we must use sector indices which always return full OHLC data
app.get('/api/pivot-all', async (req, res) => {
  try {
    const fnoSymbols = await getFNOSymbols();

    // Fetch all sectors in parallel
    const sectorResults = await Promise.allSettled(
      SECTORS.map(sector =>
        nseGet(`https://www.nseindia.com/api/equity-stockIndices?index=${encodeURIComponent(sector)}`)
          .then(data => {
            const meta = data.metadata?.indexName || sector;
            return (data.data||[]).filter(s => s.symbol !== meta && fnoSymbols.has(s.symbol) && s.lastPrice && s.dayHigh && s.dayLow);
          })
      )
    );

    // Merge, deduplicate by symbol
    const seen = new Set();
    const allStocks = [];
    sectorResults.forEach(r => {
      if (r.status === 'fulfilled') {
        r.value.forEach(s => {
          if (!seen.has(s.symbol)) {
            seen.add(s.symbol);
            allStocks.push(s);
          }
        });
      }
    });

    const result = allStocks.map(calcPivotSignals).filter(Boolean).sort((a,b)=>b.pChange-a.pChange);
    res.json({ stocks:result, todayDate:'live', prevDate:'approx', total:result.length });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// ── Money flow ────────────────────────────────────────────────────────────────
app.get('/api/money-flow', async (req, res) => {
  try {
    const fnoSymbols = await getFNOSymbols();
    const results = await Promise.allSettled(
      SECTORS.map(async sector => {
        const data   = await nseGet(`https://www.nseindia.com/api/equity-stockIndices?index=${encodeURIComponent(sector)}`);
        const meta   = data.metadata?.indexName || sector;
        const stocks = (data.data||[]).filter(s=>s.symbol!==meta && fnoSymbols.has(s.symbol));
        let posMF=0, negMF=0, totalMF=0;
        const stockFlows = stocks.map(s => {
          const tp  = (s.dayHigh+s.dayLow+s.lastPrice)/3;
          const mf  = tp*(s.totalTradedVolume||0);
          const dir = s.pChange>=0?'buy':'sell';
          if (dir==='buy') posMF+=mf; else negMF+=mf;
          totalMF += mf;
          return { symbol:s.symbol, lastPrice:s.lastPrice, pChange:s.pChange, tp:+tp.toFixed(2), volume:s.totalTradedVolume, mf:+mf.toFixed(0), dir };
        }).sort((a,b)=>b.mf-a.mf);
        const mfRatio = negMF>0?posMF/negMF:posMF>0?999:1;
        const mfi     = +(100-(100/(1+mfRatio))).toFixed(2);
        const netMF   = posMF-negMF;
        const mfPct   = totalMF>0?+((netMF/totalMF)*100).toFixed(2):0;
        return { sector, name:sector.replace('NIFTY ',''), mfi, mfPct, posMF:+posMF.toFixed(0), negMF:+negMF.toFixed(0), netMF:+netMF.toFixed(0), totalMF:+totalMF.toFixed(0), stockCount:stocks.length, buyCount:stockFlows.filter(s=>s.dir==='buy').length, sellCount:stockFlows.filter(s=>s.dir==='sell').length, stocks:stockFlows, sectorPct:data.metadata?.percChange??0 };
      })
    );
    res.json({ sectors: results.filter(r=>r.status==='fulfilled').map(r=>r.value).sort((a,b)=>b.mfi-a.mfi) });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

app.listen(PORT, () => console.log(`NSE Heatmap server running → http://localhost:${PORT}`));
