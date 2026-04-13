require('dotenv').config();

const express = require('express');
const fetch = require('node-fetch');
const cors = require('cors');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const DHAN_BASE = 'https://api.dhan.co/v2';
const DHAN_CLIENT_ID = process.env.DHAN_CLIENT_ID;
const DHAN_ACCESS_TOKEN = process.env.DHAN_ACCESS_TOKEN;

if (!DHAN_CLIENT_ID || !DHAN_ACCESS_TOKEN) {
  console.error('Missing DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN in environment');
  process.exit(1);
}

/**
 * IMPORTANT:
 * You must replace these security IDs with the exact IDs from Dhan instrument master.
 * The structure is correct, but IDs below are placeholders except where noted.
 */
const INDEX_MAP = {
  'NIFTY 50': { exchangeSegment: 'IDX_I', securityId: 13, instrument: 'INDEX' }, // Nifty commonly documented as 13
  'BANKNIFTY': { exchangeSegment: 'IDX_I', securityId: 25, instrument: 'INDEX' }, // verify from instrument master
  'FINNIFTY': { exchangeSegment: 'IDX_I', securityId: 27, instrument: 'INDEX' },  // verify
  'MIDCPNIFTY': { exchangeSegment: 'IDX_I', securityId: 442, instrument: 'INDEX' } // verify
};

/**
 * You need to fill these from Dhan instrument master.
 * Key = symbol, value = securityId and segment.
 */
const STOCKS = {
  RELIANCE: { exchangeSegment: 'NSE_EQ', securityId: 2885, instrument: 'EQUITY' }, // example, verify
  TCS: { exchangeSegment: 'NSE_EQ', securityId: 11536, instrument: 'EQUITY' },      // example, verify
  INFY: { exchangeSegment: 'NSE_EQ', securityId: 1594, instrument: 'EQUITY' }       // example, verify
};

/**
 * Since Dhan does NOT give NSE sector constituent endpoints like your current server,
 * you must maintain sector/index baskets locally.
 */
const BASKETS = {
  'NIFTY 50': ['RELIANCE', 'TCS', 'INFY'],
  'NIFTY IT': ['TCS', 'INFY'],
  'NIFTY BANK': [],
  'NIFTY PHARMA': [],
  'NIFTY AUTO': [],
  'NIFTY FMCG': [],
  'NIFTY METAL': [],
  'NIFTY REALTY': [],
  'NIFTY ENERGY': [],
  'NIFTY MEDIA': [],
  'NIFTY PSU BANK': [],
  'NIFTY PRIVATE BANK': [],
  'NIFTY FIN SERVICE': []
};

const SECTORS = [
  'NIFTY BANK',
  'NIFTY IT',
  'NIFTY PHARMA',
  'NIFTY AUTO',
  'NIFTY FMCG',
  'NIFTY METAL',
  'NIFTY REALTY',
  'NIFTY ENERGY',
  'NIFTY MEDIA',
  'NIFTY PSU BANK',
  'NIFTY PRIVATE BANK',
  'NIFTY FIN SERVICE'
];

async function dhanPost(endpoint, body) {
  const res = await fetch(`${DHAN_BASE}${endpoint}`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'access-token': DHAN_ACCESS_TOKEN,
      'client-id': DHAN_CLIENT_ID
    },
    body: JSON.stringify(body)
  });

  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    throw new Error(`Dhan non-JSON response from ${endpoint}: ${text}`);
  }

  if (!res.ok || json.status !== 'success') {
    throw new Error(`Dhan API error on ${endpoint}: ${JSON.stringify(json)}`);
  }

  return json;
}

function buildMarketFeedPayload(symbols) {
  const grouped = {};

  for (const sym of symbols) {
    const meta = STOCKS[sym] || INDEX_MAP[sym];
    if (!meta) continue;
    if (!grouped[meta.exchangeSegment]) grouped[meta.exchangeSegment] = [];
    grouped[meta.exchangeSegment].push(Number(meta.securityId));
  }

  return grouped;
}

function invertPayloadMap(symbols) {
  const map = {};
  for (const sym of symbols) {
    const meta = STOCKS[sym] || INDEX_MAP[sym];
    if (!meta) continue;
    map[`${meta.exchangeSegment}:${meta.securityId}`] = sym;
  }
  return map;
}

async function getOhlcQuotes(symbols) {
  const payload = buildMarketFeedPayload(symbols);
  const inv = invertPayloadMap(symbols);

  const resp = await dhanPost('/marketfeed/ohlc', payload);
  const out = [];

  for (const [segment, ids] of Object.entries(resp.data || {})) {
    for (const [securityId, row] of Object.entries(ids || {})) {
      const sym = inv[`${segment}:${securityId}`];
      if (!sym) continue;

      out.push({
        symbol: sym,
        exchangeSegment: segment,
        securityId: Number(securityId),
        lastPrice: row.last_price || 0,
        open: row.open || 0,
        dayHigh: row.high || 0,
        dayLow: row.low || 0,
        previousClose: row.close || 0
      });
    }
  }

  return out;
}

async function getQuoteSnapshots(symbols) {
  const payload = buildMarketFeedPayload(symbols);
  const inv = invertPayloadMap(symbols);

  const resp = await dhanPost('/marketfeed/quote', payload);
  const out = [];

  for (const [segment, ids] of Object.entries(resp.data || {})) {
    for (const [securityId, row] of Object.entries(ids || {})) {
      const sym = inv[`${segment}:${securityId}`];
      if (!sym) continue;

      out.push({
        symbol: sym,
        exchangeSegment: segment,
        securityId: Number(securityId),
        lastPrice: row.last_price || 0,
        open: row.open || 0,
        dayHigh: row.high || 0,
        dayLow: row.low || 0,
        previousClose: row.close || 0,
        change: row.last_price && row.close ? +(row.last_price - row.close).toFixed(2) : 0,
        pChange: row.last_price && row.close ? +(((row.last_price - row.close) / row.close) * 100).toFixed(2) : 0,
        totalTradedVolume: row.volume || 0
      });
    }
  }

  return out;
}

function computePivots(high, low, close) {
  const pp = (high + low + close) / 3;
  return {
    pp: +pp.toFixed(2),
    r1: +(2 * pp - low).toFixed(2),
    r2: +(pp + high - low).toFixed(2),
    s1: +(2 * pp - high).toFixed(2),
    s2: +(pp - high + low).toFixed(2)
  };
}

function calcPivotSignals(s) {
  const ltp = s.lastPrice;
  if (!ltp || !s.dayHigh || !s.dayLow || !s.previousClose) return null;

  const todayP = computePivots(s.dayHigh, s.dayLow, s.previousClose);

  // Approximation because Dhan snapshot does not directly give the previous day full pivot source here
  const prevHigh = Math.max(s.open || s.previousClose, s.previousClose);
  const prevLow = Math.min(s.open || s.previousClose, s.previousClose);
  const prevP = computePivots(prevHigh, prevLow, s.previousClose);

  const ltpAboveTodayR1 = ltp > todayP.r1;
  const ltpBelowTodayS1 = ltp < todayP.s1;
  const r1Rising = todayP.r1 > prevP.r1;
  const s1Falling = todayP.s1 < prevP.s1;

  return {
    symbol: s.symbol,
    lastPrice: ltp,
    change: s.change,
    pChange: s.pChange,
    dayHigh: s.dayHigh,
    dayLow: s.dayLow,
    previousClose: s.previousClose,
    totalTradedVolume: s.totalTradedVolume,
    open: s.open,
    today: { ...todayP, brokeR1: ltpAboveTodayR1, brokeS1: ltpBelowTodayS1 },
    prev: { ...prevP },
    r1Rising,
    s1Falling,
    brokeBothR1: ltpAboveTodayR1 && r1Rising,
    brokeBothS1: ltpBelowTodayS1 && s1Falling,
    breakTime: { r1: null, s1: null }
  };
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function getOptionChain(symbol, expiry) {
  const meta = INDEX_MAP[symbol] || STOCKS[symbol];
  if (!meta) throw new Error(`Unknown symbol: ${symbol}`);

  const expiryResp = await dhanPost('/optionchain/expirylist', {
    UnderlyingScrip: Number(meta.securityId),
    UnderlyingSeg: meta.exchangeSegment
  });

  const expiries = expiryResp.data || [];
  const chosenExpiry = expiry || expiries[0];
  if (!chosenExpiry) throw new Error(`No expiries found for ${symbol}`);

  // Dhan option chain unique request limit is strict, so keep calls controlled
  await sleep(350);

  const chainResp = await dhanPost('/optionchain', {
    UnderlyingScrip: Number(meta.securityId),
    UnderlyingSeg: meta.exchangeSegment,
    Expiry: chosenExpiry
  });

  const data = chainResp.data || {};
  const oc = data.oc || {};
  const spot = data.last_price || 0;

  const strikes = Object.keys(oc).map(Number).sort((a, b) => a - b);
  if (!strikes.length) throw new Error(`No option chain rows for ${symbol}`);

  const atm = strikes.reduce((prev, curr) =>
    Math.abs(curr - spot) < Math.abs(prev - spot) ? curr : prev,
    strikes[0]
  );

  const atmIdx = strikes.indexOf(atm);
  const selectedStrikes = strikes.slice(Math.max(0, atmIdx - 5), Math.min(strikes.length, atmIdx + 6));

  const rows = selectedStrikes.map(strike => {
    const row = oc[String(strike)] || oc[String(strike.toFixed(6))] || {};
    const ce = row.ce || {};
    const pe = row.pe || {};

    return {
      strike,
      isATM: strike === atm,
      label: strike === atm ? 'ATM' : strike < atm ? 'ITM' : 'OTM',
      CE: {
        oi: ce.oi || 0,
        oiChange: ((ce.oi || 0) - (ce.previous_oi || 0)),
        oiChangePct: ce.previous_oi ? +((((ce.oi || 0) - ce.previous_oi) / ce.previous_oi) * 100).toFixed(2) : 0,
        ltp: ce.last_price || 0,
        volume: ce.volume || 0,
        iv: ce.implied_volatility || 0,
        bid: ce.top_bid_price || 0,
        ask: ce.top_ask_price || 0
      },
      PE: {
        oi: pe.oi || 0,
        oiChange: ((pe.oi || 0) - (pe.previous_oi || 0)),
        oiChangePct: pe.previous_oi ? +((((pe.oi || 0) - pe.previous_oi) / pe.previous_oi) * 100).toFixed(2) : 0,
        ltp: pe.last_price || 0,
        volume: pe.volume || 0,
        iv: pe.implied_volatility || 0,
        bid: pe.top_bid_price || 0,
        ask: pe.top_ask_price || 0
      }
    };
  });

  const totalCEoi = rows.reduce((s, r) => s + r.CE.oi, 0);
  const totalPEoi = rows.reduce((s, r) => s + r.PE.oi, 0);
  const pcr = totalCEoi > 0 ? +(totalPEoi / totalCEoi).toFixed(2) : 0;

  let maxPainStrike = atm;
  let minLoss = Infinity;
  selectedStrikes.forEach(testStrike => {
    const loss = rows.reduce((sum, r) => {
      const ceLoss = r.CE.oi * Math.max(0, r.strike - testStrike);
      const peLoss = r.PE.oi * Math.max(0, testStrike - r.strike);
      return sum + ceLoss + peLoss;
    }, 0);
    if (loss < minLoss) {
      minLoss = loss;
      maxPainStrike = testStrike;
    }
  });

  return {
    symbol,
    spot,
    expiry: chosenExpiry,
    expiries,
    atm,
    pcr,
    maxPain: maxPainStrike,
    rows
  };
}

/* ROUTES */

app.get('/api/indices', async (req, res) => {
  try {
    const rows = await getOhlcQuotes(Object.keys(INDEX_MAP));
    res.json({ data: rows });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/heatmap', async (req, res) => {
  const index = req.query.index || 'NIFTY 50';
  try {
    const symbols = BASKETS[index] || [];
    const indexMeta = INDEX_MAP[index] ? [index] : [];
    const rows = await getQuoteSnapshots([...symbols, ...indexMeta]);

    const indexRow = rows.find(r => r.symbol === index);
    const stocks = rows.filter(r => r.symbol !== index);

    res.json({
      metadata: indexRow ? {
        indexName: index,
        last: indexRow.lastPrice,
        change: indexRow.change,
        percChange: indexRow.pChange,
        open: indexRow.open,
        high: indexRow.dayHigh,
        low: indexRow.dayLow,
        previousClose: indexRow.previousClose
      } : { indexName: index },
      data: [
        ...(indexRow ? [indexRow] : []),
        ...stocks
      ]
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/gainers-losers', async (req, res) => {
  try {
    const symbols = [...new Set(Object.values(BASKETS).flat())];
    const rows = await getQuoteSnapshots(symbols);

    const gainers = [...rows].sort((a, b) => b.pChange - a.pChange).slice(0, 10);
    const losers = [...rows].sort((a, b) => a.pChange - b.pChange).slice(0, 10);

    res.json({ gainers, losers });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/sectors', async (req, res) => {
  try {
    const sectorRows = [];

    for (const sector of SECTORS) {
      const symbols = BASKETS[sector] || [];
      if (!symbols.length) {
        sectorRows.push({
          name: sector.replace('NIFTY ', ''),
          fullName: sector,
          pChange: 0,
          last: 0,
          change: 0
        });
        continue;
      }

      const rows = await getQuoteSnapshots(symbols);
      const avgPct = rows.length ? rows.reduce((s, r) => s + r.pChange, 0) / rows.length : 0;
      const avgLast = rows.length ? rows.reduce((s, r) => s + r.lastPrice, 0) / rows.length : 0;

      sectorRows.push({
        name: sector.replace('NIFTY ', ''),
        fullName: sector,
        pChange: +avgPct.toFixed(2),
        last: +avgLast.toFixed(2),
        change: 0
      });
    }

    res.json({
      sectors: sectorRows.sort((a, b) => b.pChange - a.pChange)
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/sector-stocks', async (req, res) => {
  const index = req.query.index;
  if (!index) return res.status(400).json({ error: 'index required' });

  try {
    const symbols = BASKETS[index] || [];
    const rows = await getQuoteSnapshots(symbols);
    res.json({
      index,
      stocks: rows.sort((a, b) => b.pChange - a.pChange)
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/pivot-stocks', async (req, res) => {
  const index = req.query.index;
  if (!index) return res.status(400).json({ error: 'index required' });

  try {
    const symbols = BASKETS[index] || [];
    const rows = await getQuoteSnapshots(symbols);
    const stocks = rows.map(calcPivotSignals).filter(Boolean).sort((a, b) => b.pChange - a.pChange);

    res.json({
      index,
      stocks,
      todayDate: 'live',
      prevDate: 'approx'
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/pivot-all', async (req, res) => {
  try {
    const symbols = [...new Set(SECTORS.flatMap(s => BASKETS[s] || []))];
    const rows = await getQuoteSnapshots(symbols);
    const stocks = rows.map(calcPivotSignals).filter(Boolean).sort((a, b) => b.pChange - a.pChange);

    res.json({
      stocks,
      todayDate: 'live',
      prevDate: 'approx',
      total: stocks.length
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/money-flow', async (req, res) => {
  try {
    const sectors = [];

    for (const sector of SECTORS) {
      const symbols = BASKETS[sector] || [];
      const rows = await getQuoteSnapshots(symbols);

      let posMF = 0, negMF = 0, totalMF = 0;
      const stockFlows = rows.map(s => {
        const tp = (s.dayHigh + s.dayLow + s.lastPrice) / 3;
        const mf = tp * (s.totalTradedVolume || 0);
        const dir = s.pChange >= 0 ? 'buy' : 'sell';

        if (dir === 'buy') posMF += mf;
        else negMF += mf;
        totalMF += mf;

        return {
          symbol: s.symbol,
          lastPrice: s.lastPrice,
          pChange: s.pChange,
          tp: +tp.toFixed(2),
          volume: s.totalTradedVolume,
          mf: +mf.toFixed(0),
          dir
        };
      }).sort((a, b) => b.mf - a.mf);

      const mfRatio = negMF > 0 ? posMF / negMF : posMF > 0 ? 999 : 1;
      const mfi = +(100 - (100 / (1 + mfRatio))).toFixed(2);
      const netMF = posMF - negMF;
      const mfPct = totalMF > 0 ? +((netMF / totalMF) * 100).toFixed(2) : 0;

      sectors.push({
        sector,
        name: sector.replace('NIFTY ', ''),
        mfi,
        mfPct,
        posMF: +posMF.toFixed(0),
        negMF: +negMF.toFixed(0),
        netMF: +netMF.toFixed(0),
        totalMF: +totalMF.toFixed(0),
        stockCount: rows.length,
        buyCount: stockFlows.filter(s => s.dir === 'buy').length,
        sellCount: stockFlows.filter(s => s.dir === 'sell').length,
        stocks: stockFlows,
        sectorPct: rows.length ? +(rows.reduce((s, r) => s + r.pChange, 0) / rows.length).toFixed(2) : 0
      });
    }

    res.json({
      sectors: sectors.sort((a, b) => b.mfi - a.mfi)
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/option-chain', async (req, res) => {
  const symbol = (req.query.symbol || 'NIFTY 50').toUpperCase();
  const normalized =
    symbol === 'NIFTY' ? 'NIFTY 50' :
    symbol === 'BANKNIFTY' ? 'BANKNIFTY' :
    symbol === 'FINNIFTY' ? 'FINNIFTY' :
    symbol === 'MIDCPNIFTY' ? 'MIDCPNIFTY' : symbol;

  try {
    const data = await getOptionChain(normalized, req.query.expiry || '');
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/fno-list', async (req, res) => {
  try {
    res.json({ symbols: Object.keys(STOCKS).sort() });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/pcr-batch', async (req, res) => {
  const symbols = (req.query.symbols || '').split(',').filter(Boolean).slice(0, 10);

  try {
    const pcr = {};
    for (const sym of symbols) {
      const normalized =
        sym === 'NIFTY' ? 'NIFTY 50' :
        sym === 'BANKNIFTY' ? 'BANKNIFTY' :
        sym === 'FINNIFTY' ? 'FINNIFTY' :
        sym === 'MIDCPNIFTY' ? 'MIDCPNIFTY' : sym;

      const data = await getOptionChain(normalized);
      pcr[sym] = data.pcr;
      await sleep(350);
    }

    res.json({ pcr });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/oi-batch', async (req, res) => {
  const symbols = (req.query.symbols || '').split(',').filter(Boolean).slice(0, 10);

  try {
    const oi = {};

    for (const sym of symbols) {
      const normalized =
        sym === 'NIFTY' ? 'NIFTY 50' :
        sym === 'BANKNIFTY' ? 'BANKNIFTY' :
        sym === 'FINNIFTY' ? 'FINNIFTY' :
        sym === 'MIDCPNIFTY' ? 'MIDCPNIFTY' : sym;

      const data = await getOptionChain(normalized);
      const atmRow = data.rows.find(r => r.isATM) || data.rows[0];

      oi[sym] = {
        sym,
        spot: data.spot,
        atm: data.atm,
        expiry: data.expiry,
        ce: {
          oi: atmRow?.CE?.oi || 0,
          oiChange: atmRow?.CE?.oiChange || 0,
          oiChgPct: atmRow?.CE?.oiChangePct || 0,
          ltp: atmRow?.CE?.ltp || 0,
          iv: atmRow?.CE?.iv || 0
        },
        pe: {
          oi: atmRow?.PE?.oi || 0,
          oiChange: atmRow?.PE?.oiChange || 0,
          oiChgPct: atmRow?.PE?.oiChangePct || 0,
          ltp: atmRow?.PE?.ltp || 0,
          iv: atmRow?.PE?.iv || 0
        },
        total: {
          ceOi: data.rows.reduce((s, r) => s + r.CE.oi, 0),
          peOi: data.rows.reduce((s, r) => s + r.PE.oi, 0),
          ceOiChg: data.rows.reduce((s, r) => s + r.CE.oiChange, 0),
          peOiChg: data.rows.reduce((s, r) => s + r.PE.oiChange, 0)
        },
        pcr: data.pcr
      };

      await sleep(350);
    }

    res.json({ oi });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Dhan Heatmap server running -> http://localhost:${PORT}`);
});
