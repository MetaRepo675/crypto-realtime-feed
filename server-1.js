// ============================================================
// Project 1: Real-time Crypto Price Tracker via WebSocket
// Stack: Node.js + ws + Express
// ============================================================

const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const http = require('http');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// ── Simulated exchange feed (replace with real Binance/OKX WS in prod) ──
const PAIRS = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT', 'XRP/USDT'];
const prices = {
  'BTC/USDT': 65000,
  'ETH/USDT': 3200,
  'BNB/USDT': 580,
  'SOL/USDT': 145,
  'XRP/USDT': 0.52,
};

// Simulate price fluctuation (±0.3% per tick)
function simulateMarket() {
  PAIRS.forEach((pair) => {
    const change = (Math.random() - 0.5) * 0.006;
    prices[pair] = parseFloat((prices[pair] * (1 + change)).toFixed(8));
  });
}

// Build tick payload
function buildTick() {
  return {
    type: 'PRICE_UPDATE',
    ts: Date.now(),
    data: PAIRS.map((pair) => ({
      pair,
      price: prices[pair],
      change24h: parseFloat(((Math.random() - 0.4) * 10).toFixed(2)), // mock
    })),
  };
}

// ── Client subscription management ──
const subscriptions = new Map(); // ws → Set<pair>

function broadcast(payload) {
  const msg = JSON.stringify(payload);
  wss.clients.forEach((client) => {
    if (client.readyState !== WebSocket.OPEN) return;
    const subs = subscriptions.get(client);
    // Filter: only send pairs the client subscribed to
    const filtered = {
      ...payload,
      data: payload.data.filter((d) => !subs || subs.size === 0 || subs.has(d.pair)),
    };
    if (filtered.data.length > 0) client.send(JSON.stringify(filtered));
  });
}

// ── WebSocket connection handler ──
wss.on('connection', (ws, req) => {
  console.log(`[WS] Client connected: ${req.socket.remoteAddress}`);
  subscriptions.set(ws, new Set()); // empty = all pairs

  ws.send(JSON.stringify({ type: 'CONNECTED', pairs: PAIRS }));

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);

      if (msg.type === 'SUBSCRIBE') {
        const subs = subscriptions.get(ws);
        (msg.pairs || []).forEach((p) => { if (PAIRS.includes(p)) subs.add(p); });
        ws.send(JSON.stringify({ type: 'SUBSCRIBED', pairs: [...subs] }));
      }

      if (msg.type === 'UNSUBSCRIBE') {
        const subs = subscriptions.get(ws);
        (msg.pairs || []).forEach((p) => subs.delete(p));
        ws.send(JSON.stringify({ type: 'UNSUBSCRIBED', pairs: [...subs] }));
      }

      if (msg.type === 'PING') {
        ws.send(JSON.stringify({ type: 'PONG', ts: Date.now() }));
      }
    } catch {
      ws.send(JSON.stringify({ type: 'ERROR', message: 'Invalid JSON' }));
    }
  });

  ws.on('close', () => {
    subscriptions.delete(ws);
    console.log('[WS] Client disconnected');
  });

  ws.on('error', (err) => console.error('[WS] Error:', err.message));
});

// ── Market feed loop (every 500ms) ──
setInterval(() => {
  simulateMarket();
  broadcast(buildTick());
}, 500);

// ── REST fallback: snapshot ──
app.get('/api/prices', (_req, res) => {
  res.json({ ts: Date.now(), prices });
});

app.get('/health', (_req, res) => res.json({ status: 'ok', clients: wss.clients.size }));

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => console.log(`[SERVER] Listening on :${PORT}`));

module.exports = { app, server };
