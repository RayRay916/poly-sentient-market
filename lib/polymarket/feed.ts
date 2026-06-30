// Shared-feed receiver: the autonomous trader's window of poly-dash's world.
//
// poly-dash (the live manual Polymarket dashboard) already maintains every
// upstream stream (CLOB WS, Coinbase/Pyth/Kraken/Chainlink BTC, Gamma) and the
// deterministic 5m/15m/1h window rollover. This module is a READ-ONLY consumer:
//
//   • ws://127.0.0.1:4300/ws   — real-time: prices{,15m,1h}, canonical/btc, book,
//                                 balance, positions, window{,15m,1h} (rollover)
//   • http://127.0.0.1:4300/api/status — full window structure (token ids, tick
//                                 size, neg-risk, fee, strike); bootstrap + 30s
//                                 safety poll + an immediate refetch on rollover.
//
// No upstream sockets are duplicated. The pipeline reads `market(tf)` (a
// PolyMarket with yes/no aliases) and `quote()` exactly as it read Kalshi data.

import type { PolyBook, PolyMarket, Timeframe } from './types';
import { TF_INTERVAL_SEC } from './types';
import type { BTCQuote } from '../types';

const DASH_HTTP = process.env.POLY_DASH_URL ?? 'http://127.0.0.1:4300';
const DASH_WS = process.env.POLY_DASH_WS ?? 'ws://127.0.0.1:4300/ws';

const num = (v: unknown): number => {
  const n = typeof v === 'number' ? v : parseFloat(String(v ?? ''));
  return Number.isFinite(n) ? n : 0;
};
const str = (v: unknown): string => (typeof v === 'string' ? v : String(v ?? ''));

class PolyFeed {
  private ws: WebSocket | null = null;
  private started = false;
  private windows: Record<Timeframe, PolyMarket | null> = { '5m': null, '15m': null, '1h': null };
  private books = new Map<string, PolyBook>();
  private positionsRaw: Record<string, unknown> = {};
  private btcQuote: BTCQuote = {
    price: 0,
    percent_change_1h: 0,
    percent_change_24h: 0,
    volume_24h: 0,
    market_cap: 0,
    last_updated: new Date(0).toISOString(),
  };
  private balanceUsdc = 0;
  private statusTimer: ReturnType<typeof setInterval> | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private lastBootstrapAt = 0;

  /** Idempotently begin consuming the feed. Safe to call from many entry points. */
  start(): void {
    if (this.started) return;
    this.started = true;
    void this.bootstrap();
    this.connect();
    this.statusTimer = setInterval(() => void this.bootstrap(), 30_000);
  }

  // ── Public reads ──────────────────────────────────────────────────────────
  market(tf: Timeframe): PolyMarket | null {
    return this.windows[tf];
  }
  quote(): BTCQuote {
    return this.btcQuote;
  }
  orderbook(tokenId: string): PolyBook | null {
    return this.books.get(tokenId) ?? null;
  }
  balanceUsd(): number {
    return this.balanceUsdc;
  }
  positions(): Record<string, unknown> {
    return this.positionsRaw;
  }
  isReady(tf: Timeframe = '5m'): boolean {
    const w = this.windows[tf];
    return !!w && w.upTokenId.length > 0;
  }

  // ── /api/status bootstrap (full window structure) ─────────────────────────
  private async bootstrap(): Promise<void> {
    this.lastBootstrapAt = Date.now();
    try {
      const res = await fetch(`${DASH_HTTP}/api/status`, { cache: 'no-store' });
      if (!res.ok) return;
      const j = (await res.json()) as Record<string, unknown>;
      this.applyStatusWindow('5m', j.window);
      this.applyStatusWindow('15m', j.window15m);
      this.applyStatusWindow('1h', j.window1h);
      const bal = j.usdcBalance ?? (j.balance as Record<string, unknown> | undefined)?.usdc;
      if (typeof bal === 'number') this.balanceUsdc = bal;
      if (j.canonical && typeof (j.canonical as Record<string, unknown>).price === 'number') {
        this.setBtc(num((j.canonical as Record<string, unknown>).price));
      }
    } catch {
      /* poly-dash momentarily unreachable — keep last-known; WS + next poll recover */
    }
  }

  private applyStatusWindow(tf: Timeframe, w: unknown): void {
    if (!w || typeof w !== 'object') return;
    const o = w as Record<string, unknown>;
    if (!o.upTokenId || str(o.upTokenId).length === 0) return;
    this.windows[tf] = this.toMarket(tf, o);
  }

  /** Build a PolyMarket from a poly-dash window object (status or `window*` ws). */
  private toMarket(tf: Timeframe, o: Record<string, unknown>): PolyMarket {
    const interval = TF_INTERVAL_SEC[tf];
    const secIn = num(o.secInWindow);
    const closeMs = Date.now() + Math.max(0, interval - secIn) * 1000;
    const up_bid = num(o.upBid);
    const up_ask = num(o.upAsk);
    const down_bid = num(o.downBid);
    const down_ask = num(o.downAsk);
    const slug = str(o.slug);
    return {
      platform: 'polymarket',
      tf,
      slug,
      ticker: slug,
      event_ticker: slug,
      title: slug,
      conditionId: str(o.conditionId),
      upTokenId: str(o.upTokenId),
      downTokenId: str(o.downTokenId),
      up_bid,
      up_ask,
      down_bid,
      down_ask,
      yes_bid: up_bid,
      yes_ask: up_ask,
      no_bid: down_bid,
      no_ask: down_ask,
      last_price: up_ask > 0 && up_bid > 0 ? Math.round((up_ask + up_bid) / 2) : up_ask || up_bid,
      strike: num(o.btcOpen),
      floor_strike: num(o.btcOpen),
      closeMs,
      close_time: new Date(closeMs).toISOString(),
      expiration_time: new Date(closeMs).toISOString(),
      secInWindow: secIn,
      status: str(o.status) || 'active',
      tickSize: num(o.tickSize) || 0.01,
      negRisk: o.negRisk === true,
      minOrderSize: num(o.minOrderSize) || 5,
      feeRateBps: num(o.feeRateBps) || 0,
    };
  }

  // ── /ws real-time ─────────────────────────────────────────────────────────
  private connect(): void {
    try {
      const ws = new WebSocket(DASH_WS);
      this.ws = ws;
      ws.onopen = () => {
        /* poly-dash pushes immediately; nothing to send */
      };
      ws.onmessage = (ev: MessageEvent) => this.onMessage(ev);
      ws.onclose = () => this.scheduleReconnect();
      ws.onerror = () => {
        try {
          ws.close();
        } catch {
          /* ignore */
        }
      };
    } catch {
      this.scheduleReconnect();
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, 2000);
  }

  private onMessage(ev: MessageEvent): void {
    let msg: { type?: string; data?: unknown };
    try {
      const raw = typeof ev.data === 'string' ? ev.data : String(ev.data);
      msg = JSON.parse(raw) as { type?: string; data?: unknown };
    } catch {
      return;
    }
    const t = msg.type ?? '';
    const d = (msg.data ?? {}) as Record<string, unknown>;
    switch (t) {
      case 'prices':
        this.applyPrices('5m', d);
        break;
      case 'prices15m':
        this.applyPrices('15m', d);
        break;
      case 'prices1h':
        this.applyPrices('1h', d);
        break;
      // Rollover — token ids/slug/strike changed. Refetch full structure (debounced).
      case 'window':
      case 'window15m':
      case 'window1h':
        if (Date.now() - this.lastBootstrapAt > 1500) void this.bootstrap();
        break;
      case 'strike':
        this.applyStrike('5m', d);
        break;
      case 'strike15m':
        this.applyStrike('15m', d);
        break;
      case 'strike1h':
        this.applyStrike('1h', d);
        break;
      case 'canonical':
        if (typeof d.price === 'number') this.setBtc(d.price);
        break;
      case 'btc':
        if (this.btcQuote.price === 0 && typeof d.mid === 'number') this.setBtc(d.mid);
        break;
      case 'book': {
        const tokenId = str(d.token_id);
        if (tokenId)
          this.books.set(tokenId, {
            tokenId,
            bids: (d.bids as [number, number][]) ?? [],
            asks: (d.asks as [number, number][]) ?? [],
            ts: num(d.ts),
          });
        break;
      }
      case 'balance':
        if (typeof d.usdc === 'number') this.balanceUsdc = d.usdc;
        break;
      case 'positions':
        this.positionsRaw = d;
        break;
      default:
        break;
    }
  }

  private applyPrices(tf: Timeframe, d: Record<string, unknown>): void {
    const w = this.windows[tf];
    if (!w) return;
    w.up_bid = w.yes_bid = num(d.upBid);
    w.up_ask = w.yes_ask = num(d.upAsk);
    w.down_bid = w.no_bid = num(d.downBid);
    w.down_ask = w.no_ask = num(d.downAsk);
    w.last_price = w.up_ask > 0 && w.up_bid > 0 ? Math.round((w.up_ask + w.up_bid) / 2) : w.up_ask || w.up_bid;
    const secIn = num(d.secInWindow);
    w.secInWindow = secIn;
    w.closeMs = Date.now() + Math.max(0, TF_INTERVAL_SEC[tf] - secIn) * 1000;
    w.close_time = w.expiration_time = new Date(w.closeMs).toISOString();
  }

  private applyStrike(tf: Timeframe, d: Record<string, unknown>): void {
    const w = this.windows[tf];
    if (!w || typeof d.btcOpen !== 'number') return;
    w.strike = w.floor_strike = d.btcOpen;
  }

  private setBtc(price: number): void {
    this.btcQuote = { ...this.btcQuote, price, last_updated: new Date().toISOString() };
  }
}

/** Process-wide singleton. */
export const polyFeed = new PolyFeed();
