// Polymarket data model for the autonomous trader.
//
// `PolyMarket` is the venue-neutral "market" the agent pipeline reasons over. To
// keep the existing pipeline (written for a yes/no market) UNTOUCHED, it carries
// both canonical Polymarket `up_*`/`down_*` fields AND `yes_*`/`no_*` aliases
// (yes = up, down = no). New execution code reads up/down + the token ids; the
// agents keep reading yes/no. All prices are integer cents 1..99.

import type { BTCQuote, OHLCVCandle } from '../types';

export type { BTCQuote, OHLCVCandle };

export type Timeframe = '5m' | '15m' | '1h';

export const TF_INTERVAL_SEC: Record<Timeframe, number> = {
  '5m': 300,
  '15m': 900,
  '1h': 3600,
};

export interface PolyMarket {
  platform: 'polymarket';
  tf: Timeframe;
  /** Polymarket slug, e.g. "btc-updown-5m-1782784200". Also exposed as ticker/event_ticker. */
  slug: string;
  ticker: string;        // = slug (pipeline reads market.ticker)
  event_ticker: string;  // = slug (windowKey)
  title: string;         // = slug
  conditionId: string;
  upTokenId: string;
  downTokenId: string;

  // Canonical Polymarket legs (cents 1..99)
  up_bid: number;
  up_ask: number;
  down_bid: number;
  down_ask: number;

  // Pipeline-compat aliases (yes = up, no = down) — agents read these unchanged.
  yes_bid: number;
  yes_ask: number;
  no_bid: number;
  no_ask: number;
  last_price: number;

  // Strike + timing
  strike: number;        // btcOpen (BTC price-to-beat)
  floor_strike: number;  // alias (MarketDiscovery reads floor_strike)
  closeMs: number;       // window close, epoch ms
  close_time: string;    // ISO (minutesUntilExpiry compat)
  expiration_time: string; // = close_time
  secInWindow: number;
  status: string;

  // Execution metadata (passed straight to poly-sentient-exec :4321)
  tickSize: number;
  negRisk: boolean;
  minOrderSize: number;
  feeRateBps: number;
}

/** A mirrored L2 book for one token: [priceCents, sizeShares][]. */
export interface PolyBook {
  tokenId: string;
  bids: [number, number][];
  asks: [number, number][];
  ts: number;
}
