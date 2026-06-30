# poly-sentient-market: Kalshi → Polymarket conversion plan

**Supersedes** `PORT-FROM-POLY-DASH.md` (which was written in the wrong direction — Polymarket→Kalshi). This is the corrected spec.

## Goal

Convert `poly-sentient-market` — a complete, production-class **autonomous** trading platform originally built for Kalshi — into a **functional Polymarket** autonomous trader. It runs **independently, 24/7, alongside** `~/poly-dash` (Ray's live *manual* Polymarket dashboard), as its **own process with its own wallet**.

Two hard requirements from the owner:
1. **Shared source of truth for streaming data.** Do NOT open new upstream connections (Polymarket CLOB WS, Coinbase, Chainlink, BTC feeds). poly-dash already maintains them — poly-sentient is a **receiver on the same feed**. One set of streams, two consumers.
2. **"Shoe-in" poly-dash's proven logic** — its window dynamics, order execution, and manual-trading functions — into poly-sentient's existing autonomous engine, replacing the Kalshi equivalents. Keep the sentient brain; swap its market I/O to Polymarket.

## Architecture (two independent processes)

```
            ┌─────────────────────────── poly-dash (Rust, :4300) ───────────────────────────┐
 upstream → │  CLOB market WS · user WS · Binance/Pyth/Kraken/Coinbase · Chainlink · Gamma   │
            │  → btc_aggregator (canonical BTC) → window engine (5m/15m/1h) → AppState        │
            │  → broadcast({type,data}) over /ws  ·  manual trading API (/api/buy …)          │
            └───────────────┬───────────────────────────────────────────────────────────────┘
                            │  SHARED read-only data feed (subscribe, don't duplicate)
                            ▼
            ┌─────────────────────── poly-sentient-market (Node, autonomous) ────────────────┐
            │  feed receiver  →  ServerAgent loop  →  agent pipeline (Markov→…→Execution)      │
            │  →  PolymarketExecutor (its OWN wallet)  →  signed CLOB orders                   │
            │  Next.js dashboard + SSE (manual capability retained)                            │
            └────────────────────────────────────────────────────────────────────────────────┘
```

- **Data** is shared (poly-dash → poly-sentient, read-only).
- **Execution** is independent (poly-sentient signs/sends its own orders with its own wallet — it never trades through poly-dash's wallet).

## What STAYS (poly-sentient's autonomous brain — market-agnostic)

Keep verbatim; these don't know or care what venue they trade:
- `lib/server-agent.ts` / `server-agent-hourly.ts` — the `ServerAgent` lifecycle (schedule → poll → pipeline → place → repeat), trade log, KV persistence, SSE.
- `lib/agents/*` — `index.ts` (pipeline DAG), `markov.ts`, `sentiment.ts`, `probability-model.ts`, `risk-manager.ts`, `grok-trading-agent.ts`, `execution.ts` (decision-level: aggressive/passive limit pricing), `price-feed.ts`, `market-discovery.ts`.
- `lib/llm-client.ts`, `lib/indicators.ts`, `lib/markov/*`, `lib/trade-log.ts`, `lib/agent-store*.ts`, `lib/pipeline-lock.ts`, the whole Next.js UI + `/api/agent/*` SSE.

These only need their **input shape** (a "market" with `yes/no` bid/ask cents, strike, minutesUntilExpiry) and their **one output sink** (`placeOrder`) re-pointed.

## The three seams to swap

### Seam 1 — Data acquisition  →  shared poly-dash feed
Today: `app/api/markets`, `app/api/orderbook`, `app/api/btc-price`, `app/api/market-quote`, `lib/kalshi.ts` (REST polling of Kalshi + Coinbase/CMC).
Becomes: **`lib/polymarket/feed.ts`** — a WS client that subscribes to poly-dash's broadcast and maintains a local mirror. poly-dash already emits everything the pipeline needs:

| Pipeline input | poly-dash broadcast `type` | maps to |
|---|---|---|
| BTC quote/price | `pretick` / `prices` / canonical | live BTC + microstructure |
| active market (yes/no→**up/down** bid/ask cents) | `window` / `prices` (`upBid/upAsk/downBid/downAsk`) | the current 5m/15m/1h window |
| strike | `strike` (`btcOpen`) | window strike |
| orderbook | `book` (`bids/asks`, cents) | L2 ladder per token |
| minutesUntilExpiry / window close | `window` (`windowStart`, `secInWindow`) | derived |
| candles (15m/1h/4h) | poly-dash `chart_log` / Binance REST (shared) | OHLCV |

Result: `MarketDiscovery`/`PriceFeed` read from the mirror instead of fetching. **No new upstream sockets.**

### Seam 2 — Order execution / account  →  independent Polymarket executor
Today: `lib/kalshi-trade.ts` (`placeOrder`/`sellOrder`/`limitSellOrder`/`cancelOrder`/`getBalance`/`getPositions`), `lib/kalshi-auth.ts` (RSA-PSS).
Becomes: **`lib/polymarket/exec.ts`** — places **signed Polymarket CLOB orders with poly-sentient's own wallet**, porting poly-dash's execution discipline:
- tick-exact price alignment, GTC/FAK order types, smart (book-walk) + split execution,
- the **window guard** (`check_order_window_guard`: slug-aligned, end-of-window buffer, token match),
- `$1`-CLOB-min via bump-qty-not-price.
- `side: yes/no` → Polymarket `up/down` token id; price cents → `Decimal` 0–1.

### Seam 3 — Window/ticker math  →  Polymarket windows (from the shared feed)
Today: `lib/kalshi.ts` (`KXBTC15M`/`KXBTCD` ET tickers, 15-min boundaries), `getWindowClose()` in `server-agent.ts`.
Becomes: window identity comes **over the feed** — poly-dash already runs the deterministic two-phase 5m/15m/1h rollover and broadcasts the live `condition_id` + up/down token ids + `windowStart`/`secInWindow`/strike. poly-sentient just consumes the active window. (poly-dash's slug math is ported as a fallback only if the feed is briefly unavailable.)

## Data-model rename (Kalshi → Polymarket)
- `KalshiMarket` → `PolyMarket` (`yes_ask/no_ask/yes_bid/no_bid` kept as `up_ask/down_ask/…`; add `condition_id`, `up_token_id`, `down_token_id`, `slug`).
- `yes/no` → `up/down` throughout the pipeline's market type (decision logic unchanged — it's just two complementary legs).
- Keep **cents** as the price unit end-to-end (Polymarket is 0–1; convert at the executor boundary only).
- Replace the Kalshi maker-fee formula with Polymarket's `fee_rate_bps` (from the window/feed).

## Strip list (remove or repoint)
- **Delete:** `lib/kalshi-auth.ts`, `lib/kalshi-trade.ts`, `lib/kalshi.ts`, `lib/kalshi-credentials.ts`; `app/api/kalshi-connect`, `app/api/kxbtcd-debug`, `app/api/market-quote`; Kalshi maker-fee + `KXBTC*` math.
- **Repoint to Polymarket:** `app/api/markets`, `app/api/orderbook`, `app/api/btc-price`, `app/api/place-order`, `app/api/sell-order`, `app/api/limit-sell-order`, `app/api/cancel-order`, `app/api/positions`, `app/api/balance` → the feed mirror / `PolymarketExecutor`.
- **Settings:** swap Kalshi-credential upload for Polymarket wallet/API config.

## Implementation phases
1. **Feed receiver** (`lib/polymarket/feed.ts`) + the `PolyMarket` data model; prove the pipeline runs on shared data with execution stubbed (dry-run).
2. **Executor** (`lib/polymarket/exec.ts`) with the window guard; wire `server-agent.placeOrder()` → executor; paper-mode first.
3. **Strip Kalshi**, rename types, repoint API routes, update settings UI.
4. **Deploy** the autonomous process at `s.wersdfwer.com` (infra already wired: nginx → :4320, PM2, DNS) and run alongside poly-dash.

## OPEN DECISIONS (confirm before build)
1. **How poly-sentient receives the shared feed** — (a) subscribe to poly-dash's existing `/ws` on `:4300`, or (b) add a dedicated read-only broadcast port to poly-dash for consumers (cleaner separation; no coupling to the dashboard socket).
2. **How poly-sentient signs/sends its own orders** — (a) a minimal **Rust exec microservice** forked from poly-dash (literally reuses `polyfill-rs` + the guard; poly-sentient POSTs to it with its own wallet), or (b) the official **`@polymarket/clob-client` TS SDK** natively in the Node app (no Rust dep, single language, but re-implements the signing poly-dash already has).

## DECISIONS (LOCKED 2026-06-30)
1. Feed: poly-sentient subscribes to poly-dash existing /ws on :4300.
2. Executor: minimal Rust exec microservice forked from poly-dash (polyfill-rs), poly-sentient own wallet, on 127.0.0.1:4321.
