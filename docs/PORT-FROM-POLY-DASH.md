# Porting the poly-dash backend into poly-sentient-market (Kalshi)

**Status:** port-prep reference · **Source:** `~/poly-dash` (Rust, single binary) · **Target:** `~/poly-sentient-market` (Next.js + Python, Kalshi-only)
**Scope of this doc:** the four backend subsystems requested — (1) WebSocket layer, (2) env credentials, (3) order execution, (4) window-transition logic — documented from the live poly-dash source, each followed by a concrete Kalshi mapping grounded in the fork's existing `lib/kalshi-*.ts`.

> poly-dash is a battle-tested Polymarket CLOB engine for 5m/15m/1h BTC up/down binary options. poly-sentient-market is the Kalshi fork. The goal is to lift poly-dash's proven real-time/rollover/execution machinery onto Kalshi's API surface (RSA-PSS REST + WS v2, `KXBTCD`/`KXBTC15M` event tickers) without re-deriving the hard-won edge cases.

---

## 0. Source architecture at a glance (poly-dash)

Single Rust binary (`axum` 0.8 + `tokio` + `polyfill-rs` for the Polymarket CLOB). Entry point `src/main.rs`:

- Loads `.env` with `dotenvy::dotenv_override()` (the `.env` file *wins* over inherited shell env — defends against a stale `POLY_API_SECRET` leaking in from the SSH session).
- Builds one `polyfill_rs::ClobClient` (the single signing/order surface), wrapped in `Arc<TradingClient>`.
- Creates one shared `Arc<AppState>` (an `RwLock<Inner>` + a `tokio::sync::broadcast` channel for browser push).
- Spawns ~15 independent `tokio` tasks (the task graph below), then serves the axum router on `127.0.0.1:4300` (local only; Cloudflare/nginx fronts it).

Task graph (all `tokio::spawn`ed from `main`):

| Task | File | Role |
|------|------|------|
| `ws_polymarket::run` | `src/ws_polymarket.rs` | **Market-data WS** — subscribes CLOB market channel, mirrors L2 books, throttled browser broadcast |
| `ws_user::run` | `src/ws_user.rs` | **User WS** — authenticated fills/orders, zero-reconnect window swap |
| `ws_binance/pyth/kraken::run` | `src/ws_*.rs` | BTC spot price feeds (3 independent WS) |
| `chainlink_poller::run` | `src/chainlink_poller.rs` | On-chain BTC/USD oracle poll (Polygon) |
| `btc_aggregator::run` | `src/btc_aggregator.rs` | Canonical BTC = median across fresh sources |
| `market::run` / `market_15m` / `market_1h` | `src/market*.rs` | **Window lifecycle** (one task per timeframe) |
| `deathbot/pattern_b/dipbot::run` | `src/*.rs` | Strategy bots (consume state, submit via `TradingClient`) |
| positions poller (inline) | `src/main.rs` | Data-API positions refresh every 500ms |
| balance poller (inline) | `src/main.rs` | `get_balance_allowance` every 1s |
| browser WS server | `src/ws_push.rs` | `/ws` — fan-out of the broadcast channel |

State shape (`src/state.rs`): `AppState { inner: RwLock<Inner>, browser_tx: broadcast::Sender<String>, chart_log }`. `Inner` holds three `WindowState` (5m/15m/1h), the BTC price structs, an L2 `BookMirror` per token (`BTreeMap<cents,size>`), positions, balance, open_orders, bot_state, and the order queue. **Everything the UI sees is pushed through `AppState::broadcast(type, data)`** which wraps `{type, data}` and sends to `browser_tx`.

---

## 1. WebSocket layer

poly-dash runs **three distinct WS roles**. Keep them conceptually separate when porting.

### 1a. Market-data WS (upstream) — `src/ws_polymarket.rs`

- **Endpoint:** `wss://ws-subscriptions-clob.polymarket.com/ws/market` (unauthenticated, public book data).
- **Connect:** `tokio_tungstenite::connect_async`, then **`set_nodelay(true)` on the raw TCP socket** — without this, small ticks get Nagle-batched up to 40ms, killing reaction time. (Port note: Node `ws` → `socket.setNoDelay(true)`.)
- **Subscribe message** (exact shape matters — server silently ignores malformed):
  ```json
  { "type":"market", "operation":"subscribe", "assets_ids":[...],
    "initial_dump":true, "custom_feature_enabled":true }
  ```
  Unsubscribe: same with `"operation":"unsubscribe"` and just `assets_ids`.
- **Subscription set:** the union of non-empty up/down token ids across **all three windows** (5m+15m+1h = up to 6 asset ids).
- **Inbound events parsed** (`parse_and_update`):
  - `book` (full snapshot): `{event_type:"book", asset_id, bids:[{price,size}], asks:[{price,size}]}` → `state.book_snapshot()` replaces the whole ladder.
  - `price_change` (incremental): `{event_type:"price_change", price_changes:[{asset_id, price, size, side, best_bid, best_ask}]}` → `state.book_apply_change()` per level + top-of-book update.
  - Prices are **stringified floats** → parse to f64 → store as **integer cents** (`(p*100).round() as u32`).
- **Two throttle gates** (this is the important bit — the upstream is ~1000–1500 msg/s):
  - `should_broadcast_book(token, now_ms)` — max one book broadcast per token per **100ms** (5 Hz/token).
  - `should_broadcast_prices(stream, tob, now_ms)` — only when top-of-book **changed**, or a **1000ms keepalive** elapsed (so `secInWindow` keeps advancing in the browser).
  - **The `AppState` mirror is always updated immediately; only browser *delivery* is throttled.** Strategies read fresh state; the browser is rate-limited.
- **Keepalive / liveness:** app-level `Ping` every **8s**; replies `Pong` to server pings.
- **Roll handling:** a 2s `chart_tick` diffs the current window token set vs the subscribed set; on change it sends unsubscribe(stale) + subscribe(new). Disconnect → reconnect after 2s, **without zeroing prices** (preserve last-known).

### 1b. User WS (authenticated) — `src/ws_user.rs`

- **Endpoint:** `wss://ws-subscriptions-clob.polymarket.com/ws/user`, authenticated via `polyfill_rs::WebSocketStream::new(URL).with_auth(creds)` then `subscribe_user_channel(condition_ids)`.
- **Carries:** `Trade` (fills) and `Order` (open-order lifecycle) messages.
  - Trade lifecycle is `Matched → Mined → Confirmed`; **only the first `Matched` is folded into positions** (else the same fill counts 3×). See `apply_trade`.
  - Order terminal statuses `FILLED`/`CANCELLED`/`MATCHED` remove from `open_orders`; everything else upserts.
- **Zero-reconnect window swap** (the key pattern): on rollover, **subscribe NEW first, then unsubscribe OLD** so there's no data gap at the boundary; clears stale `open_orders`. Window identity is tracked in locals (`w5`,`w15`) and compared against the wall clock every 500ms — never against state (which can be mid-transition).
- Cold-start barrier: waits up to ~2s for `market::run` to populate `condition_id` before falling back to a Gamma resolve.

### 1c. Browser push WS (downstream) — `src/ws_push.rs`

- **Endpoint:** `GET /ws` (axum `WebSocketUpgrade`). Each client `subscribe()`s to the `broadcast` channel.
- **Ping every 25s** (Cloudflare kills idle WS at ~100s).
- Handles broadcast **lag** explicitly: `RecvError::Lagged` → warn + resync (don't drop the client). This was a real bug — the naive match arm silently stalled the feed.
- Envelope is always `{ "type": "<msgtype>", "data": <payload> }`. Message types currently emitted: `window`/`window15m`/`window1h`, `strike*`, `prices*`, `book`, `chart`/`chart15m`, `positions`, `balance`, `open_orders`, `order`, `pretick`.

### → Kalshi mapping (1)

| poly-dash | Kalshi equivalent |
|-----------|-------------------|
| Market WS `…/ws/market`, `assets_ids` subscribe | **Kalshi WS v2** `wss://api.elections.kalshi.com/trade-api/ws/v2`. Subscribe via `{"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta","ticker_v2"],"market_tickers":[...]}}` |
| `book` / `price_change` events | `orderbook_snapshot` + `orderbook_delta` (yes/no sides, prices already in **cents 1–99**, so the `*100` conversion poly-dash does is *not* needed — Kalshi is natively integer cents) |
| User WS `…/ws/user` (fills/orders) | Kalshi `fill` channel on the same v2 socket (the WS connection itself is RSA-PSS authenticated — see §2). Replaces REST positions polling the fork does today |
| `set_nodelay(true)` | `socket.setNoDelay(true)` on the Node `ws` client |
| 5 Hz book / 1 Hz price throttle | keep verbatim — Kalshi `orderbook_delta` is also chatty |
| `/ws` browser push, `{type,data}` envelope | the fork currently uses **SSE** (`/api/agent/stream`, `lib/server-agent.ts`). Either (a) add a real `/ws` route, or (b) reuse SSE with the same `{type,data}` envelope. SSE is simpler in Next.js and already wired |

**Gap:** the fork has **no Kalshi market-data WebSocket today** — it polls REST (`lib/kalshi.ts` + the agent loop). This is the single biggest thing to port: a persistent authenticated WS that mirrors the orderbook into a server-side state object and pushes deltas to the browser, instead of per-tick REST.

---

## 2. Env credentials

### Source (poly-dash) — `src/main.rs`, `.env`

Loaded with `dotenvy::dotenv_override()` (file wins over shell). Required vars:

```
POLY_PRIVATE_KEY     # EOA private key (0x…) — EIP-712 order signing
POLY_ADDRESS         # optional proxy/funder address (sig type 1 = POLY_PROXY)
POLY_API_KEY         # CLOB API key  ┐
POLY_API_SECRET      # CLOB secret   ├ L2 HMAC headers for REST + user WS auth
POLY_PASSPHRASE      # CLOB passphrase┘
RUST_LOG=poly_dash=info,tower_http=info
```

The live `.env` *also* already carries (unused by the Rust binary, but present): `DB_PATH`, `PORT`, `KALSHI_API_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH`, `CORS_ORIGIN`, and `ARB_BOT_*`. The client is constructed once:

```rust
ClobClient::from_config(ClientConfig {
    base_url: "https://clob.polymarket.com", chain: 137,
    private_key: Some(POLY_PRIVATE_KEY),
    api_credentials: Some({api_key, secret, passphrase}),
    signature_type: Some(1u8),  // POLY_PROXY
    funder: POLY_ADDRESS,
})
```

### Target (poly-sentient-market) — already implemented

Kalshi uses **RSA-PSS request signing**, not HMAC + wallet. Already present in the fork:

- `lib/kalshi-auth.ts` — `buildKalshiHeaders(method, path)`:
  - payload = **`` `${timestampMs}${METHOD}${path}` ``** (direct concat, **no separators**).
  - sign: `createSign('RSA-SHA256')` with `padding: RSA_PKCS1_PSS_PADDING`, `saltLength: RSA_PSS_SALTLEN_DIGEST`, base64.
  - headers: `KALSHI-ACCESS-KEY` (key id), `KALSHI-ACCESS-TIMESTAMP` (ms string), `KALSHI-ACCESS-SIGNATURE`.
- Credential resolution order (`loadCreds`): `.kalshi-credentials.json` (UI-uploaded, gitignored) → `KALSHI_API_KEY` + (`KALSHI_PRIVATE_KEY` inline PEM **or** `KALSHI_PRIVATE_KEY_PATH` file). Inline PEM (`\n`-unescaped) is required for Vercel/serverless.
- `lib/kalshi-credentials.ts` — read/write/delete the local store.
- `.env.local.example` keys: `KALSHI_API_KEY`, `KALSHI_PRIVATE_KEY_PATH=./kalshi_private_key.pem` (+ the LLM/ROMA provider keys unrelated to this port).

### → Kalshi mapping (2)

| poly-dash concept | Kalshi |
|---|---|
| `POLY_PRIVATE_KEY` (EIP-712 wallet) | RSA private key PEM (`KALSHI_PRIVATE_KEY[_PATH]`) — **no wallet/chain** |
| `POLY_API_KEY/SECRET/PASSPHRASE` HMAC | the RSA key id is the only "api key"; signature is per-request RSA-PSS |
| `signature_type / funder / chain 137` | **N/A** — Kalshi is a centralized exchange, no on-chain funder |
| `dotenv_override()` (file beats shell) | Next.js loads `.env.local` automatically; mirror the "explicit creds win" intent by preferring `.kalshi-credentials.json` (already done) |

**Action:** nothing to port here — the fork's auth is complete and correct. Just ensure the **WS** connection (new, §1) reuses `buildKalshiHeaders('GET','/trade-api/ws/v2')` for the upgrade request's auth headers.

---

## 3. Order execution

### Source (poly-dash) — `src/api.rs` + `src/validation.rs`

All order paths funnel through **`exec_order()`**, which enforces a strict pipeline:

1. **`check_order_window_guard(state, token_id)`** (see §4) — reject stale/closing/mismatched-window orders *before* signing.
2. **Tick-exact decimal conversion** — `tick_size_exact_decimal()` + `price_exact_decimal()`. Critical edge case: `Decimal::from_f64_retain(0.01)` carries binary drift and fails the CLOB's `is_price_tick_aligned` modulo check → *every* order rejected with "Price is not in range of tick_size". Fix: snap to the canonical `Decimal::new(1, scale)` for the 4 known tick sizes (0.1/0.01/0.001/0.0001) and build price from a snapped integer-ticks string.
3. **`create_order(args, opts)` then `post_order(signed, post_opts)`** — two-step sign-then-submit via polyfill-rs.
4. Broadcast `order` result `{success, state, order_id, taking_amount, making_amount, tx_hashes, trade_ids}`.

Order types (`parse_order_type`): `GTC` (default limit), `FAK`/`IOC` (immediate-or-cancel, used for market & smart), `FOK`, `GTD`. Sides `BUY`/`SELL`.

HTTP surface (`router()`):

| Route | Handler | Notes |
|---|---|---|
| `POST /api/buy` · `/api/sell` | `post_buy`/`post_sell` | limit GTC (sell can pass `order_type:"FAK"`) |
| `POST /api/market-buy` · `/api/market-sell` | FAK at 0.99 / 0.01 | sweep price |
| `POST /api/smart-buy` · `/api/smart-sell` | `smart_exec` | **walks the L2 mirror** to find the price covering `size`, optional `max_slippage_cents`, fires one FAK |
| `POST /api/split-buy` · `/api/split-sell` | `split_exec` | **N concurrent FAK slices** at a buffered top-of-book price (`join_all`) |
| `POST /api/sell-all/{token}` | `post_sell_all` | FAK 0.01 for full position |
| `POST /api/cancel-all/5m` · `/15m` | `cancel_market_orders(condition_id)` | **scoped by condition_id** — never global |
| `POST /api/cancel-all/global` | `cancel_all()` | blast-radius rescue only |
| `POST /api/cancel-order/{id}` | single cancel | |
| `POST /api/queue` / `DELETE /api/queue` | enqueue/clear | queued orders fire at next 5m rollover (`market::execute_queued_orders`) |

**Min-size discipline** (`validation.rs` + `min_order_size_for`): min is **per-market** (`WindowState::min_order_size`, not a global const). Two CLOB minimums coexist: **≥5 shares** *and* **≥$1 notional**. The `$1` rule is met by **bumping share count at the cheap price, never inflating price** — `buy_eligible_size(price_cents, size)` returns `ceil(100/price_cents)` when `size*price < $1` (e.g. `5@4¢ → 25@4¢`, *not* `5@20¢`). `split_exec` additionally collapses to fewer/one slice below $15 notional and caps at 2 slices.

`TradingClient` wraps `polyfill: ClobClient`, `chain_id`, `funder` and is shared `Arc` across all handlers and bots.

### Target (poly-sentient-market) — already partly implemented

`lib/kalshi-trade.ts` (RSA-PSS REST, **prices in cents 1–99**, **contracts** not shares):

- `placeOrder({ticker, side:'yes'|'no', count, yesPrice?, noPrice?, ioc?, clientOrderId?})` → `POST /trade-api/v2/portfolio/orders`, `action:'buy'`, `time_in_force: ioc ? 'immediate_or_cancel' : 'good_till_canceled'`.
- `sellOrder` (market: price 1¢ = "accept any bid", IOC), `limitSellOrder` (99¢ GTC take-profit), `cancelOrder(id)` → `DELETE …/orders/{id}`, `getBalance`, `getPositions` (`market_positions`).
- Error normalization handles Kalshi's `authentication_error`-during-maintenance quirk (3–5 AM ET weekdays).

### → Kalshi mapping (3)

| poly-dash | Kalshi |
|---|---|
| `token_id` (up/down ERC-1155) | `ticker` (market) + `side:'yes'/'no'` |
| price as `Decimal` tick-aligned 0–1 | integer **cents 1–99** (`yes_price`/`no_price`) — **drop the whole tick-exact-decimal machinery**, Kalshi has no sub-cent ticks |
| `GTC`/`FAK`/`FOK`/`GTD` | `time_in_force: 'good_till_canceled' | 'immediate_or_cancel'` (Kalshi has no FOK/GTD in the same form) |
| `create_order`+`post_order` (sign+submit) | single signed `POST /portfolio/orders` (already done) |
| `cancel_market_orders(condition_id)` scoped | Kalshi: cancel per-order (`DELETE …/orders/{id}`) or `DELETE …/orders/batched`; **port the "scope cancels to the current window's ticker, never blast-all" discipline** |
| `≥5 shares AND ≥$1 notional`, bump-qty-not-price | Kalshi min is **1 contract**, no $1 rule — the `buy_eligible_size` trick is **unnecessary**; drop it. Keep the validation *structure* (`validate_*` returning typed errors) |
| `smart_exec` book-walk FAK | **worth porting** — walk the Kalshi orderbook (yes/no) to size an IOC at a slippage cap |
| `split_exec` concurrent slices | lower value on Kalshi (no $1 min, deeper books on `KXBTCD`); port only if you see partial-fill issues |

**Action:** the fork's order *submission* is done. Port the **pre-trade guard** (§4) and optionally `smart_exec`'s book-walk; **delete** the tick-decimal and $1-notional logic (Polymarket-specific).

---

## 4. Window-transition logic  ⟵ highest-value port

This is poly-dash's crown jewel and exactly what the fork lacks. One task per timeframe (`market.rs` 5m, `market_15m.rs`, `market_1h.rs`) — **identical structure**, differing only in `WINDOW_SECS` and slug format.

### Deterministic window identity (no discovery, pure function of the clock)

```rust
WINDOW_SECS = 300 (5m) | 900 (15m) | 3600 (1h)
window_start = (unix_secs / WINDOW_SECS) * WINDOW_SECS
slug_5m  = format!("btc-updown-5m-{window_start}")
slug_15m = format!("btc-updown-15m-{window_start}")
slug_1h  = America/New_York "bitcoin-up-or-down-%B-%-d-%Y-%-I%P-et" (DST-safe via chrono-tz)
```

Two processes computing this at the same instant always agree — the current window is **never looked up**, it's derived. `current_window()` returns `{slug, window_start, window_end, sec_in_window}`.

### Two-phase, zero-gap rollover (the core loop, 100ms tick)

Each tick:

1. **Prefetch** — when `secs_remaining ≤ PREFETCH_LEAD` (10s for 5m/15m, 30s for 1h), resolve *next* window's tokens from Gamma (`resolve_tokens` → `condition_id`, `up_token_id`, `down_token_id`) and stash them. Makes the swap network-free.
2. **Transition (`now.slug != current_slug`)** — split into:
   - **Phase 1 (synchronous, <1ms):** commit the new `WindowState` immediately using prefetched tokens + a **placeholder strike** (`state.btc_best()`) and default metadata (`tick_size 0.01`, `min 5.0`, `fee 0`). This unblocks the order guard, UI, and bots against the new `condition_id` *now*. Broadcast `window`. Fire any queued orders.
   - **Phase 2 (spawned `tokio::task`, fire-and-forget):** the slow stuff in parallel — canonical/Chainlink strike resolution, both CLOB order books, and the fee rate (`tokio::join!`). When all return, **a staleness guard re-reads `state.window().slug`; if it rolled again, abandon** (don't corrupt the live window). Otherwise patch `btc_open`, top-of-book, `tick_size`, `neg_risk`, `min_order_size`, `fee_rate_bps` in place and broadcast `strike` + `prices`.
3. **Else (same window, new second):** just advance `sec_in_window` (lock taken only when the Unix second actually changes — ~9/10 ticks are no-ops at 100ms).

**Gamma failure policy:** on token-resolve failure, **do not advance `current_slug`** — retry every 100ms. Advancing would park the guard with empty tokens for the whole window, silently rejecting every order with a misleading error.

**Strike sourcing:** prefers the **canonical median** (Pyth+Coinbase+Kraken+Chainlink, ±$10 vs Polymarket's `priceToBeat`) over the Chainlink historical-round walk (±$50–120, kept for audit only). `fetch_chainlink_btc` walks `getRoundData` back to the round active at `window_start`.

### The order guard — `check_order_window_guard()` (`src/api.rs`)

Three reject conditions, checked **in this order** (slug alignment first so a stale token can't slip through during a roll):

1. **State-lag at rollover:** if `state.window.slug != current_window().slug` (clock has advanced past stored state) → reject "transitioning; retry".
2. **End-of-window buffer:** `WINDOW_END_GUARD_SECS = 3` — reject the last 3s of any window (CLOB round-trip 200–500ms would land post-resolution).
3. **Token mismatch:** the submitted `token_id` must match the *currently active* window's up/down — else reject. Protects against a stale dashboard firing into a just-rolled window.

The guard runs across all three timeframes (5m/15m/1h) and is shared by both the HTTP order path and the bots.

### Target (poly-sentient-market) — what exists today

`lib/kalshi.ts` computes tickers but **only as ET-formatted strings, with REST polling and no rollover orchestration**:

- `getCurrentEventTicker()` → `KXBTC15M-{YY}{MON}{DD}{HHMM}` (advances to end of current 15-min block, ET).
- `getCurrentKXBTCDEventTicker(offsetHours)` → `KXBTCD-{YY}{MON}{DD}{HH}` (closing ET hour).
- `parseKXBTC15MCloseMs` / `parseKXBTCDCloseMs` — reverse the ticker to a UTC close ms (hardcoded EDT/EST offset by month — **note: not true DST-rule, approximate**).
- `findNearestMarket`, `minutesUntilExpiry`, `secondsUntilExpiry` — pick nearest-expiry, time-left helpers.

There is **no prefetch, no two-phase commit, no staleness guard, no pre-trade window guard, no zero-gap WS resubscribe.**

### → Kalshi mapping (4)

| poly-dash | Kalshi |
|---|---|
| `slug = btc-updown-5m-{window_start}` (unix) | `KXBTC15M-{YY}{MON}{DD}{HHMM}` / `KXBTCD-{YY}{MON}{DD}{HH}` (ET strings — already in `lib/kalshi.ts`) |
| `resolve_tokens(slug)` → condition_id + up/down token ids (Gamma) | resolve `event_ticker` → `markets[]` via `GET /trade-api/v2/markets?event_ticker=…`; the "up/down token" pair becomes a **single market + yes/no side** |
| `sec_in_window`, `window_end` from unix math | derive from `close_time` (`secondsUntilExpiry`) — **or** better, compute from the ticker like poly-dash does the unix math, to stay clock-deterministic and avoid trusting server clocks |
| 100ms two-phase rollover + prefetch | port directly: a server-side timer that, near `close_time`, prefetches the next event's market; at the boundary commits the new active market synchronously, then enriches (orderbook snapshot, strike) async with a staleness re-check |
| `WINDOW_END_GUARD_SECS = 3` end buffer | keep 3s (Kalshi REST is slower than CLOB — consider 5s); reject orders in the final seconds before `close_time` |
| `check_order_window_guard` (slug + buffer + token match) | the **most important port** — gate every Kalshi `placeOrder` on: active-ticker matches the clock, not within end-buffer, ticker is the live window's |
| canonical-median strike vs Chainlink walk | Kalshi BTC markets settle on a published index — capture the strike from the market metadata / `CMC_API_KEY` feed already wired; keep the "placeholder now, refine async" pattern |

**Where it lives in Next.js:** poly-dash's per-timeframe `tokio` task has no direct Next.js analog (serverless = no long-lived loop). Options:
- **(a)** A small **persistent Node sidecar** (like `python-service/` but a Node process) holding the WS + window state + timer, exposing REST/SSE to the Next.js app — closest to poly-dash, recommended.
- **(b)** A `setInterval` inside a long-running custom server (`proxy.ts` already exists) if you don't deploy serverless.
- **(c)** Reuse the existing Python `trade_daemon.py` as the stateful loop and have Next.js read its state.

---

## 5. Port checklist (suggested order)

1. **State container** — port `AppState`'s window/book/positions/balance shape into the stateful sidecar (§0, §4). Single source of truth; UI reads only from it.
2. **Window engine** — port the deterministic-ticker + two-phase prefetch rollover + staleness guard for `KXBTC15M` and `KXBTCD` (§4). Reuse `lib/kalshi.ts` ticker math.
3. **Pre-trade guard** — port `check_order_window_guard` and gate `lib/kalshi-trade.ts::placeOrder` on it (§3, §4). Highest safety ROI.
4. **Market-data WS** — add the authenticated Kalshi WS v2 client mirroring orderbooks into state, replacing REST polling; keep the 5 Hz/1 Hz throttles (§1).
5. **User/fills WS** — subscribe the `fill` channel to drive positions live instead of polling `getPositions` (§1b).
6. **Browser push** — emit `{type,data}` over SSE (existing) or a new `/ws`; reuse poly-dash's message-type vocabulary (§1c).
7. **(optional) smart/split execution** — port `smart_exec` book-walk; skip `split_exec`/`buy_eligible_size`/tick-decimal (Polymarket-specific, §3).

## 6. Explicitly DROP (Polymarket-only, do not port)

- Tick-exact `Decimal` machinery (`tick_size_exact_decimal`, `price_exact_decimal`) — Kalshi is integer cents.
- `$1-notional` bump (`buy_eligible_size`) and the ≥5-share min — Kalshi min is 1 contract.
- `signature_type` / `funder` / `chain 137` / EIP-712 wallet — Kalshi is centralized RSA-PSS.
- Chainlink on-chain oracle walk & multi-RPC failover — Kalshi settles on its own index; use the existing CMC/price feed.
- Gamma slug resolution — replaced by Kalshi `event_ticker` → markets.

---

### Source file index (poly-dash, for deep dives)
`src/main.rs` (boot/env/tasks) · `src/state.rs` (AppState/WindowState/BookMirror/broadcast) · `src/api.rs` (router, exec_order, guard, smart/split) · `src/validation.rs` (order bounds) · `src/ws_polymarket.rs` (market WS) · `src/ws_user.rs` (user WS) · `src/ws_push.rs` (browser WS) · `src/market.rs` / `market_15m.rs` / `market_1h.rs` (window lifecycle).

### Target file index (poly-sentient-market, current Kalshi primitives)
`lib/kalshi-auth.ts` (RSA-PSS headers) · `lib/kalshi-credentials.ts` (cred store) · `lib/kalshi-trade.ts` (orders/balance/positions) · `lib/kalshi.ts` (base URL, KXBTC15M/KXBTCD ticker math, expiry helpers) · `app/api/place-order|sell-order|cancel-order|positions|balance|orderbook|markets/route.ts` (REST surface) · `lib/server-agent.ts` (+ `/api/agent/stream` SSE).
