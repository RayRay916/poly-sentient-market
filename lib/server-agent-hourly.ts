/**
 * Hourly server-side trading agent — POLYMARKET 1h BTC Up/Down.
 *
 * 1h twin of server-agent.ts: shares poly-dash's feed (market/quote/orderbook/
 * balance), executes via the exec microservice with the trader's own wallet, and
 * settles from poly-dash /api/recent-results. Window = 60 min, ET-hour aligned.
 */

import { EventEmitter } from 'events'
import { runAgentPipeline } from './agents'
import { tryLockPipeline, releasePipelineLock } from './pipeline-lock'
import { appendTrade, updateTrade } from './trade-log'
import { writeFileSync, readFileSync, mkdirSync, existsSync } from 'fs'
import { join } from 'path'
import type {
  PipelineState, AgentTrade, AgentStats,
  KalshiMarket, KalshiOrderbook, BTCQuote, OHLCVCandle,
} from './types'
import type { AIProvider } from './llm-client'
import { recordTradeResult } from './agents/markov'
import type { AgentStateSnapshot, AgentPhase } from './agent-shared'
import { hourlyAgentStore } from './agent-store-hourly'
import { polyFeed } from './polymarket/feed'
import { placeOrder as execPlaceOrder, getBalanceUsd } from './polymarket/exec'
import type { PolyMarket } from './polymarket/types'
import { TF_INTERVAL_SEC } from './polymarket/types'

// ── Polymarket wiring (1h) ─────────────────────────────────────────────────────
const DASH = process.env.POLY_DASH_URL ?? 'http://127.0.0.1:4300'
const WINDOW_MS = TF_INTERVAL_SEC['1h'] * 1000
const FAST_PATH_MIN_D = parseFloat(process.env.POLY_FAST_D ?? '0.05')

const polyFee = (_c: number, _p: number): number => 0

async function buy(
  market: PolyMarket, side: 'yes' | 'no', priceCents: number, contracts: number,
): Promise<{ ok: boolean; filled: number; orderId?: string; error?: string }> {
  const tokenId = side === 'yes' ? market.upTokenId : market.downTokenId
  const r = await execPlaceOrder({ tokenId, priceCents, size: contracts, orderType: 'FAK', tickSize: market.tickSize, negRisk: market.negRisk })
  const taking = r.takingAmount ? parseFloat(r.takingAmount) : 0
  const filled = r.ok ? (taking > 0 ? taking : (r.state === 'FILLED' ? contracts : 0)) : 0
  return { ok: r.ok, filled, orderId: r.orderId, error: r.error }
}

async function fetchOutcome(closeMs: number): Promise<'up' | 'down' | null> {
  try {
    const ws = Math.round(closeMs / 1000) - TF_INTERVAL_SEC['1h']
    const res = await fetch(`${DASH}/api/recent-results?tf=1h`, { cache: 'no-store' })
    if (!res.ok) return null
    const j = await res.json() as { results?: { ws: number; outcome: string }[] }
    const row = (j.results ?? []).find(r => r.ws === ws)
    return row?.outcome === 'up' ? 'up' : row?.outcome === 'down' ? 'down' : null
  } catch { return null }
}

// ── Constants ────────────────────────────────────────────────────────────────
const TARGET_MINUTES_BEFORE_CLOSE = 50
const MIN_MINUTES_LEFT            = 10
const MAX_ENTRY_PRICE_YES         = 65
const MAX_ENTRY_PRICE_NO          = 65
const MIN_ENTRY_PRICE             = 55
const POST_WINDOW_BUFFER_MS       = 15_000
const SCAN_INTERVAL_MS            = 30_000
const LOG_PREFIX                  = '[HourlyAgent]'

// ── Normal CDF (Abramowitz & Stegun) ──────────────────────────────────────────
function normalCDF(x: number): number {
  const t = 1 / (1 + 0.3275911 * Math.abs(x))
  const poly = t * (0.254829592 + t * (-0.284496736 + t * (1.421413741 + t * (-1.453152027 + t * 1.061405429))))
  const result = 1 - poly * Math.exp(-x * x)
  return x >= 0 ? result : 1 - result
}

// ── Config persistence (separate file from 15-min agent) ─────────────────────
const DATA_DIR   = process.env.VERCEL ? '/tmp' : join(process.cwd(), 'data')
const HOURLY_CFG = join(DATA_DIR, 'agent-config-hourly.json')
const HOURLY_LOG = join(DATA_DIR, 'trade-log-hourly.json')

interface HourlyConfig { active: boolean; allowance: number; kellyMode: boolean; bankroll: number; kellyPct: number }

function ensureDir() { if (!existsSync(DATA_DIR)) mkdirSync(DATA_DIR, { recursive: true }) }
function saveHourlyCfg(cfg: HourlyConfig): void { ensureDir(); writeFileSync(HOURLY_CFG, JSON.stringify(cfg, null, 2)) }
function loadHourlyCfg(): HourlyConfig | null {
  try { return existsSync(HOURLY_CFG) ? JSON.parse(readFileSync(HOURLY_CFG, 'utf-8')) : null } catch { return null }
}
function readHourlyLog(): AgentTrade[] {
  try { return existsSync(HOURLY_LOG) ? JSON.parse(readFileSync(HOURLY_LOG, 'utf-8')) : [] } catch { return [] }
}
function appendHourlyTrade(trade: AgentTrade): void {
  ensureDir(); const existing = readHourlyLog(); const deduped = existing.filter(t => t.id !== trade.id)
  writeFileSync(HOURLY_LOG, JSON.stringify([...deduped, trade], null, 2)); appendTrade(trade)
}
function updateHourlyTrade(id: string, patch: Partial<AgentTrade>): void {
  ensureDir(); const trades = readHourlyLog(); const idx = trades.findIndex(t => t.id === id)
  if (idx !== -1) { trades[idx] = { ...trades[idx], ...patch }; writeFileSync(HOURLY_LOG, JSON.stringify(trades, null, 2)) }
  updateTrade(id, patch)
}

// ── Window timing (ET-hour aligned == UTC-hour aligned) ───────────────────────
function getHourlyWindowClose(): number {
  return Math.ceil(Date.now() / WINDOW_MS) * WINDOW_MS
}
function getHourlyDelayMs(): { delayMs: number; closeMs: number; minutesLeft: number } {
  const closeMs = getHourlyWindowClose()
  const minutesLeft = (closeMs - Date.now()) / 60_000
  let delayMs: number
  if (minutesLeft >= MIN_MINUTES_LEFT && minutesLeft <= TARGET_MINUTES_BEFORE_CLOSE) delayMs = 0
  else if (minutesLeft > TARGET_MINUTES_BEFORE_CLOSE) delayMs = (minutesLeft - TARGET_MINUTES_BEFORE_CLOSE) * 60_000
  else { const nextClose = closeMs + WINDOW_MS; delayMs = nextClose - Date.now() - TARGET_MINUTES_BEFORE_CLOSE * 60_000 }
  return { delayMs: Math.max(0, delayMs), closeMs, minutesLeft }
}

function computeStats(trades: AgentTrade[]): AgentStats {
  const confirmed = trades.filter(t => t.liveOrderId)
  const settled = confirmed.filter(t => t.status !== 'open')
  const wins = settled.filter(t => t.status === 'won')
  const windowKeys = [...new Set(confirmed.map(t => t.windowKey))]
  const windowPnls = windowKeys.map(wk => confirmed.filter(t => t.windowKey === wk).reduce((s, t) => s + (t.pnl ?? 0), 0))
  return {
    windowsTraded: windowKeys.length, totalSlices: confirmed.length,
    totalDeployed: confirmed.reduce((s, t) => s + t.cost, 0), totalPnl: settled.reduce((s, t) => s + (t.pnl ?? 0), 0),
    wins: wins.length, losses: settled.length - wins.length, winRate: settled.length > 0 ? wins.length / settled.length : 0,
    bestWindow: windowPnls.length ? Math.max(...windowPnls) : 0, worstWindow: windowPnls.length ? Math.min(...windowPnls) : 0,
  }
}

// ── Agent class ───────────────────────────────────────────────────────────────
class HourlyServerAgent extends EventEmitter {
  private active = false
  private allowance = 100
  private initialAllowance = 100
  private isRunning = false
  private windowKey: string | null = null
  private currentMarketTicker = ''
  private windowBetPlaced = false
  private currentD = 0
  private lastPollAt: number | null = null
  private nextCycleIn = 0
  private error: string | null = null
  private orderError: string | null = null
  private trades: AgentTrade[] = readHourlyLog()
  private pipeline: PipelineState | null = null
  private autoTimeout: NodeJS.Timeout | null = null
  private pollerInterval: NodeJS.Timeout | null = null
  private countdownInterval: NodeJS.Timeout | null = null
  private settlementInterval: NodeJS.Timeout | null = null
  private nextRunAt = 0
  private strikePrice = 0
  private gkVol = 0.002
  private orderFailed = false
  private pipelineError = false
  private kellyMode = false
  private kellyPct = 0.18
  private bankroll = 0
  private agentPhase: AgentPhase = 'idle'
  private windowCloseAt = 0
  private lastKvSave = 0
  private lastCycleAt = 0

  private saveConfig() {
    saveHourlyCfg({ active: this.active, allowance: this.allowance, kellyMode: this.kellyMode, bankroll: this.bankroll, kellyPct: this.kellyPct })
  }
  private restoreConfig() {
    hourlyAgentStore.loadState().then(kvState => {
      if (kvState?.active) {
        hourlyAgentStore.loadTrades().then(t => { if (t.length) this.trades = t }).catch(() => {})
        this.start(kvState.allowance, kvState.kellyMode, kvState.bankroll)
        return
      }
      const cfg = loadHourlyCfg()
      if (cfg?.active) this.start(cfg.allowance, cfg.kellyMode, cfg.bankroll, cfg.kellyPct)
    }).catch(() => {
      const cfg = loadHourlyCfg()
      if (cfg?.active) this.start(cfg.allowance, cfg.kellyMode, cfg.bankroll, cfg.kellyPct)
    })
  }
  private flushToKV(force = false) {
    const now = Date.now()
    if (!force && now - this.lastKvSave < 10_000) return
    this.lastKvSave = now
    hourlyAgentStore.saveState(this.getState()).catch(() => {})
    hourlyAgentStore.saveTrades(this.trades).catch(() => {})
  }

  start(allowance: number, kellyMode = false, bankroll?: number, kellyPct = 0.18) {
    polyFeed.start()
    if (this.active) {
      this.allowance = allowance; this.kellyMode = kellyMode; this.kellyPct = kellyPct
      if (kellyMode && bankroll && bankroll > 0) { this.bankroll = bankroll; this.allowance = Math.max(1, bankroll * kellyPct) }
      this.pushState(); return
    }
    this.kellyMode = kellyMode; this.kellyPct = kellyPct
    this.bankroll = kellyMode && bankroll && bankroll > 0 ? bankroll : 0
    this.allowance = kellyMode ? Math.max(1, this.bankroll * kellyPct) : allowance
    this.initialAllowance = this.allowance
    this.active = true; this.error = null; this.orderError = null; this.agentPhase = 'waiting'
    this.startCountdown(); this.startSettlementLoop(); this.scheduleNextRun(); this.saveConfig(); this.pushState(true)
    console.log(`${LOG_PREFIX} Started — ${kellyMode ? `Kelly ${kellyPct * 100}% bankroll=$${this.bankroll} allowance=$${this.allowance.toFixed(2)}` : `fixed allowance=$${allowance}`}`)
  }
  stop() {
    this.active = false; this.isRunning = false; this.agentPhase = 'idle'
    this.clearTimers(); this.saveConfig(); this.pushState(true)
    console.log(`${LOG_PREFIX} Stopped`)
  }
  setAllowance(amount: number, kellyMode?: boolean, bankroll?: number) {
    if (kellyMode !== undefined) this.kellyMode = kellyMode
    if (this.kellyMode && bankroll && bankroll > 0) { this.bankroll = bankroll; this.allowance = Math.max(1, bankroll * this.kellyPct) }
    else if (!this.kellyMode) this.allowance = Math.max(0, amount)
    this.saveConfig(); this.pushState()
  }
  clearHistory() {
    this.trades = []; this.windowKey = null; this.windowBetPlaced = false
    ensureDir(); writeFileSync(HOURLY_LOG, '[]'); this.pushState()
  }
  async triggerCycle() {
    if (this.isRunning) return
    if (this.autoTimeout) { clearTimeout(this.autoTimeout); this.autoTimeout = null }
    this.stopPoller(); await this.runCycle()
  }
  getState(): AgentStateSnapshot {
    return {
      active: this.active, allowance: this.allowance, initialAllowance: this.initialAllowance,
      bankroll: this.bankroll, kellyMode: this.kellyMode, aiMode: false, isRunning: this.isRunning,
      windowKey: this.windowKey, windowBetPlaced: this.windowBetPlaced, currentD: this.currentD,
      lastPollAt: this.lastPollAt, nextCycleIn: this.nextCycleIn, error: this.error, orderError: this.orderError,
      trades: this.trades, stats: computeStats(this.trades), pipeline: this.pipeline,
      strikePrice: this.strikePrice, gkVol: this.gkVol, agentPhase: this.agentPhase, windowCloseAt: this.windowCloseAt,
    }
  }

  private pushState(forceKv = false) { this.emit('state', this.getState()); this.flushToKV(forceKv) }
  private startCountdown() {
    if (this.countdownInterval) clearInterval(this.countdownInterval)
    this.countdownInterval = setInterval(() => {
      const remaining = Math.max(0, Math.round((this.nextRunAt - Date.now()) / 1000))
      if (remaining !== this.nextCycleIn) { this.nextCycleIn = remaining; this.pushState() }
    }, 1000)
  }
  private clearTimers() {
    if (this.autoTimeout) { clearTimeout(this.autoTimeout); this.autoTimeout = null }
    if (this.countdownInterval) { clearInterval(this.countdownInterval); this.countdownInterval = null }
    if (this.settlementInterval) { clearInterval(this.settlementInterval); this.settlementInterval = null }
    this.stopPoller()
  }
  private stopPoller() { if (this.pollerInterval) { clearInterval(this.pollerInterval); this.pollerInterval = null } }
  private schedule(fn: () => void, ms: number) {
    if (this.autoTimeout) { clearTimeout(this.autoTimeout); this.autoTimeout = null }
    this.autoTimeout = setTimeout(() => { this.autoTimeout = null; if (this.active) fn() }, ms)
  }

  private scheduleNextRun() {
    if (!this.active) return
    if (this.autoTimeout) { clearTimeout(this.autoTimeout); this.autoTimeout = null }
    this.stopPoller(); this.windowBetPlaced = false; this.strikePrice = 0; this.lastCycleAt = 0
    const { delayMs, closeMs: cm } = getHourlyDelayMs()
    if (delayMs <= 0) this.startPoller(cm)
    else {
      this.nextRunAt = Date.now() + delayMs; this.nextCycleIn = Math.round(delayMs / 1000); this.agentPhase = 'waiting'
      console.log(`${LOG_PREFIX} Next window in ${Math.round(delayMs / 60_000)}min`)
      this.schedule(() => { const { closeMs } = getHourlyDelayMs(); this.startPoller(closeMs) }, delayMs)
    }
    this.pushState()
  }

  // Fast-path entry — deterministic, no LLM (mirrors server-agent.ts).
  private async fastEntry(d: number, closeMs: number): Promise<void> {
    if (!this.active || this.windowBetPlaced || !this.windowKey) return
    const minutesLeft = (closeMs - Date.now()) / 60_000
    if (minutesLeft < MIN_MINUTES_LEFT) return
    const side: 'yes' | 'no' = d > 0 ? 'yes' : 'no'
    try {
      const market = polyFeed.market('1h')
      if (!market || !market.upTokenId) return
      const askPrice = side === 'yes' ? market.yes_ask : market.no_ask
      const maxP = side === 'yes' ? MAX_ENTRY_PRICE_YES : MAX_ENTRY_PRICE_NO
      if (askPrice < MIN_ENTRY_PRICE || askPrice > maxP) return
      const pModel = normalCDF(Math.abs(d))
      const p_d = askPrice / 100
      const b = (1 - p_d) / p_d
      const pWin = side === 'yes' ? pModel : (1 - pModel)
      const kellyFrac = Math.max(0, (b * pWin - (1 - pWin)) / b)
      if (kellyFrac <= 0) return
      const capital = this.bankroll > 0 ? this.bankroll : this.allowance
      const contracts = Math.max(1, Math.round((kellyFrac * 0.18 * capital) / p_d))
      if (contracts * p_d < 1) return
      const ioPrice = Math.min(99, askPrice + 3)
      let res = await buy(market, side, ioPrice, contracts)
      if (!res.ok || res.filled <= 0) { const retry = await buy(market, side, Math.min(99, askPrice + 5), contracts); if (!retry.ok || retry.filled <= 0) return; res = retry }
      const filled = res.filled || contracts
      this.windowBetPlaced = true; this.agentPhase = 'bet_placed'; this.orderError = null
      const trade: AgentTrade = {
        id: `hfast-${Date.now()}`, cycleId: -1, windowKey: market.slug, sliceNum: 1, side,
        limitPrice: askPrice, contracts: filled, cost: filled * p_d, marketTicker: market.slug,
        strikePrice: this.strikePrice, btcPriceAtEntry: undefined, expiresAt: market.close_time,
        enteredAt: new Date().toISOString(), status: 'open', pModel, pMarket: p_d, edge: 0,
        signals: { sentimentScore: 0, sentimentMomentum: 0, orderbookSkew: 0, sentimentLabel: 'fast_entry', pLLM: pModel, confidence: 'medium', gkVol: this.gkVol, distancePct: d, minutesLeft, aboveStrike: d > 0, priceMomentum1h: 0 },
        liveOrderId: res.orderId, orderError: undefined,
      }
      this.trades = [...this.trades, trade]; appendHourlyTrade(trade)
      if (this.kellyMode) this.bankroll = Math.max(1, this.bankroll - filled * p_d)
      console.log(`${LOG_PREFIX} ⚡ Fast-path ${side.toUpperCase()} ${filled}× @ ${askPrice}¢ on ${market.slug}`)
      this.pushState(true)
    } catch (e) { console.error(`${LOG_PREFIX} fast-path error:`, e) }
  }

  private startPoller(closeMs: number) {
    this.stopPoller(); this.windowCloseAt = closeMs
    this.agentPhase = this.strikePrice > 0 ? 'monitoring' : 'bootstrap'; this.pushState()
    let pollInFlight = false
    const check = async () => {
      if (!this.active || this.isRunning || this.windowBetPlaced || pollInFlight) return
      pollInFlight = true
      const minutesLeft = (closeMs - Date.now()) / 60_000
      if (minutesLeft < MIN_MINUTES_LEFT) {
        this.stopPoller()
        if (!this.windowBetPlaced) {
          const waitMs = Math.max(POST_WINDOW_BUFFER_MS, closeMs - Date.now() + POST_WINDOW_BUFFER_MS)
          this.agentPhase = 'waiting'; this.nextRunAt = Date.now() + waitMs; this.nextCycleIn = Math.round(waitMs / 1000)
          this.pushState(); this.schedule(() => this.scheduleNextRun(), waitMs)
        }
        pollInFlight = false; return
      }
      try {
        const price = polyFeed.quote().price
        if (price > 0 && this.strikePrice > 0) { this.currentD = ((price - this.strikePrice) / this.strikePrice) * 100; this.lastPollAt = Date.now(); this.pushState() }
      } catch { /* ignore */ }
      if (!this.windowBetPlaced && this.strikePrice > 0 && Math.abs(this.currentD) >= FAST_PATH_MIN_D) {
        await this.fastEntry(this.currentD, closeMs)
        if (this.windowBetPlaced) { pollInFlight = false; return }
      }
      const now = Date.now()
      if (now - this.lastCycleAt >= SCAN_INTERVAL_MS) {
        this.lastCycleAt = now; this.stopPoller(); pollInFlight = false
        console.log(`${LOG_PREFIX} ${minutesLeft.toFixed(1)}min left — scanning Markov signal`)
        await this.runCycle(); return
      }
      pollInFlight = false
    }
    check(); this.pollerInterval = setInterval(check, 2_000)
  }

  private async runCycle() {
    if (this.isRunning) return
    this.isRunning = true; this.error = null
    const wasBootstrap = this.strikePrice <= 0
    this.agentPhase = wasBootstrap ? 'bootstrap' : 'pipeline'
    this.emit('pipeline_start', {}); this.pushState()
    const { closeMs } = getHourlyDelayMs()
    try {
      polyFeed.start()
      const pm = polyFeed.market('1h')
      if (!pm || !pm.upTokenId || pm.up_ask <= 0 || pm.up_ask >= 100) throw new Error('No active Polymarket 1h window from poly-dash feed')
      const markets: KalshiMarket[] = [pm as unknown as KalshiMarket]

      const price = polyFeed.quote().price
      if (!(price > 0)) throw new Error('BTC price unavailable from poly-dash feed')
      let quote: BTCQuote = { price, percent_change_1h: 0, percent_change_24h: 0, volume_24h: 0, market_cap: price * 19_700_000, last_updated: new Date().toISOString() }

      const [c15m, c1m, c1h, c4h] = await Promise.all([
        fetch('https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=900&limit=13', { cache: 'no-store' }).catch(() => null),
        fetch('https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=60&limit=16', { cache: 'no-store' }).catch(() => null),
        fetch('https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=3600&limit=25', { cache: 'no-store' }).catch(() => null),
        fetch('https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=14400&limit=10', { cache: 'no-store' }).catch(() => null),
      ])

      const balUsd = await getBalanceUsd().catch(() => null)
      const actualBalanceCents = balUsd != null ? Math.round(balUsd * 100) : 0
      const portfolioValueCents = (this.kellyMode && this.bankroll > 0) ? Math.max(actualBalanceCents, Math.round(this.bankroll * 100)) : actualBalanceCents

      const parseCandles = async (res: Response | null): Promise<OHLCVCandle[]> => {
        if (!res?.ok) return []
        try { const r = await res.json(); return Array.isArray(r) ? r : [] } catch { return [] }
      }
      const [candles15m, candles1m, candles1h, candles4h] = await Promise.all([
        parseCandles(c15m).then(r => r.slice(1, 13)), parseCandles(c1m),
        parseCandles(c1h).then(r => r.slice(1, 13)), parseCandles(c4h).then(r => r.slice(1, 8)),
      ])
      if (quote.percent_change_1h === 0 && candles1h.length >= 1) {
        const price1hAgo = candles1h[0][4]
        if (price1hAgo > 0) quote = { ...quote, percent_change_1h: ((quote.price - price1hAgo) / price1hAgo) * 100 }
      }

      const orderbook: KalshiOrderbook | null = (() => {
        const up = polyFeed.orderbook(pm.upTokenId); const down = polyFeed.orderbook(pm.downTokenId)
        if (!up && !down) return null
        const lvls = (b?: { bids: [number, number][] }) => (b?.bids ?? []).map(([price, size]) => ({ price, delta: size }))
        return { yes: lvls(up ?? undefined), no: lvls(down ?? undefined) }
      })()

      const provider = (process.env.AI_PROVIDER ?? 'grok') as AIProvider
      const romaMode = process.env.ROMA_MODE ?? 'keen'
      if (!tryLockPipeline()) throw new Error('Pipeline already running')
      let result: PipelineState
      try {
        result = await runAgentPipeline(
          markets, quote, orderbook, provider, romaMode, false,
          undefined, undefined, candles15m, candles1m, null, undefined, undefined,
          (key, agentResult) => this.emit('agent', { key, result: agentResult }),
          portfolioValueCents, undefined, candles1h, candles4h,
          pm as unknown as KalshiMarket, { maxEntryPrice: MAX_ENTRY_PRICE_YES },
        )
      } finally { releasePipelineLock() }
      this.pipeline = result
      await this.processResult(result, wasBootstrap, quote.price, closeMs)
    } catch (err) {
      console.error(`${LOG_PREFIX} runCycle error:`, err); this.error = String(err); this.pipelineError = true
    } finally {
      this.isRunning = false
      if (this.active) {
        const { minutesLeft, closeMs: freshClose } = getHourlyDelayMs()
        const failed = this.orderFailed; const pipeErr = this.pipelineError
        this.orderFailed = false; this.pipelineError = false
        if (pipeErr) {
          const retryMs = 10_000; this.nextRunAt = Date.now() + retryMs; this.nextCycleIn = Math.round(retryMs / 1000); this.agentPhase = 'error'
          console.log(`${LOG_PREFIX} Pipeline error — retrying in 10s`); this.schedule(() => this.scheduleNextRun(), retryMs)
        } else if (failed && minutesLeft >= MIN_MINUTES_LEFT) {
          this.nextRunAt = Date.now() + 60_000; this.nextCycleIn = 60
          this.schedule(() => { const { closeMs: cm } = getHourlyDelayMs(); this.startPoller(cm) }, 60_000)
        } else if (!this.windowBetPlaced && minutesLeft >= MIN_MINUTES_LEFT) {
          this.agentPhase = 'monitoring'; this.startPoller(freshClose)
        } else {
          const waitMs = Math.max(POST_WINDOW_BUFFER_MS, freshClose - Date.now() + POST_WINDOW_BUFFER_MS)
          this.agentPhase = this.windowBetPlaced ? 'bet_placed' : 'waiting'; this.nextRunAt = Date.now() + waitMs; this.nextCycleIn = Math.round(waitMs / 1000)
          this.schedule(() => this.scheduleNextRun(), waitMs)
        }
      }
      this.pushState()
    }
  }

  private async processResult(data: PipelineState, isBootstrap: boolean, btcPrice: number, closeMs: number) {
    const exec = data.agents.execution.output
    const md = data.agents.marketDiscovery.output
    const pf = data.agents.priceFeed.output
    const prob = data.agents.probability.output
    const risk = data.agents.markov.output
    const sent = data.agents.sentiment.output
    const evTicker = (md.activeMarket as { slug?: string } | undefined)?.slug ?? md.activeMarket?.ticker ?? null
    if (md.strikePrice > 0) this.strikePrice = md.strikePrice
    if (prob.gkVol15m && prob.gkVol15m > 0) this.gkVol = prob.gkVol15m
    if (md.activeMarket?.ticker) this.currentMarketTicker = md.activeMarket.ticker
    this.currentD = pf.aboveStrike ? pf.distanceFromStrikePct : -pf.distanceFromStrikePct
    if (evTicker && evTicker !== this.windowKey) { this.windowKey = evTicker; this.windowBetPlaced = false }
    if (isBootstrap) {
      const distPct = pf.aboveStrike ? pf.distanceFromStrikePct : -pf.distanceFromStrikePct
      console.log(`${LOG_PREFIX} Bootstrap: strike=$${md.strikePrice} BTC=${distPct >= 0 ? '+' : ''}${distPct.toFixed(2)}% from strike`); return
    }
    const minsUntilClose = (closeMs - Date.now()) / 60_000
    if (exec.action !== 'PASS' && exec.side != null && exec.limitPrice != null && risk.approved && md.activeMarket && evTicker && this.allowance >= 1 && !this.windowBetPlaced && minsUntilClose >= MIN_MINUTES_LEFT) {
      const market = polyFeed.market('1h')
      if (!market || !market.upTokenId) { console.warn(`${LOG_PREFIX} feed market unavailable at order time — skip`); return }
      let liveLimitPrice = exec.limitPrice
      const freshPrice = exec.side === 'yes' ? market.yes_ask : market.no_ask
      if (freshPrice > 0) {
        const maxFreshPrice = exec.side === 'yes' ? MAX_ENTRY_PRICE_YES : MAX_ENTRY_PRICE_NO
        if (freshPrice > maxFreshPrice) { console.log(`${LOG_PREFIX} Fresh ${exec.side}_ask=${freshPrice}¢ > ${maxFreshPrice}¢ cap — SKIP`); return }
        liveLimitPrice = freshPrice
      }
      const costPerContract = liveLimitPrice / 100
      const contracts = Math.max(1, Math.floor(this.allowance / costPerContract))
      const cost = contracts * costPerContract
      let liveOrderId: string | undefined; let orderErrorMsg: string | undefined; let iocUnfilled = false
      try {
        let res = await buy(market, exec.side, Math.min(99, liveLimitPrice + 3), contracts)
        if ((!res.ok || res.filled <= 0) && res.ok) { console.log(`${LOG_PREFIX} FAK unfilled — retrying +5¢`); res = await buy(market, exec.side, Math.min(99, liveLimitPrice + 5), contracts) }
        if (res.ok && res.filled > 0) { liveOrderId = res.orderId; console.log(`${LOG_PREFIX} FAK filled ${res.filled}`) }
        else if (!res.ok) orderErrorMsg = res.error ?? 'Order failed'
        else { iocUnfilled = true; orderErrorMsg = 'FAK unfilled — no liquidity'; console.warn(`${LOG_PREFIX} ${orderErrorMsg}`) }
      } catch (e) { orderErrorMsg = String(e) }
      const trade: AgentTrade = {
        id: `h-${data.cycleId}-${Date.now()}`, cycleId: data.cycleId, windowKey: evTicker, sliceNum: 1, side: exec.side,
        limitPrice: liveLimitPrice, contracts, cost, marketTicker: market.slug, strikePrice: md.strikePrice,
        btcPriceAtEntry: btcPrice, expiresAt: (md.activeMarket as unknown as PolyMarket).close_time ?? market.close_time,
        enteredAt: new Date().toISOString(), status: 'open', pModel: prob.pModel, pMarket: prob.pMarket, edge: prob.edge,
        signals: { sentimentScore: sent.score, sentimentMomentum: sent.momentum, orderbookSkew: sent.orderbookSkew, sentimentLabel: sent.label, pLLM: prob.pModel, confidence: prob.confidence, gkVol: prob.gkVol15m ?? null, distancePct: pf.distanceFromStrikePct, minutesLeft: md.minutesUntilExpiry, aboveStrike: pf.aboveStrike, priceMomentum1h: pf.priceChangePct1h },
        liveOrderId, orderError: orderErrorMsg,
      }
      this.trades = [...this.trades, trade]; appendHourlyTrade(trade)
      if (liveOrderId) {
        this.windowBetPlaced = true; this.orderError = null; this.agentPhase = 'bet_placed'
        if (this.kellyMode) this.bankroll = Math.max(1, this.bankroll - cost)
        console.log(`${LOG_PREFIX} ✓ Bet placed — ${exec.side.toUpperCase()} ${contracts}× @ ${liveLimitPrice}¢ on ${evTicker}`)
      } else if (iocUnfilled) { this.orderError = orderErrorMsg ?? 'Skipped — no fill'; this.agentPhase = 'pass_skipped' }
      else if (orderErrorMsg) { this.orderFailed = true; this.orderError = orderErrorMsg; this.agentPhase = 'order_failed'; console.error(`${LOG_PREFIX} ✗ Order failed: ${orderErrorMsg}`) }
    }
    await this.settleExpired(pf.currentPrice)
  }

  private async settleExpired(settlementPrice: number) {
    const now = Date.now()
    const expired = this.trades.filter(t => t.status === 'open' && t.liveOrderId && now >= new Date(t.expiresAt).getTime())
    if (!expired.length) return
    const settled = await Promise.all(expired.map(async t => {
      const outcome = await fetchOutcome(new Date(t.expiresAt).getTime())
      if (outcome) {
        const win = (t.side === 'yes' && outcome === 'up') || (t.side === 'no' && outcome === 'down')
        const fee = polyFee(t.contracts, t.limitPrice ?? Math.round(t.cost / t.contracts * 100))
        return { ...t, status: (win ? 'won' : 'lost') as 'won' | 'lost', settlementPrice, pnl: win ? t.contracts - t.cost - fee : -t.cost - fee }
      }
      return t
    }))
    const justSettled = settled.filter(s => s.status !== 'open')
    if (!justSettled.length) return
    this.trades = this.trades.map(t => settled.find(s => s.id === t.id) ?? t)
    for (const t of justSettled) { updateHourlyTrade(t.id, { status: t.status, pnl: t.pnl, settlementPrice: t.settlementPrice }); if (t.pnl != null) recordTradeResult(t.pnl) }
    if (this.kellyMode) {
      for (const t of justSettled) { if (t.status === 'won') { const fee = polyFee(t.contracts, t.limitPrice ?? 0); this.bankroll += t.contracts - fee } }
      this.bankroll = Math.max(1, this.bankroll); this.allowance = Math.max(1, Math.round(this.bankroll * this.kellyPct * 100) / 100); this.saveConfig()
    }
    this.pushState(); console.log(`${LOG_PREFIX} Settled ${justSettled.length} trade(s)`)
  }

  private startSettlementLoop() {
    if (this.settlementInterval) clearInterval(this.settlementInterval)
    this.settlementInterval = setInterval(() => { if (this.active) this.settleExpired(0).catch(e => console.error(`${LOG_PREFIX} settlement loop error:`, e)) }, 60_000)
  }
}

const g = globalThis as typeof globalThis & { _hourlyServerAgent?: HourlyServerAgent }
if (!g._hourlyServerAgent) {
  g._hourlyServerAgent = new HourlyServerAgent()
  setImmediate(() => { g._hourlyServerAgent!['restoreConfig']() })
}
export const hourlyServerAgent = g._hourlyServerAgent
