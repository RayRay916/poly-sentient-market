/**
 * Server-side autonomous trading agent — POLYMARKET.
 *
 * Runs entirely in Node.js — immune to browser tab throttling/suspension.
 * Browser clients subscribe for real-time updates via /api/agent/stream (SSE).
 *
 * Market I/O is shared with poly-dash:
 *   • market data + BTC quote + orderbook + balance  ← polyFeed (poly-dash /ws + /api/status)
 *   • order execution                                ← poly-sentient-exec (:4321), own wallet
 *   • settlement                                     ← poly-dash /api/recent-results
 *
 * Lifecycle (unchanged):
 *   start(allowance) → scheduleNextRun() → [wait] → startDPoller() →
 *   [poll BTC every 2s, run Markov pipeline every Ns] → Markov approves →
 *   runCycle() → processResult() → placeOrder() → next window → repeat
 */

import { EventEmitter } from 'events'
import { runAgentPipeline } from './agents'
import { tryLockPipeline, releasePipelineLock } from './pipeline-lock'
import { appendTrade, updateTrade, readTradeLog, clearTradeLog, saveAgentConfig, loadAgentConfig } from './trade-log'
import type {
  PipelineState, AgentTrade, AgentStats,
  KalshiMarket, KalshiOrderbook, BTCQuote, OHLCVCandle, DerivativesSignal,
} from './types'
import type { AIProvider } from './llm-client'
import { KELLY_FRACTION } from './agent-shared'
import { recordTradeResult } from './agents/markov'
import type { AgentStateSnapshot, AgentPhase } from './agent-shared'
import { agentStore } from './agent-store'
import { polyFeed } from './polymarket/feed'
import { placeOrder as execPlaceOrder, getBalanceUsd } from './polymarket/exec'
import type { PolyMarket, Timeframe } from './polymarket/types'
import { TF_INTERVAL_SEC } from './polymarket/types'

// ── Polymarket wiring ─────────────────────────────────────────────────────────
const DASH = process.env.POLY_DASH_URL ?? 'http://127.0.0.1:4300'
/** Which poly-dash window this autonomous trader trades (5m/15m/1h). Default 15m. */
const TF: Timeframe = (process.env.POLY_TF as Timeframe) || '15m'
const WINDOW_MS = TF_INTERVAL_SEC[TF] * 1000

/** Polymarket binary up/down markets carry ~no taker fee on these contracts; the
 *  signer applies feeRateBps server-side. Keep the agent's EV/Kelly math fee-free
 *  (set >0 here if a venue fee is ever introduced). */
const polyFee = (_contracts: number, _priceCents: number): number => 0

/** Buy the up/down token for a yes/no side via the exec microservice. */
async function buy(
  market: PolyMarket, side: 'yes' | 'no', priceCents: number, contracts: number,
): Promise<{ ok: boolean; filled: number; orderId?: string; error?: string }> {
  const tokenId = side === 'yes' ? market.upTokenId : market.downTokenId
  const r = await execPlaceOrder({
    tokenId, priceCents, size: contracts, orderType: 'FAK',
    tickSize: market.tickSize, negRisk: market.negRisk,
  })
  const taking = r.takingAmount ? parseFloat(r.takingAmount) : 0
  const filled = r.ok ? (taking > 0 ? taking : (r.state === 'FILLED' ? contracts : 0)) : 0
  return { ok: r.ok, filled, orderId: r.orderId, error: r.error }
}

/** Resolve a window's outcome from poly-dash. windowStart = closeMs/1000 - interval. */
async function fetchOutcome(closeMs: number): Promise<'up' | 'down' | null> {
  try {
    const ws = Math.round(closeMs / 1000) - TF_INTERVAL_SEC[TF]
    const res = await fetch(`${DASH}/api/recent-results?tf=${TF}`, { cache: 'no-store' })
    if (!res.ok) return null
    const j = await res.json() as { results?: { ws: number; outcome: string }[] }
    const row = (j.results ?? []).find(r => r.ws === ws)
    return row?.outcome === 'up' ? 'up' : row?.outcome === 'down' ? 'down' : null
  } catch { return null }
}

// ── Constants ────────────────────────────────────────────────────────────────
const TARGET_MINUTES_BEFORE_CLOSE = 14  // start monitoring 14 min before close
const MIN_MINUTES_LEFT       = 2         // safety floor: don't trade with < 2 min left
const POST_WINDOW_BUFFER_MS  = 5_000
const MIN_FAST_ENTRY_PRICE     = 55   // ¢ — matches risk manager floor
const MAX_FAST_ENTRY_PRICE_YES = 72   // ¢
const MAX_FAST_ENTRY_PRICE_NO  = 65   // ¢
const FAST_PATH_MIN_D = parseFloat(process.env.POLY_FAST_D ?? '0.05')  // |% from strike| that arms the no-LLM fast-path

// ── Normal CDF approximation (Abramowitz & Stegun) ───────────────────────────
function normalCDF(x: number): number {
  const t = 1 / (1 + 0.3275911 * Math.abs(x))
  const poly = t * (0.254829592 + t * (-0.284496736 + t * (1.421413741 + t * (-1.453152027 + t * 1.061405429))))
  const result = 1 - poly * Math.exp(-x * x)
  return x >= 0 ? result : 1 - result
}

// ── Window timing helpers ────────────────────────────────────────────────────
function getWindowClose(): number {
  return Math.ceil(Date.now() / WINDOW_MS) * WINDOW_MS
}

function getDelayMs(): { delayMs: number; closeMs: number; minutesLeft: number } {
  const closeMs    = getWindowClose()
  const minutesLeft = (closeMs - Date.now()) / 60_000
  let delayMs: number

  if (minutesLeft >= MIN_MINUTES_LEFT && minutesLeft <= TARGET_MINUTES_BEFORE_CLOSE) {
    delayMs = 0  // already inside monitoring window
  } else if (minutesLeft > TARGET_MINUTES_BEFORE_CLOSE) {
    delayMs = (minutesLeft - TARGET_MINUTES_BEFORE_CLOSE) * 60_000
  } else {
    const nextCloseMs = closeMs + WINDOW_MS
    delayMs = nextCloseMs - Date.now() - TARGET_MINUTES_BEFORE_CLOSE * 60_000
  }

  return { delayMs: Math.max(0, delayMs), closeMs, minutesLeft }
}

function computeStats(trades: AgentTrade[]): AgentStats {
  const confirmed   = trades.filter(t => t.liveOrderId)
  const settled     = confirmed.filter(t => t.status !== 'open')
  const wins        = settled.filter(t => t.status === 'won')
  const windowKeys  = [...new Set(confirmed.map(t => t.windowKey))]
  const windowPnls  = windowKeys.map(wk =>
    confirmed.filter(t => t.windowKey === wk).reduce((s, t) => s + (t.pnl ?? 0), 0)
  )
  return {
    windowsTraded:  windowKeys.length,
    totalSlices:    confirmed.length,
    totalDeployed:  confirmed.reduce((s, t) => s + t.cost, 0),
    totalPnl:       settled.reduce((s, t) => s + (t.pnl ?? 0), 0),
    wins:           wins.length,
    losses:         settled.length - wins.length,
    winRate:        settled.length > 0 ? wins.length / settled.length : 0,
    bestWindow:     windowPnls.length ? Math.max(...windowPnls) : 0,
    worstWindow:    windowPnls.length ? Math.min(...windowPnls) : 0,
  }
}

// ── Server Agent ─────────────────────────────────────────────────────────────
class ServerAgent extends EventEmitter {
  private active           = false
  private allowance        = 100
  private initialAllowance = 100
  private isRunning        = false
  private windowKey:           string | null = null
  private currentMarketTicker: string        = ''   // Polymarket slug of the active window
  private windowBetPlaced = false
  private currentD     = 0
  private lastPollAt:  number | null = null
  private nextCycleIn  = 0
  private error:       string | null = null
  private orderError:  string | null = null
  private trades:      AgentTrade[]  = readTradeLog()
  private pipeline:    PipelineState | null = null

  private autoTimeout:       NodeJS.Timeout | null = null
  private pollerInterval:    NodeJS.Timeout | null = null
  private countdownInterval: NodeJS.Timeout | null = null
  private settlementInterval: NodeJS.Timeout | null = null
  private nextRunAt    = 0
  private strikePrice  = 0
  private gkVol        = 0.002
  private orderFailed    = false
  private pipelineError  = false
  private kellyMode      = false
  private kellyPct       = 0.18
  private aiMode         = false
  private bankroll       = 0
  private orModel:     string | undefined
  private agentPhase: AgentPhase = 'idle'
  private windowCloseAt = 0
  private lastKvSave    = 0
  private lastCycleAt  = 0

  // ── Config persistence ─────────────────────────────────────────────────────

  private saveConfig() {
    saveAgentConfig({
      active:    this.active,
      allowance: this.allowance,
      kellyMode: this.kellyMode,
      aiMode:    this.aiMode,
      bankroll:  this.bankroll,
      kellyPct:  this.kellyPct,
      orModel:   this.orModel,
    })
  }

  private restoreConfig() {
    agentStore.loadState().then(kvState => {
      if (kvState?.active) {
        console.log(`[ServerAgent] Restoring from KV — active=${kvState.active} allowance=$${kvState.allowance} aiMode=${kvState.aiMode}`)
        agentStore.loadTrades().then(kvTrades => {
          if (kvTrades.length) this.trades = kvTrades
        }).catch(() => {})
        this.start(kvState.allowance, undefined, kvState.kellyMode, kvState.bankroll, undefined, kvState.aiMode ?? false)
        return
      }
      const cfg = loadAgentConfig()
      if (!cfg?.active) return
      console.log(`[ServerAgent] Restoring from disk — kellyMode=${cfg.kellyMode} aiMode=${cfg.aiMode} bankroll=$${cfg.bankroll} allowance=$${cfg.allowance}`)
      this.start(cfg.allowance, cfg.orModel, cfg.kellyMode, cfg.bankroll, cfg.kellyPct, cfg.aiMode)
    }).catch(() => {
      const cfg = loadAgentConfig()
      if (cfg?.active) this.start(cfg.allowance, cfg.orModel, cfg.kellyMode, cfg.bankroll, cfg.kellyPct, cfg.aiMode)
    })
  }

  private flushToKV(force = false) {
    const now = Date.now()
    if (!force && now - this.lastKvSave < 10_000) return
    this.lastKvSave = now
    agentStore.saveState(this.getState()).catch(() => {})
    agentStore.saveTrades(this.trades).catch(() => {})
  }

  // ── Public API ─────────────────────────────────────────────────────────────

  start(allowance: number, orModel?: string, kellyMode = false, bankroll?: number, kellyPct = 0.25, aiMode = false) {
    polyFeed.start()  // ensure the shared feed is consuming poly-dash
    if (this.active) {
      this.allowance  = allowance
      this.orModel    = orModel
      this.kellyMode  = kellyMode
      this.aiMode     = aiMode
      this.kellyPct   = kellyPct
      if (kellyMode && bankroll && bankroll > 0) {
        this.bankroll  = bankroll
        this.allowance = Math.max(1, bankroll * kellyPct)
      }
      this.pushState()
      return
    }
    this.kellyMode        = kellyMode
    this.aiMode           = aiMode
    this.kellyPct         = kellyPct
    this.bankroll         = kellyMode && bankroll && bankroll > 0 ? bankroll : 0
    this.allowance        = kellyMode ? Math.max(1, this.bankroll * kellyPct) : allowance
    this.initialAllowance = this.allowance
    this.orModel          = orModel
    this.active           = true
    this.error            = null
    this.orderError       = null
    this.agentPhase       = 'waiting'
    this.startCountdown()
    this.startSettlementLoop()
    this.scheduleNextRun()
    this.saveConfig()
    this.pushState(true)
    console.log(`[ServerAgent] Started — ${kellyMode ? `Kelly ${kellyPct*100}% bankroll=$${this.bankroll} allowance=$${this.allowance.toFixed(2)}` : `fixed allowance=$${allowance}`} | mode=${aiMode ? 'Grok AI' : 'ROMA'} | tf=${TF}`)
  }

  stop() {
    this.active     = false
    this.isRunning  = false
    this.agentPhase = 'idle'
    this.clearTimers()
    this.saveConfig()
    this.pushState(true)
    console.log('[ServerAgent] Stopped')
  }

  setAllowance(amount: number, kellyMode?: boolean, bankroll?: number) {
    if (kellyMode !== undefined) this.kellyMode = kellyMode
    if (this.kellyMode && bankroll && bankroll > 0) {
      this.bankroll  = bankroll
      this.allowance = Math.max(1, bankroll * KELLY_FRACTION)
    } else if (!this.kellyMode) {
      this.allowance = Math.max(0, amount)
    }
    this.saveConfig()
    this.pushState()
  }

  clearHistory() {
    this.trades          = []
    this.windowKey       = null
    this.windowBetPlaced = false
    clearTradeLog()
    this.pushState()
  }

  async triggerCycle() {
    if (this.isRunning) return
    if (this.autoTimeout) { clearTimeout(this.autoTimeout); this.autoTimeout = null }
    this.stopDPoller()
    await this.runCycle()
  }

  getState(): AgentStateSnapshot {
    return {
      active:           this.active,
      allowance:        this.allowance,
      initialAllowance: this.initialAllowance,
      bankroll:         this.bankroll,
      kellyMode:        this.kellyMode,
      aiMode:           this.aiMode,
      isRunning:        this.isRunning,
      windowKey:        this.windowKey,
      windowBetPlaced:  this.windowBetPlaced,
      currentD:         this.currentD,
      lastPollAt:       this.lastPollAt,
      nextCycleIn:      this.nextCycleIn,
      error:            this.error,
      orderError:       this.orderError,
      trades:           this.trades,
      stats:            computeStats(this.trades),
      pipeline:         this.pipeline,
      strikePrice:      this.strikePrice,
      gkVol:            this.gkVol,
      agentPhase:       this.agentPhase,
      windowCloseAt:    this.windowCloseAt,
    }
  }

  // ── Private helpers ────────────────────────────────────────────────────────

  private pushState(forceKv = false) {
    const state = this.getState()
    this.emit('state', state)
    this.flushToKV(forceKv)
  }

  private startCountdown() {
    if (this.countdownInterval) clearInterval(this.countdownInterval)
    this.countdownInterval = setInterval(() => {
      const remaining = Math.max(0, Math.round((this.nextRunAt - Date.now()) / 1000))
      if (remaining !== this.nextCycleIn) {
        this.nextCycleIn = remaining
        this.pushState()
      }
    }, 1000)
  }

  private clearTimers() {
    if (this.autoTimeout)        { clearTimeout(this.autoTimeout);          this.autoTimeout        = null }
    if (this.countdownInterval)  { clearInterval(this.countdownInterval);   this.countdownInterval  = null }
    if (this.settlementInterval) { clearInterval(this.settlementInterval);  this.settlementInterval = null }
    this.stopDPoller()
  }

  private schedule(fn: () => void, ms: number) {
    if (this.autoTimeout) { clearTimeout(this.autoTimeout); this.autoTimeout = null }
    this.autoTimeout = setTimeout(() => {
      this.autoTimeout = null
      if (this.active) fn()
    }, ms)
  }

  private stopDPoller() {
    if (this.pollerInterval) { clearInterval(this.pollerInterval); this.pollerInterval = null }
  }

  private startSettlementLoop() {
    if (this.settlementInterval) clearInterval(this.settlementInterval)
    this.settlementInterval = setInterval(() => {
      if (this.active) this.checkSettlements().catch(e => console.error('[ServerAgent] settlement loop error:', e))
    }, 30_000)
  }

  private async checkSettlements() {
    const now = Date.now()
    const expired = this.trades.filter(
      t => t.status === 'open' && t.liveOrderId && now >= new Date(t.expiresAt).getTime()
    )
    if (!expired.length) return

    const settled = await Promise.all(expired.map(async t => {
      const outcome = await fetchOutcome(new Date(t.expiresAt).getTime())
      if (outcome) {
        const win = (t.side === 'yes' && outcome === 'up') || (t.side === 'no' && outcome === 'down')
        const fee = polyFee(t.contracts, t.limitPrice ?? Math.round(t.cost / t.contracts * 100))
        return { ...t, status: (win ? 'won' : 'lost') as 'won' | 'lost', pnl: win ? t.contracts - t.cost - fee : -t.cost - fee }
      }
      return t
    }))

    const justSettled = settled.filter(s => s.status !== 'open')
    if (!justSettled.length) return

    this.trades = this.trades.map(t => settled.find(s => s.id === t.id) ?? t)

    for (const t of justSettled) {
      updateTrade(t.id, { status: t.status, pnl: t.pnl, settlementPrice: t.settlementPrice })
      if (t.pnl != null) recordTradeResult(t.pnl)
    }

    if (this.kellyMode) {
      for (const t of justSettled) {
        const fee = polyFee(t.contracts, t.limitPrice ?? Math.round(t.cost / t.contracts * 100))
        if (t.status === 'won') this.bankroll += t.contracts - fee
        else                    this.bankroll -= fee
      }
      this.bankroll  = Math.max(1, this.bankroll)
      this.allowance = Math.max(1, Math.round(this.bankroll * this.kellyPct * 100) / 100)
      this.saveConfig()
      console.log(`[ServerAgent] Kelly update — bankroll=$${this.bankroll.toFixed(2)} → allowance=$${this.allowance.toFixed(2)}`)
    }

    this.pushState()
    console.log(`[ServerAgent] Settled ${justSettled.length} trade(s) via background loop`)
  }

  /**
   * Fast-path entry: places an order in ~1s when d triggers, WITHOUT waiting for
   * the full ROMA pipeline. Uses d-sign for direction and normalCDF(d) as the
   * probability estimate for Kelly sizing. Reads the live market straight from
   * the shared feed (already real-time — no REST refetch needed).
   */
  private async fastEntry(d: number, closeMs: number): Promise<void> {
    if (!this.active || this.windowBetPlaced || !this.windowKey) return
    const minutesLeft = (closeMs - Date.now()) / 60_000
    if (minutesLeft < MIN_MINUTES_LEFT) return

    const side: 'yes' | 'no' = d > 0 ? 'yes' : 'no'

    try {
      const market = polyFeed.market(TF)
      if (!market || !market.upTokenId) return

      const askPrice = side === 'yes' ? market.yes_ask : market.no_ask
      const maxFastPrice = side === 'yes' ? MAX_FAST_ENTRY_PRICE_YES : MAX_FAST_ENTRY_PRICE_NO
      if (askPrice < MIN_FAST_ENTRY_PRICE || askPrice > maxFastPrice) {
        console.log(`[ServerAgent] Fast-path: ${side}_ask=${askPrice}¢ outside [${MIN_FAST_ENTRY_PRICE}, ${maxFastPrice}]¢ — skip`)
        return
      }

      const pModel       = normalCDF(Math.abs(d))
      const p_d          = askPrice / 100
      const feePerC      = 0
      const netWinPerC   = (1 - p_d) - feePerC
      const totalCostPerC = p_d + feePerC
      const b            = netWinPerC / totalCostPerC
      const pWin      = side === 'yes' ? pModel : (1 - pModel)
      const kellyFrac = Math.max(0, (b * pWin - (1 - pWin)) / b)
      if (kellyFrac <= 0) { console.log(`[ServerAgent] Fast-path: Kelly=0 at ${askPrice}¢ — skip`); return }
      const edgePct = (pWin * netWinPerC + (1 - pWin) * (-p_d - feePerC)) * 100
      if (edgePct < 6) { console.log(`[ServerAgent] Fast-path: edge ${edgePct.toFixed(2)}% < 6% — skip`); return }
      const capital = this.bankroll > 0 ? this.bankroll : this.allowance
      const halfKellyCapital = kellyFrac * 0.18 * capital
      const contracts        = Math.max(1, Math.round(halfKellyCapital / totalCostPerC))
      const cost             = contracts * totalCostPerC
      if (cost < 1) return
      const expectedProfit = netWinPerC * contracts
      if (expectedProfit < 2.00) { console.log(`[ServerAgent] Fast-path: net profit $${expectedProfit.toFixed(2)} < $2.00 — skip`); return }

      console.log(`[ServerAgent] ⚡ Fast-path: ${side.toUpperCase()} ${contracts}× @ ${askPrice}¢ | d=${d.toFixed(3)} pModel=${(pModel*100).toFixed(1)}% Kelly=${(kellyFrac*100).toFixed(1)}%`)

      const ioPrice = Math.min(99, askPrice + 3)
      let res = await buy(market, side, ioPrice, contracts)
      if (!res.ok || res.filled <= 0) {
        const retry = await buy(market, side, Math.min(99, askPrice + 5), contracts)
        if (!retry.ok || retry.filled <= 0) {
          console.log(`[ServerAgent] Fast-path: both FAK attempts unfilled — falling through to pipeline`)
          return
        }
        res = retry
      }

      const actualFilled = res.filled || contracts
      const actualCost   = actualFilled * (askPrice / 100)
      this.windowBetPlaced = true
      this.agentPhase      = 'bet_placed'
      this.orderError      = null

      const evTicker = market.slug
      const trade: AgentTrade = {
        id:              `fast-${Date.now()}`,
        cycleId:         -1,
        windowKey:       evTicker,
        sliceNum:        1,
        side,
        limitPrice:      askPrice,
        contracts:       actualFilled,
        cost:            actualCost,
        marketTicker:    market.slug,
        strikePrice:     this.strikePrice,
        btcPriceAtEntry: undefined,
        expiresAt:       market.close_time,
        enteredAt:       new Date().toISOString(),
        status:          'open',
        pModel,
        pMarket:         askPrice / 100,
        edge:            edgePct,
        signals: {
          sentimentScore: 0, sentimentMomentum: 0, orderbookSkew: 0, sentimentLabel: 'fast_entry',
          pLLM: pModel, confidence: Math.abs(d) >= 1.1 ? 'high' : 'medium', gkVol: this.gkVol,
          distancePct: (Math.exp(this.gkVol * Math.sqrt(minutesLeft / 15) * Math.abs(d)) - 1) * 100,
          minutesLeft, aboveStrike: d > 0, priceMomentum1h: 0,
        },
        liveOrderId:  res.orderId,
        orderError:   undefined,
      }
      this.trades = [...this.trades, trade]
      appendTrade(trade)
      if (this.kellyMode) this.bankroll = Math.max(1, this.bankroll - actualCost)

      console.log(`[ServerAgent] ✓ Fast-path filled — ${side.toUpperCase()} ${actualFilled}× @ ${askPrice}¢ on ${evTicker}`)
      this.pushState(true)
    } catch (e) {
      console.error('[ServerAgent] Fast-path error:', e)
    }
  }

  private startDPoller(closeMs: number) {
    this.stopDPoller()
    this.windowCloseAt = closeMs
    this.agentPhase    = this.strikePrice > 0 ? 'monitoring' : 'bootstrap'
    this.pushState()

    const SCAN_INTERVAL_MS = 5_000

    let pollInFlight = false
    const check = async () => {
      if (!this.active || this.isRunning || this.windowBetPlaced || pollInFlight) return
      pollInFlight = true

      const minutesLeft = (closeMs - Date.now()) / 60_000

      if (minutesLeft < MIN_MINUTES_LEFT) {
        this.stopDPoller()
        if (!this.windowBetPlaced) {
          const waitMs = Math.max(POST_WINDOW_BUFFER_MS, closeMs - Date.now() + POST_WINDOW_BUFFER_MS)
          this.agentPhase  = 'waiting'
          this.nextRunAt   = Date.now() + waitMs
          this.nextCycleIn = Math.round(waitMs / 1000)
          this.schedule(() => this.scheduleNextRun(), waitMs)
          this.pushState()
          console.log(`[ServerAgent] Window expiring without bet — next window in ${Math.round(waitMs/1000)}s`)
        }
        pollInFlight = false
        return
      }

      if (this.strikePrice <= 0) {
        this.stopDPoller()
        pollInFlight = false
        await this.runCycle()
        return
      }

      // Live % distance from strike — straight off the shared canonical BTC.
      try {
        const price = polyFeed.quote().price
        if (price > 0) {
          this.currentD   = ((price - this.strikePrice) / this.strikePrice) * 100
          this.lastPollAt = Date.now()
          this.pushState()
        }
      } catch {}

      // No-LLM deterministic fast-path: when BTC is decisively off the strike,
      // enter directly. The full ROMA/Grok pipeline needs an LLM key; this path
      // does not. Fires at most once per window (windowBetPlaced guard).
      if (!this.windowBetPlaced && Math.abs(this.currentD) >= FAST_PATH_MIN_D) {
        await this.fastEntry(this.currentD, closeMs)
        if (this.windowBetPlaced) { pollInFlight = false; return }
      }

      const now = Date.now()
      if (now - this.lastCycleAt >= SCAN_INTERVAL_MS) {
        this.lastCycleAt = now
        this.stopDPoller()
        pollInFlight = false
        console.log(`[ServerAgent] ${minutesLeft.toFixed(1)}min left — scanning Markov signal`)
        await this.runCycle()
        return
      }

      pollInFlight = false
    }

    check()
    this.pollerInterval = setInterval(check, 2_000)
  }

  private scheduleNextRun() {
    if (!this.active) return
    if (this.autoTimeout) { clearTimeout(this.autoTimeout); this.autoTimeout = null }
    this.stopDPoller()
    this.windowBetPlaced = false
    this.strikePrice     = 0
    this.lastPollAt      = null
    this.currentD        = 0
    this.lastCycleAt     = 0

    const { delayMs, closeMs } = getDelayMs()
    this.windowCloseAt = closeMs

    if (delayMs === 0) {
      this.nextRunAt   = 0
      this.nextCycleIn = 0
      this.startDPoller(closeMs)
    } else {
      this.agentPhase  = 'waiting'
      this.nextRunAt   = Date.now() + delayMs
      this.nextCycleIn = Math.round(delayMs / 1000)
      this.schedule(() => {
        const { closeMs: cm } = getDelayMs()
        this.startDPoller(cm)
      }, delayMs)
    }

    this.pushState()
  }

  // ── Core cycle ─────────────────────────────────────────────────────────────

  private async runCycle() {
    if (this.isRunning) return
    this.isRunning  = true
    this.error      = null
    const wasBootstrap = this.strikePrice <= 0
    this.agentPhase = wasBootstrap ? 'bootstrap' : 'pipeline'
    this.emit('pipeline_start', {})
    this.pushState()

    try {
      polyFeed.start()

      // ── Active Polymarket window from the shared feed ──────────────────────
      const pm = polyFeed.market(TF)
      if (!pm || !pm.upTokenId || pm.up_ask <= 0 || pm.up_ask >= 100) {
        throw new Error(`No active Polymarket ${TF} window from poly-dash feed yet`)
      }
      const markets: KalshiMarket[] = [pm as unknown as KalshiMarket]

      // ── BTC quote from the shared canonical feed ───────────────────────────
      const price = polyFeed.quote().price
      if (!(price > 0)) throw new Error('BTC price unavailable from poly-dash feed')
      const quote: BTCQuote = {
        price, percent_change_1h: 0, percent_change_24h: 0,
        volume_24h: 0, market_cap: price * 19_700_000, last_updated: new Date().toISOString(),
      }

      // ── Candles (Coinbase REST) + derivatives (Bybit) — external, not via Kalshi ──
      const [candleRes, liveCandleRes, bybitRes] = await Promise.all([
        fetch('https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=900&limit=13', { cache: 'no-store' }).catch(() => null),
        fetch('https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=60&limit=16', { cache: 'no-store' }).catch(() => null),
        fetch('https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT', { cache: 'no-store' }).catch(() => null),
      ])

      // ── Portfolio value: shared wallet balance (cents), or Kelly bankroll ──
      const balUsd = await getBalanceUsd().catch(() => null)
      const actualBalanceCents = balUsd != null ? Math.round(balUsd * 100) : 0
      const portfolioValueCents = (this.kellyMode && this.bankroll > 0)
        ? Math.max(actualBalanceCents, Math.round(this.bankroll * 100))
        : actualBalanceCents

      let candles: OHLCVCandle[] = []
      if (candleRes?.ok) { const r = await candleRes.json(); candles = Array.isArray(r) ? r.slice(1, 13) : [] }
      let liveCandles: OHLCVCandle[] = []
      if (liveCandleRes?.ok) { const r = await liveCandleRes.json(); liveCandles = Array.isArray(r) ? r : [] }

      let derivatives: DerivativesSignal | null = null
      if (bybitRes?.ok) {
        const d = await bybitRes.json()
        const t = d?.result?.list?.[0]
        if (t) {
          const markPrice  = parseFloat(t.markPrice)
          const indexPrice = parseFloat(t.indexPrice)
          const fundingRate = parseFloat(t.fundingRate)
          if (markPrice > 0 && indexPrice > 0 && !isNaN(fundingRate)) {
            derivatives = { fundingRate, basis: ((markPrice - indexPrice) / indexPrice) * 100, markPrice, indexPrice, source: 'bybit' }
          }
        }
      }

      // ── Orderbook from the shared feed (up=yes, down=no) ───────────────────
      const orderbook: KalshiOrderbook | null = (() => {
        const up = polyFeed.orderbook(pm.upTokenId)
        const down = polyFeed.orderbook(pm.downTokenId)
        if (!up && !down) return null
        const lvls = (b?: { bids: [number, number][] }) => (b?.bids ?? []).map(([price, size]) => ({ price, delta: size }))
        return { yes: lvls(up ?? undefined), no: lvls(down ?? undefined) }
      })()

      // ── Run pipeline ───────────────────────────────────────────────────────
      const provider  = (process.env.AI_PROVIDER ?? 'grok') as AIProvider
      const romaMode  = process.env.ROMA_MODE ?? 'keen'

      if (!tryLockPipeline()) throw new Error('Pipeline already running')

      let result: PipelineState
      try {
        result = await runAgentPipeline(
          markets, quote, orderbook, provider, romaMode, this.aiMode,
          undefined, undefined,
          candles, liveCandles, derivatives, this.orModel, undefined,
          (key, agentResult) => this.emit('agent', { key, result: agentResult }),
          portfolioValueCents,
        )
      } finally {
        releasePipelineLock()
      }

      this.pipeline = result
      await this.processResult(result, wasBootstrap)

    } catch (err) {
      console.error('[ServerAgent] runCycle error:', err)
      this.error        = String(err)
      this.pipelineError = true
    } finally {
      this.isRunning = false

      if (this.active) {
        const { minutesLeft, closeMs: freshClose } = getDelayMs()
        const failed       = this.orderFailed
        const pipeErr      = this.pipelineError
        this.orderFailed   = false
        this.pipelineError = false

        if (pipeErr) {
          const retryMs    = 5_000
          this.nextRunAt   = Date.now() + retryMs
          this.nextCycleIn = Math.round(retryMs / 1000)
          this.agentPhase  = 'error'
          console.log('[ServerAgent] Pipeline error — retrying in 5s')
          this.schedule(() => this.scheduleNextRun(), retryMs)
        } else if (failed && minutesLeft >= MIN_MINUTES_LEFT) {
          this.nextRunAt   = Date.now() + 60_000
          this.nextCycleIn = 60
          this.schedule(() => {
            const { closeMs: cm } = getDelayMs()
            this.startDPoller(cm)
          }, 60_000)
        } else if (!this.windowBetPlaced && minutesLeft >= MIN_MINUTES_LEFT) {
          this.agentPhase = 'monitoring'
          this.startDPoller(freshClose)
        } else {
          const waitMs     = Math.max(POST_WINDOW_BUFFER_MS, freshClose - Date.now() + POST_WINDOW_BUFFER_MS)
          this.agentPhase  = this.windowBetPlaced ? 'bet_placed' : 'waiting'
          this.nextRunAt   = Date.now() + waitMs
          this.nextCycleIn = Math.round(waitMs / 1000)
          this.schedule(() => this.scheduleNextRun(), waitMs)
        }
      }

      this.pushState()
    }
  }

  // ── Process pipeline result & place order ──────────────────────────────────

  private async processResult(data: PipelineState, isBootstrap: boolean) {
    const exec  = data.agents.execution.output
    const md    = data.agents.marketDiscovery.output
    const pf    = data.agents.priceFeed.output
    const prob  = data.agents.probability.output
    const risk  = data.agents.markov.output
    const sent  = data.agents.sentiment.output

    const evTicker = (md.activeMarket as { slug?: string; event_ticker?: string } | undefined)?.slug
      ?? md.activeMarket?.ticker
      ?? null

    if (md.strikePrice > 0)                        this.strikePrice          = md.strikePrice
    if (prob.gkVol15m && prob.gkVol15m > 0)        this.gkVol                = prob.gkVol15m
    if (md.activeMarket?.ticker)                   this.currentMarketTicker  = md.activeMarket.ticker
    this.currentD = pf.aboveStrike ? pf.distanceFromStrikePct : -pf.distanceFromStrikePct

    if (evTicker && evTicker !== this.windowKey) {
      this.windowKey       = evTicker
      this.windowBetPlaced = false
    }

    if (isBootstrap) {
      const distPct = pf.aboveStrike ? pf.distanceFromStrikePct : -pf.distanceFromStrikePct
      this.currentD = distPct
      console.log(`[ServerAgent] Bootstrap: strike=$${md.strikePrice} BTC=${distPct >= 0 ? '+' : ''}${distPct.toFixed(2)}% from strike — Markov poller scanning`)
      return
    }

    const msUntilClose    = this.windowCloseAt > 0 ? this.windowCloseAt - Date.now() : Infinity
    const minsUntilClose  = msUntilClose / 60_000
    if (
      exec.action !== 'PASS' &&
      exec.side   != null    &&
      exec.limitPrice != null &&
      risk.approved          &&
      md.activeMarket        &&
      evTicker               &&
      this.allowance >= 1    &&
      !this.windowBetPlaced  &&
      minsUntilClose >= MIN_MINUTES_LEFT
    ) {
      // Fresh price straight off the real-time feed (no REST refetch).
      const market = polyFeed.market(TF)
      if (!market || !market.upTokenId) { console.warn('[ServerAgent] feed market unavailable at order time — skip'); return }
      let liveLimitPrice = exec.limitPrice
      const freshPrice = exec.side === 'yes' ? market.yes_ask : market.no_ask
      if (freshPrice > 0) {
        const maxFreshPrice = exec.side === 'yes' ? MAX_FAST_ENTRY_PRICE_YES : MAX_FAST_ENTRY_PRICE_NO
        if (freshPrice > maxFreshPrice) {
          console.log(`[ServerAgent] Fresh quote: ${exec.side}_ask=${freshPrice}¢ > ${maxFreshPrice}¢ cap — SKIP`)
          return
        }
        liveLimitPrice = freshPrice
      }

      const costPerContract = liveLimitPrice / 100
      const contracts       = Math.max(1, Math.floor(this.allowance / costPerContract))
      const cost            = contracts * costPerContract

      let liveOrderId: string | undefined
      let orderErrorMsg: string | undefined
      let iocUnfilled   = false

      try {
        let res = await buy(market, exec.side, Math.min(99, liveLimitPrice + 3), contracts)
        if ((!res.ok || res.filled <= 0) && res.ok) {
          console.log(`[ServerAgent] FAK unfilled — retrying with +5¢`)
          res = await buy(market, exec.side, Math.min(99, liveLimitPrice + 5), contracts)
        }
        if (res.ok && res.filled > 0) {
          liveOrderId = res.orderId
          console.log(`[ServerAgent] FAK filled ${res.filled} shares`)
        } else if (!res.ok) {
          orderErrorMsg = res.error ?? 'Order failed'
        } else {
          iocUnfilled   = true
          orderErrorMsg = 'FAK unfilled — no liquidity, skipping window'
          console.warn(`[ServerAgent] ${orderErrorMsg}`)
        }
      } catch (e) {
        orderErrorMsg = String(e)
      }

      const trade: AgentTrade = {
        id:               `${data.cycleId}-${Date.now()}`,
        cycleId:          data.cycleId,
        windowKey:        evTicker,
        sliceNum:         1,
        side:             exec.side,
        limitPrice:       liveLimitPrice,
        contracts,
        cost,
        marketTicker:     market.slug,
        strikePrice:      md.strikePrice,
        btcPriceAtEntry:  pf.currentPrice,
        expiresAt:        (md.activeMarket as unknown as PolyMarket).close_time ?? market.close_time,
        enteredAt:        new Date().toISOString(),
        status:           'open',
        pModel:           prob.pModel,
        pMarket:          prob.pMarket,
        edge:             prob.edge,
        signals: {
          sentimentScore: sent.score, sentimentMomentum: sent.momentum, orderbookSkew: sent.orderbookSkew,
          sentimentLabel: sent.label, pLLM: prob.pModel, confidence: prob.confidence,
          gkVol: prob.gkVol15m ?? null, distancePct: pf.distanceFromStrikePct,
          minutesLeft: md.minutesUntilExpiry, aboveStrike: pf.aboveStrike, priceMomentum1h: pf.priceChangePct1h,
        },
        liveOrderId,
        orderError:       orderErrorMsg,
      }

      this.trades = [...this.trades, trade]
      appendTrade(trade)

      if (liveOrderId) {
        this.windowBetPlaced = true
        this.orderError      = null
        this.agentPhase      = 'bet_placed'
        if (this.kellyMode) this.bankroll = Math.max(1, this.bankroll - cost)
        console.log(`[ServerAgent] ✓ Bet placed — ${exec.side.toUpperCase()} ${contracts}× @ ${liveLimitPrice}¢ on ${evTicker}`)
      } else if (iocUnfilled) {
        this.orderError  = orderErrorMsg ?? 'Skipped — no fill'
        this.agentPhase  = 'pass_skipped'
        console.log(`[ServerAgent] Skipping window — ${this.orderError}`)
      } else if (orderErrorMsg) {
        this.orderFailed = true
        this.orderError  = orderErrorMsg
        this.agentPhase  = 'order_failed'
        console.error(`[ServerAgent] ✗ Order failed: ${orderErrorMsg}`)
      }
    }

    // Settle expired trades
    const now          = Date.now()
    const expiredTrades = this.trades.filter(
      t => t.status === 'open' && t.liveOrderId && now >= new Date(t.expiresAt).getTime()
    )
    if (expiredTrades.length > 0) {
      const settled = await Promise.all(expiredTrades.map(async t => {
        const outcome = await fetchOutcome(new Date(t.expiresAt).getTime())
        if (outcome) {
          const win = (t.side === 'yes' && outcome === 'up') || (t.side === 'no' && outcome === 'down')
          const fee = polyFee(t.contracts, t.limitPrice ?? Math.round(t.cost / t.contracts * 100))
          const pnl = win ? (t.contracts - t.cost) - fee : -t.cost - fee
          return { ...t, status: (win ? 'won' : 'lost') as 'won' | 'lost', settlementPrice: pf.currentPrice, pnl }
        }
        return t
      }))
      const justSettled = settled.filter(s => s.status !== 'open')
      this.trades = this.trades.map(t => settled.find(s => s.id === t.id) ?? t)

      for (const t of justSettled) {
        updateTrade(t.id, { status: t.status, pnl: t.pnl, settlementPrice: t.settlementPrice })
        if (t.pnl != null) recordTradeResult(t.pnl)
      }

      if (this.kellyMode && justSettled.length > 0) {
        for (const t of justSettled) {
          if (t.status === 'won') {
            const fee = polyFee(t.contracts, t.limitPrice ?? Math.round(t.cost / t.contracts * 100))
            this.bankroll += t.contracts - fee
          }
        }
        this.bankroll  = Math.max(1, this.bankroll)
        this.allowance = Math.max(1, Math.round(this.bankroll * this.kellyPct * 100) / 100)
        this.saveConfig()
        console.log(`[ServerAgent] Kelly update — bankroll=$${this.bankroll.toFixed(2)} → allowance=$${this.allowance.toFixed(2)}`)
      }
    }
  }
}

// Singleton pinned to globalThis — survives Next.js HMR and is shared across
// all API routes that run in the same warm Node.js instance.
const g = globalThis as typeof globalThis & { _serverAgent?: ServerAgent }
if (!g._serverAgent) {
  g._serverAgent = new ServerAgent()
  setImmediate(() => { g._serverAgent!['restoreConfig']() })
}
export const serverAgent = g._serverAgent
