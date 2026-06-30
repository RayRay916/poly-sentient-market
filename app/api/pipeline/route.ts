import { NextResponse, type NextRequest } from 'next/server'
import { runAgentPipeline } from '@/lib/agents'
import { getBalanceUsd } from '@/lib/polymarket/exec'
import { polyFeed } from '@/lib/polymarket/feed'
import type { KalshiMarket, KalshiOrderbook, BTCQuote, OHLCVCandle, DerivativesSignal } from '@/lib/types'
import type { AIProvider } from '@/lib/llm-client'
import { tryLockPipeline, releasePipelineLock } from '@/lib/pipeline-lock'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'
export const maxDuration = 300  // 5 min — blitz ROMA makes ~6 LLM calls per solve (~90-150s)


export async function GET(req: NextRequest) {
  // Reject concurrent pipeline runs — each ROMA solve takes ~90-150s; stacking requests
  // fills the Python service queue with zombie tasks and causes cascading timeouts.
  if (!tryLockPipeline()) {
    return NextResponse.json({ error: 'Pipeline already running — retry in ~2min' }, { status: 429 })
  }

  const p = process.env.AI_PROVIDER ?? 'grok'
  const validProviders = ['anthropic', 'openai', 'grok', 'openrouter', 'huggingface'] as const
  const provider: AIProvider = (validProviders as readonly string[]).includes(p) ? p as AIProvider : 'grok'

  // ── Parse mode params FIRST — determines which data paths are required ──────
  // Must be before any 503 gates so hourly mode isn't blocked by a missing 15m market.
  const romaMode    = req.nextUrl.searchParams.get('mode') ?? process.env.ROMA_MODE ?? 'keen'
  const aiRisk      = req.nextUrl.searchParams.get('aiRisk') === 'true'
  const marketMode  = (req.nextUrl.searchParams.get('marketMode') ?? '15m') as '15m' | 'hourly'
  const isHourlyMode = marketMode === 'hourly'

  const p2raw = req.nextUrl.searchParams.get('provider2') ?? process.env.AI_PROVIDER2
  const provider2: AIProvider | undefined =
    p2raw && (validProviders as readonly string[]).includes(p2raw) ? p2raw as AIProvider : undefined

  const providersRaw = req.nextUrl.searchParams.get('providers') ?? process.env.AI_PROVIDERS ?? ''
  const providers: AIProvider[] | undefined = providersRaw
    ? (providersRaw.split(',').filter(p => (validProviders as readonly string[]).includes(p)) as AIProvider[])
    : undefined

  const orModelOverride = req.nextUrl.searchParams.get('orModel') || undefined

  const rawMinGap      = req.nextUrl.searchParams.get('minGap')
  const rawPersistTau  = req.nextUrl.searchParams.get('persistTau')
  const rawMaxPrice    = req.nextUrl.searchParams.get('maxEntryPrice')
  const strategyParams = (rawMinGap || rawPersistTau || rawMaxPrice) ? {
    minGap:        rawMinGap     ? parseFloat(rawMinGap)     : undefined,
    persistTau:    rawPersistTau ? parseFloat(rawPersistTau) : undefined,
    maxEntryPrice: rawMaxPrice   ? parseInt(rawMaxPrice)     : undefined,
  } : undefined

  let apiKeys: Record<string, string> | undefined
  const keysHeader = req.headers.get('x-provider-keys')
  if (keysHeader) {
    try {
      apiKeys = JSON.parse(Buffer.from(keysHeader, 'base64').toString('utf8'))
    } catch { /* ignore malformed header */ }
  }

  // ── Data-fetching phase (before stream starts) ──────────────────────────
  // Any errors here return a plain HTTP response. Once we start the SSE stream,
  // errors are sent as SSE events and the lock is released in the stream's finally.
  let streamStarted = false
  try {
    // Market source: the shared poly-dash feed (no venue REST calls). The feed
    // already returns clean PolyMarket objects with yes/no aliases, so there's
    // nothing to normalize or filter here.
    polyFeed.start()

    // 15m BTC Up/Down window — the default trading market. Cast PolyMarket → the
    // agent pipeline's market type (it carries the yes/no fields the agents read).
    const m15 = polyFeed.market('15m')
    const markets: KalshiMarket[] = m15 ? [m15 as unknown as KalshiMarket] : []

    // In 15m mode, a missing market means we can't trade — abort early.
    // In hourly mode, we continue and the 1h window below provides the market.
    if (!markets.length && !isHourlyMode) {
      console.warn('[pipeline] No active 15m BTC Up/Down market in the feed')
      return NextResponse.json({ error: 'No active 15m market found' }, { status: 503 })
    }

    // BTC price — from the shared feed (the BTC index feed), with a Coinbase
    // Exchange ticker fallback if the feed hasn't warmed yet.
    let quote: BTCQuote | null = polyFeed.quote()
    if (!quote || !(quote.price > 0)) {
      const cbExRes = await fetch('https://api.exchange.coinbase.com/products/BTC-USD/ticker', { cache: 'no-store' }).catch(() => null)
      if (cbExRes?.ok) {
        const cb = await cbExRes.json()
        const price = parseFloat(cb?.price)
        if (price > 0) {
          quote = { price, percent_change_1h: 0, percent_change_24h: 0, volume_24h: 0, market_cap: price * 19_700_000, last_updated: new Date().toISOString() }
        }
      }
    }

    if (!quote || !(quote.price > 0)) {
      console.warn('[pipeline] BTC price unavailable — all sources failed')
      return NextResponse.json({ error: 'BTC price unavailable — all sources failed' }, { status: 503 })
    }
    console.log(`[pipeline] BTC spot: $${quote.price.toLocaleString()}`)

    // Live wallet value (USDC dollars → cents) from the Polymarket exec service.
    let portfolioValueCents = 0
    const balUsd = await getBalanceUsd().catch(() => null)
    if (balUsd != null) portfolioValueCents = Math.round(balUsd * 100)

    // Coinbase candles format: [time_s, low, high, open, close, vol] newest-first
    // granularity=900→15m, 60→1m, 3600→1h, 14400→4h
    const [candleRes, liveCandleRes, candle1hRes, candle4hRes, bybitRes] = await Promise.all([
      fetch('https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=900&limit=14', { cache: 'no-store' }).catch(() => null),
      fetch('https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=60&limit=17', { cache: 'no-store' }).catch(() => null),
      fetch('https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=3600&limit=13', { cache: 'no-store' }).catch(() => null),
      fetch('https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=14400&limit=8', { cache: 'no-store' }).catch(() => null),
      fetch('https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT', { cache: 'no-store' }).catch(() => null),
    ])

    let candles: OHLCVCandle[] = []
    if (candleRes?.ok) {
      const raw = await candleRes.json()
      candles = Array.isArray(raw) ? raw.slice(1, 13) as OHLCVCandle[] : []
    }

    let liveCandles: OHLCVCandle[] = []
    if (liveCandleRes?.ok) {
      const raw = await liveCandleRes.json()
      liveCandles = Array.isArray(raw) ? raw as OHLCVCandle[] : []
    }

    let candles1h: OHLCVCandle[] = []
    if (candle1hRes?.ok) {
      const raw = await candle1hRes.json()
      candles1h = Array.isArray(raw) ? raw.slice(1, 13) as OHLCVCandle[] : []
    }

    let candles4h: OHLCVCandle[] = []
    if (candle4hRes?.ok) {
      const raw = await candle4hRes.json()
      candles4h = Array.isArray(raw) ? raw.slice(1, 8) as OHLCVCandle[] : []
    }

    console.log(`[pipeline] candles: 15m=${candles.length} 1m=${liveCandles.length} 1h=${candles1h.length} 4h=${candles4h.length} | cb1h=${candle1hRes?.status ?? 'fail'} cb4h=${candle4hRes?.status ?? 'fail'}`)

    let derivatives: DerivativesSignal | null = null
    if (bybitRes?.ok) {
      const data = await bybitRes.json()
      const ticker = data?.result?.list?.[0]
      if (ticker) {
        const markPrice = parseFloat(ticker.markPrice)
        const indexPrice = parseFloat(ticker.indexPrice)
        const fundingRate = parseFloat(ticker.fundingRate)
        if (markPrice > 0 && indexPrice > 0 && !isNaN(fundingRate)) {
          derivatives = { fundingRate, basis: ((markPrice - indexPrice) / indexPrice) * 100, markPrice, indexPrice, source: 'bybit' }
        }
      }
    }

    // ── Hourly mode: use the 1h BTC Up/Down window as the active market ─────
    // The agent pipeline accepts a null orderbook and degrades gracefully; the
    // feed exposes per-token books separately (polyFeed.orderbook) when needed.
    const orderbook: KalshiOrderbook | null = null
    let kxbtcdMarket: KalshiMarket | null = null

    if (isHourlyMode) {
      const m1h = polyFeed.market('1h')
      kxbtcdMarket = m1h ? (m1h as unknown as KalshiMarket) : null
      if (!kxbtcdMarket) {
        console.warn('[pipeline] No active 1h BTC Up/Down market in the feed')
        return NextResponse.json({ error: 'KXBTCD_NO_MARKET', message: 'No active hourly BTC Up/Down market right now — the window may be rolling over.' }, { status: 503 })
      }
      console.log(`[pipeline] hourly: selected ${kxbtcdMarket.ticker}`)
    }

    // ── SSE stream phase ──────────────────────────────────────────────────
    // All data is fetched; start the event stream. Lock is released in stream's finally.
    const encoder = new TextEncoder()
    const stream = new ReadableStream({
      async start(controller) {
        function enc(event: string, data: unknown) {
          controller.enqueue(encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`))
        }
        try {
          const pipeline = await runAgentPipeline(
            markets, quote!, orderbook, provider, romaMode, aiRisk,
            provider2, providers,
            candles, liveCandles, derivatives, orModelOverride, req.signal,
            (key, result) => enc('agent', { key, result }),
            portfolioValueCents,
            apiKeys,
            candles1h,
            candles4h,
            isHourlyMode ? kxbtcdMarket : null,  // only activate the hourly market in hourly mode
            strategyParams,
          )
          enc('done', pipeline)
        } catch (err) {
          if (err instanceof Error && err.name === 'AbortError') {
            enc('aborted', {})
          } else {
            enc('error', { message: String(err) })
          }
        } finally {
          releasePipelineLock()
          controller.close()
        }
      },
    })

    streamStarted = true
    return new Response(stream, {
      headers: {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache, no-transform',
        'X-Accel-Buffering': 'no',  // disable nginx buffering
      },
    })
  } finally {
    // Only release lock here if stream never started (data-fetch error path).
    // If the stream started, it owns the lock and releases it in its own finally.
    if (!streamStarted) releasePipelineLock()
  }
}
