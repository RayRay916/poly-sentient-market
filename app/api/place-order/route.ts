import { NextRequest, NextResponse } from 'next/server'
import { placeOrder } from '@/lib/polymarket/exec'
import { polyFeed } from '@/lib/polymarket/feed'
import type { Timeframe } from '@/lib/polymarket/types'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'

function resolveTok(ticker: string, side: string): { tokenId?: string; tickSize?: number; negRisk?: boolean } {
  for (const tf of ['5m', '15m', '1h'] as Timeframe[]) {
    const m = polyFeed.market(tf)
    if (m && m.slug === ticker) {
      const tokenId = side === 'yes' || side === 'up' ? m.upTokenId : m.downTokenId
      return { tokenId, tickSize: m.tickSize, negRisk: m.negRisk }
    }
  }
  return {}
}

export async function POST(req: NextRequest) {
  try {
    polyFeed.start()
    const body = await req.json()
    const { ticker, side, count, yesPrice, noPrice } = body

    if (!ticker || !side || !count) {
      return NextResponse.json({ ok: false, error: 'Missing required fields: ticker, side, count' }, { status: 400 })
    }

    if (!['yes', 'no'].includes(side)) {
      return NextResponse.json({ ok: false, error: 'side must be "yes" or "no"' }, { status: 400 })
    }

    const { tokenId, tickSize, negRisk } = resolveTok(ticker, side)
    if (!tokenId) {
      return NextResponse.json({ ok: false, error: `No market found for ticker: ${ticker}` }, { status: 404 })
    }

    const rawPrice = yesPrice ?? noPrice ?? body.price
    const priceCents = Math.max(1, Math.min(99, Math.round(Number(rawPrice))))
    const size = Number(count)

    const result = await placeOrder({ tokenId, priceCents, size, orderType: 'FAK', tickSize, negRisk })
    return NextResponse.json(result, { status: result.ok ? 200 : 422 })
  } catch (err) {
    return NextResponse.json({ ok: false, error: String(err) }, { status: 500 })
  }
}
