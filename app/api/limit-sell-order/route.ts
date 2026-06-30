import { NextRequest, NextResponse } from 'next/server'
import { marketSell } from '@/lib/polymarket/exec'
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
    const { ticker, side, count } = await req.json()
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
    // Polymarket exec has no resting limit-sell; take-profit at resolution is automatic.
    // Map limit-sell to a market-sell of the position.
    const result = await marketSell(tokenId, Number(count), { tickSize, negRisk })
    return NextResponse.json(
      { ...result, note: 'limit-sell mapped to market-sell; take-profit at resolution is automatic on Polymarket' },
      { status: result.ok ? 200 : 422 },
    )
  } catch (err) {
    return NextResponse.json({ ok: false, error: String(err) }, { status: 500 })
  }
}
