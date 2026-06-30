import { NextRequest, NextResponse } from 'next/server'
import { polyFeed } from '@/lib/polymarket/feed'
import type { Timeframe } from '@/lib/polymarket/types'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'

export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ ticker: string }> }
) {
  polyFeed.start()
  // `ticker` is now a Polymarket slug — match it against the live windows.
  const { ticker } = await params
  const tfs: Timeframe[] = ['5m', '15m', '1h']
  for (const tf of tfs) {
    const m = polyFeed.market(tf)
    if (m && m.slug === ticker) {
      return NextResponse.json({ market: m })
    }
  }
  return NextResponse.json({ error: 'market not found' }, { status: 404 })
}
