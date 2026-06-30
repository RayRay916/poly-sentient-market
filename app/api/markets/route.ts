import { NextResponse } from 'next/server'
import { polyFeed } from '@/lib/polymarket/feed'
import type { PolyMarket, Timeframe } from '@/lib/polymarket/types'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'

export async function GET() {
  polyFeed.start()
  const tfs: Timeframe[] = ['5m', '15m', '1h']
  const markets = tfs
    .map(tf => polyFeed.market(tf))
    .filter((m): m is PolyMarket => m !== null)
  return NextResponse.json({ markets })
}
