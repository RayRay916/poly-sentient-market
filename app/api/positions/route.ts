import { NextResponse } from 'next/server'
import { polyFeed } from '@/lib/polymarket/feed'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'

export async function GET() {
  polyFeed.start()
  // Polymarket positions come straight from the shared feed (keyed by tokenId).
  // Resting orders/fills aren't tracked separately here — keep the keys the
  // frontend reads, but empty.
  return NextResponse.json({
    positions: polyFeed.positions(),
    orders: [],
    fills: [],
  })
}
