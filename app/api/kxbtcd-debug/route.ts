import { NextResponse } from 'next/server'
import { polyFeed } from '@/lib/polymarket/feed'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'

/**
 * Debug endpoint — shows the current Polymarket BTC Up/Down windows the shared
 * feed is tracking across all three timeframes.
 * GET /api/kxbtcd-debug
 * Returns: { windows: { '5m', '15m', '1h' } }
 */
export async function GET() {
  polyFeed.start()
  return NextResponse.json({
    windows: {
      '5m': polyFeed.market('5m'),
      '15m': polyFeed.market('15m'),
      '1h': polyFeed.market('1h'),
    },
  })
}
