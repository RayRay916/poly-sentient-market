import { NextRequest, NextResponse } from 'next/server'
import { polyFeed } from '@/lib/polymarket/feed'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'

export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ ticker: string }> }
) {
  polyFeed.start()
  // `ticker` is now a Polymarket tokenId (the mirrored L2 book key).
  const { ticker } = await params
  const book = polyFeed.orderbook(ticker)
  return NextResponse.json(book ?? { error: 'no book' })
}
