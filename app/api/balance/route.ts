import { NextResponse } from 'next/server'
import { polyFeed } from '@/lib/polymarket/feed'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'

export async function GET() {
  polyFeed.start()
  // USDC balance from the shared feed, reported in cents to match the old shape.
  return NextResponse.json({
    balance: Math.round(polyFeed.balanceUsd() * 100),
    portfolio_value: 0,
  })
}
