import { NextRequest, NextResponse } from 'next/server'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'

export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ orderId: string }> },
) {
  const { orderId } = await params
  // Polymarket has no per-order GET via exec — order/position state is tracked
  // through the positions feed, not by polling individual orders.
  return NextResponse.json({ orderId, status: 'unknown', note: 'tracked via positions feed' }, { status: 200 })
}
