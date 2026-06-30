import { NextRequest, NextResponse } from 'next/server'
import { cancelOrder } from '@/lib/polymarket/exec'

export const runtime = 'nodejs'
export const dynamic = 'force-dynamic'

export async function DELETE(_req: NextRequest, { params }: { params: Promise<{ orderId: string }> }) {
  const { orderId } = await params
  const result = await cancelOrder(orderId)
  return NextResponse.json(result, { status: result.ok ? 200 : 422 })
}
