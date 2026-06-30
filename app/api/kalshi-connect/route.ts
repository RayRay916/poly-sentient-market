import { NextResponse } from 'next/server'
import { getBalanceUsd } from '@/lib/polymarket/exec'

export const runtime = 'nodejs'

/**
 * Wallet status for the Polymarket trader.
 * The signing wallet lives in the Rust exec service's .env (exec-rs), not in
 * user-uploaded credentials — so there is nothing to store/encrypt here. The
 * frontend still calls /api/kalshi-connect, so the path is kept.
 */

/** GET — report the Polymarket exec wallet status. */
export async function GET() {
  const balanceUsd = await getBalanceUsd()
  return NextResponse.json({
    connected: balanceUsd != null,
    source: 'exec-wallet',
    balanceUsd: balanceUsd ?? 0,
  })
}

/** POST — no-op; the wallet is provisioned in exec-rs/.env, not via the UI. */
export async function POST() {
  return NextResponse.json({ ok: true })
}

/** DELETE — no-op; the wallet is not user-managed. */
export async function DELETE() {
  return NextResponse.json({ ok: true })
}
