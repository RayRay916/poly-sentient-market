import { NextResponse } from 'next/server'
import { getBalanceUsd } from '@/lib/polymarket/exec'

export const runtime = 'nodejs'

/** GET — return current env config (secrets redacted to presence flags) */
export async function GET() {
  const orKey        = process.env.OPENROUTER_API_KEY
  const xaiKey       = process.env.XAI_API_KEY
  const anthropicKey = process.env.ANTHROPIC_API_KEY

  return NextResponse.json({
    aiProvider:        process.env.AI_PROVIDER        || null,
    romaMode:          process.env.ROMA_MODE          || null,
    romaMaxDepth:      process.env.ROMA_MAX_DEPTH     || null,
    openrouterKeySet:  !!orKey,
    openrouterKeyHint: orKey ? `${orKey.slice(0, 8)}…` : null,
    xaiKeySet:         !!xaiKey,
    anthropicKeySet:   !!anthropicKey,
    walletConfigured:  (await getBalanceUsd()) != null,
    execUrl:           process.env.POLY_EXEC_URL || 'http://127.0.0.1:4321',
  })
}
