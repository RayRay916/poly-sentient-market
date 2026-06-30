// Polymarket order execution for the autonomous trader.
//
// Thin client over the local Rust exec microservice (`poly-sentient-exec`,
// 127.0.0.1:4321) which owns the wallet + polyfill-rs signing (forked from
// poly-dash). poly-sentient never signs orders itself — it passes the token id +
// limit price + tick size (all already known from the shared poly-dash feed) and
// the Rust service signs/posts to the CLOB.
//
// PAPER MODE: when POLY_PAPER=1 (or paper:true is passed) no real order is sent;
// a simulated fill is returned so the autonomous loop can run end-to-end safely.

const EXEC_BASE = process.env.POLY_EXEC_URL ?? 'http://127.0.0.1:4321';

function isPaper(explicit?: boolean): boolean {
  if (explicit !== undefined) return explicit;
  return process.env.POLY_PAPER === '1' || process.env.POLY_PAPER === 'true';
}

export interface PlaceOrderParams {
  /** CLOB token id of the leg to BUY (up or down). */
  tokenId: string;
  /** Limit price in cents, 1..99. */
  priceCents: number;
  /** Share count. */
  size: number;
  /** GTC (default) | FAK | FOK. */
  orderType?: 'GTC' | 'FAK' | 'FOK';
  /** Market tick size (from the feed); default 0.01. */
  tickSize?: number;
  /** Neg-risk market flag (from the feed); default false. */
  negRisk?: boolean;
  /** Force paper mode for this call (overrides the POLY_PAPER env). */
  paper?: boolean;
}

export interface OrderResult {
  ok: boolean;
  orderId?: string;
  state?: string;
  takingAmount?: string;
  makingAmount?: string;
  error?: string;
  paper?: boolean;
  raw?: unknown;
}

async function post(path: string, body: unknown): Promise<Record<string, unknown>> {
  const res = await fetch(`${EXEC_BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  return (await res.json()) as Record<string, unknown>;
}

/** Buy `size` shares of `tokenId` at `priceCents`. The directional bet (up/down)
 *  is expressed by which token id is passed. */
export async function placeOrder(p: PlaceOrderParams): Promise<OrderResult> {
  const priceCents = Math.max(1, Math.min(99, Math.round(p.priceCents)));
  if (isPaper(p.paper)) {
    return { ok: true, paper: true, state: 'PAPER_FILLED', orderId: `paper-${Date.now()}` };
  }
  try {
    const d = await post('/buy', {
      token_id: p.tokenId,
      price_cents: priceCents,
      size: p.size,
      order_type: p.orderType ?? 'GTC',
      tick_size: p.tickSize ?? 0.01,
      neg_risk: p.negRisk ?? false,
    });
    return {
      ok: d.success === true,
      orderId: typeof d.order_id === 'string' ? d.order_id : undefined,
      state: typeof d.state === 'string' ? d.state : undefined,
      takingAmount: typeof d.taking_amount === 'string' ? d.taking_amount : undefined,
      makingAmount: typeof d.making_amount === 'string' ? d.making_amount : undefined,
      error: typeof d.error === 'string' && d.error.length ? d.error : undefined,
      raw: d,
    };
  } catch (e) {
    return { ok: false, error: String(e) };
  }
}

/** Market-sell the full `size` of `tokenId` (FAK at 1¢ "accept any bid"). */
export async function marketSell(
  tokenId: string,
  size: number,
  opts?: { tickSize?: number; negRisk?: boolean; paper?: boolean },
): Promise<OrderResult> {
  if (isPaper(opts?.paper)) return { ok: true, paper: true, state: 'PAPER_SOLD' };
  try {
    const d = await post('/market-sell', {
      token_id: tokenId,
      size,
      tick_size: opts?.tickSize ?? 0.01,
      neg_risk: opts?.negRisk ?? false,
    });
    return { ok: d.success === true, state: typeof d.state === 'string' ? d.state : undefined, raw: d };
  } catch (e) {
    return { ok: false, error: String(e) };
  }
}

export async function cancelOrder(orderId: string, paper?: boolean): Promise<OrderResult> {
  if (isPaper(paper)) return { ok: true, paper: true };
  try {
    const d = await post(`/cancel/${encodeURIComponent(orderId)}`, {});
    return { ok: d.success === true, raw: d };
  } catch (e) {
    return { ok: false, error: String(e) };
  }
}

/** Live USDC balance from the exec service's wallet (dollars). */
export async function getBalanceUsd(): Promise<number | null> {
  try {
    const res = await fetch(`${EXEC_BASE}/balance`, { cache: 'no-store' });
    const d = (await res.json()) as Record<string, unknown>;
    return typeof d.usdc === 'number' ? d.usdc : null;
  } catch {
    return null;
  }
}
