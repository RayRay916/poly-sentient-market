// Venue-neutral window-timing helpers (nearest-expiry + time-to-close math).
// Pure functions — no venue API. Operate on any market carrying close/expiry ISO times.

export function findNearestMarket<T extends { expiration_time: string }>(markets: T[]): T | null {
  if (!markets.length) return null
  return [...markets].sort(
    (a, b) => new Date(a.expiration_time).getTime() - new Date(b.expiration_time).getTime(),
  )[0]
}

export function minutesUntilExpiry(market: { close_time?: string; expiration_time?: string }): number {
  const closeTime = market.close_time || market.expiration_time || ''
  const ms = new Date(closeTime).getTime() - Date.now()
  return Number.isFinite(ms) ? Math.max(0, ms / 60_000) : 0
}

export function secondsUntilExpiry(market: { close_time?: string; expiration_time?: string }): number {
  const closeTime = market.close_time || market.expiration_time || ''
  const ms = new Date(closeTime).getTime() - Date.now()
  return Number.isFinite(ms) ? Math.max(0, ms / 1_000) : 0
}
