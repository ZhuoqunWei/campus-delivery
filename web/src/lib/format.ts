import type { GeoPoint } from '../api/types'

export function timeAgo(iso: string | undefined, now: number): string {
  if (!iso) return 'never'
  const s = Math.max(0, Math.round((now - Date.parse(iso)) / 1000))
  if (s < 60) return `${s}s ago`
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s ago`
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m ago`
}

export function duration(fromIso: string, toIso: string | undefined, now: number): string {
  const ms = (toIso ? Date.parse(toIso) : now) - Date.parse(fromIso)
  const s = Math.max(0, Math.round(ms / 1000))
  if (s < 60) return `${s}s`
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`
}

export function secondsUntil(iso: string, now: number): number {
  return Math.max(0, Math.round((Date.parse(iso) - now) / 1000))
}

export function clock(iso: string | undefined): string {
  if (!iso) return '—'
  return new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

export function shortId(id: string): string {
  return id.slice(0, 8)
}

export function distanceMeters(a: GeoPoint, b: GeoPoint): number {
  const R = 6371000
  const toRad = (d: number) => (d * Math.PI) / 180
  const dLat = toRad(b.lat - a.lat)
  const dLng = toRad(b.lng - a.lng)
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(a.lat)) * Math.cos(toRad(b.lat)) * Math.sin(dLng / 2) ** 2
  return 2 * R * Math.asin(Math.sqrt(h))
}

export function fmtDistance(m: number): string {
  return m < 1000 ? `${Math.round(m)} m` : `${(m / 1000).toFixed(1)} km`
}

/** Client-side staleness: server receipt time is compared to the client clock. */
export function isStale(lastSeenAt: string | undefined, now: number, staleAfterSeconds: number): boolean {
  return !lastSeenAt || now - Date.parse(lastSeenAt) > staleAfterSeconds * 1000
}
