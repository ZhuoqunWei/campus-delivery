import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { ApiError, api } from '../../api/client'
import type { Courier, GeoPoint, Order } from '../../api/types'
import { MapView } from '../../components/MapView'
import { StaleDot } from '../../components/StaleDot'
import { StatusBadge } from '../../components/StatusBadge'
import { useAuth } from '../../auth/AuthContext'
import { useLive } from '../../live/useLive'
import { useConfig } from '../../lib/ConfigContext'
import { distanceMeters, fmtDistance, secondsUntil, shortId } from '../../lib/format'
import { useNow } from '../../lib/useNow'

/**
 * Developer/demo console for the courier role. The real courier client is the SwiftUI app; this
 * page drives the same API: go online, receive an offer, accept, send positions, pick up, deliver.
 */
export function CourierConsole() {
  const cfg = useConfig()
  const live = useLive()
  const now = useNow()
  const { user } = useAuth()
  const [me, setMe] = useState<Courier | null>(null)
  const [open, setOpen] = useState<Order[]>([])
  const [autoPing, setAutoPing] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const position = useRef<GeoPoint | null>(null)

  const refreshMe = useCallback(() => api.courier.me().then((r) => { setMe(r.courier); position.current = r.courier.location ?? null }).catch(() => {}), [])
  useEffect(() => { void refreshMe() }, [refreshMe])

  // Live feed keeps my own courier row fresh (status changes, positions); fall back to polling /me.
  const liveMe = live.couriers.find((c) => c.id === user?.id)
  const courier = liveMe ? { ...me, ...liveMe } as Courier : me

  const offer = useMemo(() => live.orders.find((o) => o.status === 'OFFERED' && o.offer?.courierId === user?.id), [live.orders, user])
  const activeOrder = useMemo(() => live.orders.find((o) => o.courierId === user?.id && (o.status === 'ACCEPTED' || o.status === 'PICKED_UP')), [live.orders, user])

  useEffect(() => {
    if (courier?.status !== 'ONLINE') { setOpen([]); return }
    const load = () => api.courier.open().then(setOpen).catch(() => {})
    load()
    const id = window.setInterval(load, 5000)
    return () => window.clearInterval(id)
  }, [courier?.status, live.lastEventId])

  const run = async (fn: () => Promise<unknown>, after?: () => void) => {
    setError(null)
    try {
      await fn()
      after?.()
    } catch (e) {
      setError(e instanceof ApiError ? `${e.code ?? e.status}: ${e.message}` : String(e))
    }
  }

  const sendPosition = useCallback((p: GeoPoint) => {
    position.current = p
    return run(() => api.courier.locations([{ lat: p.lat, lng: p.lng, recordedAt: new Date().toISOString() }]).then((r) => setMe(r.courier)))
  }, [])

  // Re-send the current position every few seconds so the dispatcher does not see us as stale.
  useEffect(() => {
    if (!autoPing || courier?.status !== 'ONLINE') return
    const id = window.setInterval(() => { if (position.current) void sendPosition(position.current) }, Math.max(5, cfg.staleAfterSeconds / 3) * 1000)
    return () => window.clearInterval(id)
  }, [autoPing, courier?.status, cfg.staleAfterSeconds, sendPosition])

  const toggle = () => run(() => api.courier.setStatus(courier?.status === 'ONLINE' ? 'OFFLINE' : 'ONLINE').then(setMe))
  const driveTo = (p: GeoPoint) => sendPosition({ lat: p.lat + (Math.random() - 0.5) * 0.0004, lng: p.lng + (Math.random() - 0.5) * 0.0004 })

  const mapOrders = [offer, activeOrder].filter(Boolean) as Order[]
  const self = courier?.location ?? null

  return (
    <div className="page two-col">
      <aside className="sidebar">
        <section className="card">
          <div className="spread">
            <h2>{courier?.name ?? user?.name}</h2>
            <button className={`btn ${courier?.status === 'ONLINE' ? '' : 'primary'}`} onClick={toggle} disabled={!!activeOrder}>
              {courier?.status === 'ONLINE' ? 'Go offline' : 'Go online'}
            </button>
          </div>
          <p className="muted small">
            {courier?.status === 'ONLINE' ? 'Online — you will receive offers for nearby orders.' : 'Offline — go online and click the map to set your position.'}
          </p>
          <div className="courier-card">
            <span>Position: {self ? `${self.lat.toFixed(5)}, ${self.lng.toFixed(5)}` : 'unknown (click the map)'}</span>
            <StaleDot lastSeenAt={courier?.lastSeenAt} now={now} />
          </div>
          <label className="check"><input type="checkbox" checked={autoPing} onChange={(e) => setAutoPing(e.target.checked)} /> keep sending my position automatically</label>
          {error && <div className="error">{error}</div>}
        </section>

        {offer && offer.offer && (
          <section className="card highlight">
            <h2>New offer <span className="countdown">{secondsUntil(offer.offer.expiresAt, now)}s</span></h2>
            <p><b>{offer.merchant.name}</b> → drop-off {self ? fmtDistance(distanceMeters(offer.merchant.location, offer.dropoff)) : ''} away from pickup</p>
            <p className="muted small">{offer.items.map((i) => `${i.quantity}× ${i.name}`).join(', ')}{offer.note ? ` · “${offer.note}”` : ''}</p>
            <div className="inline">
              <button className="btn primary" onClick={() => run(() => api.courier.accept(offer.id))}>Accept</button>
              <button className="btn" onClick={() => run(() => api.courier.decline(offer.offer!.id))}>Decline</button>
            </div>
          </section>
        )}

        {activeOrder && (
          <section className="card">
            <h2>Active order #{shortId(activeOrder.id)} <StatusBadge status={activeOrder.status} /></h2>
            <p><b>{activeOrder.merchant.name}</b> → {activeOrder.customerName}</p>
            <p className="muted small">{activeOrder.items.map((i) => `${i.quantity}× ${i.name}`).join(', ')}{activeOrder.note ? ` · “${activeOrder.note}”` : ''}</p>
            {activeOrder.status === 'ACCEPTED' && (
              <div className="inline">
                <button className="btn" onClick={() => driveTo(activeOrder.merchant.location)}>Drive to merchant</button>
                <button className="btn primary" onClick={() => run(() => api.courier.pickup(activeOrder.id))}>Mark picked up</button>
              </div>
            )}
            {activeOrder.status === 'PICKED_UP' && (
              <div className="inline">
                <button className="btn" onClick={() => driveTo(activeOrder.dropoff)}>Drive to drop-off</button>
                <button className="btn primary" onClick={() => run(() => api.courier.deliver(activeOrder.id))}>Mark delivered</button>
              </div>
            )}
          </section>
        )}

        <section className="card">
          <h2>Open orders nearby <span className="muted">({open.length})</span></h2>
          <p className="muted small">Unoffered orders within {(cfg.assignmentRadiusMeters / 1000).toFixed(1)} km of your position. Anyone free may take them; the exclusivity rule only applies while an offer is pending.</p>
          <ul className="list">
            {open.map((o) => (
              <li key={o.id} className="row">
                <span><b>{o.merchant.name}</b> <span className="muted small">#{shortId(o.id)}{self ? ` · ${fmtDistance(distanceMeters(self, o.merchant.location))}` : ''}</span></span>
                <button className="btn small" disabled={!!activeOrder || courier?.status !== 'ONLINE'} onClick={() => run(() => api.courier.accept(o.id))}>Accept</button>
              </li>
            ))}
          </ul>
        </section>
      </aside>
      <main className="map-pane">
        <MapView center={self ?? cfg.campus} orders={mapOrders} couriers={[]} now={now} onClick={(p) => void sendPosition(p)} selfPosition={self}
          highlightOrderId={activeOrder?.id ?? offer?.id ?? null} />
        <div className="map-hint">Click anywhere to move here (sends a location batch)</div>
      </main>
    </div>
  )
}
