import { useEffect, useMemo, useRef, useState, type FormEvent } from 'react'
import { ApiError, api } from '../../api/client'
import type { GeoPoint, Merchant, Order } from '../../api/types'
import { isTerminal } from '../../api/types'
import { MapView } from '../../components/MapView'
import { StaleDot } from '../../components/StaleDot'
import { StatusBadge } from '../../components/StatusBadge'
import { Timeline } from '../../components/Timeline'
import { useLive } from '../../live/useLive'
import { useConfig } from '../../lib/ConfigContext'
import { clock, secondsUntil, shortId, timeAgo } from '../../lib/format'
import { useNow } from '../../lib/useNow'

export function CustomerPage() {
  const cfg = useConfig()
  const live = useLive()
  const now = useNow()
  const [merchants, setMerchants] = useState<Merchant[]>([])
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [dropoff, setDropoff] = useState<GeoPoint | null>(null)

  useEffect(() => {
    api.merchants().then(setMerchants).catch(() => setMerchants([]))
  }, [])

  const orders = live.orders
  const selected = useMemo(() => orders.find((o) => o.id === selectedId) ?? orders.find((o) => !isTerminal(o.status)) ?? orders[0], [orders, selectedId])
  const couriers = selected?.courier ? [selected.courier] : []
  const fitTo = selected ? [selected.merchant.location, selected.dropoff, ...(selected.courier?.location ? [selected.courier.location] : [])] : null

  return (
    <div className="page two-col">
      <aside className="sidebar">
        <PlaceOrder merchants={merchants} dropoff={dropoff} onDropoff={setDropoff} onPlaced={(o) => { setSelectedId(o.id); setDropoff(null) }} />

        <section className="card">
          <h2>My orders <span className="muted">({orders.length})</span></h2>
          {orders.length === 0 && <p className="muted">No orders yet. Pick a merchant, click the map for a drop-off, and place one.</p>}
          <ul className="list">
            {orders.map((o) => (
              <li key={o.id} className={`row ${o.id === selected?.id ? 'selected' : ''}`} onClick={() => setSelectedId(o.id)}>
                <div>
                  <b>{o.merchant.name}</b> <span className="muted">#{shortId(o.id)}</span>
                  <div className="muted small">{clock(o.createdAt)} · {o.items.map((i) => `${i.quantity}× ${i.name}`).join(', ') || 'no items'}</div>
                </div>
                <StatusBadge status={o.status} />
              </li>
            ))}
          </ul>
        </section>

        {selected && <OrderDetail order={selected} now={now} />}
      </aside>
      <main className="map-pane">
        <MapView center={cfg.campus} merchants={merchants} orders={selected ? [selected] : []} couriers={couriers} now={now}
          onClick={setDropoff} picker={dropoff} highlightOrderId={selected?.id} fitTo={dropoff ? null : fitTo} />
        <div className="map-hint">Click the map to set a drop-off point{selected?.courier ? ' · courier position updates live' : ''}</div>
      </main>
    </div>
  )
}

function OrderDetail({ order, now }: { order: Order; now: number }) {
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const cancellable = order.status === 'CREATED' || order.status === 'OFFERED' || order.status === 'ACCEPTED'
  const cancel = async () => {
    setBusy(true)
    setError(null)
    try {
      await api.customer.cancel(order.id, 'Changed my mind')
    } catch (e) {
      setError(e instanceof ApiError ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }
  return (
    <section className="card">
      <h2>Order #{shortId(order.id)} <StatusBadge status={order.status} /></h2>
      <p className="muted small">{order.merchant.name} → {order.dropoff.lat.toFixed(5)}, {order.dropoff.lng.toFixed(5)}{order.note ? ` · “${order.note}”` : ''}</p>
      <Timeline order={order} />
      {order.status === 'OFFERED' && order.offer && (
        <p className="muted small">Offered to {order.offer.courierName} · expires in {secondsUntil(order.offer.expiresAt, now)}s</p>
      )}
      {order.courier && !isTerminal(order.status) && (
        <div className="courier-card">
          <b>{order.courier.name}</b>
          <StaleDot lastSeenAt={order.courier.lastSeenAt} now={now} />
          {order.courier.locationAt && <span className="muted small">position recorded {timeAgo(order.courier.locationAt, now)}</span>}
        </div>
      )}
      {error && <div className="error">{error}</div>}
      {cancellable && <button className="btn danger" onClick={cancel} disabled={busy}>Cancel order</button>}
    </section>
  )
}

function PlaceOrder({ merchants, dropoff, onDropoff, onPlaced }: { merchants: Merchant[]; dropoff: GeoPoint | null; onDropoff: (p: GeoPoint | null) => void; onPlaced: (o: Order) => void }) {
  const [merchantId, setMerchantId] = useState('')
  const [note, setNote] = useState('')
  const [items, setItems] = useState('1x Burrito bowl\n2x Iced tea')
  const [status, setStatus] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  // One idempotency key per *attempt series*: retries after a network failure reuse it, so a
  // request that actually reached the server is never duplicated.
  const keyRef = useRef<string>(crypto.randomUUID())

  useEffect(() => {
    if (!merchantId && merchants.length) setMerchantId(merchants[0].id)
  }, [merchants, merchantId])

  const parsedItems = items.split('\n').map((l) => l.trim()).filter(Boolean).map((l) => {
    const m = /^(\d+)\s*[x×]\s*(.+)$/i.exec(l)
    return m ? { name: m[2].trim(), quantity: Number(m[1]) } : { name: l, quantity: 1 }
  })

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    if (!dropoff || !merchantId) return
    setBusy(true)
    setError(null)
    const body = { merchantId, dropoff, note: note || undefined, items: parsedItems }
    let attempt = 0
    while (true) {
      attempt++
      try {
        setStatus(attempt === 1 ? 'Placing order…' : `Retrying with the same Idempotency-Key (attempt ${attempt})…`)
        const order = await api.customer.create(keyRef.current, body)
        keyRef.current = crypto.randomUUID()
        setStatus(null)
        setNote('')
        onPlaced(order)
        break
      } catch (err) {
        if (err instanceof ApiError) {
          setError(err.message)
          setStatus(null)
          if (err.status === 422) keyRef.current = crypto.randomUUID()
          break
        }
        if (attempt >= 4) {
          setError('Network error — gave up after 4 attempts. Submitting again is safe: the same key is reused.')
          setStatus(null)
          break
        }
        await new Promise((r) => setTimeout(r, 400 * attempt))
      }
    }
    setBusy(false)
  }

  return (
    <form className="card" onSubmit={submit}>
      <h2>Place an order</h2>
      <label>Merchant
        <select value={merchantId} onChange={(e) => setMerchantId(e.target.value)}>
          {merchants.map((m) => <option key={m.id} value={m.id}>{m.name}</option>)}
        </select>
      </label>
      <label>Drop-off
        <div className="inline">
          <input readOnly value={dropoff ? `${dropoff.lat.toFixed(5)}, ${dropoff.lng.toFixed(5)}` : ''} placeholder="Click the map…" />
          {dropoff && <button type="button" className="btn ghost" onClick={() => onDropoff(null)}>clear</button>}
        </div>
      </label>
      <label>Items <span className="muted small">(one per line, “2x Name”)</span>
        <textarea rows={2} value={items} onChange={(e) => setItems(e.target.value)} />
      </label>
      <label>Note<input value={note} onChange={(e) => setNote(e.target.value)} placeholder="Leave at the front desk" /></label>
      {status && <div className="info">{status}</div>}
      {error && <div className="error">{error}</div>}
      <button className="btn primary" disabled={busy || !dropoff || !merchantId}>Place order</button>
      <div className="muted small">Idempotency-Key: <code>{keyRef.current.slice(0, 8)}…</code></div>
    </form>
  )
}
