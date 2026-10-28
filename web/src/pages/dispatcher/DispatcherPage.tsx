import { useMemo, useState } from 'react'
import { ApiError, api } from '../../api/client'
import type { Courier, Order } from '../../api/types'
import { isTerminal } from '../../api/types'
import { MapView } from '../../components/MapView'
import { StaleDot } from '../../components/StaleDot'
import { StatusBadge } from '../../components/StatusBadge'
import { useLive } from '../../live/useLive'
import { useConfig } from '../../lib/ConfigContext'
import { duration, isStale, shortId, timeAgo } from '../../lib/format'
import { useNow } from '../../lib/useNow'

type Tab = 'active' | 'stuck' | 'recent'

export function DispatcherPage() {
  const cfg = useConfig()
  const live = useLive()
  const now = useNow()
  const [tab, setTab] = useState<Tab>('active')
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const active = useMemo(() => live.orders.filter((o) => !isTerminal(o.status)), [live.orders])
  const stuck = useMemo(() => active.filter((o) => now - Date.parse(o.statusChangedAt) > cfg.stuckAfterSeconds * 1000), [active, now, cfg.stuckAfterSeconds])
  const recent = useMemo(() => live.orders.filter((o) => isTerminal(o.status)).slice(0, 50), [live.orders])
  const rows = tab === 'active' ? active : tab === 'stuck' ? stuck : recent

  const online = live.couriers.filter((c) => c.status === 'ONLINE')
  const staleCount = online.filter((c) => isStale(c.lastSeenAt, now, cfg.staleAfterSeconds)).length
  const freeCouriers = online.filter((c) => !c.activeOrderId && !isStale(c.lastSeenAt, now, cfg.staleAfterSeconds))
  const selected = live.orders.find((o) => o.id === selectedId)

  const run = async (fn: () => Promise<unknown>) => {
    setError(null)
    try {
      await fn()
    } catch (e) {
      setError(e instanceof ApiError ? `${e.code ?? e.status}: ${e.message}` : String(e))
    }
  }

  return (
    <div className="page two-col wide">
      <aside className="sidebar">
        <section className="stats">
          <Stat label="Active orders" value={active.length} />
          <Stat label="Unassigned" value={active.filter((o) => o.status === 'CREATED' || o.status === 'OFFERED').length} tone={active.some((o) => o.status === 'CREATED') ? 'warn' : undefined} />
          <Stat label="Stuck" value={stuck.length} tone={stuck.length ? 'bad' : undefined} />
          <Stat label="Couriers online" value={online.length} />
          <Stat label="Stale positions" value={staleCount} tone={staleCount ? 'warn' : undefined} />
        </section>

        <section className="card grow">
          <div className="tabs">
            {(['active', 'stuck', 'recent'] as Tab[]).map((t) => (
              <button key={t} className={`tab ${tab === t ? 'on' : ''}`} onClick={() => setTab(t)}>
                {t === 'active' ? `Active (${active.length})` : t === 'stuck' ? `Stuck (${stuck.length})` : `Recent (${recent.length})`}
              </button>
            ))}
          </div>
          {error && <div className="error">{error}</div>}
          <table className="table">
            <thead>
              <tr><th>Order</th><th>Status</th><th>In status</th><th>Courier</th><th></th></tr>
            </thead>
            <tbody>
              {rows.length === 0 && <tr><td colSpan={5} className="muted">Nothing here.</td></tr>}
              {rows.map((o) => (
                <OrderRow key={o.id} order={o} now={now} selected={o.id === selectedId} onSelect={() => setSelectedId(o.id)}
                  freeCouriers={freeCouriers} onCancel={(reason) => run(() => api.dispatch.cancel(o.id, reason, o.version))}
                  onReassign={(courierId) => run(() => api.dispatch.reassign(o.id, courierId, o.version))} />
              ))}
            </tbody>
          </table>
        </section>

        <section className="card">
          <h2>Couriers <span className="muted">({online.length} online)</span></h2>
          <ul className="list compact">
            {live.couriers.filter((c) => c.status === 'ONLINE' || c.lastSeenAt).map((c) => (
              <li key={c.id} className="row">
                <span><b>{c.name}</b> <span className="muted small">{c.status.toLowerCase()}{c.activeOrderId ? ` · on #${shortId(c.activeOrderId)}` : ' · free'}</span></span>
                <StaleDot lastSeenAt={c.lastSeenAt} now={now} />
              </li>
            ))}
          </ul>
        </section>
      </aside>
      <main className="map-pane">
        <MapView center={cfg.campus} zoom={15} orders={active} couriers={live.couriers.filter((c) => c.status === 'ONLINE' || (c.lastSeenAt && now - Date.parse(c.lastSeenAt) < 600_000))}
          now={now} highlightOrderId={selectedId} fitTo={selected ? [selected.merchant.location, selected.dropoff] : null} />
        <div className="legend">
          <span><i className="sw" style={{ background: '#f59e0b' }} /> finding courier</span>
          <span><i className="sw" style={{ background: '#a855f7' }} /> offered</span>
          <span><i className="sw" style={{ background: '#3b82f6' }} /> accepted</span>
          <span><i className="sw" style={{ background: '#0ea5e9' }} /> picked up</span>
          <span><i className="sw round" style={{ background: '#16a34a' }} /> courier free</span>
          <span><i className="sw round" style={{ background: '#2563eb' }} /> courier busy</span>
          <span><i className="sw round" style={{ background: '#9ca3af' }} /> stale</span>
        </div>
      </main>
    </div>
  )
}

function Stat({ label, value, tone }: { label: string; value: number; tone?: 'warn' | 'bad' }) {
  return (
    <div className={`stat ${tone ?? ''}`}>
      <div className="stat-value">{value}</div>
      <div className="stat-label">{label}</div>
    </div>
  )
}

function OrderRow({ order: o, now, selected, onSelect, freeCouriers, onCancel, onReassign }: {
  order: Order; now: number; selected: boolean; onSelect: () => void; freeCouriers: Courier[]
  onCancel: (reason: string) => void; onReassign: (courierId?: string) => void
}) {
  const [menu, setMenu] = useState(false)
  const terminal = isTerminal(o.status)
  return (
    <tr className={selected ? 'selected' : ''} onClick={onSelect}>
      <td>
        <b>#{shortId(o.id)}</b>
        <div className="muted small">{o.merchant.name} → {o.customerName}</div>
      </td>
      <td>
        <StatusBadge status={o.status} raw />
        {o.status === 'OFFERED' && o.offer && <div className="muted small">to {o.offer.courierName}</div>}
      </td>
      <td title={`since ${o.statusChangedAt}`}>{duration(o.statusChangedAt, terminal ? o.statusChangedAt : undefined, now)}{terminal ? ` · ${timeAgo(o.statusChangedAt, now)}` : ''}</td>
      <td>
        {o.courier ? (
          <span>{o.courier.name} <StaleDot lastSeenAt={o.courier.lastSeenAt} now={now} withText={false} /></span>
        ) : <span className="muted">—</span>}
      </td>
      <td className="actions" onClick={(e) => e.stopPropagation()}>
        {!terminal && (
          <div className="menu-wrap">
            <button className="btn small" onClick={() => setMenu((m) => !m)}>Actions ▾</button>
            {menu && (
              <div className="menu" onMouseLeave={() => setMenu(false)}>
                {o.status !== 'PICKED_UP' && (
                  <>
                    <div className="menu-title">Reassign to</div>
                    {freeCouriers.length === 0 && <div className="muted small">No free, fresh couriers</div>}
                    {freeCouriers.filter((c) => c.id !== o.courierId).slice(0, 8).map((c) => (
                      <button key={c.id} className="menu-item" onClick={() => { setMenu(false); onReassign(c.id) }}>{c.name}</button>
                    ))}
                    {(o.status === 'ACCEPTED' || o.status === 'OFFERED') && (
                      <button className="menu-item" onClick={() => { setMenu(false); onReassign(undefined) }}>Back to pool (auto re-offer)</button>
                    )}
                    <hr />
                  </>
                )}
                <button className="menu-item danger" onClick={() => { setMenu(false); const r = window.prompt('Cancel reason', 'Merchant closed'); if (r !== null) onCancel(r || 'Cancelled by dispatcher') }}>Cancel order</button>
              </div>
            )}
          </div>
        )}
      </td>
    </tr>
  )
}
