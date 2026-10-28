import type { Courier, Order } from '../api/types'

export type ConnectionStatus = 'offline' | 'connecting' | 'syncing' | 'live'

export interface LiveSnapshot {
  orders: Order[]
  couriers: Courier[]
  status: ConnectionStatus
  lastEventId: number | null
  reconnects: number
  replayed: number
}

type Listener = () => void

/**
 * Client-side mirror of what the WebSocket feed sends. Two rules keep it correct under
 * reconnects and duplicate frames: an order is only replaced by a higher version, and a courier's
 * position only moves forward in device time.
 */
export class LiveStore {
  private orders = new Map<string, Order>()
  private couriers = new Map<string, Courier>()
  private listeners = new Set<Listener>()
  private snapshot: LiveSnapshot = { orders: [], couriers: [], status: 'offline', lastEventId: null, reconnects: 0, replayed: 0 }

  get lastEventId() {
    return this.snapshot.lastEventId
  }

  subscribe = (l: Listener) => {
    this.listeners.add(l)
    return () => this.listeners.delete(l)
  }

  getSnapshot = () => this.snapshot

  reset() {
    this.orders.clear()
    this.couriers.clear()
    this.snapshot = { orders: [], couriers: [], status: 'offline', lastEventId: null, reconnects: 0, replayed: 0 }
    this.emit()
  }

  setStatus(status: ConnectionStatus, extra: Partial<LiveSnapshot> = {}) {
    this.snapshot = { ...this.snapshot, status, ...extra }
    this.emit()
  }

  applySnapshot(msg: { lastEventId: number; orders: Order[]; couriers: Courier[] }) {
    this.orders = new Map(msg.orders.map((o) => [o.id, o]))
    this.couriers = new Map(msg.couriers.map((c) => [c.id, c]))
    this.commit({ lastEventId: msg.lastEventId, status: 'live' })
  }

  applyResumed(msg: { lastEventId: number; replayed: number; couriers: Courier[] }) {
    for (const c of msg.couriers) this.mergeCourier(c)
    this.commit({ lastEventId: msg.lastEventId, status: 'live', replayed: this.snapshot.replayed + msg.replayed })
  }

  applyEvent(msg: { id: number; eventType: string; payload: { order?: Order; courier?: Courier } }) {
    if (this.snapshot.lastEventId !== null && msg.id <= this.snapshot.lastEventId) return // duplicate
    if (msg.payload.order) this.upsertOrder(msg.payload.order)
    if (msg.payload.courier) this.mergeCourier(msg.payload.courier)
    this.commit({ lastEventId: msg.id })
  }

  applyLocation(msg: { courierId: string; lat: number; lng: number; recordedAt: string; receivedAt: string }) {
    const current = this.couriers.get(msg.courierId)
    if (current?.locationAt && current.locationAt >= msg.recordedAt) return // out of order: ignore
    const next: Courier = {
      id: msg.courierId,
      name: current?.name ?? 'Courier',
      status: current?.status ?? 'ONLINE',
      activeOrderId: current?.activeOrderId,
      location: { lat: msg.lat, lng: msg.lng },
      locationAt: msg.recordedAt,
      lastSeenAt: msg.receivedAt,
      stale: false,
    }
    this.couriers.set(msg.courierId, next)
    for (const [id, o] of this.orders) {
      if (o.courierId === msg.courierId) this.orders.set(id, { ...o, courier: { ...(o.courier ?? next), ...next } })
    }
    this.commit({})
  }

  private upsertOrder(o: Order) {
    const cur = this.orders.get(o.id)
    if (cur && cur.version > o.version) return
    // Keep the freshest courier position we know about; the order payload may carry an older one.
    const known = o.courierId ? this.couriers.get(o.courierId) : undefined
    const courier = o.courier && known && known.locationAt && (!o.courier.locationAt || known.locationAt > o.courier.locationAt)
      ? { ...o.courier, location: known.location, locationAt: known.locationAt, lastSeenAt: known.lastSeenAt, stale: known.stale }
      : o.courier
    this.orders.set(o.id, { ...o, courier })
    if (o.courier) this.mergeCourier({ ...o.courier, activeOrderId: o.status === 'ACCEPTED' || o.status === 'PICKED_UP' ? o.id : undefined })
  }

  private mergeCourier(c: Courier) {
    const cur = this.couriers.get(c.id)
    if (cur?.locationAt && c.locationAt && cur.locationAt > c.locationAt) {
      this.couriers.set(c.id, { ...c, location: cur.location, locationAt: cur.locationAt, lastSeenAt: cur.lastSeenAt ?? c.lastSeenAt })
    } else {
      this.couriers.set(c.id, { ...cur, ...c })
    }
  }

  private commit(extra: Partial<LiveSnapshot>) {
    this.snapshot = {
      ...this.snapshot,
      ...extra,
      orders: [...this.orders.values()].sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1)),
      couriers: [...this.couriers.values()].sort((a, b) => a.name.localeCompare(b.name)),
    }
    this.emit()
  }

  private emit() {
    for (const l of this.listeners) l()
  }
}

export const liveStore = new LiveStore()
