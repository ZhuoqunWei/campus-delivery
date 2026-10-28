/**
 * Campus Delivery simulator.
 *
 * Spawns N couriers that log in, go online, wander around campus, accept offers (usually), drive
 * to the merchant, pick up, drive to the drop-off and deliver. Positions are sent in small batches
 * with deliberate jitter: some batches are shuffled, some are delayed so they arrive after newer
 * ones. That exercises the out-of-order handling on the server. A customer loop places orders at a
 * steady rate and occasionally retries a request with the same Idempotency-Key.
 *
 * Node >= 22 (global fetch + WebSocket). Configure with environment variables (see CONFIG below).
 */

const CONFIG = {
  apiUrl: process.env.API_URL ?? 'http://localhost:8080',
  couriers: num('COURIERS', 10),
  customers: num('CUSTOMERS', 3),
  ordersPerMinute: num('ORDERS_PER_MINUTE', 2),
  password: process.env.SEED_PASSWORD ?? 'password',
  tickMs: num('TICK_MS', 2000),
  speedMps: num('SPEED_MPS', 9),
  wanderRadiusM: num('WANDER_RADIUS_M', 1200),
  acceptDelayMs: [num('ACCEPT_DELAY_MIN_MS', 800), num('ACCEPT_DELAY_MAX_MS', 4000)] as const,
  ignoreOfferProbability: num('IGNORE_OFFER_PROBABILITY', 0.1),
  shuffleBatchProbability: num('SHUFFLE_BATCH_PROBABILITY', 0.15),
  delayBatchProbability: num('DELAY_BATCH_PROBABILITY', 0.1),
  duplicateOrderProbability: num('DUPLICATE_ORDER_PROBABILITY', 0.15),
}

function num(name: string, def: number): number {
  const v = process.env[name]
  return v === undefined || v === '' ? def : Number(v)
}

// ---------------------------------------------------------------- types

interface GeoPoint { lat: number; lng: number }
interface Merchant { id: string; name: string; location: GeoPoint }
interface Offer { id: string; courierId: string; expiresAt: string }
interface Order { id: string; status: string; version: number; merchant: Merchant; dropoff: GeoPoint; courierId?: string; offer?: Offer }
interface LoginResponse { token: string; user: { id: string; name: string; role: string } }

// ---------------------------------------------------------------- helpers

const log = (who: string, msg: string) => console.log(`${new Date().toISOString().slice(11, 19)} ${who.padEnd(11)} ${msg}`)
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
const rand = (min: number, max: number) => min + Math.random() * (max - min)
const chance = (p: number) => Math.random() < p

function distanceM(a: GeoPoint, b: GeoPoint): number {
  const R = 6371000
  const toRad = (d: number) => (d * Math.PI) / 180
  const dLat = toRad(b.lat - a.lat)
  const dLng = toRad(b.lng - a.lng)
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(a.lat)) * Math.cos(toRad(b.lat)) * Math.sin(dLng / 2) ** 2
  return 2 * R * Math.asin(Math.sqrt(h))
}

/** Move `meters` from `from` towards `to` (straight line; no routing engine, by design). */
function stepTowards(from: GeoPoint, to: GeoPoint, meters: number): GeoPoint {
  const d = distanceM(from, to)
  if (d <= meters) return to
  const f = meters / d
  return { lat: from.lat + (to.lat - from.lat) * f, lng: from.lng + (to.lng - from.lng) * f }
}

function offsetM(p: GeoPoint, northM: number, eastM: number): GeoPoint {
  return { lat: p.lat + northM / 111320, lng: p.lng + eastM / (111320 * Math.cos((p.lat * Math.PI) / 180)) }
}

function randomAround(center: GeoPoint, radiusM: number): GeoPoint {
  const r = radiusM * Math.sqrt(Math.random())
  const a = Math.random() * 2 * Math.PI
  return offsetM(center, r * Math.cos(a), r * Math.sin(a))
}

interface Session { token: string; email: string }

async function api<T>(session: Session | string | null, method: string, path: string, body?: unknown, headers: Record<string, string> = {}, retried = false): Promise<T> {
  const token = typeof session === 'string' ? session : session?.token ?? null
  const h: Record<string, string> = { 'content-type': 'application/json', ...headers }
  if (token) h.authorization = `Bearer ${token}`
  const res = await fetch(CONFIG.apiUrl + path, { method, headers: h, body: body === undefined ? undefined : JSON.stringify(body) })
  if (res.status === 401 && session && typeof session !== 'string' && !retried) {
    // JWTs expire after 12h; long-running simulations simply sign in again.
    session.token = (await login(session.email)).token
    return api<T>(session, method, path, body, headers, true)
  }
  if (!res.ok) {
    let detail = res.statusText
    try {
      const p = (await res.json()) as { code?: string; detail?: string }
      detail = `${p.code ?? res.status}: ${p.detail ?? ''}`
    } catch { /* ignore */ }
    throw new Error(`${method} ${path} -> ${res.status} ${detail}`)
  }
  return res.status === 204 ? (undefined as T) : ((await res.json()) as T)
}

async function login(email: string): Promise<LoginResponse> {
  return api<LoginResponse>(null, 'POST', '/api/auth/login', { email, password: CONFIG.password })
}

async function waitForBackend() {
  for (let i = 0; ; i++) {
    try {
      const res = await fetch(CONFIG.apiUrl + '/actuator/health')
      if (res.ok) return
    } catch { /* not up yet */ }
    if (i % 5 === 0) log('sim', `waiting for backend at ${CONFIG.apiUrl}…`)
    await sleep(2000)
  }
}

// ---------------------------------------------------------------- courier

type Phase = 'idle' | 'offered' | 'to_merchant' | 'to_dropoff'

class Courier {
  session: Session = { token: '', email: '' }
  get token() { return this.session.token }
  id = ''
  name: string
  pos: GeoPoint
  target: GeoPoint
  phase: Phase = 'idle'
  order: Order | null = null
  offer: { order: Order; decideAt: number; ignore: boolean } | null = null
  pending: { lat: number; lng: number; recordedAt: string }[] = []
  lastEventId: number | null = null
  ws: WebSocket | null = null
  closed = false

  constructor(readonly index: number, readonly center: GeoPoint) {
    this.name = `courier${index}`
    this.pos = randomAround(center, CONFIG.wanderRadiusM)
    this.target = randomAround(center, CONFIG.wanderRadiusM)
  }

  async start() {
    this.session = { token: '', email: `${this.name}@campus.test` }
    const res = await login(this.session.email)
    this.session.token = res.token
    this.id = res.user.id
    await api(this.session, 'POST', '/api/courier/status', { status: 'ONLINE' })
    await this.flush(true)
    this.connect()
    log(this.name, `online at ${this.pos.lat.toFixed(4)},${this.pos.lng.toFixed(4)}`)
  }

  connect() {
    if (this.closed) return
    const url = `${CONFIG.apiUrl.replace(/^http/, 'ws')}/ws?token=${encodeURIComponent(this.session.token)}`
    const ws = new WebSocket(url)
    this.ws = ws
    ws.onopen = () => ws.send(JSON.stringify({ type: 'subscribe', lastEventId: this.lastEventId }))
    ws.onmessage = (ev) => this.onMessage(JSON.parse(String(ev.data)))
    ws.onclose = () => {
      if (this.closed) return
      setTimeout(() => this.connect(), 1000 + Math.random() * 2000)
    }
    ws.onerror = () => ws.close()
  }

  onMessage(msg: { type: string; id?: number; lastEventId?: number; eventType?: string; payload?: { order?: Order }; orders?: Order[] }) {
    if (msg.type === 'snapshot') {
      this.lastEventId = msg.lastEventId ?? null
      for (const o of msg.orders ?? []) this.applyOrder(o)
      return
    }
    if (msg.type === 'resumed') {
      this.lastEventId = msg.lastEventId ?? this.lastEventId
      return
    }
    if (msg.type !== 'event' || !msg.id) return
    if (this.lastEventId !== null && msg.id <= this.lastEventId) return
    this.lastEventId = msg.id
    const order = msg.payload?.order
    if (order) this.applyOrder(order)
  }

  applyOrder(o: Order) {
    const mine = o.courierId === this.id
    const offeredToMe = o.status === 'OFFERED' && o.offer?.courierId === this.id
    if (offeredToMe) {
      if (this.phase === 'idle' || (this.phase === 'offered' && this.offer?.order.id === o.id)) {
        if (!this.offer || this.offer.order.id !== o.id) {
          const ignore = chance(CONFIG.ignoreOfferProbability)
          this.offer = { order: o, decideAt: Date.now() + rand(CONFIG.acceptDelayMs[0], CONFIG.acceptDelayMs[1]), ignore }
          this.phase = 'offered'
          log(this.name, `offered ${short(o.id)} from ${o.merchant.name}${ignore ? ' (will ignore it)' : ''}`)
        }
      }
      return
    }
    if (mine && (o.status === 'ACCEPTED' || o.status === 'PICKED_UP')) {
      this.order = o
      this.offer = null
      this.phase = o.status === 'ACCEPTED' ? 'to_merchant' : 'to_dropoff'
      this.target = o.status === 'ACCEPTED' ? o.merchant.location : o.dropoff
      return
    }
    // Lost the offer/order (expired, withdrawn, cancelled, reassigned, delivered).
    if (this.offer?.order.id === o.id && !offeredToMe) {
      log(this.name, `offer for ${short(o.id)} is gone (${o.status}${o.offer ? ' -> other courier' : ''})`)
      this.offer = null
      this.phase = 'idle'
      this.target = randomAround(this.center, CONFIG.wanderRadiusM)
    }
    if (this.order?.id === o.id && (!mine || o.status === 'DELIVERED' || o.status === 'CANCELLED')) {
      if (o.status !== 'DELIVERED') log(this.name, `order ${short(o.id)} ${o.status.toLowerCase()}${!mine ? ' (reassigned)' : ''}`)
      this.order = null
      this.phase = 'idle'
      this.target = randomAround(this.center, CONFIG.wanderRadiusM)
    }
  }

  async tick() {
    const dt = CONFIG.tickMs / 1000
    // 1. decide on offers
    if (this.phase === 'offered' && this.offer && Date.now() >= this.offer.decideAt && !this.offer.ignore) {
      const id = this.offer.order.id
      this.offer.decideAt = Number.MAX_SAFE_INTEGER
      try {
        const o = await api<Order>(this.session, 'POST', `/api/courier/orders/${id}/accept`)
        log(this.name, `accepted ${short(id)}`)
        this.applyOrder(o)
      } catch (e) {
        log(this.name, `accept ${short(id)} failed: ${(e as Error).message}`)
        this.offer = null
        this.phase = 'idle'
      }
    }
    // 2. move
    const speed = this.phase === 'idle' || this.phase === 'offered' ? CONFIG.speedMps * 0.4 : CONFIG.speedMps
    this.pos = stepTowards(this.pos, this.target, speed * dt * rand(0.8, 1.2))
    this.pending.push({ lat: this.pos.lat, lng: this.pos.lng, recordedAt: new Date().toISOString() })
    if ((this.phase === 'idle' || this.phase === 'offered') && distanceM(this.pos, this.target) < 5) {
      this.target = randomAround(this.center, CONFIG.wanderRadiusM)
    }
    // 3. arrive
    if (this.order && this.phase === 'to_merchant' && distanceM(this.pos, this.order.merchant.location) < 15) {
      await this.transition('pickup', 'to_dropoff', this.order.dropoff)
    } else if (this.order && this.phase === 'to_dropoff' && distanceM(this.pos, this.order.dropoff) < 15) {
      await this.transition('deliver', 'idle', randomAround(this.center, CONFIG.wanderRadiusM))
    }
    // 4. send positions in batches of two ticks
    if (this.pending.length >= 2) await this.flush(false)
  }

  private async transition(action: 'pickup' | 'deliver', next: Phase, target: GeoPoint) {
    const id = this.order!.id
    try {
      await api(this.session, 'POST', `/api/courier/orders/${id}/${action}`)
      log(this.name, `${action === 'pickup' ? 'picked up' : 'delivered'} ${short(id)}`)
      this.phase = next
      this.target = target
      if (next === 'idle') this.order = null
    } catch (e) {
      log(this.name, `${action} ${short(id)} failed: ${(e as Error).message}`)
      this.order = null
      this.phase = 'idle'
      this.target = target
    }
  }

  private async flush(initial: boolean) {
    const batch = initial ? [{ lat: this.pos.lat, lng: this.pos.lng, recordedAt: new Date().toISOString() }] : this.pending.splice(0)
    if (chance(CONFIG.shuffleBatchProbability)) batch.reverse() // points out of order inside the batch
    const send = () => api(this.session, 'POST', '/api/courier/locations', { points: batch }).catch((e) => log(this.name, `ping failed: ${(e as Error).message}`))
    if (!initial && chance(CONFIG.delayBatchProbability)) {
      setTimeout(send, CONFIG.tickMs * 3) // arrives after newer batches: must not move the marker back
    } else {
      await send()
    }
  }
}

// ---------------------------------------------------------------- customers

async function customerLoop(center: GeoPoint, merchants: Merchant[]) {
  const customers: (LoginResponse & { session: Session })[] = []
  for (let i = 1; i <= CONFIG.customers; i++) {
    const email = `customer${i}@campus.test`
    const res = await login(email)
    customers.push({ ...res, session: { token: res.token, email } })
  }
  const interval = 60000 / Math.max(CONFIG.ordersPerMinute, 0.01)
  log('customers', `${customers.length} customers, ~${CONFIG.ordersPerMinute} orders/min`)
  while (true) {
    await sleep(interval * rand(0.5, 1.5))
    const c = customers[Math.floor(Math.random() * customers.length)]
    const m = merchants[Math.floor(Math.random() * merchants.length)]
    const dropoff = randomAround(center, 900)
    const key = crypto.randomUUID()
    const body = { merchantId: m.id, dropoff, note: chance(0.3) ? 'Call on arrival' : undefined, items: [{ name: 'Lunch special', quantity: 1 + Math.floor(Math.random() * 2) }] }
    try {
      const o = await api<Order>(c.session, 'POST', '/api/customer/orders', body, { 'Idempotency-Key': key })
      log(c.user.name, `ordered ${short(o.id)} from ${m.name}`)
      if (chance(CONFIG.duplicateOrderProbability)) {
        const again = await api<Order>(c.session, 'POST', '/api/customer/orders', body, { 'Idempotency-Key': key })
        log(c.user.name, `retried with same key -> ${again.id === o.id ? 'same order (idempotent)' : 'DUPLICATE ' + short(again.id)}`)
      }
    } catch (e) {
      log(c.user.name, `order failed: ${(e as Error).message}`)
    }
  }
}

const short = (id: string) => id.slice(0, 8)

// ---------------------------------------------------------------- main

async function main() {
  await waitForBackend()
  const dispatcher = await login('dispatcher@campus.test')
  const cfg = await api<{ campus: GeoPoint & { name: string } }>(dispatcher.token, 'GET', '/api/config')
  const merchants = await api<Merchant[]>(dispatcher.token, 'GET', '/api/merchants')
  const center = { lat: cfg.campus.lat, lng: cfg.campus.lng }
  log('sim', `${cfg.campus.name}: ${merchants.length} merchants, ${CONFIG.couriers} couriers, tick ${CONFIG.tickMs}ms`)

  const couriers: Courier[] = []
  for (let i = 1; i <= CONFIG.couriers; i++) {
    const c = new Courier(i, center)
    await c.start()
    couriers.push(c)
    await sleep(150)
  }
  void customerLoop(center, merchants)

  const shutdown = async () => {
    log('sim', 'shutting down: couriers going offline')
    for (const c of couriers) {
      c.closed = true
      c.ws?.close()
      await api(c.session, 'POST', '/api/courier/status', { status: 'OFFLINE' }).catch(() => {})
    }
    process.exit(0)
  }
  process.on('SIGINT', shutdown)
  process.on('SIGTERM', shutdown)

  while (true) {
    const started = Date.now()
    await Promise.all(couriers.map((c) => c.tick().catch((e) => log(c.name, `tick error: ${(e as Error).message}`))))
    await sleep(Math.max(0, CONFIG.tickMs - (Date.now() - started)))
  }
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
