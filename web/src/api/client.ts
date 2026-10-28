import type { AppConfig, Courier, LocationBatchResult, Merchant, Order, User } from './types'

export class ApiError extends Error {
  constructor(
    public status: number,
    public code: string | undefined,
    message: string,
  ) {
    super(message)
  }
}

let currentToken: string | null = null
let onUnauthorized: (() => void) | null = null

export function setToken(token: string | null) {
  currentToken = token
}

export function setUnauthorizedHandler(handler: () => void) {
  onUnauthorized = handler
}

async function request<T>(method: string, path: string, body?: unknown, headers: Record<string, string> = {}): Promise<T> {
  const h: Record<string, string> = { 'Content-Type': 'application/json', ...headers }
  if (currentToken) h.Authorization = `Bearer ${currentToken}`
  const res = await fetch(path, { method, headers: h, body: body === undefined ? undefined : JSON.stringify(body) })
  if (!res.ok) {
    let problem: { code?: string; detail?: string } | undefined
    try {
      problem = await res.json()
    } catch {
      /* not a problem+json body */
    }
    if (res.status === 401 && onUnauthorized && path !== '/api/auth/login') onUnauthorized()
    throw new ApiError(res.status, problem?.code, problem?.detail ?? res.statusText)
  }
  if (res.status === 204) return undefined as T
  return (await res.json()) as T
}

export const api = {
  login: (email: string, password: string) =>
    request<{ token: string; user: User }>('POST', '/api/auth/login', { email, password }),
  me: () => request<User>('GET', '/api/auth/me'),
  config: () => request<AppConfig>('GET', '/api/config'),
  merchants: () => request<Merchant[]>('GET', '/api/merchants'),

  customer: {
    orders: () => request<Order[]>('GET', '/api/customer/orders'),
    order: (id: string) => request<Order>('GET', `/api/customer/orders/${id}`),
    create: (idempotencyKey: string, body: { merchantId: string; dropoff: { lat: number; lng: number }; note?: string; items: { name: string; quantity: number }[] }) =>
      request<Order>('POST', '/api/customer/orders', body, { 'Idempotency-Key': idempotencyKey }),
    cancel: (id: string, reason?: string) => request<Order>('POST', `/api/customer/orders/${id}/cancel`, { reason }),
  },

  courier: {
    me: () => request<{ courier: Courier; orders: Order[] }>('GET', '/api/courier/me'),
    setStatus: (status: 'ONLINE' | 'OFFLINE') => request<Courier>('POST', '/api/courier/status', { status }),
    orders: () => request<Order[]>('GET', '/api/courier/orders'),
    open: () => request<Order[]>('GET', '/api/courier/orders/open'),
    accept: (id: string) => request<Order>('POST', `/api/courier/orders/${id}/accept`),
    decline: (offerId: string) => request<Order>('POST', `/api/courier/offers/${offerId}/decline`),
    pickup: (id: string) => request<Order>('POST', `/api/courier/orders/${id}/pickup`),
    deliver: (id: string) => request<Order>('POST', `/api/courier/orders/${id}/deliver`),
    locations: (points: { lat: number; lng: number; recordedAt: string }[]) =>
      request<LocationBatchResult>('POST', '/api/courier/locations', { points }),
  },

  dispatch: {
    orders: () => request<Order[]>('GET', '/api/dispatch/orders'),
    stuck: (olderThanSeconds?: number) =>
      request<Order[]>('GET', `/api/dispatch/orders/stuck${olderThanSeconds ? `?olderThanSeconds=${olderThanSeconds}` : ''}`),
    couriers: () => request<Courier[]>('GET', '/api/dispatch/couriers'),
    cancel: (id: string, reason: string, expectedVersion?: number) =>
      request<Order>('POST', `/api/dispatch/orders/${id}/cancel`, { reason, expectedVersion }),
    reassign: (id: string, courierId?: string, expectedVersion?: number) =>
      request<Order>('POST', `/api/dispatch/orders/${id}/reassign`, { courierId, expectedVersion }),
  },
}
