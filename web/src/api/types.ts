export type Role = 'CUSTOMER' | 'COURIER' | 'DISPATCHER'
export type OrderStatus = 'CREATED' | 'OFFERED' | 'ACCEPTED' | 'PICKED_UP' | 'DELIVERED' | 'CANCELLED'
export type CourierStatus = 'ONLINE' | 'OFFLINE'

export interface GeoPoint {
  lat: number
  lng: number
}

export interface Merchant {
  id: string
  name: string
  location: GeoPoint
}

export interface Courier {
  id: string
  name: string
  status: CourierStatus
  location?: GeoPoint
  locationAt?: string
  lastSeenAt?: string
  stale: boolean
  activeOrderId?: string
}

export interface Offer {
  id: string
  courierId: string
  courierName: string
  offeredAt: string
  expiresAt: string
}

export interface OrderItem {
  name: string
  quantity: number
}

export interface Order {
  id: string
  status: OrderStatus
  version: number
  customerId: string
  customerName: string
  merchant: Merchant
  dropoff: GeoPoint
  note?: string
  items: OrderItem[]
  courierId?: string
  courier?: Courier
  offer?: Offer
  createdAt: string
  updatedAt: string
  statusChangedAt: string
  acceptedAt?: string
  pickedUpAt?: string
  deliveredAt?: string
  cancelledAt?: string
  cancelReason?: string
}

export interface User {
  id: string
  email: string
  name: string
  role: Role
}

export interface AppConfig {
  campus: { name: string; lat: number; lng: number }
  staleAfterSeconds: number
  assignmentRadiusMeters: number
  offerTtlSeconds: number
  stuckAfterSeconds: number
}

export interface LocationBatchResult {
  accepted: number
  rejected: number
  advanced: boolean
  courier: Courier
}

export const TERMINAL: OrderStatus[] = ['DELIVERED', 'CANCELLED']
export const isTerminal = (s: OrderStatus) => TERMINAL.includes(s)
