import type { OrderStatus } from '../api/types'

const LABELS: Record<OrderStatus, string> = {
  CREATED: 'Finding courier',
  OFFERED: 'Offered',
  ACCEPTED: 'Accepted',
  PICKED_UP: 'On the way',
  DELIVERED: 'Delivered',
  CANCELLED: 'Cancelled',
}

export function StatusBadge({ status, raw = false }: { status: OrderStatus; raw?: boolean }) {
  return <span className={`badge status-${status}`}>{raw ? status : LABELS[status]}</span>
}
