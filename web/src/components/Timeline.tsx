import type { Order } from '../api/types'
import { clock } from '../lib/format'

export function Timeline({ order }: { order: Order }) {
  const steps: { label: string; at?: string; done: boolean }[] = [
    { label: 'Order placed', at: order.createdAt, done: true },
    { label: 'Courier accepted', at: order.acceptedAt, done: !!order.acceptedAt },
    { label: 'Picked up', at: order.pickedUpAt, done: !!order.pickedUpAt },
    { label: 'Delivered', at: order.deliveredAt, done: !!order.deliveredAt },
  ]
  if (order.status === 'CANCELLED') steps.push({ label: `Cancelled${order.cancelReason ? ` — ${order.cancelReason}` : ''}`, at: order.cancelledAt, done: true })
  return (
    <ol className="timeline">
      {steps.map((s) => (
        <li key={s.label} className={s.done ? 'done' : ''}>
          <span className="tl-dot" />
          <span className="tl-label">{s.label}</span>
          <span className="tl-time">{s.done ? clock(s.at) : ''}</span>
        </li>
      ))}
    </ol>
  )
}
