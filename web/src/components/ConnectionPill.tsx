import type { LiveSnapshot } from '../live/LiveStore'

const LABEL = { live: 'Live', syncing: 'Syncing…', connecting: 'Connecting…', offline: 'Offline · retrying' }

export function ConnectionPill({ live }: { live: LiveSnapshot }) {
  const title = `last event #${live.lastEventId ?? '–'} · ${live.reconnects} reconnect(s) · ${live.replayed} replayed`
  return (
    <span className={`pill conn-${live.status}`} title={title}>
      <span className="dot" /> {LABEL[live.status]}
      {live.lastEventId !== null && <span className="muted"> #{live.lastEventId}</span>}
    </span>
  )
}
