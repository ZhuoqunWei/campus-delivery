import { useConfig } from '../lib/ConfigContext'
import { isStale, timeAgo } from '../lib/format'

/** Green = heard from within the staleness window, grey = last-known position may be old. */
export function StaleDot({ lastSeenAt, now, withText = true }: { lastSeenAt?: string; now: number; withText?: boolean }) {
  const cfg = useConfig()
  const stale = isStale(lastSeenAt, now, cfg.staleAfterSeconds)
  return (
    <span className={`stale ${stale ? 'is-stale' : 'is-fresh'}`} title={stale ? 'Position may be out of date' : 'Position is fresh'}>
      <span className="dot" />
      {withText && <span>{stale ? `stale · last seen ${timeAgo(lastSeenAt, now)}` : `live · ${timeAgo(lastSeenAt, now)}`}</span>}
    </span>
  )
}
