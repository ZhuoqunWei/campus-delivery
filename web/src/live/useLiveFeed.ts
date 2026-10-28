import { useEffect, useSyncExternalStore } from 'react'
import { liveStore, type LiveSnapshot } from './LiveStore'

const wsBase = () => `${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}`

/**
 * Keeps one WebSocket open for the session. On (re)connect it subscribes with the last event id it
 * has seen, so the server replays what was missed instead of resending everything.
 */
export function useLiveFeed(token: string | null): LiveSnapshot {
  useEffect(() => {
    if (!token) {
      liveStore.reset()
      return
    }
    let ws: WebSocket | null = null
    let closed = false
    let attempt = 0
    let reconnectTimer: number | undefined
    let heartbeat: number | undefined

    const connect = () => {
      liveStore.setStatus('connecting')
      ws = new WebSocket(`${wsBase()}/ws?token=${encodeURIComponent(token)}`)
      ws.onopen = () => {
        const resuming = liveStore.lastEventId !== null
        liveStore.setStatus('syncing', resuming && attempt > 0 ? { reconnects: liveStore.getSnapshot().reconnects + 1 } : {})
        attempt = 0
        ws?.send(JSON.stringify({ type: 'subscribe', lastEventId: liveStore.lastEventId }))
        heartbeat = window.setInterval(() => ws?.readyState === WebSocket.OPEN && ws.send(JSON.stringify({ type: 'ping' })), 20_000)
      }
      ws.onmessage = (ev) => {
        const msg = JSON.parse(ev.data)
        switch (msg.type) {
          case 'snapshot':
            liveStore.applySnapshot(msg)
            break
          case 'resumed':
            liveStore.applyResumed(msg)
            break
          case 'event':
            liveStore.applyEvent(msg)
            break
          case 'location':
            liveStore.applyLocation(msg)
            break
          default:
            break
        }
      }
      ws.onclose = () => {
        window.clearInterval(heartbeat)
        if (closed) return
        liveStore.setStatus('offline')
        const delay = Math.min(15_000, 500 * 2 ** attempt) + Math.random() * 300
        attempt++
        reconnectTimer = window.setTimeout(connect, delay)
      }
      ws.onerror = () => ws?.close()
    }
    connect()
    return () => {
      closed = true
      window.clearTimeout(reconnectTimer)
      window.clearInterval(heartbeat)
      ws?.close()
      liveStore.reset()
    }
  }, [token])

  return useSyncExternalStore(liveStore.subscribe, liveStore.getSnapshot, liveStore.getSnapshot)
}
