import { useSyncExternalStore } from 'react'
import { liveStore, type LiveSnapshot } from './LiveStore'

/** Read the live mirror from any component; the socket itself is owned by the Layout via useLiveFeed. */
export function useLive(): LiveSnapshot {
  return useSyncExternalStore(liveStore.subscribe, liveStore.getSnapshot, liveStore.getSnapshot)
}
