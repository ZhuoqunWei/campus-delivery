import L from 'leaflet'
import { useEffect } from 'react'
import { CircleMarker, MapContainer, Marker, Polyline, Popup, TileLayer, useMap, useMapEvents } from 'react-leaflet'
import type { Courier, GeoPoint, Merchant, Order } from '../api/types'
import { isTerminal } from '../api/types'
import { useConfig } from '../lib/ConfigContext'
import { isStale, timeAgo } from '../lib/format'

interface Props {
  center: GeoPoint
  zoom?: number
  merchants?: Merchant[]
  orders?: Order[]
  couriers?: Courier[]
  now: number
  onClick?: (p: GeoPoint) => void
  highlightOrderId?: string | null
  picker?: GeoPoint | null
  selfPosition?: GeoPoint | null
  fitTo?: GeoPoint[] | null
}

const merchantIcon = L.divIcon({ className: 'mk mk-merchant', html: '<span>M</span>', iconSize: [22, 22], iconAnchor: [11, 11] })
const dropoffIcon = L.divIcon({ className: 'mk mk-dropoff', html: '<span>⌂</span>', iconSize: [22, 22], iconAnchor: [11, 22] })
const pickerIcon = L.divIcon({ className: 'mk mk-picker', html: '<span>+</span>', iconSize: [26, 26], iconAnchor: [13, 13] })
const selfIcon = L.divIcon({ className: 'mk mk-self', html: '<span>●</span>', iconSize: [22, 22], iconAnchor: [11, 11] })

function courierIcon(c: Courier, stale: boolean, busy: boolean) {
  const cls = stale ? 'mk-courier-stale' : busy ? 'mk-courier-busy' : 'mk-courier-free'
  return L.divIcon({ className: `mk mk-courier ${cls}`, html: `<span>${c.name.replace(/[^0-9]/g, '') || '•'}</span>`, iconSize: [24, 24], iconAnchor: [12, 12] })
}

function ClickHandler({ onClick }: { onClick?: (p: GeoPoint) => void }) {
  useMapEvents({ click: (e) => onClick?.({ lat: e.latlng.lat, lng: e.latlng.lng }) })
  return null
}

function FitBounds({ points }: { points: GeoPoint[] | null | undefined }) {
  const map = useMap()
  useEffect(() => {
    if (points && points.length > 0) map.fitBounds(L.latLngBounds(points.map((p) => [p.lat, p.lng])), { padding: [40, 40], maxZoom: 16 })
  }, [map, points?.map((p) => `${p.lat},${p.lng}`).join('|')]) // eslint-disable-line react-hooks/exhaustive-deps
  return null
}

const STATUS_COLOR: Record<string, string> = {
  CREATED: '#f59e0b',
  OFFERED: '#a855f7',
  ACCEPTED: '#3b82f6',
  PICKED_UP: '#0ea5e9',
  DELIVERED: '#22c55e',
  CANCELLED: '#9ca3af',
}

export function MapView({ center, zoom = 15, merchants = [], orders = [], couriers = [], now, onClick, highlightOrderId, picker, selfPosition, fitTo }: Props) {
  const cfg = useConfig()
  const active = orders.filter((o) => !isTerminal(o.status) || o.id === highlightOrderId)
  return (
    <MapContainer center={[center.lat, center.lng]} zoom={zoom} className="map" scrollWheelZoom>
      <TileLayer attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>' url="https://tile.openstreetmap.org/{z}/{x}/{y}.png" />
      <ClickHandler onClick={onClick} />
      <FitBounds points={fitTo} />

      {merchants.map((m) => (
        <Marker key={m.id} position={[m.location.lat, m.location.lng]} icon={merchantIcon}>
          <Popup><b>{m.name}</b></Popup>
        </Marker>
      ))}

      {active.map((o) => {
        const hl = o.id === highlightOrderId
        const color = STATUS_COLOR[o.status] ?? '#666'
        return (
          <span key={o.id}>
            <Polyline positions={[[o.merchant.location.lat, o.merchant.location.lng], [o.dropoff.lat, o.dropoff.lng]]} pathOptions={{ color, weight: hl ? 4 : 2, opacity: hl ? 0.9 : 0.45, dashArray: o.status === 'CREATED' || o.status === 'OFFERED' ? '6 6' : undefined }} />
            <Marker position={[o.dropoff.lat, o.dropoff.lng]} icon={dropoffIcon} opacity={hl || !highlightOrderId ? 1 : 0.6}>
              <Popup><b>{o.merchant.name}</b> → {o.customerName}<br />{o.status}</Popup>
            </Marker>
            {hl && <CircleMarker center={[o.dropoff.lat, o.dropoff.lng]} radius={14} pathOptions={{ color, fill: false, weight: 2 }} />}
          </span>
        )
      })}

      {couriers.filter((c) => c.location).map((c) => {
        const stale = isStale(c.lastSeenAt, now, cfg.staleAfterSeconds)
        const busy = !!c.activeOrderId
        return (
          <Marker key={c.id} position={[c.location!.lat, c.location!.lng]} icon={courierIcon(c, stale, busy)} zIndexOffset={500}>
            <Popup>
              <b>{c.name}</b> · {c.status}<br />
              {stale ? 'Stale — ' : ''}last seen {timeAgo(c.lastSeenAt, now)}<br />
              {busy ? 'Delivering' : 'Free'}
            </Popup>
          </Marker>
        )
      })}

      {picker && <Marker position={[picker.lat, picker.lng]} icon={pickerIcon} zIndexOffset={900} />}
      {selfPosition && <Marker position={[selfPosition.lat, selfPosition.lng]} icon={selfIcon} zIndexOffset={1000} />}
    </MapContainer>
  )
}
