import { createContext, useContext } from 'react'
import type { AppConfig } from '../api/types'

export const DEFAULT_CONFIG: AppConfig = {
  campus: { name: 'Campus', lat: 37.8719, lng: -122.2585 },
  staleAfterSeconds: 30,
  assignmentRadiusMeters: 3000,
  offerTtlSeconds: 20,
  stuckAfterSeconds: 180,
}

export const ConfigContext = createContext<AppConfig>(DEFAULT_CONFIG)
export const useConfig = () => useContext(ConfigContext)
