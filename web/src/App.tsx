import { useEffect, useState, type ReactNode } from 'react'
import { BrowserRouter, Link, Navigate, Route, Routes, useLocation } from 'react-router-dom'
import { api } from './api/client'
import type { AppConfig, Role } from './api/types'
import { AuthProvider, useAuth } from './auth/AuthContext'
import { ConnectionPill } from './components/ConnectionPill'
import { useLiveFeed } from './live/useLiveFeed'
import { ConfigContext, DEFAULT_CONFIG } from './lib/ConfigContext'
import { CourierConsole } from './pages/courier/CourierConsole'
import { CustomerPage } from './pages/customer/CustomerPage'
import { DispatcherPage } from './pages/dispatcher/DispatcherPage'
import { HOME, LoginPage } from './pages/LoginPage'

function Layout({ children }: { children: ReactNode }) {
  const { user, token, logout } = useAuth()
  const live = useLiveFeed(token)
  const [config, setConfig] = useState<AppConfig>(DEFAULT_CONFIG)
  const loc = useLocation()

  useEffect(() => {
    if (token) api.config().then(setConfig).catch(() => setConfig(DEFAULT_CONFIG))
  }, [token])

  return (
    <ConfigContext.Provider value={config}>
      <header className="topbar">
        <Link to="/" className="brand">Campus Delivery <span className="muted">· {config.campus.name}</span></Link>
        <nav>
          {user?.role === 'DISPATCHER' && <Link className={loc.pathname.startsWith('/dispatcher') ? 'on' : ''} to="/dispatcher">Dispatch</Link>}
          {user?.role === 'CUSTOMER' && <Link className={loc.pathname.startsWith('/customer') ? 'on' : ''} to="/customer">Orders</Link>}
          {user?.role === 'COURIER' && <Link className={loc.pathname.startsWith('/courier') ? 'on' : ''} to="/courier">Courier console</Link>}
        </nav>
        <div className="spacer" />
        {user && <ConnectionPill live={live} />}
        {user && <span className="user">{user.name} <span className="muted">· {user.role.toLowerCase()}</span></span>}
        {user && <button className="btn ghost" onClick={logout}>Sign out</button>}
      </header>
      {children}
    </ConfigContext.Provider>
  )
}

function RequireRole({ role, children }: { role: Role; children: ReactNode }) {
  const { user } = useAuth()
  if (!user) return <Navigate to="/login" replace />
  if (user.role !== role) return <Navigate to={HOME[user.role]} replace />
  return <>{children}</>
}

function Home() {
  const { user } = useAuth()
  return <Navigate to={user ? HOME[user.role] : '/login'} replace />
}

export default function App() {
  return (
    <AuthProvider>
      <BrowserRouter>
        <Layout>
          <Routes>
            <Route path="/login" element={<LoginPage />} />
            <Route path="/customer" element={<RequireRole role="CUSTOMER"><CustomerPage /></RequireRole>} />
            <Route path="/dispatcher" element={<RequireRole role="DISPATCHER"><DispatcherPage /></RequireRole>} />
            <Route path="/courier" element={<RequireRole role="COURIER"><CourierConsole /></RequireRole>} />
            <Route path="*" element={<Home />} />
          </Routes>
        </Layout>
      </BrowserRouter>
    </AuthProvider>
  )
}
