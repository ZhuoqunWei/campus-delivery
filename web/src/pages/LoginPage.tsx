import { useState, type FormEvent } from 'react'
import { useNavigate } from 'react-router-dom'
import { ApiError } from '../api/client'
import type { Role } from '../api/types'
import { useAuth } from '../auth/AuthContext'

export const HOME: Record<Role, string> = { CUSTOMER: '/customer', COURIER: '/courier', DISPATCHER: '/dispatcher' }

const SEEDED = [
  { label: 'Customer', email: 'customer1@campus.test' },
  { label: 'Courier', email: 'courier1@campus.test' },
  { label: 'Dispatcher', email: 'dispatcher@campus.test' },
]
const SEED_PASSWORD = import.meta.env.VITE_SEED_PASSWORD ?? 'password'

export function LoginPage() {
  const { login } = useAuth()
  const nav = useNavigate()
  const [email, setEmail] = useState(SEEDED[0].email)
  const [password, setPassword] = useState(SEED_PASSWORD)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  const submit = async (e?: FormEvent, override?: { email: string }) => {
    e?.preventDefault()
    setBusy(true)
    setError(null)
    try {
      const user = await login(override?.email ?? email, password)
      nav(HOME[user.role], { replace: true })
    } catch (err) {
      setError(err instanceof ApiError ? err.message : 'Cannot reach the server')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="login">
      <form className="card login-card" onSubmit={submit}>
        <h1>Campus Delivery</h1>
        <p className="muted">Order → courier accept → live tracking → dispatcher visibility.</p>
        <label>Email<input type="email" value={email} onChange={(e) => setEmail(e.target.value)} required /></label>
        <label>Password<input type="password" value={password} onChange={(e) => setPassword(e.target.value)} required /></label>
        {error && <div className="error">{error}</div>}
        <button className="btn primary" disabled={busy}>Sign in</button>
        <div className="quick">
          <span className="muted">Seeded accounts:</span>
          {SEEDED.map((s) => (
            <button type="button" key={s.email} className="btn ghost" disabled={busy} onClick={() => { setEmail(s.email); void submit(undefined, { email: s.email }) }}>
              {s.label}
            </button>
          ))}
        </div>
      </form>
    </div>
  )
}
