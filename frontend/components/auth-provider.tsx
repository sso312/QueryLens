"use client"

import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react"

export type AuthUser = {
  id: string
  username: string
  name: string
  role: string
}

type LoginInput = {
  username: string
  password: string
}

type RegisterInput = {
  username: string
  password: string
  name: string
  role?: string
}

type StoredUser = AuthUser & {
  password: string
}

type AuthContextValue = {
  user: AuthUser | null
  isHydrated: boolean
  login: (input: LoginInput) => Promise<{ ok: boolean; error?: string }>
  register: (input: RegisterInput) => Promise<{ ok: boolean; error?: string }>
  logout: () => void
}

const AUTH_STORAGE_KEY = "querylens.auth.user"
const AUTH_USERS_STORAGE_KEY = "querylens.auth.users"

const DEMO_USERS: StoredUser[] = [
  { id: "user-researcher-01", username: "researcher_01", password: "team9KDT__2026", name: "김연구원", role: "연구원" },
  { id: "user-admin-01", username: "admin_01", password: "admin1234", name: "박교수", role: "관리자" },
]

const AuthContext = createContext<AuthContextValue | undefined>(undefined)

const sanitizeAuthUser = (value: unknown): AuthUser | null => {
  if (!value || typeof value !== "object") return null
  const user = value as Partial<AuthUser>
  if (!user.id || !user.username || !user.name || !user.role) return null
  return {
    id: String(user.id),
    username: String(user.username),
    name: String(user.name),
    role: String(user.role),
  }
}

const sanitizeStoredUsers = (value: unknown): StoredUser[] => {
  if (!Array.isArray(value)) return []
  const next: StoredUser[] = []
  for (const item of value) {
    if (!item || typeof item !== "object") continue
    const row = item as Partial<StoredUser>
    if (!row.id || !row.username || !row.name || !row.role || !row.password) continue
    next.push({
      id: String(row.id),
      username: String(row.username),
      name: String(row.name),
      role: String(row.role),
      password: String(row.password),
    })
  }
  return next
}

const loadStoredUsers = (): StoredUser[] => {
  try {
    const raw = localStorage.getItem(AUTH_USERS_STORAGE_KEY)
    if (!raw) return [...DEMO_USERS]
    const parsed = sanitizeStoredUsers(JSON.parse(raw))
    if (parsed.length === 0) return [...DEMO_USERS]
    return parsed
  } catch {
    return [...DEMO_USERS]
  }
}

const saveStoredUsers = (users: StoredUser[]) => {
  localStorage.setItem(AUTH_USERS_STORAGE_KEY, JSON.stringify(users))
}

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<AuthUser | null>(null)
  const [users, setUsers] = useState<StoredUser[]>(DEMO_USERS)
  const [isHydrated, setIsHydrated] = useState(false)

  useEffect(() => {
    try {
      const loadedUsers = loadStoredUsers()
      setUsers(loadedUsers)
      saveStoredUsers(loadedUsers)

      const raw = localStorage.getItem(AUTH_STORAGE_KEY)
      if (!raw) return
      const parsed = JSON.parse(raw)
      const nextUser = sanitizeAuthUser(parsed)
      if (nextUser) {
        setUser(nextUser)
      }
    } catch {
      localStorage.removeItem(AUTH_STORAGE_KEY)
    } finally {
      setIsHydrated(true)
    }
  }, [])

  useEffect(() => {
    const onStorage = (event: StorageEvent) => {
      if (event.key !== AUTH_STORAGE_KEY) return
      if (!event.newValue) {
        setUser(null)
        return
      }
      try {
        const parsed = JSON.parse(event.newValue)
        setUser(sanitizeAuthUser(parsed))
      } catch {
        setUser(null)
      }
    }
    window.addEventListener("storage", onStorage)
    return () => window.removeEventListener("storage", onStorage)
  }, [])

  const login = useCallback(async ({ username, password }: LoginInput) => {
    const normalized = username.trim().toLowerCase()
    const matched = users.find(
      (item) => item.username.toLowerCase() === normalized && item.password === password
    )
    if (!matched) {
      return { ok: false, error: "아이디 또는 비밀번호가 올바르지 않습니다." }
    }
    const nextUser: AuthUser = {
      id: matched.id,
      username: matched.username,
      name: matched.name,
      role: matched.role,
    }
    setUser(nextUser)
    localStorage.setItem(AUTH_STORAGE_KEY, JSON.stringify(nextUser))
    return { ok: true }
  }, [users])

  const register = useCallback(async ({ username, password, name, role }: RegisterInput) => {
    const normalized = username.trim().toLowerCase()
    const nextName = name.trim()
    const nextRole = (role || "연구원").trim() || "연구원"

    if (!normalized || normalized.length < 4) {
      return { ok: false, error: "아이디는 4자 이상이어야 합니다." }
    }
    if (!password || password.length < 8) {
      return { ok: false, error: "비밀번호는 8자 이상이어야 합니다." }
    }
    if (!nextName) {
      return { ok: false, error: "이름을 입력해 주세요." }
    }
    if (users.some((item) => item.username.toLowerCase() === normalized)) {
      return { ok: false, error: "이미 사용 중인 아이디입니다." }
    }

    const created: StoredUser = {
      id: `user-${Date.now()}`,
      username: normalized,
      password,
      name: nextName,
      role: nextRole,
    }
    const nextUsers = [...users, created]
    setUsers(nextUsers)
    saveStoredUsers(nextUsers)
    return { ok: true }
  }, [users])

  const logout = useCallback(() => {
    setUser(null)
    localStorage.removeItem(AUTH_STORAGE_KEY)
  }, [])

  const value = useMemo(
    () => ({
      user,
      isHydrated,
      login,
      register,
      logout,
    }),
    [user, isHydrated, login, register, logout]
  )

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

export function useAuth() {
  const context = useContext(AuthContext)
  if (!context) {
    throw new Error("useAuth must be used within AuthProvider")
  }
  return context
}
