const DASHBOARD_CACHE_KEY = "dashboard_cache_v1"
const DASHBOARD_CACHE_VERSION = 1 as const

export type DashboardCacheSnapshot<TQuery = unknown, TFolder = unknown> = {
  version: typeof DASHBOARD_CACHE_VERSION
  updatedAt: string
  queries: TQuery[]
  folders: TFolder[]
}

type DashboardCacheData<TQuery, TFolder> = {
  queries: TQuery[]
  folders: TFolder[]
}

const memoryCache = new Map<string, DashboardCacheSnapshot<unknown, unknown>>()

const toScopedCacheKey = (user: string | null | undefined) => {
  const normalized = String(user || "").trim()
  return normalized ? `${DASHBOARD_CACHE_KEY}:${normalized}` : DASHBOARD_CACHE_KEY
}

const isCacheSnapshot = (value: unknown): value is DashboardCacheSnapshot<unknown, unknown> => {
  if (!value || typeof value !== "object") return false
  const source = value as Record<string, unknown>
  if (source.version !== DASHBOARD_CACHE_VERSION) return false
  if (!Array.isArray(source.queries) || !Array.isArray(source.folders)) return false
  return true
}

const normalizeSnapshot = <TQuery, TFolder>(
  source: DashboardCacheData<TQuery, TFolder>
): DashboardCacheSnapshot<TQuery, TFolder> => ({
  version: DASHBOARD_CACHE_VERSION,
  updatedAt: new Date().toISOString(),
  queries: Array.isArray(source.queries) ? source.queries : [],
  folders: Array.isArray(source.folders) ? source.folders : [],
})

export const readDashboardCache = <TQuery = unknown, TFolder = unknown>(
  user: string | null | undefined
): DashboardCacheSnapshot<TQuery, TFolder> | null => {
  const scopedKey = toScopedCacheKey(user)
  const inMemory = memoryCache.get(scopedKey)
  if (inMemory && isCacheSnapshot(inMemory)) {
    return inMemory as DashboardCacheSnapshot<TQuery, TFolder>
  }
  if (typeof window === "undefined") return null
  try {
    const raw = window.localStorage.getItem(scopedKey)
    if (!raw) return null
    const parsed = JSON.parse(raw)
    if (!isCacheSnapshot(parsed)) {
      window.localStorage.removeItem(scopedKey)
      return null
    }
    memoryCache.set(scopedKey, parsed)
    return parsed as DashboardCacheSnapshot<TQuery, TFolder>
  } catch {
    return null
  }
}

export const writeDashboardCache = <TQuery = unknown, TFolder = unknown>(
  user: string | null | undefined,
  source: DashboardCacheData<TQuery, TFolder>
): DashboardCacheSnapshot<TQuery, TFolder> => {
  const scopedKey = toScopedCacheKey(user)
  const snapshot = normalizeSnapshot(source)
  memoryCache.set(scopedKey, snapshot as DashboardCacheSnapshot<unknown, unknown>)
  if (typeof window !== "undefined") {
    try {
      window.localStorage.setItem(scopedKey, JSON.stringify(snapshot))
    } catch {}
  }
  return snapshot
}

export const updateDashboardCache = <TQuery = unknown, TFolder = unknown>(
  user: string | null | undefined,
  updater: (current: DashboardCacheData<TQuery, TFolder>) => DashboardCacheData<TQuery, TFolder>
): DashboardCacheSnapshot<TQuery, TFolder> => {
  const current = readDashboardCache<TQuery, TFolder>(user)
  const next = updater({
    queries: current?.queries || [],
    folders: current?.folders || [],
  })
  return writeDashboardCache(user, next)
}

export const clearDashboardCache = (user: string | null | undefined) => {
  const scopedKey = toScopedCacheKey(user)
  memoryCache.delete(scopedKey)
  if (typeof window !== "undefined") {
    try {
      window.localStorage.removeItem(scopedKey)
    } catch {}
  }
}
