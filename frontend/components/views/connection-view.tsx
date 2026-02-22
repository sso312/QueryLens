"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Switch } from "@/components/ui/switch"
import { Badge } from "@/components/ui/badge"
import { Checkbox } from "@/components/ui/checkbox"
import { 
  Database, 
  CheckCircle2, 
  XCircle, 
  Shield, 
  Lock,
  RefreshCw,
  AlertTriangle,
  Server,
  Eye
} from "lucide-react"
import { cn } from "@/lib/utils"
import { useAuth } from "@/components/auth-provider"

interface TableScope {
  id: string
  name: string
  schema: string
  description?: string
  rowCount?: string
  selected: boolean
}

interface PoolStatus {
  open?: boolean
  busy?: number | null
  open_connections?: number | null
  max?: number | null
}

interface ConnectionSettings {
  host: string
  port: string
  database: string
  username: string
  password?: string
  sslMode?: string
  defaultSchema?: string
}

interface TableScopeSettings {
  selected_ids: string[]
}

interface TableCatalogResponse {
  owner?: string
  tables?: { name: string; schema?: string; columns?: number; primary_keys?: string[] }[]
}

const DEFAULT_TABLE_SCOPES: TableScope[] = [
  { id: "patients", name: "patients", schema: "mimiciv_hosp", description: "환자 기본 정보", rowCount: "382,278", selected: true },
  { id: "admissions", name: "admissions", schema: "mimiciv_hosp", description: "입원 기록", rowCount: "524,520", selected: true },
  { id: "diagnoses_icd", name: "diagnoses_icd", schema: "mimiciv_hosp", description: "ICD 진단 코드", rowCount: "5,280,857", selected: true },
  { id: "procedures_icd", name: "procedures_icd", schema: "mimiciv_hosp", description: "ICD 시술 코드", rowCount: "704,124", selected: true },
  { id: "labevents", name: "labevents", schema: "mimiciv_hosp", description: "검사 결과", rowCount: "122,103,667", selected: false },
  { id: "prescriptions", name: "prescriptions", schema: "mimiciv_hosp", description: "처방 정보", rowCount: "17,021,399", selected: false },
  { id: "icustays", name: "icustays", schema: "mimiciv_icu", description: "ICU 재원 기록", rowCount: "76,943", selected: true },
  { id: "chartevents", name: "chartevents", schema: "mimiciv_icu", description: "차트 이벤트", rowCount: "329,499,788", selected: false },
]

export function ConnectionView() {
  const { user } = useAuth()
  const connectionPlaceholderClass = "placeholder:text-muted-foreground/60"
  const apiBaseUrl = (process.env.NEXT_PUBLIC_API_BASE_URL || "").replace(/\/$/, "")
  const apiUrl = (path: string) => (apiBaseUrl ? `${apiBaseUrl}${path}` : path)
  const stateUser = (user?.id || user?.username || user?.name || "").trim()
  const apiUrlWithUser = (path: string) => {
    const base = apiUrl(path)
    if (!stateUser) return base
    const separator = base.includes("?") ? "&" : "?"
    return `${base}${separator}user=${encodeURIComponent(stateUser)}`
  }
  const getTableColumns = (width: number) => {
    if (width >= 1536) return 4
    if (width >= 1024) return 3
    if (width >= 640) return 2
    return 1
  }
  const [connectionState, setConnectionState] = useState<"checking" | "connected" | "disconnected">("checking")
  const [isReadOnly, setIsReadOnly] = useState(true)
  const [isTesting, setIsTesting] = useState(false)
  const [poolStatus, setPoolStatus] = useState<PoolStatus | null>(null)
  const [statusError, setStatusError] = useState<string | null>(null)
  const [isConnectionSaving, setIsConnectionSaving] = useState(false)
  const [connectionSaveMessage, setConnectionSaveMessage] = useState<string | null>(null)
  const [connectionSaveError, setConnectionSaveError] = useState<string | null>(null)
  const [isTableScopeSaving, setIsTableScopeSaving] = useState(false)
  const [tableScopeSaveMessage, setTableScopeSaveMessage] = useState<string | null>(null)
  const [tableScopeSaveError, setTableScopeSaveError] = useState<string | null>(null)
  const [tableColumns, setTableColumns] = useState(() =>
    typeof window === "undefined" ? 1 : getTableColumns(window.innerWidth)
  )
  const [tablePage, setTablePage] = useState(1)
  const [lastChecked, setLastChecked] = useState<Date | null>(null)
  const [connectionConfig, setConnectionConfig] = useState({
    host: "",
    port: "",
    database: "",
    username: "",
    password: "",
    sslMode: "disable",
    defaultSchema: ""
  })

  const [tableScopes, setTableScopes] = useState<TableScope[]>(DEFAULT_TABLE_SCOPES)
  const isConnected = connectionState === "connected"
  const isCheckingConnection = connectionState === "checking"
  const oraAuthFailed = Boolean(statusError?.toUpperCase().includes("ORA-01017"))

  const readError = async (res: Response) => {
    const text = await res.text()
    try {
      const json = JSON.parse(text)
      if (json?.detail) return String(json.detail)
    } catch {}
    return text || `${res.status} ${res.statusText}`
  }

  const fetchPoolStatus = async () => {
    setIsTesting(true)
    setStatusError(null)
    try {
      const res = await fetch(apiUrlWithUser("/admin/oracle/pool/status"))
      if (!res.ok) {
        throw new Error(await readError(res))
      }
      const data: PoolStatus = await res.json()
      setPoolStatus(data)
      setConnectionState(Boolean(data?.open) ? "connected" : "disconnected")
    } catch (err: any) {
      setConnectionState("disconnected")
      setPoolStatus(null)
      setStatusError(err?.message || "연결 상태를 확인할 수 없습니다.")
    } finally {
      setIsTesting(false)
      setLastChecked(new Date())
    }
  }

  const handleTestConnection = async () => {
    await fetchPoolStatus()
  }

  const loadSettings = async () => {
    try {
      const [connectionRes, scopeRes, tablesRes] = await Promise.all([
        fetch(apiUrlWithUser("/admin/settings/connection")),
        fetch(apiUrlWithUser("/admin/settings/table-scope")),
        fetch(apiUrl("/admin/metadata/tables")),
      ])
      const fallbackSelected = new Set(
        DEFAULT_TABLE_SCOPES.filter(table => table.selected).map(table => table.id)
      )
      let selectedIds = fallbackSelected
      if (connectionRes.ok) {
        const data: Partial<ConnectionSettings> = await connectionRes.json()
        setConnectionConfig(prev => ({
          ...prev,
          ...data,
          password: data.password ?? prev.password ?? ""
        }))
      }
      if (scopeRes.ok) {
        const data: TableScopeSettings = await scopeRes.json()
        if (Array.isArray(data?.selected_ids) && data.selected_ids.length > 0) {
          selectedIds = new Set(data.selected_ids.map(id => id.toLowerCase()))
        }
      }
      if (tablesRes.ok) {
        const data: TableCatalogResponse = await tablesRes.json()
        if (Array.isArray(data?.tables) && data.tables.length > 0) {
          const owner = String(data.owner || "")
          const schemaCandidates = Array.from(
            new Set(
              data.tables
                .map((table) => String(table.schema || "").trim())
                .filter(Boolean)
            )
          )
          if (schemaCandidates.length === 1) {
            setConnectionConfig(prev => (
              prev.defaultSchema?.trim() ? prev : { ...prev, defaultSchema: schemaCandidates[0] }
            ))
          }
          const nextScopes = data.tables.map(table => {
            const name = String(table.name || "")
            const id = name.toLowerCase()
            const schemaLabel = String(table.schema || owner || "")
            const description =
              typeof table.columns === "number" ? `컬럼 ${table.columns}개` : undefined
            return {
              id,
              name: id,
              schema: schemaLabel || "default",
              description,
              selected: selectedIds.has(id),
            } as TableScope
          })
          setTableScopes(nextScopes)
          return
        }
      }
      setTableScopes(prev => prev.map(table => ({
        ...table,
        selected: selectedIds.has(table.id.toLowerCase()),
      })))
    } catch {}
  }

  const handleSaveConnectionSettings = async () => {
    const host = connectionConfig.host.trim()
    const port = connectionConfig.port.trim()
    const database = connectionConfig.database.trim()
    const username = connectionConfig.username.trim()
    const password = connectionConfig.password.trim()
    const sslMode = connectionConfig.sslMode.trim()
    const defaultSchema = connectionConfig.defaultSchema.trim()

    if (!host || !port || !database || !username) {
      setConnectionSaveError("호스트/포트/데이터베이스/사용자명은 필수 입력입니다.")
      setConnectionSaveMessage(null)
      return
    }
    if (!password) {
      setConnectionSaveError("비밀번호는 필수 입력입니다.")
      setConnectionSaveMessage(null)
      return
    }
    if (host.toLowerCase() === "mimic-iv.hospital.edu") {
      setConnectionSaveError("mimic-iv.hospital.edu 는 예시값입니다. 실제 Oracle 호스트를 입력해 주세요.")
      setConnectionSaveMessage(null)
      return
    }

    setIsConnectionSaving(true)
    setConnectionSaveMessage(null)
    setConnectionSaveError(null)
    setTableScopeSaveMessage(null)
    setTableScopeSaveError(null)
    try {
      const connectionPayload: ConnectionSettings = {
        host,
        port,
        database,
        username,
        password,
        sslMode: sslMode || "disable",
        defaultSchema: defaultSchema || undefined,
      }
      const connectionRes = await fetch(apiUrlWithUser("/admin/settings/connection"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(connectionPayload),
      })
      if (!connectionRes.ok) {
        throw new Error(await readError(connectionRes))
      }
      setConnectionSaveMessage("연결 설정이 저장되었습니다.")
      await Promise.all([fetchPoolStatus(), loadSettings()])
    } catch (err: any) {
      setConnectionSaveError(err?.message || "연결 설정 저장에 실패했습니다.")
    } finally {
      setIsConnectionSaving(false)
    }
  }

  const handleSaveTableScope = async () => {
    setIsTableScopeSaving(true)
    setTableScopeSaveMessage(null)
    setTableScopeSaveError(null)
    try {
      const tablePayload: TableScopeSettings = {
        selected_ids: tableScopes.filter(t => t.selected).map(t => t.id),
      }
      const scopeRes = await fetch(apiUrlWithUser("/admin/settings/table-scope"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(tablePayload),
      })
      if (!scopeRes.ok) {
        throw new Error(await readError(scopeRes))
      }
      setTableScopeSaveMessage("테이블 스코프가 저장되었습니다.")
      await loadSettings()
    } catch (err: any) {
      setTableScopeSaveError(err?.message || "테이블 스코프 저장에 실패했습니다.")
    } finally {
      setIsTableScopeSaving(false)
    }
  }

  const toggleTable = (id: string) => {
    setTableScopes(prev => prev.map(t => 
      t.id === id ? { ...t, selected: !t.selected } : t
    ))
  }

  const selectedCount = tableScopes.filter(t => t.selected).length
  const allSelected = tableScopes.length > 0 && tableScopes.every(t => t.selected)
  const anySelected = selectedCount > 0
  const tablesPerPage = Math.max(1, tableColumns * 4)
  const totalPages = Math.max(1, Math.ceil(tableScopes.length / tablesPerPage))
  const pagedTables = tableScopes.slice(
    (tablePage - 1) * tablesPerPage,
    tablePage * tablesPerPage
  )

  useEffect(() => {
    setConnectionState("checking")
    fetchPoolStatus()
    loadSettings()
  }, [stateUser])

  useEffect(() => {
    const handleResize = () => {
      setTableColumns(getTableColumns(window.innerWidth))
    }
    handleResize()
    window.addEventListener("resize", handleResize)
    return () => window.removeEventListener("resize", handleResize)
  }, [])

  useEffect(() => {
    if (tablePage > totalPages) {
      setTablePage(totalPages)
    }
  }, [tablePage, totalPages])

  return (
    <div className="p-4 sm:p-6 space-y-4 sm:space-y-6 w-full max-w-none">
      {/* Connection Status */}
      <Card className={cn(
        "border-2 transition-colors",
        isCheckingConnection
          ? "border-border bg-secondary/20"
          : isConnected
            ? "border-primary/50 bg-primary/5"
            : "border-destructive/50 bg-destructive/5"
      )}>
        <CardContent className="p-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className={cn(
                "flex items-center justify-center w-10 h-10 rounded-full",
                isCheckingConnection
                  ? "bg-secondary"
                  : isConnected
                    ? "bg-primary/20"
                    : "bg-destructive/20"
              )}>
                {isCheckingConnection ? (
                  <RefreshCw className="w-5 h-5 animate-spin text-muted-foreground" />
                ) : isConnected ? (
                  <CheckCircle2 className="w-5 h-5 text-primary" />
                ) : (
                  <XCircle className="w-5 h-5 text-destructive" />
                )}
              </div>
              <div>
                <div className="flex items-center gap-2">
                  <span className="font-medium text-foreground">
                    {isCheckingConnection ? "연결 확인 중" : isConnected ? "연결됨" : "연결 안됨"}
                  </span>
                  {isConnected && !isCheckingConnection && (
                    <Badge variant="outline" className="text-xs">
                      <Lock className="w-3 h-3 mr-1" />
                      SSL
                    </Badge>
                  )}
                </div>
                <p className="text-sm text-muted-foreground">
                  {statusError
                    ? statusError
                    : isCheckingConnection
                      ? "연결 확인 중..."
                    : isConnected
                        ? `Oracle pool ${poolStatus?.open_connections ?? "-"} / ${poolStatus?.max ?? "-"} (busy ${poolStatus?.busy ?? "-"})`
                        : "데이터베이스에 연결되지 않았습니다"}
                </p>
                {oraAuthFailed && (
                  <p className="text-xs text-muted-foreground mt-1">
                    DB 사용자명에는 앱 로그인 아이디가 아닌 Oracle 계정을 입력하세요.
                  </p>
                )}
                {lastChecked && (
                  <p className="text-xs text-muted-foreground mt-1">
                    마지막 확인: {lastChecked.toLocaleTimeString()}
                  </p>
                )}
              </div>
            </div>
            <Button 
              variant="outline" 
              onClick={handleTestConnection}
              disabled={isTesting}
            >
              <RefreshCw className={cn("w-4 h-4 mr-2", isTesting && "animate-spin")} />
              {isTesting ? "테스트 중..." : "연결 테스트"}
            </Button>
          </div>
        </CardContent>
      </Card>

      <div className="grid lg:grid-cols-2 gap-4 sm:gap-6 items-stretch">
        {/* Connection Configuration */}
        <Card className="h-full">
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-lg">
              <Server className="w-5 h-5" />
              연결 설정
            </CardTitle>
            <CardDescription>데이터베이스 연결 정보를 입력하세요</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="host">호스트</Label>
                <Input 
                  id="host" 
                  value={connectionConfig.host}
                  onChange={(e) => setConnectionConfig(prev => ({ ...prev, host: e.target.value }))}
                  placeholder="예: db.example.com"
                  className={connectionPlaceholderClass}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="port">포트</Label>
                <Input 
                  id="port" 
                  value={connectionConfig.port}
                  onChange={(e) => setConnectionConfig(prev => ({ ...prev, port: e.target.value }))}
                  placeholder="예: 1234"
                  className={connectionPlaceholderClass}
                />
              </div>
            </div>
            <div className="space-y-2">
              <Label htmlFor="database">데이터베이스</Label>
              <Input 
                id="database" 
                value={connectionConfig.database}
                onChange={(e) => setConnectionConfig(prev => ({ ...prev, database: e.target.value }))}
                placeholder="예: ORCLPDB1"
                className={connectionPlaceholderClass}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="username">DB 사용자명</Label>
              <Input 
                id="username" 
                value={connectionConfig.username}
                onChange={(e) => setConnectionConfig(prev => ({ ...prev, username: e.target.value }))}
                placeholder="예: oracle_app_user (앱 로그인 ID 아님)"
                className={connectionPlaceholderClass}
              />
              <p className="text-xs text-muted-foreground">앱 로그인 아이디가 아니라 Oracle DB 계정을 입력하세요.</p>
            </div>
            <div className="space-y-2">
              <Label htmlFor="password">비밀번호</Label>
              <Input 
                id="password" 
                type="password"
                value={connectionConfig.password}
                onChange={(e) => setConnectionConfig(prev => ({ ...prev, password: e.target.value }))}
                placeholder="••••••••"
              />
            </div>
            <div className="pt-2 flex items-center justify-end gap-3">
              {(connectionSaveMessage || connectionSaveError) && (
                <p className={cn("text-xs", connectionSaveError ? "text-destructive" : "text-primary")}>
                  {connectionSaveError ?? connectionSaveMessage}
                </p>
              )}
              <Button onClick={handleSaveConnectionSettings} disabled={isConnectionSaving || isTableScopeSaving}>
                {isConnectionSaving ? "저장 중..." : "설정 저장"}
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Security Settings */}
        <Card className="h-full">
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-lg">
              <Shield className="w-5 h-5" />
              보안 설정
            </CardTitle>
            <CardDescription>쿼리 실행 권한 및 보안 정책을 설정합니다</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* Read-Only Mode */}
            <div className="flex items-center justify-between p-4 rounded-lg bg-secondary/50 border border-border">
              <div className="flex items-center gap-3">
                <div className="flex items-center justify-center w-8 h-8 rounded-full bg-primary/20">
                  <Eye className="w-4 h-4 text-primary" />
                </div>
                <div>
                  <div className="font-medium text-foreground">Read-Only 모드</div>
                  <div className="text-xs text-muted-foreground">SELECT/CTE 조회 쿼리만 허용 (항상 적용)</div>
                </div>
              </div>
              <Switch 
                checked={isReadOnly} 
                onCheckedChange={setIsReadOnly}
                disabled
              />
            </div>

            {/* Security Policies */}
            <div className="space-y-3">
              <div className="flex items-start gap-3 p-3 rounded-lg border border-border">
                <CheckCircle2 className="w-4 h-4 text-primary mt-0.5" />
                <div>
                  <div className="text-sm font-medium text-foreground">SQL Injection 방지</div>
                  <div className="text-xs text-muted-foreground">쓰기 키워드 및 비조회 SQL 실행 차단</div>
                </div>
              </div>
              <div className="flex items-start gap-3 p-3 rounded-lg border border-border">
                <CheckCircle2 className="w-4 h-4 text-primary mt-0.5" />
                <div>
                  <div className="text-sm font-medium text-foreground">테이블 스코프 강제</div>
                  <div className="text-xs text-muted-foreground">선택한 테이블 외 조회 차단</div>
                </div>
              </div>
              <div className="flex items-start gap-3 p-3 rounded-lg border border-border">
                <CheckCircle2 className="w-4 h-4 text-primary mt-0.5" />
                <div>
                  <div className="text-sm font-medium text-foreground">타임아웃 설정</div>
                  <div className="text-xs text-muted-foreground">쿼리 실행 30초 제한 (서버 설정값 적용)</div>
                </div>
              </div>
            </div>

            <div className="flex items-center gap-2 p-3 rounded-lg bg-yellow-500/10 border border-yellow-500/30">
              <AlertTriangle className="w-4 h-4 text-yellow-500" />
              <span className="text-xs text-yellow-500">모든 쿼리는 감사 로그에 기록됩니다</span>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Table Scope Selection */}
      <Card>
        <CardHeader>
          <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
            <div>
              <CardTitle className="flex items-center gap-2 text-lg">
                <Database className="w-5 h-5" />
                테이블 스코프 선택
              </CardTitle>
              <CardDescription>
                쿼리 대상 테이블을 선택하세요. 선택된 테이블만 NL2SQL 변환에 사용됩니다.
                <Badge variant="secondary" className="ml-2">{selectedCount}개 선택됨</Badge>
                <span className="ml-2 text-xs text-muted-foreground">
                  페이지 {tablePage}/{totalPages}
                </span>
              </CardDescription>
            </div>
            <label className="flex items-center gap-2 text-sm text-muted-foreground">
              <Checkbox
                checked={allSelected ? true : anySelected ? "indeterminate" : false}
                onCheckedChange={(checked) => {
                  const shouldSelect = checked === true
                  setTableScopes(prev => prev.map(table => ({ ...table, selected: shouldSelect })))
                }}
                aria-label="Select all tables"
              />
              Select all
            </label>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid sm:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-4 gap-3 items-stretch">
            {pagedTables.map((table) => (
              <div 
                key={table.id}
                className={cn(
                  "flex items-start gap-3 p-3 rounded-lg border transition-colors cursor-pointer h-full",
                  table.selected 
                    ? "border-primary/50 bg-primary/5" 
                    : "border-border hover:border-primary/30"
                )}
                onClick={() => toggleTable(table.id)}
              >
                <Checkbox 
                  checked={table.selected}
                  onCheckedChange={() => toggleTable(table.id)}
                  className="mt-0.5"
                />
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="font-mono text-sm font-medium text-foreground">{table.name}</span>
                    <Badge variant="outline" className="text-[10px]">{table.schema}</Badge>
                  </div>
                  {table.description && (
                    <p className="text-xs text-muted-foreground mt-1">{table.description}</p>
                  )}
                  {table.rowCount && (
                    <p className="text-[10px] text-muted-foreground mt-1">{table.rowCount} rows</p>
                  )}
                </div>
              </div>
            ))}
          </div>
          <div className="mt-4 border-t pt-4">
            <div className="grid grid-cols-[minmax(0,1fr)_auto_auto] items-center gap-3">
              <div className="min-w-0">
                {(tableScopeSaveMessage || tableScopeSaveError) && (
                  <span className={cn("text-xs", tableScopeSaveError ? "text-destructive" : "text-primary")}>
                    {tableScopeSaveError ?? tableScopeSaveMessage}
                  </span>
                )}
              </div>
              <div className="flex items-center justify-center gap-2">
                {totalPages > 1 && (
                  <>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setTablePage(prev => Math.max(1, prev - 1))}
                      disabled={tablePage <= 1}
                    >
                      이전
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setTablePage(prev => Math.min(totalPages, prev + 1))}
                      disabled={tablePage >= totalPages}
                    >
                      다음
                    </Button>
                  </>
                )}
              </div>
              <div className="flex items-center justify-end">
                <Button onClick={handleSaveTableScope} disabled={isConnectionSaving || isTableScopeSaving}>
                  {isTableScopeSaving ? "저장 중..." : "설정 저장"}
                </Button>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
