"use client"

import { useEffect, useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { 
  FileText, 
  Clock, 
  User,
  Search,
  Download,
  Trash2,
  ChevronDown,
  ChevronRight,
  Eye,
  Code,
  BookOpen,
  CheckCircle2,
  Shield,
  Database,
  Calendar
} from "lucide-react"
import { cn } from "@/lib/utils"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { useAuth } from "@/components/auth-provider"

interface AuditLog {
  id: string
  timestamp: string
  ts?: number
  user: {
    id?: string | null
    name: string
    role: string
  }
  query: {
    original: string
    sql: string
  }
  appliedTerms: { term: string; version: string }[]
  appliedMetrics: { name: string; version: string }[]
  execution: {
    duration: string
    rowsReturned: number
    status: "success" | "error" | "warning"
  }
  resultSnapshot?: {
    summary: string
    downloadUrl: string
  }
}

interface AuditStats {
  total: number
  today: number
  success_rate: number
}

export function AuditView() {
  const { user } = useAuth()
  const apiBaseUrl = (process.env.NEXT_PUBLIC_API_BASE_URL || "").replace(/\/$/, "")
  const apiUrl = (path: string) => (apiBaseUrl ? `${apiBaseUrl}${path}` : path)
  const auditUser = (user?.id || user?.username || user?.name || "").trim()
  const apiUrlWithUser = (path: string) => {
    const base = apiUrl(path)
    if (!auditUser) return base
    const separator = base.includes("?") ? "&" : "?"
    return `${base}${separator}user=${encodeURIComponent(auditUser)}`
  }
  const fetchWithTimeout = async (input: RequestInfo, init: RequestInit = {}, timeoutMs = 15000) => {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), timeoutMs)
    try {
      return await fetch(input, { ...init, signal: controller.signal })
    } finally {
      clearTimeout(timeout)
    }
  }

  const [logs, setLogs] = useState<AuditLog[]>([])
  const [stats, setStats] = useState<AuditStats | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [searchTerm, setSearchTerm] = useState("")
  const [expandedLogs, setExpandedLogs] = useState<string[]>([])
  const [dateFilter, setDateFilter] = useState("all")
  const [deletingLogId, setDeletingLogId] = useState<string | null>(null)
  const [deleteConfirmOpen, setDeleteConfirmOpen] = useState(false)
  const [pendingDeleteLogId, setPendingDeleteLogId] = useState<string | null>(null)
  const [logPage, setLogPage] = useState(1)
  const LOGS_PER_PAGE = 5

  const readError = async (res: Response) => {
    const text = await res.text()
    try {
      const json = JSON.parse(text)
      if (json?.detail) return String(json.detail)
    } catch {}
    return text || `${res.status} ${res.statusText}`
  }

  const fetchLogs = async () => {
    setIsLoading(true)
    setLoadError(null)
    try {
      const res = await fetchWithTimeout(apiUrlWithUser("/audit/logs?limit=500"))
      if (!res.ok) {
        throw new Error(await readError(res))
      }
      const data = await res.json()
      setLogs(Array.isArray(data?.logs) ? data.logs : [])
      setStats(data?.stats || null)
    } catch (err: any) {
      setLoadError(err?.message || "감사 로그를 불러오지 못했습니다.")
      setLogs([])
      setStats(null)
    } finally {
      setIsLoading(false)
    }
  }

  useEffect(() => {
    fetchLogs()
  }, [auditUser])

  const toggleExpand = (id: string) => {
    setExpandedLogs(prev => 
      prev.includes(id) ? prev.filter(i => i !== id) : [...prev, id]
    )
  }

  const getStatusBadge = (status: string) => {
    switch (status) {
      case "success":
        return <Badge variant="default" className="text-[10px]">성공</Badge>
      case "error":
        return <Badge variant="destructive" className="text-[10px]">실패</Badge>
      case "warning":
        return <Badge variant="outline" className="text-[10px] border-yellow-500 text-yellow-500">경고</Badge>
      default:
        return null
    }
  }

  const resolveTimestampMs = (log: AuditLog) => {
    if (log.ts) return log.ts * 1000
    if (!log.timestamp) return null
    const parsed = Date.parse(log.timestamp.replace(" ", "T"))
    return Number.isNaN(parsed) ? null : parsed
  }

  const filteredLogs = logs.filter(log => {
    const search = searchTerm.trim().toLowerCase()
    const matchesSearch = !search ||
      log.query.original.toLowerCase().includes(search) ||
      log.query.sql.toLowerCase().includes(search)

    const tsMs = resolveTimestampMs(log)
    const now = Date.now()
    const matchesDate = (() => {
      if (dateFilter === "all") return true
      if (!tsMs) return true
      if (dateFilter === "today") {
        const logDate = new Date(tsMs)
        const nowDate = new Date(now)
        return logDate.toDateString() === nowDate.toDateString()
      }
      if (dateFilter === "week") {
        return tsMs >= now - 7 * 24 * 60 * 60 * 1000
      }
      if (dateFilter === "month") {
        return tsMs >= now - 30 * 24 * 60 * 60 * 1000
      }
      return true
    })()

    return matchesSearch && matchesDate
  })

  const totalLogPages = Math.max(1, Math.ceil(filteredLogs.length / LOGS_PER_PAGE))
  const pagedLogs = filteredLogs.slice((logPage - 1) * LOGS_PER_PAGE, logPage * LOGS_PER_PAGE)
  const pageTokens = buildPageTokens(totalLogPages, logPage)

  useEffect(() => {
    setLogPage(1)
  }, [searchTerm, dateFilter])

  useEffect(() => {
    if (logPage > totalLogPages) {
      setLogPage(totalLogPages)
    }
  }, [logPage, totalLogPages])

  const derivedStats = (() => {
    if (stats) return stats
    const total = logs.length
    const todayDate = new Date().toDateString()
    let today = 0
    let success = 0
    for (const log of logs) {
      const tsMs = resolveTimestampMs(log)
      if (tsMs && new Date(tsMs).toDateString() === todayDate) {
        today += 1
      }
      if (log.execution.status === "success") {
        success += 1
      }
    }
    return {
      total,
      today,
      success_rate: total ? Math.round((success / total) * 1000) / 10 : 0,
    }
  })()

  const escapeCsvCell = (value: string | number | null | undefined) => {
    const text = value == null ? "" : String(value)
    if (/[",\n]/.test(text)) {
      return `"${text.replace(/"/g, '""')}"`
    }
    return text
  }

  const buildCsv = (items: AuditLog[]) => {
    const header = [
      "timestamp",
      "user",
      "role",
      "question",
      "sql",
      "status",
      "rows",
      "duration",
      "terms",
      "metrics",
      "summary",
    ]
    const rows = items.map((log) => {
      const terms = log.appliedTerms
        .map((term) => (term.version ? `${term.term}(${term.version})` : term.term))
        .join("; ")
      const metrics = log.appliedMetrics
        .map((metric) => (metric.version ? `${metric.name}(${metric.version})` : metric.name))
        .join("; ")
      return [
        log.timestamp,
        log.user.name,
        log.user.role,
        log.query.original,
        log.query.sql,
        log.execution.status,
        log.execution.rowsReturned,
        log.execution.duration,
        terms,
        metrics,
        log.resultSnapshot?.summary || "",
      ]
        .map(escapeCsvCell)
        .join(",")
    })
    return `${header.join(",")}\n${rows.join("\n")}`
  }

  const buildJsonl = (items: AuditLog[]) => {
    return items.map((log) => JSON.stringify(log)).join("\n")
  }

  const handleExport = (format: "csv" | "jsonl") => {
    if (!filteredLogs.length) return
    const content = format === "csv" ? buildCsv(filteredLogs) : buildJsonl(filteredLogs)
    const mime = format === "csv" ? "text/csv;charset=utf-8;" : "application/jsonl;charset=utf-8;"
    const blob = new Blob([content], { type: mime })
    const url = URL.createObjectURL(blob)
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-")
    const a = document.createElement("a")
    a.href = url
    a.download = `audit-logs-${timestamp}.${format}`
    a.click()
    URL.revokeObjectURL(url)
  }

  const executeDeleteLog = async (logId: string) => {
    if (!logId || deletingLogId) return
    setDeletingLogId(logId)
    try {
      const res = await fetchWithTimeout(apiUrlWithUser(`/audit/logs/${encodeURIComponent(logId)}`), {
        method: "DELETE",
      })
      if (!res.ok) {
        throw new Error(await readError(res))
      }
      await fetchLogs()
    } catch (err: any) {
      setLoadError(err?.message || "감사 로그 삭제에 실패했습니다.")
    } finally {
      setDeletingLogId(null)
    }
  }

  const handleDeleteLog = (logId: string) => {
    if (!logId || deletingLogId) return
    setPendingDeleteLogId(logId)
    setDeleteConfirmOpen(true)
  }

  return (
    <div className="p-4 sm:p-6 space-y-4 sm:space-y-6">
      {loadError && <p className="text-xs text-destructive">{loadError}</p>}
      {!loadError && isLoading && <p className="text-xs text-muted-foreground">감사 로그를 불러오는 중...</p>}
      <div className="flex justify-end">
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button
              variant="outline"
              className="gap-2 bg-transparent w-full sm:w-auto"
              disabled={isLoading || filteredLogs.length === 0}
            >
              <Download className="w-4 h-4" />
              로그 내보내기
              <ChevronDown className="w-4 h-4" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuItem onClick={() => handleExport("csv")}>CSV</DropdownMenuItem>
            <DropdownMenuItem onClick={() => handleExport("jsonl")}>JSONL</DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>

      {/* Filters */}
      <Card>
        <CardContent className="py-3 sm:py-4">
          <div className="flex flex-col sm:flex-row items-stretch sm:items-center gap-3 sm:gap-4">
            <div className="relative flex-1 sm:max-w-md">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
              <Input 
                placeholder="질문 또는 SQL 검색..." 
                className="pl-9"
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
              />
            </div>
            <div className="flex items-center gap-2 sm:ml-auto">
              <Select value={dateFilter} onValueChange={setDateFilter}>
                <SelectTrigger className="w-full sm:w-[140px]">
                  <Calendar className="w-4 h-4 mr-2" />
                  <SelectValue placeholder="기간" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">전체 기간</SelectItem>
                  <SelectItem value="today">오늘</SelectItem>
                  <SelectItem value="week">최근 7일</SelectItem>
                  <SelectItem value="month">최근 30일</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Summary Stats */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3 sm:gap-4">
        {[
          { label: "총 쿼리 수", value: derivedStats.total.toLocaleString(), icon: Database },
          { label: "오늘 실행", value: derivedStats.today.toLocaleString(), icon: Clock },
          { label: "성공률", value: `${derivedStats.success_rate.toFixed(1)}%`, icon: CheckCircle2 },
        ].map((stat) => (
          <Card key={stat.label}>
            <CardContent className="py-4">
              <div className="flex items-center gap-3">
                <div className="flex items-center justify-center w-10 h-10 rounded-lg bg-primary/10">
                  <stat.icon className="w-5 h-5 text-primary" />
                </div>
                <div>
                  <div className="text-xl font-bold text-foreground">{stat.value}</div>
                  <div className="text-xs text-muted-foreground">{stat.label}</div>
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Audit Logs */}
      <Card>
        <CardHeader>
          <CardTitle className="text-lg flex items-center gap-2">
            <FileText className="w-5 h-5" />
            실행 기록
          </CardTitle>
          <CardDescription>각 로그를 클릭하여 상세 정보를 확인하세요</CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          {filteredLogs.length === 0 ? (
            <div className="py-8 text-center text-sm text-muted-foreground">
              {isLoading ? "감사 로그를 불러오는 중..." : "표시할 감사 로그가 없습니다."}
            </div>
          ) : (
            pagedLogs.map((log) => (
              <Collapsible 
                key={log.id} 
                open={expandedLogs.includes(log.id)}
                onOpenChange={() => toggleExpand(log.id)}
              >
                <CollapsibleTrigger asChild>
                  <div className={cn(
                    "flex items-center gap-4 p-4 rounded-lg border border-border cursor-pointer transition-colors hover:border-primary/30",
                    expandedLogs.includes(log.id) && "bg-secondary/30 border-primary/30"
                  )}>
                    {expandedLogs.includes(log.id) ? (
                      <ChevronDown className="w-4 h-4 text-muted-foreground shrink-0" />
                    ) : (
                      <ChevronRight className="w-4 h-4 text-muted-foreground shrink-0" />
                    )}
                    
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 mb-1">
                        <span className="text-sm font-medium text-foreground truncate">
                          {log.query.original}
                        </span>
                        {getStatusBadge(log.execution.status)}
                      </div>
                      <div className="flex items-center gap-4 text-xs text-muted-foreground">
                        <span className="flex items-center gap-1">
                          <Clock className="w-3 h-3" />
                          {log.timestamp}
                        </span>
                        <span className="flex items-center gap-1">
                          <User className="w-3 h-3" />
                          {log.user.name} ({log.user.role})
                        </span>
                        <span>{log.execution.rowsReturned.toLocaleString()} rows</span>
                        <span>{log.execution.duration}</span>
                      </div>
                    </div>
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      className="h-8 w-8 shrink-0"
                      onClick={(e) => {
                        e.stopPropagation()
                        handleDeleteLog(log.id)
                      }}
                      disabled={deletingLogId === log.id}
                      aria-label="로그 삭제"
                    >
                      {deletingLogId === log.id ? (
                        <span className="text-[10px] text-muted-foreground">...</span>
                      ) : (
                        <Trash2 className="w-4 h-4" />
                      )}
                    </Button>
                  </div>
                </CollapsibleTrigger>

                <CollapsibleContent>
                  <div className="ml-8 mt-2 p-4 rounded-lg bg-secondary/20 border border-border space-y-4">
                    {/* Applied Terms & Metrics */}
                    <div className="grid md:grid-cols-2 gap-4">
                      <div>
                        <div className="text-xs font-medium text-muted-foreground mb-2 flex items-center gap-1">
                          <BookOpen className="w-3 h-3" />
                          적용된 용어
                        </div>
                        <div className="flex flex-wrap gap-1">
                          {log.appliedTerms.map((term, idx) => (
                            <Badge key={idx} variant="outline" className="text-[10px]">
                              {term.term} <span className="text-muted-foreground ml-1">{term.version}</span>
                            </Badge>
                          ))}
                          {log.appliedTerms.length === 0 && (
                            <span className="text-xs text-muted-foreground">없음</span>
                          )}
                        </div>
                      </div>
                      <div>
                        <div className="text-xs font-medium text-muted-foreground mb-2 flex items-center gap-1">
                          <Shield className="w-3 h-3" />
                          적용된 지표
                        </div>
                        <div className="flex flex-wrap gap-1">
                          {log.appliedMetrics.map((metric, idx) => (
                            <Badge key={idx} variant="outline" className="text-[10px]">
                              {metric.name} <span className="text-muted-foreground ml-1">{metric.version}</span>
                            </Badge>
                          ))}
                          {log.appliedMetrics.length === 0 && (
                            <span className="text-xs text-muted-foreground">없음</span>
                          )}
                        </div>
                      </div>
                    </div>

                    {/* SQL Query */}
                    <div>
                      <div className="text-xs font-medium text-muted-foreground mb-2 flex items-center gap-1">
                        <Code className="w-3 h-3" />
                        실행된 SQL
                      </div>
                      <div className="p-3 rounded-lg bg-background text-[11px] font-mono text-foreground overflow-auto max-h-[420px] border border-border [scrollbar-gutter:stable]">
                        {log.query.sql ? (
                          <pre className="whitespace-pre-wrap leading-6">
                            <code dangerouslySetInnerHTML={{ __html: highlightSqlForDisplay(log.query.sql) }} />
                          </pre>
                        ) : (
                          <span className="text-xs text-muted-foreground">SQL 없음</span>
                        )}
                      </div>
                    </div>

                    {/* Result Snapshot */}
                    {log.resultSnapshot && (
                      <div>
                        <div className="text-xs font-medium text-muted-foreground mb-2 flex items-center gap-1">
                          <Eye className="w-3 h-3" />
                          결과 스냅샷
                        </div>
                        <div className="flex items-center justify-between p-3 rounded-lg bg-background border border-border">
                          <span className="text-sm text-foreground">{log.resultSnapshot.summary}</span>
                          <Button variant="ghost" size="sm" className="h-7 gap-1 text-xs">
                            <Download className="w-3 h-3" />
                            다운로드
                          </Button>
                        </div>
                      </div>
                    )}
                  </div>
                </CollapsibleContent>
              </Collapsible>
            ))
          )}
          {filteredLogs.length > 0 && totalLogPages > 1 && (
            <div className="pt-2 flex items-center justify-center gap-1 flex-wrap">
              {pageTokens.map((token, index) =>
                token === "ellipsis" ? (
                  <span
                    key={`ellipsis-${index}`}
                    className="inline-flex h-8 items-center px-2 text-xs text-muted-foreground"
                  >
                    ...
                  </span>
                ) : (
                  <Button
                    key={`page-${token}`}
                    type="button"
                    size="sm"
                    variant={token === logPage ? "default" : "outline"}
                    className="h-8 min-w-8 px-2 text-xs"
                    onClick={() => setLogPage(token)}
                  >
                    {token}
                  </Button>
                )
              )}
            </div>
          )}
        </CardContent>
      </Card>

      <Dialog open={deleteConfirmOpen} onOpenChange={setDeleteConfirmOpen}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>감사 로그 삭제</DialogTitle>
            <DialogDescription>
              이 감사 로그 1건을 삭제할까요?
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setDeleteConfirmOpen(false)
                setPendingDeleteLogId(null)
              }}
            >
              취소
            </Button>
            <Button
              variant="destructive"
              disabled={!pendingDeleteLogId || !!deletingLogId}
              onClick={async () => {
                if (!pendingDeleteLogId) return
                await executeDeleteLog(pendingDeleteLogId)
                setDeleteConfirmOpen(false)
                setPendingDeleteLogId(null)
              }}
            >
              삭제
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}

function buildPageTokens(totalPages: number, currentPage: number): Array<number | "ellipsis"> {
  if (totalPages <= 7) {
    return Array.from({ length: totalPages }, (_, idx) => idx + 1)
  }

  if (currentPage <= 4) {
    return [1, 2, 3, 4, 5, "ellipsis", totalPages]
  }

  if (currentPage >= totalPages - 3) {
    return [1, "ellipsis", totalPages - 4, totalPages - 3, totalPages - 2, totalPages - 1, totalPages]
  }

  return [1, "ellipsis", currentPage - 1, currentPage, currentPage + 1, "ellipsis", totalPages]
}

function formatSqlForDisplay(sql: string) {
  if (!sql?.trim()) return sql

  let formatted = sql.replace(/\s+/g, " ").trim()

  const clausePatterns: RegExp[] = [
    /\bWITH\b/gi,
    /\bSELECT\b/gi,
    /\bFROM\b/gi,
    /\bLEFT\s+OUTER\s+JOIN\b/gi,
    /\bRIGHT\s+OUTER\s+JOIN\b/gi,
    /\bFULL\s+OUTER\s+JOIN\b/gi,
    /\bLEFT\s+JOIN\b/gi,
    /\bRIGHT\s+JOIN\b/gi,
    /\bINNER\s+JOIN\b/gi,
    /\bFULL\s+JOIN\b/gi,
    /\bJOIN\b/gi,
    /\bON\b/gi,
    /\bWHERE\b/gi,
    /\bGROUP\s+BY\b/gi,
    /\bHAVING\b/gi,
    /\bORDER\s+BY\b/gi,
    /\bUNION\s+ALL\b/gi,
    /\bUNION\b/gi,
  ]

  for (const pattern of clausePatterns) {
    formatted = formatted.replace(pattern, (match, offset) => {
      const token = match.toUpperCase().replace(/\s+/g, " ")
      return offset === 0 ? token : `\n${token}`
    })
  }

  formatted = formatted.replace(/,\s*/g, ",\n  ")
  formatted = formatted.replace(/\bCASE\b/gi, "\nCASE")
  formatted = formatted.replace(/\bWHEN\b/gi, "\n  WHEN")
  formatted = formatted.replace(/\bTHEN\b/gi, "\n    THEN")
  formatted = formatted.replace(/\bELSE\b/gi, "\n  ELSE")
  formatted = formatted.replace(/\bEND\b/gi, "\nEND")
  formatted = formatted.replace(/\s+(AND|OR)\s+/gi, (_, op) => `\n  ${String(op).toUpperCase()} `)
  return formatted.replace(/\n{3,}/g, "\n\n").trim()
}

function highlightSqlForDisplay(sql: string) {
  const formatted = formatSqlForDisplay(sql)
  if (!formatted?.trim()) return ""

  const keywordPattern =
    /\b(WITH|SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|FULL|OUTER|ON|GROUP|BY|HAVING|ORDER|UNION|ALL|DISTINCT|AS|CASE|WHEN|THEN|ELSE|END|AND|OR|IN|IS|NOT|NULL|LIKE)\b/gi
  const functionPattern =
    /\b(COUNT|SUM|AVG|MIN|MAX|CAST|COALESCE|NVL|EXTRACT|ROUND|TRUNC|TO_DATE|TO_CHAR)\b(?=\s*\()/gi

  let highlighted = escapeHtml(formatted)
  const placeholders: string[] = []

  const stash = (pattern: RegExp, className: string) => {
    highlighted = highlighted.replace(pattern, (match) => {
      const token = `__SQL_TOKEN_${placeholders.length}__`
      placeholders.push(`<span class="${className}">${match}</span>`)
      return token
    })
  }

  stash(/--[^\n]*/g, "text-muted-foreground")
  stash(/'(?:''|[^'])*'/g, "text-lime-700 dark:text-lime-400")
  stash(/"(?:[^"]|"")*"/g, "text-lime-700 dark:text-lime-400")

  highlighted = highlighted.replace(
    functionPattern,
    (match) => `<span class="text-pink-600 dark:text-pink-400 font-semibold">${match.toUpperCase()}</span>`
  )
  highlighted = highlighted.replace(
    keywordPattern,
    (match) => `<span class="text-sky-600 dark:text-sky-400 font-semibold">${match.toUpperCase()}</span>`
  )

  highlighted = highlighted.replace(/__SQL_TOKEN_(\d+)__/g, (_, idx) => {
    const token = placeholders[Number(idx)]
    return token || ""
  })

  return highlighted
}

function escapeHtml(value: string) {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
}
