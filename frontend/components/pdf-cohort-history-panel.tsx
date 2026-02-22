"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Skeleton } from "@/components/ui/skeleton"
import { cn } from "@/lib/utils"
import {
  fetchPdfCohortHistoryDetail,
  fetchPdfCohortHistoryList,
  type PdfCohortHistoryDetail,
  type PdfCohortHistoryItem,
} from "@/lib/pdf-cohort-history"
import { Filter, RefreshCcw, Search } from "lucide-react"
import { PdfCohortHistoryDrawer } from "@/components/pdf-cohort-history-drawer"

type PdfCohortHistoryPanelProps = {
  userId: string
  refreshToken?: number
  className?: string
  onApplyHistory: (detail: PdfCohortHistoryDetail) => void
  onMoveToQuery: (detail: PdfCohortHistoryDetail) => void
  onOpenLinkedCohort: (cohortId: string, detail: PdfCohortHistoryDetail) => void
}

const statusLabelMap: Record<string, string> = {
  DONE: "성공",
  RUNNING: "진행중",
  ERROR: "실패",
}

const statusClassMap: Record<string, string> = {
  DONE: "bg-emerald-500/15 text-emerald-700 border-emerald-500/30",
  RUNNING: "bg-amber-500/15 text-amber-700 border-amber-500/30",
  ERROR: "bg-destructive/15 text-destructive border-destructive/30",
}

const PAGE_SIZE = 8

const formatDateTime = (value: string) => {
  const text = String(value || "").trim()
  if (!text) return "-"
  const date = new Date(text)
  if (Number.isNaN(date.getTime())) return text
  return date.toLocaleString("ko-KR")
}

export function PdfCohortHistoryPanel({
  userId,
  refreshToken = 0,
  className,
  onApplyHistory,
  onMoveToQuery,
  onOpenLinkedCohort,
}: PdfCohortHistoryPanelProps) {
  const detailCacheRef = useRef<Record<string, PdfCohortHistoryDetail>>({})
  const [query, setQuery] = useState("")
  const [statusFilter, setStatusFilter] = useState<"" | "DONE" | "RUNNING" | "ERROR">("")
  const [cohortFilter, setCohortFilter] = useState<"" | "saved" | "unsaved">("")
  const [sortOrder, setSortOrder] = useState<"newest" | "oldest">("newest")
  const [fromDate, setFromDate] = useState("")
  const [toDate, setToDate] = useState("")
  const [isFilterOpen, setIsFilterOpen] = useState(false)
  const [page, setPage] = useState(1)
  const [items, setItems] = useState<PdfCohortHistoryItem[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [selectedDetail, setSelectedDetail] = useState<PdfCohortHistoryDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState<string | null>(null)

  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE))
  const hasRunningItem = items.some((item) => item.status === "RUNNING")
  const hasRunningDetail = selectedDetail?.status === "RUNNING"

  const loadList = useCallback(
    async (silent = false) => {
      if (!silent) setLoading(true)
      setError(null)
      try {
        const payload = await fetchPdfCohortHistoryList(
          {
            query,
            status: statusFilter,
            from: fromDate || undefined,
            to: toDate || undefined,
            cohortSaved: cohortFilter,
            sort: sortOrder,
            page,
            pageSize: PAGE_SIZE,
          },
          userId
        )
        setItems(payload.items)
        setTotal(payload.total)
        if (payload.page > 0 && payload.page !== page) {
          setPage(payload.page)
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : "히스토리 목록을 불러오지 못했습니다."
        setError(message)
      } finally {
        if (!silent) setLoading(false)
      }
    },
    [cohortFilter, fromDate, page, query, sortOrder, statusFilter, toDate, userId]
  )

  const ensureDetail = useCallback(
    async (historyId: string) => {
      const cached = detailCacheRef.current[historyId]
      if (cached) {
        setSelectedDetail(cached)
        return cached
      }
      setDetailLoading(true)
      setDetailError(null)
      try {
        const detail = await fetchPdfCohortHistoryDetail(historyId, userId)
        detailCacheRef.current[historyId] = detail
        setSelectedDetail(detail)
        return detail
      } catch (err) {
        const message = err instanceof Error ? err.message : "상세 정보를 불러오지 못했습니다."
        setDetailError(message)
        setSelectedDetail(null)
        return null
      } finally {
        setDetailLoading(false)
      }
    },
    [userId]
  )

  const openDrawerWithDetail = useCallback(
    async (historyId: string) => {
      setSelectedId(historyId)
      setDrawerOpen(true)
      await ensureDetail(historyId)
    },
    [ensureDetail]
  )

  const handleApplyById = useCallback(
    async (historyId: string) => {
      const detail = await ensureDetail(historyId)
      if (!detail) return
      onApplyHistory(detail)
    },
    [ensureDetail, onApplyHistory]
  )

  const handleMoveToQueryById = useCallback(
    async (historyId: string) => {
      const detail = await ensureDetail(historyId)
      if (!detail) return
      onMoveToQuery(detail)
    },
    [ensureDetail, onMoveToQuery]
  )

  const handleLinkedCohortById = useCallback(
    async (historyId: string, cohortId: string) => {
      const detail = await ensureDetail(historyId)
      if (!detail) return
      onOpenLinkedCohort(cohortId, detail)
    },
    [ensureDetail, onOpenLinkedCohort]
  )

  useEffect(() => {
    if (page !== 1) setPage(1)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query, statusFilter, cohortFilter, sortOrder, fromDate, toDate])

  useEffect(() => {
    void loadList(false)
  }, [loadList, refreshToken])

  useEffect(() => {
    if (!hasRunningItem) return
    const timer = window.setInterval(() => {
      void loadList(true)
    }, 4000)
    return () => window.clearInterval(timer)
  }, [hasRunningItem, loadList])

  useEffect(() => {
    if (!drawerOpen || !selectedId || !hasRunningDetail) return
    const timer = window.setInterval(() => {
      void ensureDetail(selectedId)
    }, 4000)
    return () => window.clearInterval(timer)
  }, [drawerOpen, ensureDetail, hasRunningDetail, selectedId])

  const emptyStateText = useMemo(() => {
    if (query || statusFilter || cohortFilter || fromDate || toDate) {
      return "조건에 맞는 히스토리가 없습니다. 필터를 조정해보세요."
    }
    return "아직 생성된 PDF 코호트 분석 이력이 없습니다. 아래에서 PDF를 업로드해 시작하세요."
  }, [cohortFilter, fromDate, query, statusFilter, toDate])

  return (
    <>
      <Card className={cn("border border-border/70", className)}>
        <CardHeader className="pb-3 space-y-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="flex items-center gap-2">
              <CardTitle className="text-base">PDFLLM 산출물 히스토리</CardTitle>
              <Badge variant="secondary" className="text-[11px]">
                {total.toLocaleString()}건
              </Badge>
            </div>
            <div className="flex items-center gap-1.5">
              <Button
                type="button"
                variant={isFilterOpen ? "default" : "outline"}
                size="sm"
                className="h-8 text-xs"
                onClick={() => setIsFilterOpen((prev) => !prev)}
              >
                <Filter className="w-3.5 h-3.5 mr-1" />
                필터
              </Button>
              <Button type="button" variant="outline" size="sm" className="h-8 text-xs" onClick={() => void loadList(false)}>
                <RefreshCcw className="w-3.5 h-3.5 mr-1" />
                새로고침
              </Button>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <div className="relative flex-1 min-w-[220px]">
              <Search className="w-4 h-4 absolute left-2.5 top-1/2 -translate-y-1/2 text-muted-foreground" />
              <Input
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                placeholder="파일명/논문제목/저자/키워드 검색"
                className="pl-8 h-8 text-sm"
              />
            </div>
            <Select value={sortOrder} onValueChange={(value: "newest" | "oldest") => setSortOrder(value)}>
              <SelectTrigger className="h-8 w-[130px] text-xs">
                <SelectValue placeholder="정렬" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="newest">최신순</SelectItem>
                <SelectItem value="oldest">오래된순</SelectItem>
              </SelectContent>
            </Select>
          </div>

          {isFilterOpen && (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-2 rounded-lg border bg-muted/30 p-2">
              <Select value={statusFilter || "__all__"} onValueChange={(value) => setStatusFilter(value === "__all__" ? "" : (value as any))}>
                <SelectTrigger className="h-8 text-xs">
                  <SelectValue placeholder="상태" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="__all__">상태 전체</SelectItem>
                  <SelectItem value="DONE">성공</SelectItem>
                  <SelectItem value="RUNNING">진행중</SelectItem>
                  <SelectItem value="ERROR">실패</SelectItem>
                </SelectContent>
              </Select>

              <Select value={cohortFilter || "__all__"} onValueChange={(value) => setCohortFilter(value === "__all__" ? "" : (value as any))}>
                <SelectTrigger className="h-8 text-xs">
                  <SelectValue placeholder="코호트 저장" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="__all__">저장 여부 전체</SelectItem>
                  <SelectItem value="saved">코호트 저장됨</SelectItem>
                  <SelectItem value="unsaved">미저장</SelectItem>
                </SelectContent>
              </Select>

              <Input type="date" value={fromDate} onChange={(event) => setFromDate(event.target.value)} className="h-8 text-xs" />
              <Input type="date" value={toDate} onChange={(event) => setToDate(event.target.value)} className="h-8 text-xs" />
            </div>
          )}
        </CardHeader>

        <CardContent className="space-y-2">
          {loading && (
            <div className="space-y-2">
              {Array.from({ length: 5 }).map((_, index) => (
                <div key={`history-skeleton-${index}`} className="rounded-lg border p-3 space-y-2">
                  <Skeleton className="h-4 w-2/3" />
                  <Skeleton className="h-3 w-full" />
                  <Skeleton className="h-3 w-3/5" />
                </div>
              ))}
            </div>
          )}

          {!loading && error && (
            <div className="rounded-lg border border-destructive/40 bg-destructive/10 p-3 text-sm text-destructive space-y-2">
              <div>{error}</div>
              <Button type="button" variant="outline" size="sm" onClick={() => void loadList(false)}>
                <RefreshCcw className="w-3.5 h-3.5 mr-1" />
                재시도
              </Button>
            </div>
          )}

          {!loading && !error && items.length === 0 && (
            <div className="rounded-lg border border-dashed p-4 text-sm text-muted-foreground">{emptyStateText}</div>
          )}

          {!loading && !error && items.length > 0 && (
            <div className="space-y-2">
              {items.map((item) => {
                const title = item.paperTitle || item.fileName || "PDF 산출물"
                const statusClass = statusClassMap[item.status] || statusClassMap.DONE
                const statusLabel = statusLabelMap[item.status] || "성공"
                const sqlLabel = item.sqlReady ? "SQL 준비됨" : "SQL 미완성"
                const isActionDisabled = item.status !== "DONE"
                return (
                  <div
                    key={item.id}
                    role="button"
                    tabIndex={0}
                    className={cn(
                      "w-full rounded-lg border p-3 text-left hover:bg-muted/40 transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/40",
                      selectedId === item.id && "border-primary/40 bg-primary/5"
                    )}
                    onClick={() => void openDrawerWithDetail(item.id)}
                    onKeyDown={(event) => {
                      if (event.key === "Enter" || event.key === " ") {
                        event.preventDefault()
                        void openDrawerWithDetail(item.id)
                      }
                    }}
                  >
                    <div className="flex items-start justify-between gap-2">
                      <div className="min-w-0 space-y-1.5">
                        <div className="flex flex-wrap items-center gap-1.5">
                          <Badge className={statusClass}>{statusLabel}</Badge>
                          {item.cohortSaved ? (
                            <Badge variant="outline" className="text-[10px]">
                              코호트 저장됨
                            </Badge>
                          ) : (
                            <Badge variant="outline" className="text-[10px] text-muted-foreground">
                              코호트 미저장
                            </Badge>
                          )}
                          <span className="text-sm font-semibold truncate max-w-[640px]">{title}</span>
                        </div>
                        <div className="text-xs text-muted-foreground line-clamp-1">{item.criteriaSummary || "조건 요약 정보가 없습니다."}</div>
                        <div className="flex flex-wrap items-center gap-2 text-[11px] text-muted-foreground">
                          <span>{formatDateTime(item.createdAt)}</span>
                          <span>매핑 변수 {Number(item.mappedVarsCount || 0).toLocaleString()}개</span>
                          <span>{sqlLabel}</span>
                          {item.errorMessage && <span className="text-destructive line-clamp-1">오류: {item.errorMessage}</span>}
                        </div>
                      </div>

                      <div className="shrink-0 flex flex-col sm:flex-row gap-1">
                        <Button
                          type="button"
                          size="sm"
                          variant="outline"
                          className="h-7 text-[11px]"
                          onClick={(event) => {
                            event.stopPropagation()
                            void openDrawerWithDetail(item.id)
                          }}
                        >
                          상세
                        </Button>
                        <Button
                          type="button"
                          size="sm"
                          variant="outline"
                          className="h-7 text-[11px]"
                          disabled={isActionDisabled}
                          onClick={(event) => {
                            event.stopPropagation()
                            void handleApplyById(item.id)
                          }}
                        >
                          불러오기
                        </Button>
                        <Button
                          type="button"
                          size="sm"
                          variant="outline"
                          className="h-7 text-[11px]"
                          disabled={isActionDisabled}
                          onClick={(event) => {
                            event.stopPropagation()
                            void handleMoveToQueryById(item.id)
                          }}
                        >
                          채팅
                        </Button>
                        {item.linkedCohortId ? (
                          <Button
                            type="button"
                            size="sm"
                            className="h-7 text-[11px]"
                            variant="default"
                            disabled={isActionDisabled}
                            onClick={(event) => {
                              event.stopPropagation()
                              void handleLinkedCohortById(item.id, item.linkedCohortId!)
                            }}
                          >
                            코호트 열기
                          </Button>
                        ) : null}
                      </div>
                    </div>
                  </div>
                )
              })}

              <div className="flex items-center justify-between pt-1">
                <div className="text-xs text-muted-foreground">
                  {total.toLocaleString()}건 중 {items.length.toLocaleString()}건 표시
                </div>
                <div className="flex items-center gap-1.5">
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    className="h-7 text-xs"
                    onClick={() => setPage((prev) => Math.max(1, prev - 1))}
                    disabled={page <= 1}
                  >
                    이전
                  </Button>
                  <span className="text-xs text-muted-foreground min-w-[72px] text-center">
                    {page} / {totalPages}
                  </span>
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    className="h-7 text-xs"
                    onClick={() => setPage((prev) => Math.min(totalPages, prev + 1))}
                    disabled={page >= totalPages}
                  >
                    다음
                  </Button>
                </div>
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      <PdfCohortHistoryDrawer
        open={drawerOpen}
        onOpenChange={setDrawerOpen}
        detail={selectedDetail}
        loading={detailLoading}
        error={detailError}
        onRetry={() => {
          if (!selectedId) return
          void ensureDetail(selectedId)
        }}
        onApply={onApplyHistory}
        onMoveToQuery={onMoveToQuery}
        onOpenLinkedCohort={onOpenLinkedCohort}
      />
    </>
  )
}
