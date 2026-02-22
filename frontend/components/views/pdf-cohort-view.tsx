"use client"

import { useEffect, useRef, useState } from "react"
import { Card, CardContent } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Progress } from "@/components/ui/progress"
import { Input } from "@/components/ui/input"
import { Textarea } from "@/components/ui/textarea"
import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
} from "@/components/ui/dialog"
import { Upload, FileText, Sparkles, ArrowLeft, ChevronDown } from "lucide-react"
import PdfResultPanel from "./pdf-result-panel"
import { useAuth } from "@/components/auth-provider"
import { CohortLibraryDialog } from "@/components/cohort-library-dialog"
import { PdfCohortHistoryPanel } from "@/components/pdf-cohort-history-panel"
import {
    type ActiveCohortContext,
    type SavedCohort,
    persistPendingActiveCohortContext,
    toActiveCohortContext,
    toSavedCohort,
} from "@/lib/cohort-library"
import type { PdfCohortHistoryDetail } from "@/lib/pdf-cohort-history"

const ACTIVE_PDF_TASK_KEY = "pdf-cohort-active-task"

type ActivePdfTask = {
    taskId: string
}

type PdfCohortViewProps = {
    onPinnedChange?: (pinned: boolean) => void
}

export function PdfCohortView({ onPinnedChange }: PdfCohortViewProps) {
    const { user } = useAuth()
    const [isLoading, setIsLoading] = useState(false)
    const [progressValue, setProgressValue] = useState(0)
    const [error, setError] = useState<string | null>(null)
    const [message, setMessage] = useState<string | null>(null)

    // PDF 분석 결과 상태
    const [pdfResult, setPdfResult] = useState<any>(null)
    const fileInputRef = useRef<HTMLInputElement>(null)
    const pollTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
    const mountedRef = useRef(true)
    const pdfUser = (user?.id || user?.username || user?.name || "").trim()
    const [isSaveDialogOpen, setIsSaveDialogOpen] = useState(false)
    const [saveName, setSaveName] = useState("")
    const [saveDescription, setSaveDescription] = useState("")
    const [isSavingCohort, setIsSavingCohort] = useState(false)
    const [isLibraryOpen, setIsLibraryOpen] = useState(false)
    const [libraryItems, setLibraryItems] = useState<SavedCohort[]>([])
    const [isLibraryLoading, setIsLibraryLoading] = useState(false)
    const [historyRefreshTick, setHistoryRefreshTick] = useState(0)
    const MAX_COHORT_NAME_LENGTH = 120

    const clearPollTimer = () => {
        if (pollTimerRef.current) {
            clearTimeout(pollTimerRef.current)
            pollTimerRef.current = null
        }
    }

    const toNumber = (value: any, fallback = 0) => {
        const n = Number(value)
        return Number.isFinite(n) ? n : fallback
    }

    const apiPathWithUser = (path: string) => {
        if (!pdfUser) return path
        const separator = path.includes("?") ? "&" : "?"
        return `${path}${separator}user=${encodeURIComponent(pdfUser)}`
    }

    const bumpHistoryRefresh = () => {
        setHistoryRefreshTick((prev) => prev + 1)
    }

    const normalizeCohortSaveName = (value: string) => {
        const compact = String(value || "").replace(/\s+/g, " ").trim()
        return compact.slice(0, MAX_COHORT_NAME_LENGTH).trim()
    }

    const normalizeLibraryItems = (rawItems: unknown[]): SavedCohort[] =>
        rawItems
            .map((item) => toSavedCohort(item))
            .filter((item): item is SavedCohort => item !== null)

    const loadLibraryItems = async () => {
        setIsLibraryLoading(true)
        try {
            const res = await fetch(apiPathWithUser("/cohort/library?limit=200"))
            if (!res.ok) {
                setLibraryItems([])
                return
            }
            const payload = await res.json()
            const items = Array.isArray(payload?.items)
                ? payload.items
                : Array.isArray(payload?.cohorts)
                    ? payload.cohorts
                    : []
            setLibraryItems(normalizeLibraryItems(items))
        } catch {
            setLibraryItems([])
        } finally {
            setIsLibraryLoading(false)
        }
    }

    const buildPdfActiveContext = (result: any): ActiveCohortContext | null => {
        const hash = String(result?.pdf_hash || "").trim()
        const cd = result?.cohort_definition || {}
        const cohortSql = String(result?.generated_sql?.cohort_sql || result?.cohort_sql || "").trim()
        if (!cohortSql) return null
        const inclusionExclusion = Array.isArray(cd?.inclusion_exclusion)
            ? cd.inclusion_exclusion
                .map((item: any, idx: number) => {
                    const title = String(item?.title || item?.name || `조건 ${idx + 1}`).trim()
                    const operationalDefinition = String(
                        item?.operational_definition ||
                        item?.definition ||
                        item?.description ||
                        item?.criteria ||
                        ""
                    ).trim()
                    if (!operationalDefinition) return null
                    return {
                        id: String(item?.id || `ie-${idx + 1}`).trim(),
                        title,
                        operationalDefinition,
                        evidence: String(item?.evidence || "").trim() || undefined,
                    }
                })
                .filter((item: unknown): item is {
                    id: string
                    title: string
                    operationalDefinition: string
                    evidence?: string
                } => item !== null)
            : []
        const variableNames = Array.isArray(cd?.variables)
            ? cd.variables
                .map((item: any) =>
                    String(
                        item?.label ||
                        item?.name ||
                        item?.alias ||
                        item?.column_name ||
                        item?.description ||
                        ""
                    ).trim()
                )
                .filter((name: string) => Boolean(name))
                .slice(0, 24)
            : []
        const dbResult = result?.db_result || result?.cohort_result || {}
        const rawCount =
            dbResult?.total_count ??
            dbResult?.row_count ??
            result?.count_result?.patient_count
        const patientCount = Number(rawCount)
        return {
            cohortId: hash || `pdf-${Date.now()}`,
            cohortName: String(result?.filename || "PDF 기반 코호트").trim() || "PDF 기반 코호트",
            type: "PDF_DERIVED",
            cohortSql,
            patientCount: Number.isFinite(patientCount) ? patientCount : null,
            sqlFilterSummary: String(cd?.criteria_summary_ko || "").trim(),
            summaryKo: String(cd?.summary_ko || result?.summary_ko || "").trim(),
            criteriaSummaryKo: String(cd?.criteria_summary_ko || "").trim(),
            variables: variableNames,
            badgeLabel: "PDF 코호트",
            source: "pdf-cohort-confirm",
            ts: Date.now(),
            filename: String(result?.filename || "").trim(),
            pdfHash: hash,
            paperSummary: String(cd?.summary_ko || result?.summary_ko || "").trim() || undefined,
            inclusionExclusion: inclusionExclusion.length ? inclusionExclusion : undefined,
        }
    }

    const saveActiveTask = (taskId: string) => {
        if (typeof window === "undefined") return
        const payload: ActivePdfTask = { taskId }
        window.localStorage.setItem(ACTIVE_PDF_TASK_KEY, JSON.stringify(payload))
    }

    const clearActiveTask = () => {
        if (typeof window === "undefined") return
        window.localStorage.removeItem(ACTIVE_PDF_TASK_KEY)
    }

    const buildStatusUrl = (taskId: string) => {
        const statusQuery = new URLSearchParams()
        if (pdfUser) statusQuery.set("user", pdfUser)
        return statusQuery.toString()
            ? `/pdf/status/${taskId}?${statusQuery.toString()}`
            : `/pdf/status/${taskId}`
    }

    const pollTaskStatus = async (taskId: string) => {
        try {
            const statusRes = await fetch(buildStatusUrl(taskId))
            if (!statusRes.ok) {
                // 서버 재시작 후 메모리 기반 task 저장소가 초기화되면 404가 발생할 수 있음.
                // 이 경우 무한 재시도하지 말고 사용자에게 재업로드를 안내한다.
                if (statusRes.status === 404) {
                    clearPollTimer()
                    clearActiveTask()
                    setIsLoading(false)
                    setProgressValue(0)
                    setMessage("이전 분석 작업이 만료되었습니다. PDF를 다시 업로드해주세요.")
                    setError(null)
                    return
                }
                throw new Error("분석 상태 확인 실패")
            }

            const statusData = await statusRes.json()
            console.log("Task Status:", statusData)
            if (!mountedRef.current) return

            const terminalStatuses = new Set([
                "completed",
                "completed_with_ambiguities",
                "validation_failed",
                "needs_user_input",
            ])

            if (terminalStatuses.has(String(statusData.status || ""))) {
                clearPollTimer()
                clearActiveTask()
                setProgressValue(100)
                const performance = {
                    analysis_duration_ms: toNumber(statusData.analysis_duration_ms),
                    analysis_duration_sec: toNumber(statusData.analysis_duration_sec),
                    total_elapsed_ms: toNumber(statusData.total_elapsed_ms),
                    total_elapsed_sec: toNumber(statusData.total_elapsed_sec),
                    queue_wait_ms: toNumber(statusData.queue_wait_ms),
                    queue_wait_sec: toNumber(statusData.queue_wait_sec),
                    started_at: statusData.started_at || "",
                    completed_at: statusData.completed_at || "",
                    submitted_at: statusData.submitted_at || "",
                }
                setPdfResult({
                    ...(statusData.result || {}),
                    filename: statusData.filename || statusData.result?.filename,
                    task_id: statusData.task_id || "",
                    performance,
                })
                if (statusData.status === "needs_user_input") {
                    setMessage(statusData.message || "모호성 해결이 필요합니다.")
                } else if (statusData.status === "validation_failed") {
                    setMessage(statusData.message || "검증 실패 (리포트를 확인하세요).")
                } else if (statusData.status === "completed_with_ambiguities") {
                    setMessage(statusData.message || "분석 완료 (모호성 포함).")
                } else {
                    setMessage("분석이 완료되었습니다.")
                }
                setIsLoading(false)
                return
            }

            if (statusData.status === "failed") {
                clearPollTimer()
                clearActiveTask()
                setError(statusData.error || "분석 실패")
                setIsLoading(false)
                return
            }

            setProgressValue((prev) => Math.min(95, Math.max(prev + 8, 35)))
            setMessage(statusData.message || "분석 중...")
            pollTimerRef.current = setTimeout(() => {
                void pollTaskStatus(taskId)
            }, 2000)
        } catch {
            if (!mountedRef.current) return
            setMessage("연결 재시도 중...")
            pollTimerRef.current = setTimeout(() => {
                void pollTaskStatus(taskId)
            }, 2000)
        }
    }

    useEffect(() => {
        mountedRef.current = true
        if (typeof window === "undefined") return () => {}

        const raw = window.localStorage.getItem(ACTIVE_PDF_TASK_KEY)
        if (!raw) {
            return () => {
                mountedRef.current = false
                clearPollTimer()
            }
        }

        try {
            const parsed = JSON.parse(raw) as ActivePdfTask
            if (parsed?.taskId) {
                setIsLoading(true)
                setProgressValue(35)
                setMessage("분석 상태를 다시 확인 중...")
                void pollTaskStatus(parsed.taskId)
            }
        } catch {
            window.localStorage.removeItem(ACTIVE_PDF_TASK_KEY)
        }

        return () => {
            mountedRef.current = false
            clearPollTimer()
        }
    }, [])

    const handlePdfUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
        const file = e.target.files?.[0]
        if (!file) return

        clearPollTimer()
        setIsLoading(true)
        setProgressValue(8)
        setError(null)
        setMessage(null)
        setPdfResult(null)

        const formData = new FormData()
        formData.append("file", file)

        try {
            const query = new URLSearchParams()
            if (pdfUser) query.set("user", pdfUser)
            const uploadUrl = query.toString() ? `/pdf/upload?${query.toString()}` : "/pdf/upload"

            const res = await fetch(uploadUrl, {
                method: "POST",
                body: formData,
            })

            if (!res.ok) {
                const detail = await res.text()
                throw new Error(detail || "PDF 업로드 실패")
            }

            const { task_id } = await res.json()
            saveActiveTask(task_id)
            bumpHistoryRefresh()
            setMessage("PDF 분석이 시작되었습니다.")
            setProgressValue(15)
            void pollTaskStatus(task_id)
        } catch (err) {
            clearActiveTask()
            setError(err instanceof Error ? err.message : "업로드 중 오류가 발생했습니다.")
            setIsLoading(false)
        } finally {
            if (fileInputRef.current) {
                fileInputRef.current.value = ""
            }
        }
    }

    const handleReset = () => {
        clearPollTimer()
        clearActiveTask()
        setPdfResult(null)
        setError(null)
        setMessage(null)
        setProgressValue(0)
        setIsLoading(false)
        setIsSaveDialogOpen(false)
        setIsLibraryOpen(false)
    }

    useEffect(() => {
        onPinnedChange?.(Boolean(pdfResult))
    }, [pdfResult, onPinnedChange])

    useEffect(() => {
        if (!pdfResult) return
        void loadLibraryItems()
    }, [pdfResult])

    const handleConfirmPdf = async () => {
        try {
            const hash = pdfResult?.pdf_hash
            if (!hash) {
                setError("저장할 수 있는 PDF 해시 정보(pdf_hash)가 존재하지 않습니다.")
                return
            }

            const res = await fetch("/cohort/pdf/confirm", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    user: pdfUser || null,
                    pdf_hash: hash,
                    data: pdfResult,
                    status: "confirmed"
                })
            })

            if (!res.ok) {
                const errData = await res.json()
                throw new Error(errData.detail || "서버 저장 실패")
            }
            bumpHistoryRefresh()

            const context = buildPdfActiveContext(pdfResult)
            if (!context) {
                setError("코호트 SQL 정보가 없어 쿼리 화면에 컨텍스트를 전달할 수 없습니다.")
                return
            }
            persistPendingActiveCohortContext(context, pdfUser)
            if (typeof window !== "undefined") {
                window.dispatchEvent(new Event("ql-open-query-view"))
            }

            setMessage("코호트가 확정되었습니다. text-to-llm 새 채팅으로 이동해 질의를 이어서 시작할 수 있습니다.")
        } catch (err: any) {
            setError(`확정 저장 중 오류 발생: ${err.message}`)
        }
    }

    const handleOpenSaveDialog = () => {
        const defaultName =
            normalizeCohortSaveName(String(pdfResult?.filename || "PDF 기반 코호트")) || "PDF 기반 코호트"
        setSaveName(defaultName)
        setSaveDescription("")
        setIsSaveDialogOpen(true)
    }

    const savePdfCohortFromResult = async (
        sourceResult: any,
        rawName: string,
        rawDescription: string,
        queryAfterSave: boolean
    ) => {
        const cohortSql = String(sourceResult?.generated_sql?.cohort_sql || sourceResult?.cohort_sql || "").trim()
        if (!cohortSql) {
            throw new Error("저장할 코호트 SQL이 없습니다.")
        }
        const name = normalizeCohortSaveName(rawName)
        if (!name) {
            throw new Error("저장 이름을 입력하세요.")
        }
        const description = String(rawDescription || "").trim()

        const cd = sourceResult?.cohort_definition || {}
        const extractionDetails = cd?.extraction_details || {}
        const populationCriteriaFromExtraction = Array.isArray(extractionDetails?.cohort_criteria?.population)
            ? extractionDetails.cohort_criteria.population
            : []
        const populationCriteriaFromLegacy = Array.isArray(cd?.inclusion_exclusion) ? cd.inclusion_exclusion : []
        const populationCriteria =
            populationCriteriaFromExtraction.length > 0
                ? populationCriteriaFromExtraction
                : populationCriteriaFromLegacy
        const inclusionExclusion = populationCriteria
            .map((item: any, idx: number) => {
                if (typeof item === "string") {
                    const text = String(item).trim()
                    if (!text) return null
                    return {
                        id: `ie-${idx + 1}`,
                        title: `조건 ${idx + 1}`,
                        operational_definition: text,
                    }
                }
                const title = String(item?.title || item?.label || item?.name || `조건 ${idx + 1}`).trim()
                const operationalDefinition = String(
                    item?.operational_definition ||
                    item?.operationalDefinition ||
                    item?.definition ||
                    item?.description ||
                    item?.criteria ||
                    ""
                ).trim()
                if (!operationalDefinition) return null
                return {
                    id: String(item?.id || `ie-${idx + 1}`).trim(),
                    title,
                    operational_definition: operationalDefinition,
                    evidence: String(item?.evidence || "").trim() || undefined,
                }
            })
            .filter(
                (item: unknown): item is {
                    id: string
                    title: string
                    operational_definition: string
                    evidence?: string
                } => item !== null
            )

        const variables = Array.isArray(cd?.variables)
            ? cd.variables
                .map((item: any) => {
                    const key = String(item?.key || item?.name || item?.alias || item?.column_name || "").trim()
                    const label = String(item?.label || item?.name || item?.description || key).trim()
                    if (!key && !label) return null
                    return {
                        key: key || label,
                        label: label || key,
                        table: String(item?.table || item?.table_name || "").trim() || undefined,
                        mapping_id: String(item?.mapping_id || item?.mappingId || "").trim() || undefined,
                    }
                })
                .filter(
                    (item: unknown): item is {
                        key: string
                        label: string
                        table?: string
                        mapping_id?: string
                    } => item !== null
                )
            : []

        const dbResult = sourceResult?.db_result || sourceResult?.cohort_result || {}
        const rawCount =
            dbResult?.total_count ??
            dbResult?.row_count ??
            sourceResult?.count_result?.patient_count
        const numericCount = Number(rawCount)
        const patientCount = Number.isFinite(numericCount) ? numericCount : undefined

        const res = await fetch("/cohort/library", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                user: pdfUser || null,
                type: "PDF_DERIVED",
                name,
                description: description || null,
                cohort_sql: cohortSql,
                count: patientCount,
                sql_filter_summary: String(cd?.criteria_summary_ko || "").trim() || null,
                human_summary: String(cd?.summary_ko || sourceResult?.summary_ko || "").trim() || null,
                source: {
                    created_from: "PDF_ANALYSIS_PAGE",
                    pdf_name: String(sourceResult?.filename || "").trim() || null,
                    pdf_analysis_id: String(sourceResult?.task_id || "").trim() || null,
                },
                pdf_details: {
                    paper_summary: String(cd?.summary_ko || sourceResult?.summary_ko || "").trim() || null,
                    inclusion_exclusion: inclusionExclusion,
                    variables,
                },
                status: "active",
            }),
        })
        if (!res.ok) {
            const detail = await res.text()
            throw new Error(detail || "코호트 저장 실패")
        }
        const payload = await res.json()
        const saved = toSavedCohort(payload)
        if (saved && queryAfterSave) {
            const context = toActiveCohortContext(saved, "pdf-library-save")
            persistPendingActiveCohortContext(context, pdfUser)
            if (typeof window !== "undefined") {
                window.dispatchEvent(new Event("ql-open-query-view"))
            }
        }
        bumpHistoryRefresh()
        await loadLibraryItems()
        return saved
    }

    const handleSavePdfCohort = async (queryAfterSave: boolean) => {
        if (!pdfResult) {
            setError("저장할 코호트 결과가 없습니다.")
            return
        }
        setIsSavingCohort(true)
        try {
            const saved = await savePdfCohortFromResult(pdfResult, saveName, saveDescription, queryAfterSave)
            if (saved) {
                setIsSaveDialogOpen(false)
                setMessage(
                    queryAfterSave
                        ? "코호트를 저장하고 쿼리 화면으로 이동했습니다."
                        : "코호트를 라이브러리에 저장했습니다."
                )
                setError(null)
            }
        } catch (err: any) {
            setError(`코호트 저장 중 오류 발생: ${String(err?.message || err)}`)
        } finally {
            setIsSavingCohort(false)
        }
    }

    const handleQuerySavedCohort = (item: SavedCohort) => {
        const context = toActiveCohortContext(item, "pdf-library")
        persistPendingActiveCohortContext(context, pdfUser)
        setIsLibraryOpen(false)
        if (typeof window !== "undefined") {
            window.dispatchEvent(new Event("ql-open-query-view"))
        }
    }

    const handleCopySQL = async (sql: string) => {
        try {
            await navigator.clipboard.writeText(sql)
            setMessage("SQL이 클립보드에 복사되었습니다.")
        } catch {
            setError("클립보드 복사에 실패했습니다.")
        }
    }

    const handleDownloadCSV = () => {
        // CSV 다운로드 로직 구현 (필요시)
        setMessage("CSV 다운로드 기능은 준비 중입니다.")
    }

    const resolveHistoryPdfResult = (detail: PdfCohortHistoryDetail) => {
        if (detail.rawData && typeof detail.rawData === "object") {
            return detail.rawData as Record<string, unknown>
        }
        return null
    }

    const handleApplyHistoryDetail = (detail: PdfCohortHistoryDetail) => {
        const rawResult = resolveHistoryPdfResult(detail)
        if (!rawResult) {
            setError("히스토리 원본 데이터가 없어 화면에 적용할 수 없습니다.")
            return
        }
        clearPollTimer()
        clearActiveTask()
        setIsLoading(false)
        setProgressValue(0)
        setError(null)
        setMessage("히스토리 산출물을 불러왔습니다.")
        setPdfResult(rawResult)
    }

    const moveToQueryWithContext = (context: ActiveCohortContext) => {
        persistPendingActiveCohortContext(context, pdfUser)
        if (typeof window !== "undefined") {
            window.dispatchEvent(new Event("ql-open-query-view"))
        }
    }

    const handleMoveHistoryToQuery = (detail: PdfCohortHistoryDetail) => {
        const rawResult = resolveHistoryPdfResult(detail)
        if (!rawResult) {
            setError("히스토리 원본 데이터가 없어 쿼리 화면으로 이동할 수 없습니다.")
            return
        }
        const context = buildPdfActiveContext(rawResult)
        if (!context) {
            setError("코호트 SQL 정보가 없어 쿼리 화면에 컨텍스트를 전달할 수 없습니다.")
            return
        }
        setMessage("PDF 코호트 컨텍스트를 적용해 쿼리 화면으로 이동합니다.")
        setError(null)
        moveToQueryWithContext(context)
    }

    const handleOpenLinkedCohortFromHistory = async (cohortId: string, detail: PdfCohortHistoryDetail) => {
        const targetId = String(cohortId || "").trim()
        if (!targetId) {
            setError("연결된 코호트 ID가 없습니다.")
            return
        }
        try {
            const res = await fetch(apiPathWithUser(`/cohort/library/${encodeURIComponent(targetId)}`))
            if (!res.ok) {
                throw new Error("저장된 코호트를 찾지 못했습니다.")
            }
            const payload = await res.json()
            const saved = toSavedCohort(payload)
            if (!saved) {
                throw new Error("코호트 데이터 형식이 올바르지 않습니다.")
            }
            const context = toActiveCohortContext(saved, "pdf-history-linked-cohort")
            setError(null)
            setMessage("저장된 코호트를 불러와 쿼리 화면으로 이동합니다.")
            moveToQueryWithContext(context)
        } catch (err: any) {
            const fallback = resolveHistoryPdfResult(detail)
            if (fallback) {
                const context = buildPdfActiveContext(fallback)
                if (context) {
                    setError(null)
                    setMessage("연결 코호트를 찾지 못해 PDF 산출물 컨텍스트로 이동합니다.")
                    moveToQueryWithContext(context)
                    return
                }
            }
            setError(String(err?.message || err || "연결된 코호트를 여는 중 오류가 발생했습니다."))
        }
    }

    const renderHistoryDropdown = () => (
        <div className="max-w-2xl w-full mx-auto">
            <details className="group relative rounded-xl border border-border/70 bg-card/40">
                <summary className="flex list-none cursor-pointer items-center justify-between gap-3 px-4 py-3 [&::-webkit-details-marker]:hidden">
                    <div className="min-w-0">
                        <div className="text-sm font-semibold">PDFLLM 산출물 히스토리</div>
                        <p className="text-xs text-muted-foreground">
                            이전 분석 결과를 펼쳐서 바로 상세 확인/불러오기 할 수 있습니다.
                        </p>
                    </div>
                    <ChevronDown className="w-4 h-4 text-muted-foreground transition-transform duration-200 group-open:rotate-180" />
                </summary>
                <div className="pointer-events-none absolute left-0 right-0 top-[calc(100%+8px)] z-40 hidden group-open:block">
                    <div className="pointer-events-auto max-h-[72vh] overflow-y-auto rounded-xl border border-border/70 bg-background/95 shadow-2xl backdrop-blur">
                        <PdfCohortHistoryPanel
                            className="border-0 shadow-none bg-transparent"
                            userId={pdfUser}
                            refreshToken={historyRefreshTick}
                            onApplyHistory={handleApplyHistoryDetail}
                            onMoveToQuery={handleMoveHistoryToQuery}
                            onOpenLinkedCohort={handleOpenLinkedCohortFromHistory}
                        />
                    </div>
                </div>
            </details>
        </div>
    )

    // 분석 결과가 있으면 PdfResultPanel 표시
    if (pdfResult) {
        return (
            <div className="p-4 sm:p-6 space-y-4 sm:space-y-6 h-full flex flex-col overflow-y-auto">
                <div className="flex justify-end">
                    <Button variant="outline" onClick={handleReset} className="gap-2">
                        <ArrowLeft className="w-4 h-4" />
                        다른 PDF 분석하기
                    </Button>
                </div>

                <div className="flex-1 min-h-0 overflow-y-auto border rounded-lg bg-background">
                    <PdfResultPanel
                        pdfResult={pdfResult}
                        charts={[]} // 차트 데이터가 있다면 pdfResult에서 가공해서 전달
                        onSave={handleConfirmPdf}
                        onSaveToLibrary={handleOpenSaveDialog}
                        onOpenLibrary={() => {
                            void loadLibraryItems()
                            setIsLibraryOpen(true)
                        }}
                        isSavingCohort={isSavingCohort}
                        onCopySQL={handleCopySQL}
                        onDownloadCSV={handleDownloadCSV}
                        setMessage={setMessage}
                        setError={setError}
                        setPdfResult={setPdfResult}
                    />
                </div>

                <Dialog open={isSaveDialogOpen} onOpenChange={setIsSaveDialogOpen}>
                    <DialogContent className="sm:max-w-lg">
                        <DialogHeader>
                            <DialogTitle>PDF 코호트 저장</DialogTitle>
                            <DialogDescription>
                                저장 이름을 입력하면 코호트 라이브러리에 추가됩니다.
                            </DialogDescription>
                        </DialogHeader>
                        <div className="space-y-3">
                            <div className="space-y-1">
                                <label className="text-xs font-medium text-foreground">저장 이름</label>
                                <Input
                                    value={saveName}
                                    onChange={(e) => setSaveName(e.target.value)}
                                    placeholder="예: 외상성 뇌손상 코호트"
                                    maxLength={MAX_COHORT_NAME_LENGTH}
                                />
                                <p className="text-[11px] text-muted-foreground text-right">
                                    {saveName.length}/{MAX_COHORT_NAME_LENGTH}
                                </p>
                            </div>
                            <div className="space-y-1">
                                <label className="text-xs font-medium text-foreground">설명 (선택)</label>
                                <Textarea
                                    value={saveDescription}
                                    onChange={(e) => setSaveDescription(e.target.value)}
                                    placeholder="코호트 용도/메모를 입력하세요."
                                    className="min-h-[80px]"
                                />
                            </div>
                        </div>
                        <DialogFooter>
                            <Button
                                variant="outline"
                                onClick={() => setIsSaveDialogOpen(false)}
                                disabled={isSavingCohort}
                            >
                                취소
                            </Button>
                            <Button
                                variant="secondary"
                                onClick={() => {
                                    void handleSavePdfCohort(false)
                                }}
                                disabled={isSavingCohort || !saveName.trim()}
                            >
                                {isSavingCohort ? "저장 중..." : "저장"}
                            </Button>
                            <Button
                                onClick={() => {
                                    void handleSavePdfCohort(true)
                                }}
                                disabled={isSavingCohort || !saveName.trim()}
                            >
                                {isSavingCohort ? "처리 중..." : "저장 후 쿼리하기"}
                            </Button>
                        </DialogFooter>
                    </DialogContent>
                </Dialog>

                <CohortLibraryDialog
                    open={isLibraryOpen}
                    onOpenChange={setIsLibraryOpen}
                    cohorts={libraryItems}
                    loading={isLibraryLoading}
                    onRefresh={() => {
                        void loadLibraryItems()
                    }}
                    onSelectForQuery={handleQuerySavedCohort}
                />
            </div>
        )
    }

    // 초기 화면: 업로드
    return (
        <div className="p-4 sm:p-6 space-y-4 sm:space-y-6 h-full overflow-y-auto">
            {renderHistoryDropdown()}

            <div className="max-w-2xl w-full mx-auto space-y-6">
                <Card className="border-2 border-dashed border-muted-foreground/25 hover:border-primary/50 transition-colors bg-muted/50">
                    <CardContent className="flex flex-col items-center justify-center py-8 sm:py-9 space-y-3 text-center">
                        <div className="p-3 rounded-full bg-background border shadow-sm">
                            <FileText className="w-9 h-9 text-muted-foreground" />
                        </div>

                        <div className="space-y-1">
                            <h3 className="font-semibold text-lg">논문 PDF 업로드</h3>
                            <p className="text-sm text-muted-foreground">
                                클릭하거나 파일을 드래그하여 업로드하세요 (최대 10MB)
                            </p>
                        </div>

                        {error && (
                            <div className="text-sm text-destructive font-medium bg-destructive/10 px-3 py-1 rounded-md">
                                {error}
                            </div>
                        )}

                        {isLoading && !error && (
                            <div className="w-full max-w-md px-2 space-y-2">
                                <Progress value={progressValue} className="h-3" />
                                <div className="text-sm text-primary font-medium">
                                    {message || "분석 중..."}
                                </div>
                            </div>
                        )}

                        {!isLoading && message && !error && (
                            <div className="text-sm text-primary font-medium bg-primary/10 px-3 py-1 rounded-md">
                                {message}
                            </div>
                        )}

                        <div className="relative mt-2">
                            <input
                                type="file"
                                accept=".pdf"
                                onChange={handlePdfUpload}
                                disabled={isLoading}
                                ref={fileInputRef}
                                className="absolute inset-0 w-full h-full opacity-0 cursor-pointer disabled:cursor-not-allowed"
                            />
                            {!isLoading && (
                                <Button className="gap-2 pointer-events-none">
                                    <Upload className="w-4 h-4" />
                                    PDF 파일 선택
                                </Button>
                            )}
                        </div>

                        <div className="flex items-center gap-2 text-xs text-muted-foreground mt-2 bg-background/50 px-3 py-1 rounded-full border">
                            <Sparkles className="w-3 h-3 text-amber-500" />
                            <span>AI가 Inclusion/Exclusion Criteria를 자동 분석합니다</span>
                        </div>
                    </CardContent>
                </Card>

                <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
                    <div className="p-4 rounded-lg border bg-card text-center space-y-2">
                        <Badge variant="outline" className="mb-1">Step 1</Badge>
                        <h4 className="font-medium">PDF 텍스트 추출</h4>
                        <p className="text-xs text-muted-foreground">논문의 Methods 섹션을 분석하여 코호트 정의를 파악합니다.</p>
                    </div>
                    <div className="p-4 rounded-lg border bg-card text-center space-y-2">
                        <Badge variant="outline" className="mb-1">Step 2</Badge>
                        <h4 className="font-medium">임상 변수 매핑</h4>
                        <p className="text-xs text-muted-foreground">추출된 변수를 MIMIC-IV 스키마 코드와 자동으로 매핑합니다.</p>
                    </div>
                    <div className="p-4 rounded-lg border bg-card text-center space-y-2">
                        <Badge variant="outline" className="mb-1">Step 3</Badge>
                        <h4 className="font-medium">SQL 쿼리 생성</h4>
                        <p className="text-xs text-muted-foreground">실행 가능한 Oracle SQL을 생성하여 코호트를 추출합니다.</p>
                    </div>
                </div>
            </div>
        </div>
    )
}
