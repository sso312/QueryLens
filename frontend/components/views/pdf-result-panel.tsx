"use client"

import React from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import {
    Users,
    FileText,
    FileCode,
    Code,
    Copy,
    Download,
    AlertTriangle,
    ChevronRight,
    Filter,
    Table as TableIcon,
    BarChart2,
    Activity,
    Target,
    Stethoscope,
    CheckCircle2,
    BookOpen,
    Heart,
    ClipboardList,
    Sparkles,
    Database,
    Search,
    Loader2,
    Bookmark,
} from "lucide-react"
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table"
import { cn } from "@/lib/utils"

const PDF_PANEL_BG = "bg-[#eceff3]"
const PDF_PANEL_BG_SOFT = "bg-[#f3f5f8]"
const PDF_PANEL_BORDER = "border-[#d3dae3]"

/* ── tiny bar chart (copied for self-containment) ── */
function MiniBarChart({ data, title, color = "bg-primary" }: { data: { label: string; value: number }[]; title: string; color?: string }) {
    const max = Math.max(...data.map((d) => d.value)) || 1
    return (
        <div className={cn("rounded-lg border p-4", PDF_PANEL_BG, PDF_PANEL_BORDER)}>
            <div className="text-xs font-semibold mb-3 flex items-center justify-between">
                <span>{title} 분포</span>
                <span className="text-muted-foreground font-normal">{data.length}개</span>
            </div>
            <div className="flex items-end gap-1 h-16">
                {data.map((d, i) => (
                    <div key={i} className="flex-1 flex flex-col items-center gap-1 h-full">
                        <div className="flex-1 w-full flex items-end">
                            <div
                                className={cn(color, "w-full rounded-t-sm transition-all hover:opacity-80 min-h-[2px]")}
                                style={{ height: `${(d.value / max) * 100}%` }}
                                title={`${d.label}: ${d.value}`}
                            />
                        </div>
                        <span className="text-[8px] text-muted-foreground truncate w-full text-center shrink-0">{d.label}</span>
                    </div>
                ))}
            </div>
        </div>
    )
}

/* ── props ── */
interface PdfResultPanelProps {
    pdfResult: any
    charts: { title: string; data: { label: string; value: number }[] }[]
    onSave: () => void
    onSaveToLibrary?: () => void
    onOpenLibrary?: () => void
    isSavingCohort?: boolean
    onCopySQL: (sql: string) => Promise<void>
    onDownloadCSV: () => void
    setMessage: (msg: string) => void
    setError: (msg: string) => void
    setPdfResult: (result: any) => void
}

/* ── 카테고리 분류 helpers ── */
const categorize = (f: any): string => {
    const n = ((f.name || "") + " " + (f.description || "")).toLowerCase()
    const t = (f.table_name || "").toLowerCase()
    if (/age|gender|sex|race|ethnic|height|weight|bmi|los|length.of.stay|admission|patient/i.test(n)) return "인구통계"
    if (/heart.rate|hr|blood.pressure|sbp|dbp|map|spo2|temp|respiratory|rr|gcs|vital/i.test(n) || t === "chartevents") return "활력징후"
    if (/lab|creatinine|bun|potassium|sodium|glucose|hemoglobin|wbc|platelet|inr|lactate|troponin|albumin|bilirubin|bnp/i.test(n) || t === "labevents") return "실험실 검사"
    if (/icd|diagnosis|comorbid|diabetes|hypertension|copd|ckd|liver|cancer|chf|mi|stroke|sepsis|elixhauser|charlson/i.test(n) || t.includes("diagnoses")) return "동반질환"
    if (/drug|medication|vasopressor|ventilat|intubat|dialysis|rrt|procedure|treatment/i.test(n)) return "치료/처치"
    return "기타"
}

const catIcons: Record<string, React.ReactNode> = {
    "인구통계": <Users className="w-3.5 h-3.5" />,
    "활력징후": <Heart className="w-3.5 h-3.5" />,
    "실험실 검사": <Stethoscope className="w-3.5 h-3.5" />,
    "동반질환": <AlertTriangle className="w-3.5 h-3.5" />,
    "치료/처치": <Activity className="w-3.5 h-3.5" />,
    "기타": <Filter className="w-3.5 h-3.5" />,
}

const catColors: Record<string, string> = {
    "인구통계": "bg-blue-500/10 text-blue-600 border-blue-500/20",
    "활력징후": "bg-rose-500/10 text-rose-600 border-rose-500/20",
    "실험실 검사": "bg-purple-500/10 text-purple-600 border-purple-500/20",
    "동반질환": "bg-amber-500/10 text-amber-600 border-amber-500/20",
    "치료/처치": "bg-emerald-500/10 text-emerald-600 border-emerald-500/20",
    "기타": "bg-gray-500/10 text-gray-600 border-gray-500/20",
}

/* ══ Methods Summary Renderer ══ */
function MethodsSummaryRenderer({ summary }: { summary: any }) {
    if (!summary) return (
        <div className="flex flex-col items-center py-4 text-muted-foreground italic">
            <AlertTriangle className="w-5 h-5 mb-2 opacity-50" />
            추출된 방법론 요약 정보가 없습니다.
        </div>
    )

    // Handle legacy string format
    if (typeof summary === "string") {
        return (
            <div className={cn("p-5 rounded-xl border leading-relaxed text-sm text-foreground/90 whitespace-pre-wrap", PDF_PANEL_BG, PDF_PANEL_BORDER)}>
                {summary}
            </div>
        )
    }

    const structuredSummary = summary?.structured_summary

    if (!structuredSummary || Object.keys(structuredSummary).length === 0) {
        return (
            <div className={cn("flex flex-col items-center py-6 text-muted-foreground border rounded-xl", PDF_PANEL_BG_SOFT, PDF_PANEL_BORDER)}>
                <AlertTriangle className="w-6 h-6 mb-2 opacity-50" />
                <div className="text-sm font-medium">분석된 세부 방법론 정보가 부족합니다.</div>
                <div className="text-xs opacity-70 mt-1">논문 텍스트에서 연구 설계를 충분히 추출하지 못했습니다.</div>
            </div>
        )
    }

    return (
        <div className="space-y-4">
            {/* Structured Summary Sections */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                {Object.entries(structuredSummary).map(([key, value]) => (
                    <div key={key} className={cn("p-4 rounded-xl border space-y-2 hover:border-amber-500/30 transition-colors", PDF_PANEL_BG_SOFT, PDF_PANEL_BORDER)}>
                        <div className="text-[10px] font-bold text-amber-600 uppercase tracking-tighter opacity-80 flex items-center gap-1.5">
                            <ChevronRight className="w-3 h-3" />
                            {key.replace(/_/g, " ")}
                        </div>
                        <div className="text-xs leading-relaxed text-foreground/80 whitespace-pre-wrap shrink-0">
                            {value as string || "Unknown"}
                        </div>
                    </div>
                ))}
            </div>
        </div>
    )
}

/* ══════════════════════════════════════════════ */
export default function PdfResultPanel({
    pdfResult,
    charts,
    onSave,
    onSaveToLibrary,
    onOpenLibrary,
    isSavingCohort = false,
    onCopySQL,
    onDownloadCSV,
    setMessage,
    setError,
    setPdfResult,
}: PdfResultPanelProps) {
    if (pdfResult) {
        console.log("PdfResultPanel Full Data:", pdfResult);
        const debugVariables = pdfResult.cohort_definition?.variables || [];
        console.log("Extracted Variables from Result:", debugVariables);
    }
    if (!pdfResult) return null


    const [activeSqlTab, setActiveSqlTab] = React.useState("cohort")

    const cd = pdfResult.cohort_definition || {}
    const summaryKo = cd.summary_ko || pdfResult.summary_ko || ""
    const criteriaSummaryKo = cd.criteria_summary_ko || ""
    const extractionDetails = cd.extraction_details || {}
    const methodsSummary = cd.methods_summary || pdfResult.methods_summary || ""
    const baseline = cd.baseline_characteristics || {}
    const features: any[] = Array.isArray(pdfResult.features) ? pdfResult.features : []
    const variables: any[] = Array.isArray(cd.variables) ? cd.variables : []
    const cohortCriteria = extractionDetails?.cohort_criteria || {}
    const populationCriteria: any[] = Array.isArray(cohortCriteria.population) ? cohortCriteria.population : []

    // v24 대응: db_result 또는 cohort_result 사용
    const cr = pdfResult.db_result || pdfResult.cohort_result || {}
    const sqls = pdfResult.generated_sql || {}
    const cohortSql = sqls.cohort_sql || pdfResult.cohort_sql || ""
    const columns: string[] = Array.isArray(cr.columns) ? cr.columns.map((col: any) => String(col ?? "")) : []
    const rows: any[] = Array.isArray(cr.rows) ? cr.rows : []

    // 에러 핸들링: 최신 db_result.error를 우선하고, 전체 레벨 error나 구형 필드도 확인
    const resultError = cr.error || pdfResult.error || ""

    // 환자 수 계산: step_counts -> count_result -> 결과 행 내 TOTAL_COUNT 류 컬럼 -> row_count 순
    const toNumber = (value: any): number | null => {
        if (typeof value === "number" && Number.isFinite(value)) return value
        if (typeof value === "string") {
            const parsed = Number(value.replace(/,/g, "").trim())
            if (Number.isFinite(parsed)) return parsed
        }
        return null
    }
    const rawStepCounts: any[] = Array.isArray(cr.step_counts) ? cr.step_counts : []
    const stepCounts = rawStepCounts
        .map((step: any, idx: number) => {
            if (Array.isArray(step)) {
                const count = toNumber(step[1])
                if (count == null) return null
                return { name: String(step[0] ?? `step_${idx + 1}`), count }
            }
            if (step && typeof step === "object") {
                const obj = step as Record<string, unknown>
                const count = toNumber(obj.cnt ?? obj.count ?? obj.value ?? obj.n ?? obj.rows)
                if (count == null) return null
                return { name: String(obj.step_name ?? obj.name ?? obj.step ?? `step_${idx + 1}`), count }
            }
            return null
        })
        .filter((step): step is { name: string; count: number } => step !== null)

    let patientCount = 0
    if (stepCounts.length > 0) {
        patientCount = stepCounts[stepCounts.length - 1].count
    } else {
        const countFromResult = toNumber(pdfResult.count_result?.patient_count)
        if (countFromResult != null) {
            patientCount = countFromResult
        } else {
            const upperColumns = columns.map((c) => c.toUpperCase())
            const firstRow = rows.length > 0 && Array.isArray(rows[0]) ? rows[0] : null
            const preferredCountColumns = ["TOTAL_COUNT", "PATIENT_COUNT", "COHORT_SIZE", "N_PATIENTS", "N"]

            if (firstRow) {
                let extracted: number | null = null
                for (const col of preferredCountColumns) {
                    const idx = upperColumns.indexOf(col)
                    if (idx >= 0) {
                        extracted = toNumber(firstRow[idx])
                        if (extracted != null) break
                    }
                }
                if (extracted == null) {
                    const genericIdx = upperColumns.findIndex((col: string) => col.endsWith("_COUNT") || col === "COUNT" || col === "CNT")
                    if (genericIdx >= 0) extracted = toNumber(firstRow[genericIdx])
                }
                patientCount = extracted ?? toNumber(cr.row_count) ?? 0
            } else {
                patientCount = toNumber(cr.row_count) ?? 0
            }
        }
    }

    const perf = (pdfResult.performance || {}) as any
    const analysisDurationSec = toNumber(perf.analysis_duration_sec) ?? 0
    const pdfPageCount = toNumber(pdfResult.pdf_page_count)
    const hasPdfPageCount = pdfPageCount != null && pdfPageCount > 0
    const secPerPage = hasPdfPageCount && analysisDurationSec > 0 ? analysisDurationSec / pdfPageCount : null
    const pagesPerSec = hasPdfPageCount && analysisDurationSec > 0 ? pdfPageCount / analysisDurationSec : null

    const fmtMaybe = (value: number | null, digits = 3) => (value == null ? "-" : value.toFixed(digits))

    /* 카테고리별 그룹 */
    const grouped: Record<string, any[]> = {}
    features.forEach((f: any) => {
        const c = categorize(f)
        if (!grouped[c]) grouped[c] = []
        grouped[c].push(f)
    })



    return (
        <div className="p-6 pb-20 space-y-6 animate-in fade-in-0 slide-in-from-bottom-4 duration-500">

            {/* ══ 상단 배너 ══ */}
            <div className="relative overflow-hidden rounded-xl border border-primary/20 bg-gradient-to-r from-primary/5 via-primary/10 to-primary/5 p-5">
                <div className="absolute top-0 right-0 w-40 h-40 bg-primary/5 rounded-full -translate-y-1/2 translate-x-1/2 blur-2xl" />
                <div className="flex items-center justify-between relative z-10 flex-wrap gap-4">
                    <div className="flex items-center gap-4">
                        <div className="flex items-center justify-center w-12 h-12 rounded-xl bg-primary/10 border border-primary/20 shrink-0">
                            <FileText className="w-6 h-6 text-primary" />
                        </div>
                        <div>
                            <div className="flex items-center gap-2 mb-1 flex-wrap">
                                <h2 className="text-lg font-bold text-foreground">PDF 코호트 분석 완료</h2>
                                <Badge className="bg-emerald-500/15 text-emerald-600 border-emerald-500/30 text-[10px] gap-1 shrink-0">
                                    <CheckCircle2 className="w-3 h-3" /> 분석 완료
                                </Badge>
                            </div>
                            <div className="flex items-center gap-3 text-xs text-muted-foreground flex-wrap">
                                <span className="flex items-center gap-1"><FileCode className="w-3 h-3" />{pdfResult.filename || "논문 PDF"}</span>
                                {patientCount > 0 && <span className="flex items-center gap-1"><Users className="w-3 h-3" />대상 환자 {Number(patientCount).toLocaleString()}명</span>}
                                {features.length > 0 && <span className="flex items-center gap-1"><Activity className="w-3 h-3" />{features.length}개 변수</span>}
                            </div>
                            <div className="mt-1 flex items-center gap-2 text-[11px] text-muted-foreground/90 flex-wrap">
                                <span>pages {hasPdfPageCount ? pdfPageCount : "-"}</span>
                                <span>·</span>
                                <span>분석시간/페이지 {fmtMaybe(secPerPage)} sec</span>
                                <span>·</span>
                                <span>처리속도 {fmtMaybe(pagesPerSec)} pages/sec</span>
                            </div>
                        </div>
                    </div>
                    <div className="flex items-center gap-2 shrink-0">
                        <Button
                            size="sm"
                            variant="outline"
                            className="h-9 gap-2"
                            onClick={onOpenLibrary}
                        >
                            <Database className="w-4 h-4" /> 저장된 코호트
                        </Button>
                        <Button
                            size="sm"
                            variant="outline"
                            className="h-9 gap-2"
                            onClick={onSaveToLibrary}
                            disabled={isSavingCohort}
                        >
                            {isSavingCohort ? <Loader2 className="w-4 h-4 animate-spin" /> : <Bookmark className="w-4 h-4" />}
                            코호트 저장
                        </Button>
                        <Button size="sm" className="h-9 gap-2 bg-primary hover:bg-primary/90 text-primary-foreground shadow-sm" onClick={onSave}>
                            <Sparkles className="w-4 h-4" /> 코호트 확정
                        </Button>
                    </div>
                </div>
            </div>

            {/* ══ 섹션 1 · 논문 핵심 요약 ══ */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 items-stretch">
                {summaryKo && (
                    <Card className={cn("overflow-hidden border shadow-sm !py-0 !pt-0 !pb-0 !gap-0 h-full", PDF_PANEL_BG, PDF_PANEL_BORDER)}>
                        <CardHeader className="rounded-t-[inherit] border-b !flex !flex-row !items-center !gap-2.5 !px-7 !pt-4 !pb-4 bg-gradient-to-r from-blue-500/5 via-blue-500/10 to-blue-500/5">
                            <CardTitle className="text-sm font-bold leading-tight flex items-center gap-2.5">
                                <BookOpen className="w-4 h-4 text-blue-500" />
                                논문 핵심 요약
                            </CardTitle>
                        </CardHeader>
                        <CardContent className="!px-7 !pt-4 !pb-6">
                            <div className={cn("p-4 rounded-xl border leading-relaxed text-sm text-foreground/90 whitespace-pre-wrap min-h-[156px] flex items-center", PDF_PANEL_BG, PDF_PANEL_BORDER)}>
                                {summaryKo}
                            </div>
                        </CardContent>
                    </Card>
                )}

                {criteriaSummaryKo && (
                    <Card className={cn("overflow-hidden border shadow-sm !py-0 !pt-0 !pb-0 !gap-0 h-full", PDF_PANEL_BG, PDF_PANEL_BORDER)}>
                        <CardHeader className="rounded-t-[inherit] border-b !flex !flex-row !items-center !gap-2.5 !px-7 !pt-4 !pb-4 bg-gradient-to-r from-emerald-500/5 via-emerald-500/10 to-emerald-500/5">
                            <CardTitle className="text-sm font-bold leading-tight flex items-center gap-2.5">
                                <Search className="w-4 h-4 text-emerald-500" />
                                코호트 추출 조건 상세
                            </CardTitle>
                        </CardHeader>
                        <CardContent className="!px-7 !pt-4 !pb-6">
                            <div className={cn("p-4 rounded-xl border leading-relaxed text-sm text-foreground/90 whitespace-pre-wrap min-h-[156px] flex items-center", PDF_PANEL_BG, PDF_PANEL_BORDER)}>
                                {criteriaSummaryKo}
                            </div>
                        </CardContent>
                    </Card>
                )}
            </div>

            {/* ══ 세부 코호트 선정 근거 (Extraction Details) ══ */}
            {populationCriteria.length > 0 && (
                <Card className={cn("overflow-hidden border shadow-sm !py-0 !pt-0 !pb-0 !gap-0 animate-in fade-in slide-in-from-bottom-2 duration-500", PDF_PANEL_BG, PDF_PANEL_BORDER)}>
                    <CardHeader className="rounded-t-[inherit] border-b !flex !flex-row !items-center !gap-2.5 !px-7 !pt-4 !pb-4 bg-gradient-to-r from-purple-500/5 via-purple-500/10 to-purple-500/5">
                        <CardTitle className="text-sm font-bold leading-tight flex items-center gap-2.5">
                            <Target className="w-4 h-4 text-purple-500" />
                            세부 코호트 선정 및 DB 매칭 근거 (Inclusion/Exclusion)
                        </CardTitle>
                    </CardHeader>
                    <CardContent className="!px-7 !pt-4 !pb-6">
                        <div className={cn("grid grid-cols-1 gap-4", populationCriteria.length > 1 && "md:grid-cols-2")}>
                            {populationCriteria.map((item: any, idx: number) => (
                                <div key={idx} className={cn("p-4 rounded-xl border shadow-sm hover:shadow-md transition-shadow duration-300 flex flex-col gap-3", PDF_PANEL_BG, PDF_PANEL_BORDER)}>
                                    <div className="space-y-2">
                                        <div className="flex items-center justify-between">
                                            <Badge variant="outline" className={cn("text-[9px] border-0 h-5", item.type === "inclusion" ? "bg-emerald-500/15 text-emerald-600" : "bg-rose-500/15 text-rose-600")}>
                                                {item.type === "inclusion" ? "선정 기준" : "제외 기준"}
                                            </Badge>
                                            <span className={cn("text-[9px] text-muted-foreground font-mono px-1.5 py-0.5 rounded border", PDF_PANEL_BG_SOFT, PDF_PANEL_BORDER)}>#{idx + 1}</span>
                                        </div>
                                        <p className="text-sm font-semibold leading-relaxed text-foreground/90">{item.criterion}</p>
                                    </div>

                                    <div className="space-y-2 pt-3 border-t border-border/50">
                                        <div className="p-2.5 rounded-lg bg-primary/5 border border-primary/10">
                                            <div className="text-[9px] font-bold text-primary mb-1 flex items-center gap-1 uppercase tracking-tighter opacity-80">
                                                <Database className="w-2.5 h-2.5" /> Operational Definition
                                            </div>
                                            <p className="text-xs leading-relaxed text-foreground/80 font-medium">{item.operational_definition}</p>
                                        </div>
                                        <div className="p-2.5 rounded-lg bg-amber-500/5 border border-amber-500/10">
                                            <div className="text-[9px] font-bold text-amber-600 mb-1 flex items-center gap-1 uppercase tracking-tighter opacity-80">
                                                <BookOpen className="w-2.5 h-2.5" /> Evidence from Paper
                                            </div>
                                            <p className="text-xs leading-relaxed text-amber-900/80 italic line-clamp-4">"{item.evidence}"</p>
                                        </div>
                                    </div>
                                </div>
                            ))}
                        </div>
                    </CardContent>
                </Card>
            )}

            {/* ══ 섹션 2 · 연구 방법론 (Methods) ══ */}
            <Card className={cn("overflow-hidden border shadow-sm !py-0 !pt-0 !pb-0 !gap-0", PDF_PANEL_BG, PDF_PANEL_BORDER)}>
                <CardHeader className="rounded-t-[inherit] border-b !flex !flex-col !gap-2 !px-7 !pt-4 !pb-4 bg-gradient-to-r from-amber-500/5 via-amber-500/10 to-amber-500/5">
                    <CardTitle className="text-base leading-tight flex items-center gap-2.5">
                        <div className="flex items-center justify-center w-8 h-8 rounded-lg bg-amber-500/10">
                            <ClipboardList className="w-4 h-4 text-amber-500" />
                        </div>
                        연구 방법론 (Methods)
                    </CardTitle>
                    <CardDescription className="text-xs">논문 본문을 분석하여 추출된 연구 설계 및 분석 방법 요약</CardDescription>
                </CardHeader>
                <CardContent className="!px-7 !pt-5 !pb-6">
                    <MethodsSummaryRenderer summary={methodsSummary} />
                </CardContent>
            </Card>

            {/* ══ 추출된 임상 변수 리스트 (Variables Used in PDF) ══ */}
            {variables.length > 0 && (
                <Card className={cn("overflow-hidden border shadow-sm !py-0 !pt-0 !pb-0 !gap-0", PDF_PANEL_BG, PDF_PANEL_BORDER)}>
                    <CardHeader className="rounded-t-[inherit] border-b !flex !flex-col !gap-2 !px-7 !pt-4 !pb-4 bg-gradient-to-r from-blue-500/5 via-blue-500/10 to-blue-500/5">
                        <CardTitle className="text-base leading-tight flex items-center gap-2.5">
                            <div className="flex items-center justify-center w-8 h-8 rounded-lg bg-blue-500/10">
                                <Database className="w-4 h-4 text-blue-500" />
                            </div>
                            추출된 임상 변수 리스트
                        </CardTitle>
                        <CardDescription className="text-xs">
                            PDF 내에서 식별된 데이터 매핑 변수들입니다.
                        </CardDescription>
                    </CardHeader>
                    <CardContent className="!px-7 !pt-5 !pb-6">
                        <div className="space-y-3">
                            {variables.map((v: any, i: number) => (
                                <div key={i} className={cn("flex items-center justify-between p-3 rounded-lg border transition-colors", PDF_PANEL_BG_SOFT, PDF_PANEL_BORDER, "hover:bg-[#e6ebf2]")}>
                                    <div className="flex flex-col gap-1">
                                        <span className="text-xs font-bold text-foreground">{v.signal_name}</span>
                                        <span className="text-[10px] text-muted-foreground">{v.description}</span>
                                    </div>
                                    <div className="flex items-center gap-2">
                                        <Badge variant="secondary" className="text-[10px] font-mono">
                                            {v.mapping?.target_table || "Unknown"}
                                        </Badge>
                                        <Badge variant="outline" className="text-[10px] font-mono text-indigo-500 border-indigo-200 bg-indigo-50">
                                            ID: {v.mapping?.itemid || "N/A"}
                                        </Badge>
                                    </div>
                                </div>
                            ))}
                        </div>
                    </CardContent>
                </Card>
            )}

            {/* ══ Charts ══ */}
            {charts.length > 0 && (
                <Card className={cn("overflow-hidden border shadow-sm !py-0 !pt-0 !pb-0 !gap-0", PDF_PANEL_BG, PDF_PANEL_BORDER)}>
                    <CardHeader className="rounded-t-[inherit] border-b !flex !flex-row !items-center !gap-2.5 !px-7 !pt-4 !pb-4 bg-gradient-to-r from-cyan-500/5 via-cyan-500/10 to-cyan-500/5">
                        <CardTitle className="text-base leading-tight flex items-center gap-2.5">
                            <div className="flex items-center justify-center w-8 h-8 rounded-lg bg-cyan-500/10">
                                <BarChart2 className="w-4 h-4 text-cyan-500" />
                            </div>
                            데이터 분포 시각화
                        </CardTitle>
                    </CardHeader>
                    <CardContent className="!px-7 !pt-5 !pb-6">
                        <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-4">
                            {charts.map((c, i) => (
                                <MiniBarChart key={i} title={c.title} data={c.data} color={i % 2 === 0 ? "bg-primary" : "bg-blue-500"} />
                            ))}
                        </div>
                    </CardContent>
                </Card>
            )}

            {/* ══ SQL 쿼리 (접이식) ══ */}
            {cohortSql && (
                <Card className={cn("overflow-hidden border shadow-sm !py-0 !gap-0", PDF_PANEL_BG, PDF_PANEL_BORDER)}>
                    <details className="group">
                        <summary className="flex items-center justify-between p-4 cursor-pointer hover:bg-[#e6ebf2] transition-colors select-none">
                            <div className="flex items-center gap-2.5">
                                <div className="flex items-center justify-center w-8 h-8 rounded-lg bg-gray-500/10"><Code className="w-4 h-4 text-muted-foreground" /></div>
                                <span className="text-sm font-semibold text-foreground">생성된 SQL 쿼리</span>
                            </div>
                            <div className="flex items-center gap-2">

                                <Button variant="outline" size="sm" className="h-7 text-[10px] gap-1" onClick={async (e) => {
                                    e.preventDefault(); e.stopPropagation()
                                    const sqlToCopy = activeSqlTab === "cohort" ? cohortSql : (activeSqlTab === "count" ? sqls.count_sql : sqls.debug_count_sql)
                                    try { await navigator.clipboard.writeText(sqlToCopy || ""); setMessage("SQL 복사 완료") } catch { setError("복사 실패") }
                                }}>
                                    <Copy className="w-3 h-3" /> 복사
                                </Button>
                                <ChevronRight className="w-4 h-4 text-muted-foreground group-open:rotate-90 transition-transform" />
                            </div>
                        </summary>
                        <div className="px-4 pb-4">
                            {/* SQL Tabs Toggle */}
                            {(sqls.count_sql || sqls.debug_count_sql) && (
                                <div className={cn("flex gap-1 mb-3 p-1 rounded-lg w-fit border", PDF_PANEL_BG, PDF_PANEL_BORDER)}>
                                    <button
                                        type="button"
                                        className={cn("px-3 py-1 text-[10px] font-medium rounded-md transition-all", activeSqlTab === "cohort" ? "bg-[#e2e8f0] text-primary shadow-sm" : "text-muted-foreground hover:text-foreground")}
                                        onClick={() => setActiveSqlTab("cohort")}
                                    >
                                        코호트 추출 SQL
                                    </button>
                                    {sqls.count_sql && (
                                        <button
                                            type="button"
                                            className={cn("px-3 py-1 text-[10px] font-medium rounded-md transition-all", activeSqlTab === "count" ? "bg-[#e2e8f0] text-primary shadow-sm" : "text-muted-foreground hover:text-foreground")}
                                            onClick={() => setActiveSqlTab("count")}
                                        >
                                            단순 카운트 SQL
                                        </button>
                                    )}
                                    {sqls.debug_count_sql && (
                                        <button
                                            type="button"
                                            className={cn("px-3 py-1 text-[10px] font-medium rounded-md transition-all", activeSqlTab === "debug" ? "bg-[#e2e8f0] text-primary shadow-sm" : "text-muted-foreground hover:text-foreground")}
                                            onClick={() => setActiveSqlTab("debug")}
                                        >
                                            단계별 디버그 SQL
                                        </button>
                                    )}
                                </div>
                            )}

                            <div className={cn("max-h-[40vh] overflow-auto rounded-lg border p-4", PDF_PANEL_BG, PDF_PANEL_BORDER)}>
                                <pre className="text-xs leading-relaxed whitespace-pre-wrap break-all font-mono text-foreground/80">
                                    {activeSqlTab === "cohort" ? cohortSql : (activeSqlTab === "count" ? sqls.count_sql : sqls.debug_count_sql)}
                                </pre>
                            </div>
                        </div>
                    </details>
                </Card>
            )}

            {/* ══ SQL 실행 결과 (접이식) ══ */}
            {(columns.length > 0 || resultError || stepCounts.length > 0) ? (
                <Card className={cn("overflow-hidden border shadow-sm !py-0 !gap-0", PDF_PANEL_BG, PDF_PANEL_BORDER)}>
                    <details className="group" open>
                        <summary className="flex items-center justify-between p-4 cursor-pointer hover:bg-[#e6ebf2] transition-colors select-none">
                            <div className="flex items-center gap-2.5">
                                <div className="flex items-center justify-center w-8 h-8 rounded-lg bg-gray-500/10"><TableIcon className="w-4 h-4 text-muted-foreground" /></div>
                                <span className="text-sm font-semibold text-foreground">SQL 실행 결과</span>
                                <Badge variant="secondary" className="text-[10px] h-5">
                                    {resultError ? "오류 발생" : `${Number(patientCount).toLocaleString()}명`}
                                </Badge>
                            </div>
                            <div className="flex items-center gap-2">
                                {!resultError && columns.length > 0 && (
                                    <Button variant="outline" size="sm" className="h-7 text-[10px] gap-1" onClick={(e) => { e.preventDefault(); e.stopPropagation(); onDownloadCSV() }}>
                                        <Download className="w-3 h-3" /> CSV
                                    </Button>
                                )}
                                <ChevronRight className="w-4 h-4 text-muted-foreground group-open:rotate-90 transition-transform" />
                            </div>
                        </summary>
                        <div className="px-4 pb-4">
                            {/* 단계별 필터링 카운트 (Funnel Chart 스타일) */}
                            {stepCounts.length > 0 && (
                                <div className={cn("mb-6 p-4 rounded-xl border", PDF_PANEL_BG_SOFT, PDF_PANEL_BORDER)}>
                                    <div className="text-xs font-semibold mb-3 flex items-center gap-1.5 opacity-80">
                                        <Activity className="w-3.5 h-3.5 text-primary" /> 단계별 필터링 효과 (Funnel)
                                    </div>
                                    <div className="space-y-2">
                                        {stepCounts.map((s, idx: number) => {
                                            const total = stepCounts[0]?.count || 1
                                            const pct = Math.round((s.count / total) * 100)
                                            return (
                                                <div key={idx} className="space-y-1">
                                                    <div className="flex justify-between text-[10px] text-muted-foreground">
                                                        <span className="font-medium text-foreground/70">{s.name}</span>
                                                        <span>{s.count.toLocaleString()}명 ({pct}%)</span>
                                                    </div>
                                                    <div className="h-1.5 w-full bg-[#d9e0e9] rounded-full overflow-hidden">
                                                        <div className="h-full bg-primary transition-all duration-500" style={{ width: `${pct}%` }} />
                                                    </div>
                                                </div>
                                            )
                                        })}
                                    </div>
                                </div>
                            )}

                            {resultError ? (
                                <div className="rounded-lg border border-destructive/30 bg-destructive/5 p-4">
                                    <div className="flex items-center gap-2 text-destructive text-sm font-medium mb-1"><AlertTriangle className="w-4 h-4" /> SQL 실행 또는 분석 오류</div>
                                    <pre className="text-xs text-destructive/80 whitespace-pre-wrap break-all">{resultError}</pre>
                                </div>
                            ) : columns.length > 0 ? (
                                <div className={cn("overflow-x-auto rounded-lg border", PDF_PANEL_BORDER)}>
                                    <table className="w-full text-xs">
                                        <thead className={cn("border-b", PDF_PANEL_BG)}>
                                            <tr>
                                                {columns.map((col: string, i: number) => (
                                                    <th key={i} className="text-left px-3 py-2.5 font-semibold text-muted-foreground whitespace-nowrap text-[11px] uppercase tracking-wider">{col}</th>
                                                ))}
                                            </tr>
                                        </thead>
                                        <tbody className="divide-y divide-border/50">
                                            {rows.slice(0, 50).map((row: any, ri: number) => {
                                                const cells = Array.isArray(row)
                                                    ? row
                                                    : columns.map((col) => (row && typeof row === "object" ? (row as Record<string, unknown>)[col] : ""))
                                                return (
                                                <tr key={ri} className={cn("transition-colors hover:bg-[#dfe5ee]", ri % 2 === 0 ? "" : "bg-[#e9edf3]")}>
                                                    {cells.map((cell: any, ci: number) => (
                                                        <td key={ci} className="px-3 py-1.5 text-foreground/80 whitespace-nowrap">{cell == null ? "" : String(cell)}</td>
                                                    ))}
                                                </tr>
                                                )
                                            })}
                                        </tbody>
                                    </table>
                                    {((rows.length || 0) >= 100 || (toNumber(cr.row_count) ?? 0) > 50) && (
                                        <div className={cn("px-4 py-2 text-[10px] text-muted-foreground border-t", PDF_PANEL_BG_SOFT, PDF_PANEL_BORDER)}>
                                            전체 검색 결과 중 상위 {rows.length || 0}행 표시
                                        </div>
                                    )}
                                </div>
                            ) : (
                                <div className="text-sm text-muted-foreground text-center py-8">결과가 없습니다.</div>
                            )}
                        </div>
                    </details>
                </Card>
            ) : null}
        </div>
    )
}
