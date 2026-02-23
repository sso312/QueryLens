"use client"

import { useState, useEffect, useMemo, useRef } from "react"
import dynamic from "next/dynamic"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Textarea } from "@/components/ui/textarea"
import { Input } from "@/components/ui/input"
import { Checkbox } from "@/components/ui/checkbox"
import { Progress } from "@/components/ui/progress"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Send, 
  Code, 
  BarChart3,
  AlertTriangle,
  Play,
  Loader2,
  Eye,
  Pencil,
  Sparkles,
  Table2,
  FileText,
  RefreshCw,
  Copy,
  Download,
  Maximize2,
  Plus,
  Check,
  Mic,
  X,
  Trash2,
  BookmarkPlus
} from "lucide-react"
import { cn } from "@/lib/utils"
import { useAuth } from "@/components/auth-provider"
import { CohortLibraryDialog } from "@/components/cohort-library-dialog"
import {
  type ActiveCohortContext,
  type SavedCohort,
  LEGACY_PENDING_PDF_COHORT_CONTEXT_KEY,
  PENDING_ACTIVE_COHORT_CONTEXT_KEY,
  clearPendingActiveCohortContext,
  normalizeActiveCohortContext,
  scopedStorageKey,
  toActiveCohortContext,
  toSavedCohort,
} from "@/lib/cohort-library"
import {
  clearDashboardCache,
  readDashboardCache,
  updateDashboardCache,
  writeDashboardCache,
} from "@/lib/dashboard-cache"

interface ChatMessage {
  id: string
  role: "user" | "assistant"
  content: string
  timestamp: Date
}

interface PersistedChatMessage {
  id: string
  role: "user" | "assistant"
  content: string
  timestamp: string
}

interface PreviewData {
  columns: string[]
  rows: any[][]
  row_count: number
  row_cap: number
  total_count?: number | null
}

interface DemoResult {
  sql?: string
  preview?: PreviewData
  summary?: string
  source?: string
}

interface PolicyCheck {
  name: string
  passed: boolean
  message: string
}

interface PolicyResult {
  passed?: boolean
  checks?: PolicyCheck[]
}

interface OneShotPayload {
  mode: "demo" | "advanced" | "clarify"
  question: string
  assistant_message?: string
  result?: DemoResult
  risk?: { risk?: number; intent?: string }
  policy?: PolicyResult | null
  draft?: { final_sql?: string }
  final?: { final_sql?: string; risk_score?: number; used_tables?: string[] }
  clarification?: {
    reason?: string
    question?: string
    options?: string[]
    example_inputs?: string[]
  }
}

interface OneShotResponse {
  qid: string
  payload: OneShotPayload
}

interface RunResponse {
  sql: string
  result: PreviewData
  policy?: PolicyResult | null
}

interface QueryAnswerResponse {
  answer?: string
  source?: string
  suggested_questions?: string[]
  suggestions_source?: string
}

interface QueryTranscribeResponse {
  text?: string
  source?: string
  model?: string
}

interface VisualizationChartSpec {
  chart_type?: string
  x?: string
  y?: string
  group?: string
  agg?: string
}

interface VisualizationAnalysisCard {
  chart_spec?: VisualizationChartSpec
  reason?: string
  summary?: string
  figure_json?: Record<string, unknown>
  image_data_url?: string
  render_engine?: string
  code?: string
}

interface VisualizationResponsePayload {
  sql?: string
  table_preview?: Array<Record<string, unknown>>
  analyses?: VisualizationAnalysisCard[]
  insight?: string
}

type PdfCohortContext = {
  pdfName: string
  cohortSize?: number
  keyVariables?: string[]
  appliedAt: string
}

type PdfContextBannerState = {
  context: PdfCohortContext | null
  hasShownTableOnce: boolean
}

interface PersistedQueryState {
  query: string
  lastQuestion: string
  messages: PersistedChatMessage[]
  response: OneShotResponse | null
  runResult: RunResponse | null
  visualizationResult?: VisualizationResponsePayload | null
  activeCohortContext?: ActiveCohortContext | null
  suggestedQuestions: string[]
  showResults: boolean
  showSqlPanel: boolean
  showQueryResultPanel: boolean
  editedSql: string
  isEditing: boolean
}

interface ResultTabState {
  id: string
  question: string
  sql: string
  resultData: PreviewData | null
  visualization: VisualizationResponsePayload | null
  statistics: SimpleStatsRow[]
  insight: string
  status: "pending" | "error" | "success"
  error?: string | null
  response: OneShotResponse | null
  runResult: RunResponse | null
  suggestedQuestions: string[]
  showSqlPanel: boolean
  showQueryResultPanel: boolean
  editedSql: string
  isEditing: boolean
  preferredChartType?: "line" | "bar" | "pie" | null
}

interface DashboardFolderOption {
  id: string
  name: string
}

interface CategoryTypeSummary {
  value: string
  occurrences: number
}

type DashboardChartSpec = {
  id: string
  type: string
  x?: string
  y?: string
  config?: Record<string, unknown>
  thumbnailUrl?: string
  pngUrl?: string
}

type DashboardColumnStatsRow = {
  column: string
  n: number
  missing: number
  nulls: number
  min?: string | number
  q1?: string | number
  median?: string | number
  q3?: string | number
  max?: string | number
  mean?: string | number
}

type DashboardCohortProvenance = {
  source: "NONE" | "LIBRARY" | "PDF"
  libraryCohortId?: string
  libraryCohortName?: string
  pdfCohortId?: string
  pdfPaperTitle?: string
  libraryUsed?: boolean
}

type DashboardPdfAnalysisSnapshot = {
  pdfHash?: string
  pdfName?: string
  summaryKo?: string
  criteriaSummaryKo?: string
  variables?: string[]
  inclusionExclusion?: Array<{
    id: string
    title: string
    operationalDefinition: string
    evidence?: string
  }>
  source?: string
  analyzedAt?: string
  libraryUsed?: boolean
  libraryCohortId?: string
  libraryCohortName?: string
}

const MAX_PERSIST_ROWS = 200
const VIZ_CACHE_PREFIX = "viz_cache_v3:"
const VIZ_CACHE_TTL_MS = 1000 * 60 * 60 * 24
const PDF_CONTEXT_TABLE_ONCE_KEY_PREFIX = "ql_pdf_context_table_once"
const COHORT_CONTEXT_SQL_LIMIT = 3200
const CHART_CATEGORY_THRESHOLD = 10
const CHART_CATEGORY_DEFAULT_COUNT = 10
const BOX_PLOT_TRIGGER_MIN_W = 120
const BOX_PLOT_TRIGGER_MAX_W = 360
const BOX_PLOT_TRIGGER_EXTRA_PX = 64
const DEFAULT_QUICK_QUESTIONS = [
  "입원 환자 수를 월별 추이로 보여줘",
  "가장 흔한 진단 코드는 무엇인가요?",
  "ICU 재원일수가 긴 환자군을 알려줘",
]
const DEFAULT_CHAT_MODEL = "gpt-4o-mini"
const FALLBACK_CHAT_MODELS = ["gpt-4o-mini", "gpt-4o"]
const VOICE_WAVE_BAR_COUNT = 72
const VOICE_WAVE_BASELINE = 0.08
const Plot = dynamic(() => import("react-plotly.js"), { ssr: false }) as any

const hashText = (value: string) => {
  let hash = 2166136261
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i)
    hash +=
      (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24)
  }
  return (hash >>> 0).toString(16)
}

const buildVizCacheKey = (sqlText: string, questionText: string, previewData: PreviewData | null) => {
  if (!previewData) return null
  const columns = previewData.columns || []
  const rows = previewData.rows || []
  const rowCount = previewData.row_count ?? rows.length
  const head = rows.slice(0, 10)
  const tail = rows.slice(Math.max(0, rows.length - 10))
  const basis = JSON.stringify({
    q: (questionText || "").trim(),
    sql: (sqlText || "").trim(),
    columns,
    rowCount,
    head,
    tail,
  })
  return `${VIZ_CACHE_PREFIX}${hashText(basis)}`
}

const composeVisualizationUserQuery = (questionText: string) => String(questionText || "").trim()

const trimPreview = (preview?: PreviewData): PreviewData | undefined => {
  if (!preview) return preview
  const rows = Array.isArray(preview.rows) ? preview.rows : []
  const trimmedRows = rows.slice(0, MAX_PERSIST_ROWS)
  const preservedRowCount =
    typeof preview.row_count === "number" && Number.isFinite(preview.row_count)
      ? preview.row_count
      : trimmedRows.length
  return {
    ...preview,
    rows: trimmedRows,
    row_count: preservedRowCount,
  }
}

const sanitizeRunResult = (runResult: RunResponse | null): RunResponse | null => {
  if (!runResult) return null
  return {
    ...runResult,
    result: trimPreview(runResult.result) || runResult.result,
  }
}

const sanitizeVisualizationResult = (
  visualization: VisualizationResponsePayload | null
): VisualizationResponsePayload | null => {
  if (!visualization) return null
  const insight = String(visualization.insight || "").trim()
  if (!insight) return null
  // Persist only insight text to avoid storing oversized figure payloads.
  return { insight }
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  !!value && typeof value === "object" && !Array.isArray(value)

const readFiniteNumber = (value: unknown): number | null => {
  const num = typeof value === "number" ? value : Number(value)
  return Number.isFinite(num) ? num : null
}

const normalizeChartType = (chartType: unknown) => {
  const normalized = String(chartType || "").trim().toLowerCase()
  if (!normalized) return ""
  const pyramidAliases = new Set(["pyamid", "pyrmaid", "pyramind", "piramid", "pyrmid"])
  if (pyramidAliases.has(normalized)) return "pyramid"
  return normalized
}

const isPyramidChartType = (chartType: unknown) => normalizeChartType(chartType) === "pyramid"

const hasRenderableDatum = (value: unknown): boolean => {
  if (value == null) return false
  if (typeof value === "number") return Number.isFinite(value)
  if (typeof value === "string") return value.trim().length > 0
  if (value instanceof Date) return Number.isFinite(value.getTime())
  return true
}

const arrayHasRenderableDatum = (values: unknown[]): boolean => {
  for (const value of values) {
    if (Array.isArray(value)) {
      if (arrayHasRenderableDatum(value)) return true
      continue
    }
    if (hasRenderableDatum(value)) return true
  }
  return false
}

const arrayHasFiniteNumber = (values: unknown[]): boolean => {
  for (const value of values) {
    if (Array.isArray(value)) {
      if (arrayHasFiniteNumber(value)) return true
      continue
    }
    if (readFiniteNumber(value) != null) return true
  }
  return false
}

const traceHasRenderableData = (rawTrace: unknown) => {
  if (!isRecord(rawTrace)) return false
  const traceType = String(rawTrace.type || "").toLowerCase()
  const orientation = String(rawTrace.orientation || "").toLowerCase()
  const x = rawTrace.x
  const y = rawTrace.y
  const values = rawTrace.values
  const z = rawTrace.z

  if (traceType === "bar" || traceType === "histogram") {
    const valueAxis = orientation === "h" ? x : y
    const categoryAxis = orientation === "h" ? y : x
    if (!Array.isArray(valueAxis) || !valueAxis.length) return false
    if (!arrayHasFiniteNumber(valueAxis)) return false
    if (Array.isArray(categoryAxis)) return arrayHasRenderableDatum(categoryAxis)
    return true
  }
  if (traceType === "pie") {
    return Array.isArray(values) && values.length > 0 && arrayHasFiniteNumber(values)
  }
  if (traceType === "heatmap") {
    return Array.isArray(z) && z.length > 0 && arrayHasFiniteNumber(z)
  }
  if (Array.isArray(x) && Array.isArray(y)) {
    return arrayHasRenderableDatum(x) && arrayHasRenderableDatum(y)
  }
  if (Array.isArray(values)) return arrayHasRenderableDatum(values)
  if (Array.isArray(z)) return arrayHasRenderableDatum(z)
  if (Array.isArray(x)) return arrayHasRenderableDatum(x)
  if (Array.isArray(y)) return arrayHasRenderableDatum(y)
  return false
}

const figureHasRenderableData = (figure: { data?: unknown[]; layout?: Record<string, unknown> } | null) =>
  Boolean(Array.isArray(figure?.data) && figure.data.some((trace) => traceHasRenderableData(trace)))

const normalizePendingPdfCohortContext = (value: unknown): ActiveCohortContext | null => {
  const normalized = normalizeActiveCohortContext(value)
  if (!normalized) return null
  return {
    ...normalized,
    cohortSql: String(normalized.cohortSql || "").slice(0, COHORT_CONTEXT_SQL_LIMIT),
  }
}

const buildCohortStarterQuestions = (context: ActiveCohortContext) => {
  const cohortLabel = context.filename || context.cohortName || "이 코호트"
  return [
    `${cohortLabel}의 연령/성별 분포를 보여줘`,
    `${cohortLabel}의 30일 재입원율과 사망률을 보여줘`,
    `${cohortLabel}에서 ICU 입실 여부별 평균 재원일수를 비교해줘`,
  ]
}

const toPdfCohortContext = (context: ActiveCohortContext): PdfCohortContext => ({
  pdfName: context.filename || context.cohortName || "저장 코호트",
  cohortSize: context.patientCount != null ? Math.round(context.patientCount) : undefined,
  keyVariables: Array.isArray(context.variables) ? context.variables : [],
  appliedAt: new Date(context.ts || Date.now()).toISOString(),
})

const buildDashboardCohortProvenance = (
  context: ActiveCohortContext | null
): DashboardCohortProvenance => {
  if (!context) return { source: "NONE" }
  const sourceTag = String(context.source || "").toLowerCase()
  const libraryUsed = /library|selector/.test(sourceTag)
  if (context.type === "PDF_DERIVED") {
    return {
      source: "PDF",
      libraryCohortId: context.cohortId || undefined,
      libraryCohortName: context.cohortName || undefined,
      pdfCohortId: context.pdfHash || context.cohortId || undefined,
      pdfPaperTitle: context.filename || context.cohortName || undefined,
      libraryUsed,
    }
  }
  return {
    source: "LIBRARY",
    libraryCohortId: context.cohortId || undefined,
    libraryCohortName: context.cohortName || undefined,
    libraryUsed: true,
  }
}

const buildDashboardPdfAnalysisSnapshot = (
  context: ActiveCohortContext | null
): DashboardPdfAnalysisSnapshot | undefined => {
  if (!context || context.type !== "PDF_DERIVED") return undefined
  const sourceTag = String(context.source || "").toLowerCase()
  const libraryUsed = /library|selector/.test(sourceTag)
  const variables = Array.isArray(context.variables)
    ? context.variables.map((item) => String(item || "").trim()).filter(Boolean).slice(0, 24)
    : []
  const inclusionExclusion = Array.isArray(context.inclusionExclusion)
    ? context.inclusionExclusion
        .map((item, index) => ({
          id: String(item?.id || `ie-${index + 1}`).trim() || `ie-${index + 1}`,
          title: String(item?.title || `조건 ${index + 1}`).trim() || `조건 ${index + 1}`,
          operationalDefinition: String(item?.operationalDefinition || "").trim(),
          evidence: String(item?.evidence || "").trim() || undefined,
        }))
        .filter((item) => Boolean(item.operationalDefinition))
        .slice(0, 20)
    : []

  return {
    pdfHash: context.pdfHash || context.cohortId || undefined,
    pdfName: context.filename || context.cohortName || undefined,
    summaryKo: String(context.summaryKo || context.paperSummary || "").trim() || undefined,
    criteriaSummaryKo: String(context.criteriaSummaryKo || context.sqlFilterSummary || "").trim() || undefined,
    variables: variables.length ? variables : undefined,
    inclusionExclusion: inclusionExclusion.length ? inclusionExclusion : undefined,
    source: context.source || undefined,
    analyzedAt: Number.isFinite(context.ts) ? new Date(context.ts).toISOString() : undefined,
    libraryUsed,
    libraryCohortId: context.cohortId || undefined,
    libraryCohortName: context.cohortName || undefined,
  }
}

const toDashboardStatsRows = (rows: SimpleStatsRow[]): DashboardColumnStatsRow[] =>
  rows.map((row) => ({
    column: row.column,
    n: row.count,
    missing: row.missingCount,
    nulls: row.nullCount,
    min: row.min ?? undefined,
    q1: row.q1 ?? undefined,
    median: row.median ?? undefined,
    q3: row.q3 ?? undefined,
    max: row.max ?? undefined,
    mean: row.avg ?? undefined,
  }))

const buildPdfContextTableOnceKey = (userKey: string, context: ActiveCohortContext) => {
  const owner = (userKey || "anonymous").trim() || "anonymous"
  const pdfHash = context.pdfHash || "nohash"
  const ts = Number.isFinite(context.ts) ? Math.round(context.ts) : 0
  return `${PDF_CONTEXT_TABLE_ONCE_KEY_PREFIX}:${owner}:${pdfHash}:${ts}`
}

const getAxisKey = (trace: Record<string, unknown>): "x" | "y" =>
  String(trace.orientation || "").toLowerCase() === "h" ? "y" : "x"

type AxisDomain =
  | { kind: "numeric"; min: number; max: number }
  | { kind: "date"; min: number; max: number }
  | { kind: "categorical"; values: Set<string> }
  | null

const readDateMs = (value: unknown): number | null => {
  if (value instanceof Date) {
    const ms = value.getTime()
    return Number.isFinite(ms) ? ms : null
  }
  const text = String(value ?? "").trim()
  if (!text) return null
  const ms = Date.parse(text)
  return Number.isFinite(ms) ? ms : null
}

const inferAxisValueKind = (values: unknown[]): "numeric" | "date" | "categorical" => {
  if (!values.length) return "categorical"
  const numericCount = values.reduce(
    (count: number, value) => (readFiniteNumber(value) != null ? count + 1 : count),
    0
  )
  if (numericCount / values.length >= 0.85) return "numeric"
  const dateCount = values.reduce(
    (count: number, value) => (readDateMs(value) != null ? count + 1 : count),
    0
  )
  if (dateCount / values.length >= 0.85) return "date"
  return "categorical"
}

const buildAxisDomainFromRecords = (
  records: Array<Record<string, unknown>>,
  column?: string
): AxisDomain => {
  if (!column) return null
  const rawValues = records
    .map((row) => row?.[column])
    .filter((value) => {
      if (value == null) return false
      return String(value).trim().length > 0
    })
  if (!rawValues.length) return null
  const kind = inferAxisValueKind(rawValues)
  if (kind === "numeric") {
    const numericValues = rawValues
      .map((value) => readFiniteNumber(value))
      .filter((value): value is number => value != null)
    if (!numericValues.length) return null
    return { kind: "numeric", min: Math.min(...numericValues), max: Math.max(...numericValues) }
  }
  if (kind === "date") {
    const dateValues = rawValues
      .map((value) => readDateMs(value))
      .filter((value): value is number => value != null)
    if (!dateValues.length) return null
    return { kind: "date", min: Math.min(...dateValues), max: Math.max(...dateValues) }
  }
  return {
    kind: "categorical",
    values: new Set(rawValues.map((value) => String(value ?? "").trim()).filter(Boolean)),
  }
}

const filterTraceByIndexes = (
  trace: Record<string, unknown>,
  axisKey: "x" | "y",
  axisValues: unknown[],
  filteredIndexes: number[]
) => {
  if (!filteredIndexes.length) return trace
  if (filteredIndexes.length === axisValues.length) return trace

  const next = { ...trace } as Record<string, unknown>
  next[axisKey] = filteredIndexes.map((idx) => axisValues[idx])

  const otherAxisKey = axisKey === "x" ? "y" : "x"
  const otherAxis = next[otherAxisKey]
  if (Array.isArray(otherAxis) && otherAxis.length === axisValues.length) {
    next[otherAxisKey] = filteredIndexes.map((idx) => otherAxis[idx])
  }

  const traceText = next.text
  if (Array.isArray(traceText) && traceText.length === axisValues.length) {
    next.text = filteredIndexes.map((idx) => traceText[idx])
  }

  const traceCustomData = next.customdata
  if (Array.isArray(traceCustomData) && traceCustomData.length === axisValues.length) {
    next.customdata = filteredIndexes.map((idx) => traceCustomData[idx])
  }

  if (isRecord(next.marker)) {
    const marker = { ...next.marker }
    const markerColor = marker.color
    if (Array.isArray(markerColor) && markerColor.length === axisValues.length) {
      marker.color = filteredIndexes.map((idx) => markerColor[idx])
    }
    next.marker = marker
  }

  return next
}

const clampFigureToAxisDomain = (
  figure: { data?: unknown[]; layout?: Record<string, unknown> } | null,
  axisDomain: AxisDomain
): { data?: unknown[]; layout?: Record<string, unknown> } | null => {
  if (!figure || !axisDomain || !Array.isArray(figure.data)) return figure
  const epsilon = 1e-9
  const filteredData = figure.data.map((rawTrace) => {
    if (!isRecord(rawTrace)) return rawTrace
    const trace = { ...rawTrace } as Record<string, unknown>
    const axisKey = getAxisKey(trace)
    const axisValues = trace[axisKey]
    if (!Array.isArray(axisValues) || !axisValues.length) return trace

    const filteredIndexes: number[] = []
    for (let idx = 0; idx < axisValues.length; idx += 1) {
      const value = axisValues[idx]
      if (axisDomain.kind === "numeric") {
        const num = readFiniteNumber(value)
        if (num != null && num >= axisDomain.min - epsilon && num <= axisDomain.max + epsilon) {
          filteredIndexes.push(idx)
        }
        continue
      }
      if (axisDomain.kind === "date") {
        const ms = readDateMs(value)
        if (ms != null && ms >= axisDomain.min - epsilon && ms <= axisDomain.max + epsilon) {
          filteredIndexes.push(idx)
        }
        continue
      }
      const normalized = String(value ?? "").trim()
      if (axisDomain.values.has(normalized)) filteredIndexes.push(idx)
    }
    return filterTraceByIndexes(trace, axisKey, axisValues, filteredIndexes)
  })

  return {
    ...figure,
    data: filteredData,
  }
}

const isPreferredChartType = (
  chartType: unknown,
  preferred: "line" | "bar" | "pie" | null | undefined
) => {
  const normalized = normalizeChartType(chartType)
  if (!preferred) return false
  if (preferred === "pie") return normalized === "pie" || normalized === "nested_pie" || normalized === "sunburst"
  if (preferred === "line") return normalized === "line" || normalized === "line_scatter" || normalized === "area"
  if (preferred === "bar") return normalized.startsWith("bar") || normalized === "pyramid"
  return false
}

const formatChartTypeLabel = (chartType: unknown) => {
  const normalized = normalizeChartType(chartType)
  if (!normalized) return "PLOT"
  if (normalized === "pyramid") return "PYRAMID (MIRRORED)"
  return normalized.replace(/_/g, " ").toUpperCase()
}

const CHART_COLORWAY = [
  "#1d4ed8",
  "#0f766e",
  "#c2410c",
  "#be123c",
  "#6d28d9",
  "#0ea5e9",
  "#0369a1",
  "#4d7c0f",
]

const normalizePlotLayout = (
  layout: Record<string, unknown> | null | undefined,
  minMargin: { l: number; r: number; t: number; b: number }
): Record<string, unknown> => {
  const baseLayout = isRecord(layout) ? { ...layout } : {}
  delete baseLayout.height
  delete baseLayout.width

  const sourceMargin = isRecord(baseLayout.margin) ? baseLayout.margin : {}
  const marginLeft = readFiniteNumber(sourceMargin.l)
  const marginRight = readFiniteNumber(sourceMargin.r)
  const marginTop = readFiniteNumber(sourceMargin.t)
  const marginBottom = readFiniteNumber(sourceMargin.b)

  const xaxis = isRecord(baseLayout.xaxis)
    ? { ...baseLayout.xaxis, automargin: true }
    : { automargin: true }
  const yaxis = isRecord(baseLayout.yaxis)
    ? { ...baseLayout.yaxis, automargin: true }
    : { automargin: true }

  return {
    ...baseLayout,
    autosize: true,
    margin: {
      ...sourceMargin,
      l: Math.max(minMargin.l, marginLeft ?? 0),
      r: Math.max(minMargin.r, marginRight ?? 0),
      t: Math.max(minMargin.t, marginTop ?? 0),
      b: Math.max(minMargin.b, marginBottom ?? 0),
    },
    font: {
      family: "Pretendard, 'Noto Sans KR', system-ui, -apple-system, sans-serif",
      size: 13,
      color: "#0f172a",
      ...(isRecord(baseLayout.font) ? baseLayout.font : {}),
    },
    colorway: Array.isArray(baseLayout.colorway) && baseLayout.colorway.length ? baseLayout.colorway : CHART_COLORWAY,
    hovermode: baseLayout.hovermode || "x unified",
    hoverlabel: {
      bgcolor: "rgba(15, 23, 42, 0.88)",
      bordercolor: "rgba(148, 163, 184, 0.45)",
      font: { color: "#f8fafc", size: 12 },
      ...(isRecord(baseLayout.hoverlabel) ? baseLayout.hoverlabel : {}),
    },
    legend: {
      orientation: "h",
      y: 1.04,
      x: 0,
      yanchor: "bottom",
      xanchor: "left",
      bgcolor: "rgba(255,255,255,0)",
      borderwidth: 0,
      font: { size: 11, color: "#334155" },
      ...(isRecord(baseLayout.legend) ? baseLayout.legend : {}),
    },
    xaxis: {
      showgrid: true,
      gridcolor: "rgba(148,163,184,0.18)",
      zeroline: false,
      showline: true,
      linecolor: "rgba(148,163,184,0.35)",
      ticks: "outside",
      ticklen: 5,
      tickcolor: "rgba(148,163,184,0.65)",
      tickfont: { color: "#475569", size: 12 },
      ...xaxis,
    },
    yaxis: {
      showgrid: true,
      gridcolor: "rgba(148,163,184,0.18)",
      zeroline: false,
      showline: true,
      linecolor: "rgba(148,163,184,0.35)",
      ticks: "outside",
      ticklen: 5,
      tickcolor: "rgba(148,163,184,0.65)",
      tickfont: { color: "#475569", size: 12 },
      ...yaxis,
    },
    uniformtext: {
      minsize: 10,
      mode: "hide",
      ...(isRecord(baseLayout.uniformtext) ? baseLayout.uniformtext : {}),
    },
    paper_bgcolor: "transparent",
    plot_bgcolor: "transparent",
  }
}

const styleFigureData = (data: unknown[] | undefined) => {
  if (!Array.isArray(data)) return []
  return data.map((rawTrace, index) => {
    if (!isRecord(rawTrace)) return rawTrace
    const trace = { ...rawTrace } as Record<string, unknown>
    const traceType = String(trace.type || "").toLowerCase()
    const paletteColor = CHART_COLORWAY[index % CHART_COLORWAY.length]

    if (traceType === "scatter") {
      const mode = String(trace.mode || "")
      const hasLines = mode.includes("lines")
      const hasMarkers = mode.includes("markers") || !mode
      const line = isRecord(trace.line) ? { ...trace.line } : {}
      if (hasLines) {
        if (!line.color) line.color = paletteColor
        if (!readFiniteNumber(line.width)) line.width = 2.8
        if (!line.shape) line.shape = "spline"
        if (line.shape === "spline" && !readFiniteNumber(line.smoothing)) line.smoothing = 0.55
      }
      if (Object.keys(line).length) trace.line = line

      if (hasMarkers) {
        const marker = isRecord(trace.marker) ? { ...trace.marker } : {}
        if (!marker.color) marker.color = paletteColor
        if (!readFiniteNumber(marker.size)) marker.size = hasLines ? 6 : 8
        if (!readFiniteNumber(marker.opacity)) marker.opacity = 0.92
        const markerLine = isRecord(marker.line) ? { ...marker.line } : {}
        if (!markerLine.color) markerLine.color = "rgba(255,255,255,0.95)"
        if (!readFiniteNumber(markerLine.width)) markerLine.width = hasLines ? 1.2 : 0.8
        marker.line = markerLine
        trace.marker = marker
      }
      return trace
    }

    if (traceType === "bar") {
      const isHorizontal = String(trace.orientation || "").toLowerCase() === "h"
      const marker = isRecord(trace.marker) ? { ...trace.marker } : {}
      if (!marker.color) marker.color = paletteColor
      if (!readFiniteNumber(marker.opacity)) marker.opacity = 0.92
      const markerLine = isRecord(marker.line) ? { ...marker.line } : {}
      if (!markerLine.color) markerLine.color = "rgba(255,255,255,0.85)"
      if (!readFiniteNumber(markerLine.width)) markerLine.width = 0.9
      marker.line = markerLine
      trace.marker = marker
      if (!trace.textposition) trace.textposition = "auto"
      if (!trace.hovertemplate) {
        trace.hovertemplate = isHorizontal
          ? "%{y}<br>%{x:,.3g}<extra></extra>"
          : "%{x}<br>%{y:,.3g}<extra></extra>"
      }
      if (trace.cliponaxis == null) trace.cliponaxis = false
      return trace
    }

    if (traceType === "pie") {
      const marker = isRecord(trace.marker) ? { ...trace.marker } : {}
      if (!Array.isArray(marker.colors) || !marker.colors.length) marker.colors = CHART_COLORWAY
      const markerLine = isRecord(marker.line) ? { ...marker.line } : {}
      if (!markerLine.color) markerLine.color = "white"
      if (!readFiniteNumber(markerLine.width)) markerLine.width = 1.4
      marker.line = markerLine
      trace.marker = marker
      if (!trace.textinfo) trace.textinfo = "label+percent"
      if (!trace.textposition) trace.textposition = "inside"
      return trace
    }

    if (traceType === "heatmap") {
      if (!trace.colorscale) trace.colorscale = "YlGnBu"
      if (!readFiniteNumber(trace.xgap)) trace.xgap = 1
      if (!readFiniteNumber(trace.ygap)) trace.ygap = 1
      return trace
    }

    return trace
  })
}

const prepareFigureForRender = (
  figure: { data?: unknown[]; layout?: Record<string, unknown> } | null,
  minMargin: { l: number; r: number; t: number; b: number }
) => {
  if (!figure) return null
  return {
    data: styleFigureData(figure.data),
    layout: normalizePlotLayout(figure.layout, minMargin),
  }
}

const buildAutoAnalysesFromPreview = (
  columns: string[],
  records: Array<Record<string, unknown>>
): VisualizationAnalysisCard[] => {
  if (!columns.length || !records.length) return []
  const numericColumns = columns.filter((col) =>
    records.some((row) => readFiniteNumber(row?.[col]) != null)
  )
  if (!numericColumns.length) return []

  const categoryColumns = columns.filter((col) => !numericColumns.includes(col))
  const xKey = categoryColumns[0] || columns[0]
  const yKey = numericColumns.find((col) => col !== xKey) || numericColumns[0]
  if (!xKey || !yKey) return []

  const grouped = new Map<string, { total: number; count: number; order: number }>()
  records.forEach((row, rowIndex) => {
    const xRaw = row?.[xKey]
    const x = String(xRaw ?? "").trim()
    if (!x) return
    const y = readFiniteNumber(row?.[yKey])
    if (y == null) return
    if (!grouped.has(x)) grouped.set(x, { total: 0, count: 0, order: rowIndex })
    const bucket = grouped.get(x)!
    bucket.total += y
    bucket.count += 1
  })
  let points = Array.from(grouped.entries()).map(([x, bucket]) => ({
    x,
    y: Number((bucket.count ? bucket.total / bucket.count : 0).toFixed(4)),
    order: bucket.order,
  }))
  if (!points.length) return []

  const numericX = points.every((point) => readFiniteNumber(point.x) != null)
  const dateX = !numericX && points.every((point) => readDateMs(point.x) != null)
  if (numericX) {
    points = points.sort((a, b) => Number(a.x) - Number(b.x))
  } else if (dateX) {
    points = points.sort((a, b) => Number(readDateMs(a.x)) - Number(readDateMs(b.x)))
  } else {
    points = points.sort((a, b) => a.order - b.order)
  }

  const limitedPoints = points.slice(0, 60)
  const piePoints = [...points].sort((a, b) => b.y - a.y).slice(0, 12)
  const lollipopPoints = [...points].sort((a, b) => b.y - a.y).slice(0, 20)
  const lollipopStemX = lollipopPoints.flatMap((point) => [point.x, point.x, null])
  const lollipopStemY = lollipopPoints.flatMap((point) => [0, point.y, null])
  const baseLayout = {
    autosize: true,
    margin: { l: 56, r: 24, t: 24, b: 56 },
    xaxis: { title: xKey },
    yaxis: { title: yKey },
    paper_bgcolor: "transparent",
    plot_bgcolor: "transparent",
  }

  return [
    {
      chart_spec: { chart_type: "line", x: xKey, y: yKey, agg: "avg" },
      reason: "자동 대안(추세 확인)",
      summary: "자동 대안 차트입니다.",
      figure_json: {
        data: [
          {
            type: "scatter",
            mode: "lines+markers",
            x: limitedPoints.map((point) => point.x),
            y: limitedPoints.map((point) => point.y),
            name: yKey,
          },
        ],
        layout: baseLayout,
      } as Record<string, unknown>,
    },
    {
      chart_spec: { chart_type: "lollipop", x: xKey, y: yKey, agg: "avg" },
      reason: "자동 대안(Graph Gallery 스타일 순위 비교)",
      summary: "자동 대안 차트입니다.",
      figure_json: {
        data: [
          {
            type: "scatter",
            mode: "lines",
            x: lollipopStemX,
            y: lollipopStemY,
            name: "stem",
            hoverinfo: "skip",
            showlegend: false,
            line: { shape: "linear", color: "rgba(100,116,139,0.5)", width: 2 },
          },
          {
            type: "scatter",
            mode: "markers",
            x: lollipopPoints.map((point) => point.x),
            y: lollipopPoints.map((point) => point.y),
            name: yKey,
            marker: { size: 9 },
          },
        ],
        layout: baseLayout,
      } as Record<string, unknown>,
    },
    {
      chart_spec: { chart_type: "pie", x: xKey, y: yKey, agg: "sum" },
      reason: "자동 대안(비율 비교)",
      summary: "자동 대안 차트입니다.",
      figure_json: {
        data: [
          {
            type: "pie",
            labels: piePoints.map((point) => point.x),
            values: piePoints.map((point) => point.y),
            name: yKey,
            hole: 0.32,
          },
        ],
        layout: {
          ...baseLayout,
          margin: { l: 36, r: 36, t: 24, b: 24 },
        },
      } as Record<string, unknown>,
    },
    {
      chart_spec: { chart_type: "bar", x: xKey, y: yKey, agg: "avg" },
      reason: "자동 대안(범주 비교)",
      summary: "자동 대안 차트입니다.",
      figure_json: {
        data: [
          {
            type: "bar",
            x: limitedPoints.map((point) => point.x),
            y: limitedPoints.map((point) => point.y),
            name: yKey,
          },
        ],
        layout: baseLayout,
      } as Record<string, unknown>,
    },
  ]
}

const collectCategoryTypeSummariesFromFigure = (
  figure: { data?: unknown[]; layout?: Record<string, unknown> } | null
): CategoryTypeSummary[] => {
  if (!figure || !Array.isArray(figure.data)) return []
  const counts = new Map<string, number>()
  const order: string[] = []
  for (const rawTrace of figure.data) {
    if (!isRecord(rawTrace)) continue
    const axisKey = getAxisKey(rawTrace)
    const axisValues = rawTrace[axisKey]
    if (!Array.isArray(axisValues) || !axisValues.length) continue
    if (inferAxisValueKind(axisValues) !== "categorical") continue
    for (const rawValue of axisValues) {
      const value = String(rawValue ?? "").trim()
      if (!value) continue
      if (!counts.has(value)) order.push(value)
      counts.set(value, (counts.get(value) || 0) + 1)
    }
  }
  return order.map((value) => ({
    value,
    occurrences: counts.get(value) || 0,
  }))
}

const filterFigureByCategories = (
  figure: { data?: unknown[]; layout?: Record<string, unknown> } | null,
  selectedCategories: string[] | null
): { data?: unknown[]; layout?: Record<string, unknown> } | null => {
  if (!figure || !selectedCategories?.length || !Array.isArray(figure.data)) return figure
  const allowed = new Set(selectedCategories)
  const filteredData = figure.data.map((rawTrace) => {
    if (!isRecord(rawTrace)) return rawTrace
    const trace = { ...rawTrace } as Record<string, unknown>
    const axisKey = getAxisKey(trace)
    const axisValues = trace[axisKey]
    if (!Array.isArray(axisValues) || !axisValues.length) return trace
    if (inferAxisValueKind(axisValues) !== "categorical") return trace
    const normalizedAxis = axisValues.map((value) => String(value ?? "").trim())
    const filteredIndexes: number[] = []
    for (let i = 0; i < normalizedAxis.length; i += 1) {
      if (allowed.has(normalizedAxis[i])) filteredIndexes.push(i)
    }
    return filterTraceByIndexes(trace, axisKey, axisValues, filteredIndexes)
  })

  return {
    ...figure,
    data: filteredData,
  }
}

const sanitizeResponse = (response: OneShotResponse | null): OneShotResponse | null => {
  if (!response) return null
  const payload = response.payload || ({} as OneShotPayload)
  const result = payload.result
    ? {
        ...payload.result,
        preview: trimPreview(payload.result.preview),
      }
    : undefined
  const draft = payload.draft ? { final_sql: payload.draft.final_sql } : undefined
  const final = payload.final
    ? {
        final_sql: payload.final.final_sql,
        risk_score: payload.final.risk_score,
        used_tables: payload.final.used_tables,
      }
    : undefined
  const policy = payload.policy
    ? {
        passed: payload.policy.passed,
        checks: Array.isArray(payload.policy.checks)
          ? payload.policy.checks.map((item) => ({
              name: String(item.name ?? ""),
              passed: Boolean(item.passed),
              message: String(item.message ?? ""),
            }))
          : [],
      }
    : undefined
  const clarification = payload.clarification
    ? {
        reason: payload.clarification.reason,
        question: payload.clarification.question,
        options: Array.isArray(payload.clarification.options)
          ? payload.clarification.options.map((item) => String(item))
          : [],
        example_inputs: Array.isArray(payload.clarification.example_inputs)
          ? payload.clarification.example_inputs.map((item) => String(item))
          : [],
      }
    : undefined
  return {
    qid: response.qid,
    payload: {
      mode: payload.mode,
      question: payload.question,
      assistant_message: payload.assistant_message ? String(payload.assistant_message) : undefined,
      result,
      risk: payload.risk,
      policy,
      draft,
      final,
      clarification,
    },
  }
}

const serializeMessages = (messages: ChatMessage[]): PersistedChatMessage[] =>
  messages.map((message) => ({
    ...message,
    timestamp: message.timestamp.toISOString(),
  }))

const pickSupportedRecordingMimeType = () => {
  if (typeof window === "undefined" || typeof window.MediaRecorder === "undefined") return ""
  const candidates = [
    "audio/webm;codecs=opus",
    "audio/webm",
    "audio/ogg;codecs=opus",
    "audio/ogg",
    "audio/mp4",
  ]
  const supported = candidates.find((item) => window.MediaRecorder.isTypeSupported(item))
  return supported || ""
}

const blobToDataUrl = (blob: Blob): Promise<string> =>
  new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => resolve(String(reader.result || ""))
    reader.onerror = () => reject(new Error("오디오 데이터를 읽지 못했습니다."))
    reader.readAsDataURL(blob)
  })

const formatVoiceElapsed = (elapsedMs: number) => {
  const safeMs = Number.isFinite(elapsedMs) ? Math.max(0, elapsedMs) : 0
  const totalSec = Math.floor(safeMs / 1000)
  const min = Math.floor(totalSec / 60)
  const sec = totalSec % 60
  return `${String(min)}:${String(sec).padStart(2, "0")}`
}

const buildVoiceWaveLevels = (base = VOICE_WAVE_BASELINE) =>
  Array.from({ length: VOICE_WAVE_BAR_COUNT }, () => base)

const normalizeChatModelOptions = (raw: string) => {
  const parsed = String(raw || "")
    .split(",")
    .map((item) => item.trim())
    .filter((item) => item.toLowerCase().startsWith("gpt-4"))
    .filter((item) => item.length > 0)
  const unique = Array.from(new Set([...parsed, ...FALLBACK_CHAT_MODELS]))
  return unique
}

export function QueryView() {
  const { user, isHydrated: isAuthHydrated } = useAuth()
  // In Docker runtime, API is reached via env-configured base URL or same-origin rewrites.
  const apiBaseUrl = (process.env.NEXT_PUBLIC_API_BASE_URL || "").replace(/\/$/, "")
  const apiUrl = (path: string) => (apiBaseUrl ? `${apiBaseUrl}${path}` : path)
  // Keep visualization on same-origin rewrite path to avoid bypassing /visualize proxy.
  const vizUrl = (path: string) => path
  const chatUser = (user?.name || "김연구원").trim() || "김연구원"
  const chatUserRole = (user?.role || "연구원").trim() || "연구원"
  const chatHistoryUser = (user?.id || user?.username || user?.name || chatUser).trim() || chatUser
  const apiUrlWithUser = (path: string) => {
    const base = apiUrl(path)
    if (!chatHistoryUser) return base
    const separator = base.includes("?") ? "&" : "?"
    return `${base}${separator}user=${encodeURIComponent(chatHistoryUser)}`
  }
  const pendingDashboardQueryKey = chatHistoryUser
    ? `ql_pending_dashboard_query:${chatHistoryUser}`
    : "ql_pending_dashboard_query"
  const pendingActiveCohortContextKey = scopedStorageKey(PENDING_ACTIVE_COHORT_CONTEXT_KEY, chatHistoryUser)
  const pendingLegacyPdfCohortContextKey = scopedStorageKey(LEGACY_PENDING_PDF_COHORT_CONTEXT_KEY, chatHistoryUser)
  const modelStorageKey = chatHistoryUser
    ? `ql_chat_model:${chatHistoryUser}`
    : "ql_chat_model"
  const modelOptions = useMemo(() => {
    const configured = normalizeChatModelOptions(process.env.NEXT_PUBLIC_LLM_MODEL_OPTIONS || "")
    return configured.length > 0 ? configured : FALLBACK_CHAT_MODELS
  }, [])
  const ONESHOT_TIMEOUT_MS = 90_000
  const RUN_TIMEOUT_MS = 190_000
  const VISUALIZE_TIMEOUT_MS = 120_000
  const VISUALIZE_MIN_ROWS = 2
  const ANSWER_TIMEOUT_MS = 35_000
  const ANSWER_MAX_ROWS = 120
  const ANSWER_MAX_COLS = 20
  const isNetworkFetchError = (error: unknown) => {
    const name = String((error as any)?.name || "")
    const message = String((error as any)?.message || "").toLowerCase()
    return (
      name === "TypeError" ||
      message.includes("failed to fetch") ||
      message.includes("networkerror") ||
      message.includes("load failed")
    )
  }
  const normalizeRequestErrorMessage = (error: unknown, fallbackMessage: string) => {
    const raw = String((error as any)?.message || "").trim()
    const lowered = raw.toLowerCase()
    if (!raw) return fallbackMessage
    if (
      lowered.includes("failed to fetch") ||
      lowered.includes("networkerror") ||
      lowered.includes("load failed") ||
      lowered.includes("network request failed")
    ) {
      return "API 서버 연결에 실패했습니다. 백엔드가 실행 중인지 확인하고 잠시 후 다시 시도해주세요."
    }
    if (
      lowered === "internal server error" ||
      lowered.includes("500 internal server error") ||
      lowered.includes("500: internal server error")
    ) {
      return "서버에서 요청을 처리하는 중 일시적인 오류가 발생했습니다. 잠시 후 다시 시도해주세요."
    }
    return raw
  }
  const isRetryableServerErrorMessage = (message: string) => {
    const lowered = String(message || "").toLowerCase()
    return (
      lowered.includes("internal server error") ||
      lowered.includes("socket hang up") ||
      lowered.includes("econnreset") ||
      lowered.includes("bad gateway") ||
      lowered.includes("gateway timeout") ||
      lowered.includes("service unavailable")
    )
  }
  const sleepMs = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))
  const resolveFetchRetryTargets = (input: RequestInfo): RequestInfo[] => {
    if (typeof input !== "string") return [input]
    const targets: RequestInfo[] = [input]
    if (apiBaseUrl && /^https?:\/\//i.test(input) && input.startsWith(apiBaseUrl)) {
      const relativePath = input.slice(apiBaseUrl.length)
      if (relativePath.startsWith("/")) {
        targets.push(relativePath)
      }
    }
    return targets
  }
  const fetchWithTimeout = async (input: RequestInfo, init: RequestInit = {}, timeoutMs = 45000) => {
    const targets = resolveFetchRetryTargets(input)
    let lastError: unknown = null
    for (const target of targets) {
      const controller = new AbortController()
      const timeout = setTimeout(() => controller.abort(), timeoutMs)
      try {
        return await fetch(target, { ...init, signal: controller.signal })
      } catch (error: any) {
        lastError = error
        const message = String(error?.message || "")
        const isAbort =
          error?.name === "AbortError" ||
          error?.name === "TimeoutError" ||
          /aborted/i.test(message)
        if (isAbort) {
          throw new Error("요청 시간이 초과되었습니다. 잠시 후 다시 시도해주세요.")
        }
        const shouldRetry = isNetworkFetchError(error) && target !== targets[targets.length - 1]
        if (shouldRetry) continue
      } finally {
        clearTimeout(timeout)
      }
    }
    if (isNetworkFetchError(lastError)) {
      throw new Error(
        "API 서버 연결에 실패했습니다. 백엔드가 실행 중인지 확인하고 잠시 후 다시 시도해주세요."
      )
    }
    throw lastError instanceof Error ? lastError : new Error("요청에 실패했습니다.")
  }
  const createClientRequestId = () => {
    if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
      return crypto.randomUUID()
    }
    return `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`
  }
  const sampleRowsForVisualization = (rows: any[][], maxRows: number) => {
    if (!Array.isArray(rows) || rows.length <= maxRows) return rows
    const sampled: any[][] = []
    const step = rows.length / maxRows
    for (let i = 0; i < maxRows; i += 1) {
      const idx = Math.min(rows.length - 1, Math.floor(i * step))
      sampled.push(rows[idx])
    }
    return sampled
  }
  const buildResultSummaryMessage = (totalRows: number | null, fetchedRows: number) => {
    return totalRows != null
      ? `쿼리를 실행했어요. 전체 결과는 ${totalRows}행입니다.`
      : `쿼리를 실행했어요. 미리보기로 ${fetchedRows}행을 가져왔습니다.`
  }
  const requestQueryAnswerMessage = async ({
    questionText,
    sqlText,
    previewData,
    totalRows,
    fetchedRows,
  }: {
    questionText: string
    sqlText: string
    previewData: PreviewData | null
    totalRows: number | null
    fetchedRows: number
  }): Promise<{ answerText: string; suggestedQuestions: string[] }> => {
    const fallback = buildResultSummaryMessage(totalRows, fetchedRows)
    const fallbackSuggestions = buildSuggestions(questionText, previewData?.columns)
    const question = String(questionText || "").trim()
    if (!question || !previewData?.columns?.length) {
      return { answerText: fallback, suggestedQuestions: fallbackSuggestions }
    }
    const columns = previewData.columns.slice(0, ANSWER_MAX_COLS)
    const sampledRows = sampleRowsForVisualization(previewData.rows || [], ANSWER_MAX_ROWS)
    const rows = sampledRows.map((row) => columns.map((_, idx) => row?.[idx] ?? null))
    try {
      const res = await fetchWithTimeout(apiUrl("/query/answer"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question,
          sql: sqlText,
          columns,
          rows,
          total_rows: totalRows,
          fetched_rows: fetchedRows,
          model: selectedModel || undefined,
        }),
      }, ANSWER_TIMEOUT_MS)
      if (!res.ok) return { answerText: fallback, suggestedQuestions: fallbackSuggestions }
      const data: QueryAnswerResponse = await res.json()
      const answer = String(data?.answer || "").trim()
      const suggestedQuestions = Array.isArray(data?.suggested_questions)
        ? Array.from(
            new Set(
              data.suggested_questions
                .map((item) => String(item || "").trim())
                .filter(Boolean)
            )
          ).slice(0, 3)
        : []
      return {
        answerText: answer || fallback,
        suggestedQuestions:
          suggestedQuestions.length > 0 ? suggestedQuestions : fallbackSuggestions,
      }
    } catch {
      return { answerText: fallback, suggestedQuestions: fallbackSuggestions }
    }
  }
  const [query, setQuery] = useState("")
  const [selectedModel, setSelectedModel] = useState(DEFAULT_CHAT_MODEL)
  const [isSpeechSupported, setIsSpeechSupported] = useState(false)
  const [isListening, setIsListening] = useState(false)
  const [isVoiceModeOn, setIsVoiceModeOn] = useState(false)
  const [isVoiceTranscribing, setIsVoiceTranscribing] = useState(false)
  const [voiceElapsedMs, setVoiceElapsedMs] = useState(0)
  const [voiceWaveLevels, setVoiceWaveLevels] = useState<number[]>(() => buildVoiceWaveLevels())
  const [voiceInterimText, setVoiceInterimText] = useState("")
  const [isLoading, setIsLoading] = useState(false)
  const [loadingProgress, setLoadingProgress] = useState(0)
  const [showResults, setShowResults] = useState(false)
  const [showSqlPanel, setShowSqlPanel] = useState(false)
  const [showQueryResultPanel, setShowQueryResultPanel] = useState(false)
  const [editedSql, setEditedSql] = useState("")
  const [isEditing, setIsEditing] = useState(false)
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [response, setResponse] = useState<OneShotResponse | null>(null)
  const [runResult, setRunResult] = useState<RunResponse | null>(null)
  const [visualizationResult, setVisualizationResult] = useState<VisualizationResponsePayload | null>(null)
  const [visualizationLoading, setVisualizationLoading] = useState(false)
  const [visualizationError, setVisualizationError] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [resultTabs, setResultTabs] = useState<ResultTabState[]>([])
  const [activeTabId, setActiveTabId] = useState<string>("")
  const [boardSaving, setBoardSaving] = useState(false)
  const [boardMessage, setBoardMessage] = useState<string | null>(null)
  const [copiedMessageId, setCopiedMessageId] = useState<string | null>(null)
  const [isSqlCopied, setIsSqlCopied] = useState(false)
  const [isSaveDialogOpen, setIsSaveDialogOpen] = useState(false)
  const [isPastQueriesDialogOpen, setIsPastQueriesDialogOpen] = useState(false)
  const [saveTitle, setSaveTitle] = useState("")
  const [saveFolderMode, setSaveFolderMode] = useState<"existing" | "new">("existing")
  const [saveFolderId, setSaveFolderId] = useState<string>("")
  const [saveNewFolderName, setSaveNewFolderName] = useState("")
  const [saveFolderOptions, setSaveFolderOptions] = useState<DashboardFolderOption[]>([])
  const [saveFoldersLoading, setSaveFoldersLoading] = useState(false)
  const [lastQuestion, setLastQuestion] = useState<string>("")
  const [suggestedQuestions, setSuggestedQuestions] = useState<string[]>([])
  const [activeCohortContext, setActiveCohortContext] = useState<ActiveCohortContext | null>(null)
  const [savedCohorts, setSavedCohorts] = useState<SavedCohort[]>([])
  const [isCohortLibraryLoading, setIsCohortLibraryLoading] = useState(false)
  const [isCohortLibraryOpen, setIsCohortLibraryOpen] = useState(false)
  const [pdfContextBanner, setPdfContextBanner] = useState<PdfContextBannerState>({
    context: null,
    hasShownTableOnce: true,
  })
  const [quickQuestions, setQuickQuestions] = useState<string[]>(DEFAULT_QUICK_QUESTIONS)
  const [isHydrated, setIsHydrated] = useState(false)
  const [isSqlDragging, setIsSqlDragging] = useState(false)
  const [isDesktopLayout, setIsDesktopLayout] = useState(false)
  const [resultsPanelWidth, setResultsPanelWidth] = useState(55)
  const [isPanelResizing, setIsPanelResizing] = useState(false)
  const [visibleTabLimit, setVisibleTabLimit] = useState(3)
  const [selectedStatsBoxColumn, setSelectedStatsBoxColumn] = useState<string>("")
  const [showStatsBoxPlot, setShowStatsBoxPlot] = useState(false)
  const [statsBoxValueLabel, setStatsBoxValueLabel] = useState("박스플롯 컬럼 선택")
  const [statsBoxTriggerWidth, setStatsBoxTriggerWidth] = useState(BOX_PLOT_TRIGGER_MIN_W)
  const [isChartCategoryPickerOpen, setIsChartCategoryPickerOpen] = useState(false)
  const [selectedChartCategoryValues, setSelectedChartCategoryValues] = useState<string[]>([])
  const [selectedChartCategoryCount, setSelectedChartCategoryCount] = useState<string>(String(CHART_CATEGORY_DEFAULT_COUNT))
  const [draftChartCategoryValues, setDraftChartCategoryValues] = useState<string[]>([])
  const [draftChartCategoryCount, setDraftChartCategoryCount] = useState<string>(String(CHART_CATEGORY_DEFAULT_COUNT))
  const [selectedAnalysisIndex, setSelectedAnalysisIndex] = useState(0)
  const [recommendedRenderMode, setRecommendedRenderMode] = useState<"interactive" | "seaborn">("interactive")
  const [isVisualizationZoomOpen, setIsVisualizationZoomOpen] = useState(false)
  const saveTimerRef = useRef<number | null>(null)
  const boardMessageTimerRef = useRef<number | null>(null)
  const messageCopyTimerRef = useRef<number | null>(null)
  const sqlCopyTimerRef = useRef<number | null>(null)
  const mediaRecorderRef = useRef<MediaRecorder | null>(null)
  const mediaStreamRef = useRef<MediaStream | null>(null)
  const recordedChunksRef = useRef<Blob[]>([])
  const voiceStopActionRef = useRef<"cancel" | "transcribe">("cancel")
  const voiceStartedAtRef = useRef<number | null>(null)
  const voiceTimerRef = useRef<number | null>(null)
  const voiceWaveRafRef = useRef<number | null>(null)
  const voiceAudioContextRef = useRef<AudioContext | null>(null)
  const voiceAnalyserRef = useRef<AnalyserNode | null>(null)
  const voiceSourceRef = useRef<MediaStreamAudioSourceNode | null>(null)
  const voiceWaveBytesRef = useRef<Uint8Array | null>(null)
  const voiceWaveLastUpdateRef = useRef(0)
  const mainContentRef = useRef<HTMLDivElement | null>(null)
  const chatScrollRef = useRef<HTMLDivElement | null>(null)
  const lastAutoScrolledMessageIdRef = useRef<string>("")
  const tabHeaderRef = useRef<HTMLDivElement | null>(null)
  const sqlScrollRef = useRef<HTMLDivElement | null>(null)
  const sqlDragRef = useRef({
    active: false,
    startX: 0,
    startY: 0,
    scrollLeft: 0,
    scrollTop: 0,
  })
  const panelResizeRef = useRef({
    active: false,
    startX: 0,
    startRightWidth: 55,
    containerWidth: 0,
  })
  const requestTokenRef = useRef(0)
  const runQueryInFlightRef = useRef(false)
  const activeTabIdRef = useRef("")
  const activeCohortContextRef = useRef<ActiveCohortContext | null>(null)
  const statsBoxMeasureRef = useRef<HTMLSpanElement | null>(null)

  const clearBoardMessageTimer = () => {
    if (boardMessageTimerRef.current !== null) {
      window.clearTimeout(boardMessageTimerRef.current)
      boardMessageTimerRef.current = null
    }
  }

  const showTransientBoardMessage = (
    message: string,
    durationMs: number,
    options?: { closeSaveDialog?: boolean }
  ) => {
    clearBoardMessageTimer()
    setBoardMessage(message)
    boardMessageTimerRef.current = window.setTimeout(() => {
      let shouldCloseDialog = false
      setBoardMessage((current) => {
        if (current === message) {
          shouldCloseDialog = Boolean(options?.closeSaveDialog)
          return null
        }
        return current
      })
      if (shouldCloseDialog) {
        setIsSaveDialogOpen(false)
      }
      boardMessageTimerRef.current = null
    }, durationMs)
  }

  useEffect(() => {
    activeCohortContextRef.current = activeCohortContext
  }, [activeCohortContext])

  useEffect(() => {
    if (!isLoading) {
      setLoadingProgress(0)
      return
    }
    setLoadingProgress(12)
    const intervalId = window.setInterval(() => {
      setLoadingProgress((prev) => {
        if (prev >= 92) return 92
        const step = prev < 60 ? 8 : prev < 80 ? 4 : 2
        return Math.min(92, prev + step)
      })
    }, 550)
    return () => window.clearInterval(intervalId)
  }, [isLoading])

  const payload = response?.payload
  const mode = payload?.mode
  const demoResult = mode === "demo" ? payload?.result : null
  const currentSql =
    (mode === "demo"
      ? demoResult?.sql
      : payload?.final?.final_sql || payload?.draft?.final_sql) || ""
  const riskScore = payload?.final?.risk_score ?? payload?.risk?.risk
  const riskIntent = payload?.risk?.intent
  const preview = runResult?.result ?? demoResult?.preview ?? null
  const previewColumns = preview?.columns ?? []
  const previewRows = preview?.rows ?? []
  const previewRowCount = preview?.row_count ?? previewRows.length
  const hasInsufficientRowsForVisualization = previewRowCount < VISUALIZE_MIN_ROWS
  const previewRowCap = preview?.row_cap
  const previewTotalCount =
    typeof preview?.total_count === "number" && Number.isFinite(preview.total_count)
      ? preview.total_count
      : null
  const effectiveTotalRows = previewTotalCount ?? previewRowCount
  const survivalChartData = buildSurvivalFromPreview(previewColumns, previewRows)
  const totalPatients = survivalChartData?.length
    ? Math.max(...survivalChartData.map((item) => item.atRisk)) || previewRowCount
    : previewRowCount
  const totalEvents = survivalChartData?.length
    ? Math.max(...survivalChartData.map((item) => item.events))
    : 0
  const medianSurvival = (() => {
    if (!survivalChartData?.length) return 0
    const sorted = [...survivalChartData].sort((a, b) => a.time - b.time)
    const hit = sorted.find((item) => item.survival <= 50)
    return hit?.time ?? sorted[sorted.length - 1]?.time ?? 0
  })()
  const summary = demoResult?.summary
  const source = demoResult?.source
  const previewRecords = useMemo(
    () =>
      previewRows.map((row) =>
        Object.fromEntries(previewColumns.map((col, idx) => [col, row?.[idx]]))
      ),
    [previewColumns, previewRows]
  )
  const statsRows = useMemo(() => buildSimpleStats(previewColumns, previewRows), [previewColumns, previewRows])
  const boxPlotEligibleRows = useMemo(
    () =>
      statsRows.filter((row) => {
        const required = [row.min, row.q1, row.median, row.q3, row.max, row.avg]
        return row.numericCount > 0 && required.every((value) => typeof value === "number" && Number.isFinite(value))
      }),
    [statsRows]
  )
  const displaySql = (isEditing ? editedSql : runResult?.sql || currentSql) || ""
  useEffect(() => {
    setIsSqlCopied(false)
  }, [displaySql])
  const activeTab = useMemo(
    () => resultTabs.find((item) => item.id === activeTabId) || null,
    [resultTabs, activeTabId]
  )
  const preferredDashboardChartType = activeTab?.preferredChartType || null
  const topRecommendedAnalyses = useMemo(() => {
    const analyses = Array.isArray(visualizationResult?.analyses)
      ? visualizationResult.analyses.filter((item) => {
          const hasFigure = isRecord(item?.figure_json)
          const imageDataUrl = String(item?.image_data_url || "")
          const hasImage = imageDataUrl.startsWith("data:image/")
          return hasFigure || hasImage
        })
      : []
    const ranked = [...analyses]
    const preferredIndex = ranked.findIndex((item) =>
      isPreferredChartType(item?.chart_spec?.chart_type, preferredDashboardChartType)
    )
    if (preferredIndex > 0) {
      const [preferred] = ranked.splice(preferredIndex, 1)
      ranked.unshift(preferred)
    }
    const top: VisualizationAnalysisCard[] = []
    const selectedIndexes = new Set<number>()
    const existingTypes = new Set<string>()
    const pushByIndex = (index: number) => {
      if (index < 0 || index >= ranked.length) return
      if (selectedIndexes.has(index)) return
      const item = ranked[index]
      const chartType = normalizeChartType(item?.chart_spec?.chart_type)
      if (chartType && existingTypes.has(chartType)) return
      top.push(item)
      selectedIndexes.add(index)
      if (chartType) existingTypes.add(chartType)
    }

    if (ranked.length) pushByIndex(0)

    const varietyPriority = [
      "treemap",
      "confusion_matrix",
      "heatmap",
      "lollipop",
      "line_scatter",
      "line",
      "area",
      "bar_grouped",
      "bar_stacked",
      "bar_hstack",
      "bar_hgroup",
      "pyramid",
      "bar_basic",
      "bar",
      "pie",
      "nested_pie",
      "sunburst",
      "dynamic_scatter",
      "scatter",
      "hist",
      "violin",
      "box",
    ]
    for (const chartType of varietyPriority) {
      if (top.length >= 3) break
      const idx = ranked.findIndex(
        (item, index) =>
          !selectedIndexes.has(index) &&
          normalizeChartType(item?.chart_spec?.chart_type) === chartType
      )
      if (idx >= 0) pushByIndex(idx)
    }

    for (let index = 0; index < ranked.length && top.length < 3; index += 1) {
      pushByIndex(index)
    }

    for (const autoItem of buildAutoAnalysesFromPreview(previewColumns, previewRecords as Array<Record<string, unknown>>)) {
      if (top.length >= 3) break
      const autoType = normalizeChartType(autoItem?.chart_spec?.chart_type)
      if (autoType && existingTypes.has(autoType)) continue
      top.push(autoItem)
      if (autoType) existingTypes.add(autoType)
    }
    return top.slice(0, 3)
  }, [visualizationResult, preferredDashboardChartType, previewColumns, previewRecords])
  useEffect(() => {
    setSelectedAnalysisIndex(0)
  }, [activeTabId, visualizationResult, preferredDashboardChartType])
  useEffect(() => {
    if (!topRecommendedAnalyses.length) {
      if (selectedAnalysisIndex !== 0) setSelectedAnalysisIndex(0)
      return
    }
    if (selectedAnalysisIndex >= topRecommendedAnalyses.length) {
      setSelectedAnalysisIndex(0)
    }
  }, [topRecommendedAnalyses, selectedAnalysisIndex])
  const recommendedAnalysis = useMemo(() => {
    if (!topRecommendedAnalyses.length) return null
    return topRecommendedAnalyses[selectedAnalysisIndex] || topRecommendedAnalyses[0] || null
  }, [topRecommendedAnalyses, selectedAnalysisIndex])
  const recommendedFigure = useMemo(() => {
    const fig = recommendedAnalysis?.figure_json
    if (fig && typeof fig === "object") return fig as { data?: unknown[]; layout?: Record<string, unknown> }
    return null
  }, [recommendedAnalysis])
  const recommendedImageDataUrl = useMemo(() => {
    const raw = String(recommendedAnalysis?.image_data_url || "").trim()
    return raw.startsWith("data:image/") ? raw : null
  }, [recommendedAnalysis])
  const hasRecommendedSeabornImage = Boolean(recommendedImageDataUrl)
  const recommendedRenderEngine = useMemo(() => {
    const raw = String(recommendedAnalysis?.render_engine || "").trim().toLowerCase()
    if (!raw) return null
    if (raw === "seaborn") return "SEABORN"
    if (raw === "plotly") return "PLOTLY"
    return raw.toUpperCase()
  }, [recommendedAnalysis])
  const selectedXAxisDomain = useMemo(
    () =>
      buildAxisDomainFromRecords(
        previewRecords as Array<Record<string, unknown>>,
        recommendedAnalysis?.chart_spec?.x
      ),
    [previewRecords, recommendedAnalysis]
  )
  const axisSyncedRecommendedFigure = useMemo(() => {
    if (isPyramidChartType(recommendedAnalysis?.chart_spec?.chart_type)) {
      return recommendedFigure
    }
    return clampFigureToAxisDomain(recommendedFigure, selectedXAxisDomain)
  }, [recommendedFigure, selectedXAxisDomain, recommendedAnalysis?.chart_spec?.chart_type])
  const recommendedChart = useMemo(() => {
    const spec = recommendedAnalysis?.chart_spec
    if (!spec || !previewRecords.length) return null
    const chartType = normalizeChartType(spec.chart_type || "bar")
    const xKey = spec.x && previewColumns.includes(spec.x) ? spec.x : previewColumns[0]
    const candidateY = spec.y && previewColumns.includes(spec.y) ? spec.y : previewColumns.find((col) => {
      const v = previewRecords[0]?.[col]
      return Number.isFinite(Number(v))
    })

    if (!xKey) return null

    if (chartType === "scatter" && candidateY) {
      const points = previewRecords
        .map((row) => ({
          x: Number(row[xKey]),
          y: Number(row[candidateY]),
        }))
        .filter((p) => Number.isFinite(p.x) && Number.isFinite(p.y))
      return { type: "scatter" as const, xKey, yKey: candidateY, data: points }
    }

    const grouped = new Map<string, { total: number; count: number }>()
    for (const row of previewRecords) {
      const key = String(row[xKey] ?? "")
      if (!grouped.has(key)) grouped.set(key, { total: 0, count: 0 })
      const bucket = grouped.get(key)!
      if (candidateY) {
        const num = Number(row[candidateY])
        if (Number.isFinite(num)) {
          bucket.total += num
          bucket.count += 1
        }
      } else {
        bucket.count += 1
      }
    }
    const agg = String(spec.agg || (candidateY ? "avg" : "count")).toLowerCase()
    const data = Array.from(grouped.entries()).map(([x, v]) => {
      const y =
        !candidateY || agg === "count"
          ? v.count
          : agg === "sum"
            ? v.total
            : v.count > 0
              ? v.total / v.count
              : 0
      return { x, y: Number(y.toFixed(4)) }
    })
    return {
      type: chartType === "line" ? ("line" as const) : ("bar" as const),
      xKey,
      yKey: candidateY || "count",
      data,
    }
  }, [recommendedAnalysis, previewColumns, previewRecords])
  const fallbackChart = useMemo(() => {
    if (!previewColumns.length || !previewRecords.length) return null

    const numericColumns = previewColumns.filter((col) =>
      previewRecords.some((row) => Number.isFinite(Number(row?.[col])))
    )
    if (!numericColumns.length) return null

    const categoryColumns = previewColumns.filter((col) => !numericColumns.includes(col))
    const xKey = categoryColumns[0] || previewColumns[0]
    const yKey = numericColumns.find((col) => col !== xKey) || numericColumns[0]
    if (!xKey || !yKey) return null

    const grouped = new Map<string, { total: number; count: number }>()
    for (const row of previewRecords) {
      const key = String(row?.[xKey] ?? "")
      const yValue = Number(row?.[yKey])
      if (!Number.isFinite(yValue)) continue
      if (!grouped.has(key)) grouped.set(key, { total: 0, count: 0 })
      const item = grouped.get(key)!
      item.total += yValue
      item.count += 1
    }

    const data = Array.from(grouped.entries())
      .map(([x, value]) => ({
        x,
        y: Number((value.count ? value.total / value.count : 0).toFixed(4)),
      }))
      .filter((item) => item.x.length > 0)

    if (!data.length) return null
    return {
      type: "bar" as const,
      xKey,
      yKey,
      data: data.slice(0, 50),
    }
  }, [previewColumns, previewRecords])
  const chartForRender = recommendedChart || fallbackChart
  const pyramidFallbackFigure = useMemo(() => {
    const spec = recommendedAnalysis?.chart_spec
    if (!isPyramidChartType(spec?.chart_type) || !previewRecords.length) return null
    const xKey = spec?.x && previewColumns.includes(spec.x) ? spec.x : previewColumns[0]
    if (!xKey) return null

    const agg = String(spec?.agg || "sum").toLowerCase()
    const shouldAverage = agg === "avg" || agg === "mean"
    const yKey = spec?.y && previewColumns.includes(spec.y) ? spec.y : ""

    const buildFigure = (
      categories: string[],
      leftValues: number[],
      rightValues: number[],
      leftName: string,
      rightName: string
    ) => {
      if (!categories.length) return null
      const maxAbs = Math.max(
        1,
        ...leftValues.map((value) => Math.abs(value)),
        ...rightValues.map((value) => Math.abs(value))
      )
      const tickValues = [-maxAbs, -maxAbs / 2, 0, maxAbs / 2, maxAbs]
      const tickText = tickValues.map((value) =>
        Math.abs(value) >= 1000 ? Math.round(Math.abs(value)).toLocaleString() : Math.abs(value).toFixed(1)
      )
      const leftAbs = leftValues.map((value) => Math.abs(value))
      const rightAbs = rightValues.map((value) => Math.abs(value))
      return {
        data: [
          {
            type: "bar",
            orientation: "h",
            y: categories,
            x: leftAbs.map((value) => -value),
            name: leftName,
            marker: { color: "#2563eb" },
            customdata: leftAbs,
            textposition: "none",
            hovertemplate: `${xKey}: %{y}<br>${leftName}: %{customdata:,.3f}<extra></extra>`,
          },
          {
            type: "bar",
            orientation: "h",
            y: categories,
            x: rightAbs,
            name: rightName,
            marker: { color: "#ef4444" },
            customdata: rightAbs,
            textposition: "none",
            hovertemplate: `${xKey}: %{y}<br>${rightName}: %{customdata:,.3f}<extra></extra>`,
          },
        ],
        layout: {
          autosize: true,
          barmode: "relative",
          bargap: 0.22,
          margin: { l: 88, r: 28, t: 22, b: 44 },
          xaxis: {
            title: yKey || "value",
            tickvals: tickValues,
            ticktext: tickText,
            zeroline: true,
            zerolinecolor: "rgba(148,163,184,0.45)",
          },
          yaxis: {
            title: xKey,
            automargin: true,
            categoryorder: "array",
            categoryarray: categories,
          },
          hovermode: "y unified",
          legend: { orientation: "h", x: 0, y: 1.04 },
          paper_bgcolor: "transparent",
          plot_bgcolor: "transparent",
        },
      }
    }

    const groupKey = spec?.group && previewColumns.includes(spec.group) ? spec.group : ""
    if (groupKey) {
      const categoryOrder: string[] = []
      const grouped = new Map<
        string,
        Map<
          string,
          {
            total: number
            count: number
          }
        >
      >()
      const groupTotals = new Map<string, number>()
      for (const row of previewRecords) {
        const category = String(row?.[xKey] ?? "").trim()
        const group = String(row?.[groupKey] ?? "").trim()
        if (!category || !group) continue
        if (!grouped.has(category)) {
          grouped.set(category, new Map())
          categoryOrder.push(category)
        }
        const value = agg === "count" ? 1 : readFiniteNumber(row?.[yKey])
        if (value == null) continue
        const groupMap = grouped.get(category)!
        const bucket = groupMap.get(group) || { total: 0, count: 0 }
        bucket.total += value
        bucket.count += 1
        groupMap.set(group, bucket)
        groupTotals.set(group, (groupTotals.get(group) || 0) + Math.abs(value))
      }

      const groupOrder = Array.from(groupTotals.entries())
        .sort((a, b) => b[1] - a[1])
        .map(([name]) => name)
      if (groupOrder.length >= 2) {
        const leftName = groupOrder[0]
        const rightName = groupOrder[1]
        const categories: string[] = []
        const leftValues: number[] = []
        const rightValues: number[] = []
        for (const category of categoryOrder) {
          const groupMap = grouped.get(category)
          if (!groupMap) continue
          const leftBucket = groupMap.get(leftName)
          const rightBucket = groupMap.get(rightName)
          const leftValue =
            leftBucket == null
              ? 0
              : shouldAverage
                ? leftBucket.count > 0
                  ? leftBucket.total / leftBucket.count
                  : 0
                : leftBucket.total
          const rightValue =
            rightBucket == null
              ? 0
              : shouldAverage
                ? rightBucket.count > 0
                  ? rightBucket.total / rightBucket.count
                  : 0
                : rightBucket.total
          if (Math.abs(leftValue) < 1e-9 && Math.abs(rightValue) < 1e-9) continue
          categories.push(category)
          leftValues.push(leftValue)
          rightValues.push(rightValue)
        }
        const figure = buildFigure(categories, leftValues, rightValues, leftName, rightName)
        if (figure) return figure
      }
    }

    const numericColumns = previewColumns.filter((column) =>
      previewRecords.some((row) => readFiniteNumber(row?.[column]) != null)
    )
    const leftMetric = yKey && numericColumns.includes(yKey) ? yKey : numericColumns[0]
    const rightMetric = numericColumns.find((column) => column !== leftMetric && column !== xKey)
    if (!leftMetric || !rightMetric) return null

    const categoryOrder: string[] = []
    const buckets = new Map<
      string,
      {
        leftTotal: number
        leftCount: number
        rightTotal: number
        rightCount: number
      }
    >()
    for (const row of previewRecords) {
      const category = String(row?.[xKey] ?? "").trim()
      if (!category) continue
      const leftValue = readFiniteNumber(row?.[leftMetric])
      const rightValue = readFiniteNumber(row?.[rightMetric])
      if (leftValue == null && rightValue == null) continue
      if (!buckets.has(category)) {
        buckets.set(category, { leftTotal: 0, leftCount: 0, rightTotal: 0, rightCount: 0 })
        categoryOrder.push(category)
      }
      const bucket = buckets.get(category)!
      if (leftValue != null) {
        bucket.leftTotal += leftValue
        bucket.leftCount += 1
      }
      if (rightValue != null) {
        bucket.rightTotal += rightValue
        bucket.rightCount += 1
      }
    }

    const categories: string[] = []
    const leftValues: number[] = []
    const rightValues: number[] = []
    for (const category of categoryOrder) {
      const bucket = buckets.get(category)
      if (!bucket) continue
      const leftValue = shouldAverage
        ? bucket.leftCount > 0
          ? bucket.leftTotal / bucket.leftCount
          : 0
        : bucket.leftTotal
      const rightValue = shouldAverage
        ? bucket.rightCount > 0
          ? bucket.rightTotal / bucket.rightCount
          : 0
        : bucket.rightTotal
      if (Math.abs(leftValue) < 1e-9 && Math.abs(rightValue) < 1e-9) continue
      categories.push(category)
      leftValues.push(leftValue)
      rightValues.push(rightValue)
    }

    return buildFigure(categories, leftValues, rightValues, leftMetric, rightMetric)
  }, [recommendedAnalysis, previewColumns, previewRecords])
  const localFallbackFigure = useMemo(() => {
    if (pyramidFallbackFigure) return pyramidFallbackFigure
    if (!chartForRender?.data?.length) return null

    if (chartForRender.type === "scatter") {
      return {
        data: [
          {
            type: "scatter",
            mode: "markers",
            x: chartForRender.data.map((p) => p.x),
            y: chartForRender.data.map((p) => p.y),
            name: chartForRender.yKey,
            marker: { color: "#3b82f6", size: 8, opacity: 0.85 },
          },
        ],
        layout: {
          autosize: true,
          margin: { l: 52, r: 24, t: 20, b: 56 },
          xaxis: { title: chartForRender.xKey },
          yaxis: { title: chartForRender.yKey },
          paper_bgcolor: "transparent",
          plot_bgcolor: "transparent",
        },
      }
    }

    return {
      data: [
        {
          type: chartForRender.type === "line" ? "scatter" : "bar",
          mode: chartForRender.type === "line" ? "lines+markers" : undefined,
          x: chartForRender.data.map((p) => p.x),
          y: chartForRender.data.map((p) => p.y),
          name: chartForRender.yKey,
          marker: { color: chartForRender.type === "line" ? "#10b981" : "#3b82f6" },
          line: chartForRender.type === "line" ? { color: "#10b981", width: 2 } : undefined,
          text: chartForRender.type === "bar" ? chartForRender.data.map((p) => p.y) : undefined,
          textposition: chartForRender.type === "bar" ? "outside" : undefined,
        },
      ],
      layout: {
        autosize: true,
        margin: { l: 52, r: 24, t: 20, b: 56 },
        xaxis: { title: chartForRender.xKey },
        yaxis: { title: chartForRender.yKey },
        paper_bgcolor: "transparent",
        plot_bgcolor: "transparent",
      },
    }
  }, [chartForRender, pyramidFallbackFigure])
  const activeVisualizationFigure = axisSyncedRecommendedFigure || localFallbackFigure
  const chartCategorySummaries = useMemo(
    () => collectCategoryTypeSummariesFromFigure(activeVisualizationFigure),
    [activeVisualizationFigure]
  )
  const chartCategories = useMemo(
    () => chartCategorySummaries.map((item) => item.value),
    [chartCategorySummaries]
  )
  const isChartCategoryPickerEnabled =
    selectedXAxisDomain?.kind === "categorical" && chartCategories.length > CHART_CATEGORY_THRESHOLD
  const chartCategoryCountOptions = useMemo(() => {
    if (!isChartCategoryPickerEnabled) return []
    const total = chartCategories.length
    const presetCounts = Array.from(new Set([10, 20, 30, 50].filter((count) => count < total)))
    return [
      ...presetCounts.map((count) => ({ value: String(count), label: `${count}개` })),
      { value: "all", label: `전체 (${total})` },
    ]
  }, [isChartCategoryPickerEnabled, chartCategories])
  const defaultChartCategoryCount = Math.min(CHART_CATEGORY_DEFAULT_COUNT, chartCategories.length)
  const defaultChartCategorySelection = useMemo(
    () => chartCategories.slice(0, defaultChartCategoryCount),
    [chartCategories, defaultChartCategoryCount]
  )
  const applyChartCategoryCountSelection = (nextValue: string) => {
    setDraftChartCategoryCount(nextValue)
    if (!isChartCategoryPickerEnabled) return
    if (nextValue === "all") {
      setDraftChartCategoryValues(chartCategories)
      return
    }
    const parsed = Number(nextValue)
    const count = Number.isFinite(parsed)
      ? Math.max(1, Math.min(chartCategories.length, parsed))
      : Math.min(CHART_CATEGORY_DEFAULT_COUNT, chartCategories.length)
    setDraftChartCategoryValues(chartCategories.slice(0, count))
  }
  const toggleChartCategoryValue = (category: string, checked: boolean) => {
    if (!isChartCategoryPickerEnabled) return
    setDraftChartCategoryValues((previous) => {
      const fallback = defaultChartCategorySelection
      const base = previous.length ? previous : fallback
      const nextSet = new Set(base)
      if (checked) {
        nextSet.add(category)
      } else {
        if (nextSet.size === 1 && nextSet.has(category)) return base
        nextSet.delete(category)
      }
      return chartCategories.filter((value) => nextSet.has(value))
    })
  }
  useEffect(() => {
    if (!isChartCategoryPickerEnabled) {
      if (selectedChartCategoryValues.length) setSelectedChartCategoryValues([])
      if (selectedChartCategoryCount !== String(CHART_CATEGORY_DEFAULT_COUNT)) {
        setSelectedChartCategoryCount(String(CHART_CATEGORY_DEFAULT_COUNT))
      }
      if (draftChartCategoryValues.length) setDraftChartCategoryValues([])
      if (draftChartCategoryCount !== String(CHART_CATEGORY_DEFAULT_COUNT)) {
        setDraftChartCategoryCount(String(CHART_CATEGORY_DEFAULT_COUNT))
      }
      if (isChartCategoryPickerOpen) setIsChartCategoryPickerOpen(false)
      return
    }
    const validSet = new Set(chartCategories)
    const normalizedSelected = selectedChartCategoryValues.filter((value) => validSet.has(value))
    if (normalizedSelected.length !== selectedChartCategoryValues.length) {
      setSelectedChartCategoryValues(normalizedSelected)
      return
    }
    if (!normalizedSelected.length) {
      setSelectedChartCategoryValues(defaultChartCategorySelection)
      if (selectedChartCategoryCount !== String(defaultChartCategoryCount)) {
        setSelectedChartCategoryCount(String(defaultChartCategoryCount))
      }
      return
    }
    if (
      selectedChartCategoryCount !== "all" &&
      !chartCategoryCountOptions.some((option) => option.value === selectedChartCategoryCount)
    ) {
      setSelectedChartCategoryCount(String(defaultChartCategoryCount))
    }
  }, [
    isChartCategoryPickerEnabled,
    chartCategories,
    selectedChartCategoryValues,
    selectedChartCategoryCount,
    chartCategoryCountOptions,
    defaultChartCategoryCount,
    defaultChartCategorySelection,
    isChartCategoryPickerOpen,
    draftChartCategoryValues,
    draftChartCategoryCount,
  ])
  useEffect(() => {
    if (!isChartCategoryPickerOpen) return
    const nextDraftValues = selectedChartCategoryValues.length
      ? selectedChartCategoryValues
      : defaultChartCategorySelection
    setDraftChartCategoryValues(nextDraftValues)
    const countIsValid =
      selectedChartCategoryCount === "all" ||
      chartCategoryCountOptions.some((option) => option.value === selectedChartCategoryCount)
    setDraftChartCategoryCount(countIsValid ? selectedChartCategoryCount : String(defaultChartCategoryCount))
  }, [
    isChartCategoryPickerOpen,
    selectedChartCategoryValues,
    selectedChartCategoryCount,
    chartCategoryCountOptions,
    defaultChartCategorySelection,
    defaultChartCategoryCount,
  ])
  const effectiveSelectedChartCategories = useMemo(() => {
    if (!isChartCategoryPickerEnabled) return null
    if (selectedChartCategoryValues.length) return selectedChartCategoryValues
    return defaultChartCategorySelection
  }, [isChartCategoryPickerEnabled, selectedChartCategoryValues, defaultChartCategorySelection])
  const effectiveDraftChartCategories = useMemo(() => {
    if (!isChartCategoryPickerEnabled) return null
    if (draftChartCategoryValues.length) return draftChartCategoryValues
    return defaultChartCategorySelection
  }, [isChartCategoryPickerEnabled, draftChartCategoryValues, defaultChartCategorySelection])
  const selectedChartCategorySet = useMemo(
    () => new Set(effectiveDraftChartCategories || []),
    [effectiveDraftChartCategories]
  )
  const handleApplyChartCategorySelection = () => {
    if (!isChartCategoryPickerEnabled) {
      setIsChartCategoryPickerOpen(false)
      return
    }
    const nextSelected = effectiveDraftChartCategories?.length
      ? effectiveDraftChartCategories
      : defaultChartCategorySelection
    setSelectedChartCategoryValues(nextSelected)

    const draftCountIsValid =
      draftChartCategoryCount === "all" ||
      chartCategoryCountOptions.some((option) => option.value === draftChartCategoryCount)
    const hasMatchingCountOption = chartCategoryCountOptions.some(
      (option) => option.value === String(nextSelected.length)
    )
    if (nextSelected.length === chartCategories.length) {
      setSelectedChartCategoryCount("all")
    } else if (draftCountIsValid && draftChartCategoryCount !== "all") {
      setSelectedChartCategoryCount(draftChartCategoryCount)
    } else if (hasMatchingCountOption) {
      setSelectedChartCategoryCount(String(nextSelected.length))
    } else {
      setSelectedChartCategoryCount(String(defaultChartCategoryCount))
    }

    setIsChartCategoryPickerOpen(false)
  }
  const filteredRecommendedFigure = useMemo(
    () => filterFigureByCategories(axisSyncedRecommendedFigure, effectiveSelectedChartCategories),
    [axisSyncedRecommendedFigure, effectiveSelectedChartCategories]
  )
  const filteredLocalFallbackFigure = useMemo(
    () => filterFigureByCategories(localFallbackFigure, effectiveSelectedChartCategories),
    [localFallbackFigure, effectiveSelectedChartCategories]
  )
  const recommendedFigureForRender = useMemo(
    () => prepareFigureForRender(filteredRecommendedFigure, { l: 64, r: 24, t: 20, b: 56 }),
    [filteredRecommendedFigure]
  )
  const hasRecommendedPlotlyFigure = figureHasRenderableData(recommendedFigureForRender)
  useEffect(() => {
    if (hasRecommendedPlotlyFigure) {
      if (recommendedRenderMode !== "interactive") setRecommendedRenderMode("interactive")
      return
    }
    if (hasRecommendedSeabornImage && recommendedRenderMode !== "seaborn") {
      setRecommendedRenderMode("seaborn")
    }
  }, [hasRecommendedPlotlyFigure, hasRecommendedSeabornImage, recommendedRenderMode, recommendedAnalysis])
  const showRecommendedInteractive = hasRecommendedPlotlyFigure && recommendedRenderMode === "interactive"
  const showRecommendedSeaborn = hasRecommendedSeabornImage && !showRecommendedInteractive
  const canUseChartCategoryFilter =
    isChartCategoryPickerEnabled && hasRecommendedPlotlyFigure && recommendedRenderMode === "interactive"
  const localFallbackFigureForRender = useMemo(
    () => prepareFigureForRender(filteredLocalFallbackFigure, { l: 56, r: 24, t: 20, b: 56 }),
    [filteredLocalFallbackFigure]
  )

  const survivalFigure = useMemo(() => {
    if (!survivalChartData?.length) return null
    const sorted = [...survivalChartData].sort((a, b) => a.time - b.time)
    return {
      data: [
        {
          type: "scatter",
          mode: "lines",
          x: sorted.map((d) => d.time),
          y: sorted.map((d) => d.upperCI),
          line: { width: 0 },
          hoverinfo: "skip",
          showlegend: false,
          name: "Upper CI",
        },
        {
          type: "scatter",
          mode: "lines",
          x: sorted.map((d) => d.time),
          y: sorted.map((d) => d.lowerCI),
          fill: "tonexty",
          fillcolor: "rgba(62,207,142,0.18)",
          line: { width: 0 },
          hoverinfo: "skip",
          showlegend: false,
          name: "Lower CI",
        },
        {
          type: "scatter",
          mode: "lines+markers",
          x: sorted.map((d) => d.time),
          y: sorted.map((d) => d.survival),
          name: "Survival",
          line: { color: "#3ecf8e", width: 2, shape: "hv" },
          marker: { size: 5 },
          hovertemplate: "time=%{x}<br>survival=%{y:.2f}%<extra></extra>",
        },
      ],
      layout: {
        autosize: true,
        margin: { l: 56, r: 24, t: 22, b: 56 },
        xaxis: { title: "Time" },
        yaxis: { title: "Survival (%)", range: [0, 100] },
        shapes: [
          {
            type: "line",
            x0: Math.min(...sorted.map((d) => d.time)),
            x1: Math.max(...sorted.map((d) => d.time)),
            y0: 50,
            y1: 50,
            line: { color: "#94a3b8", width: 1, dash: "dash" },
          },
          {
            type: "line",
            x0: medianSurvival,
            x1: medianSurvival,
            y0: 0,
            y1: 100,
            line: { color: "#10b981", width: 1, dash: "dash" },
          },
        ],
        annotations: [
          {
            x: 0.99,
            y: 0.02,
            xref: "paper",
            yref: "paper",
            showarrow: false,
            text: `N=${totalPatients}, Events=${totalEvents}, Median=${medianSurvival.toFixed(2)}`,
            font: { size: 11, color: "#64748b" },
            xanchor: "right",
            yanchor: "bottom",
          },
        ],
        paper_bgcolor: "transparent",
        plot_bgcolor: "transparent",
      },
    }
  }, [survivalChartData, medianSurvival, totalPatients, totalEvents])
  const survivalFigureForRender = useMemo(
    () => prepareFigureForRender(survivalFigure, { l: 56, r: 24, t: 22, b: 56 }),
    [survivalFigure]
  )
  const zoomChartPayload = useMemo(() => {
    if (showRecommendedSeaborn && recommendedImageDataUrl) {
      const chartType = formatChartTypeLabel(recommendedAnalysis?.chart_spec?.chart_type || "chart")
      return {
        title: `시각화 확대 보기 (${chartType})`,
        imageDataUrl: recommendedImageDataUrl,
        renderEngine: recommendedRenderEngine || "SEABORN",
      }
    }
    if (showRecommendedInteractive && recommendedFigureForRender) {
      const chartType = formatChartTypeLabel(recommendedAnalysis?.chart_spec?.chart_type || "plotly")
      return {
        title: `시각화 확대 보기 (${chartType})`,
        data: Array.isArray(recommendedFigureForRender.data) ? recommendedFigureForRender.data : [],
        layout: normalizePlotLayout(recommendedFigureForRender.layout || {}, { l: 72, r: 36, t: 36, b: 72 }),
      }
    }
    if (recommendedImageDataUrl) {
      const chartType = formatChartTypeLabel(recommendedAnalysis?.chart_spec?.chart_type || "chart")
      return {
        title: `시각화 확대 보기 (${chartType})`,
        imageDataUrl: recommendedImageDataUrl,
        renderEngine: recommendedRenderEngine || "SEABORN",
      }
    }
    if (recommendedFigureForRender) {
      const chartType = formatChartTypeLabel(recommendedAnalysis?.chart_spec?.chart_type || "plotly")
      return {
        title: `시각화 확대 보기 (${chartType})`,
        data: Array.isArray(recommendedFigureForRender.data) ? recommendedFigureForRender.data : [],
        layout: normalizePlotLayout(recommendedFigureForRender.layout || {}, { l: 72, r: 36, t: 36, b: 72 }),
      }
    }
    if (localFallbackFigureForRender) {
      const chartType = chartForRender?.type || recommendedAnalysis?.chart_spec?.chart_type || "plotly"
      return {
        title: `시각화 확대 보기 (${formatChartTypeLabel(chartType)})`,
        data: Array.isArray(localFallbackFigureForRender.data) ? localFallbackFigureForRender.data : [],
        layout: normalizePlotLayout(
          (localFallbackFigureForRender.layout || {}) as Record<string, unknown>,
          { l: 72, r: 36, t: 36, b: 72 }
        ),
      }
    }
    if (survivalFigureForRender) {
      return {
        title: "시각화 확대 보기 (SURVIVAL)",
        data: Array.isArray(survivalFigureForRender.data) ? survivalFigureForRender.data : [],
        layout: normalizePlotLayout((survivalFigureForRender.layout || {}) as Record<string, unknown>, {
          l: 72,
          r: 36,
          t: 36,
          b: 72,
        }),
      }
    }
    return null
  }, [
    showRecommendedSeaborn,
    showRecommendedInteractive,
    recommendedImageDataUrl,
    recommendedFigureForRender,
    recommendedRenderEngine,
    recommendedAnalysis,
    localFallbackFigureForRender,
    chartForRender,
    survivalFigureForRender,
  ])
  const hasZoomChart = !hasInsufficientRowsForVisualization && Boolean(
    String((zoomChartPayload as any)?.imageDataUrl || "").startsWith("data:image/") ||
    Boolean((zoomChartPayload as any)?.data?.length)
  )
  useEffect(() => {
    if (!hasZoomChart && isVisualizationZoomOpen) {
      setIsVisualizationZoomOpen(false)
    }
  }, [hasZoomChart, isVisualizationZoomOpen])
  const handleOpenVisualizationZoom = () => {
    if (!hasZoomChart) return
    setIsVisualizationZoomOpen(true)
  }
  const handleVisualizationPanelClick = (event: React.MouseEvent<HTMLDivElement>) => {
    const target = event.target as HTMLElement | null
    if (target?.closest(".modebar")) return
    handleOpenVisualizationZoom()
  }

  const statsBoxPlotFigures = useMemo<Array<{ column: string; figure: { data: unknown[]; layout: Record<string, unknown> } }>>(() => {
    if (hasInsufficientRowsForVisualization || !previewRecords.length || !boxPlotEligibleRows.length) return []
    const figures: Array<{ column: string; figure: { data: unknown[]; layout: Record<string, unknown> } }> = []
    const seenColumns = new Set<string>()
    for (const row of boxPlotEligibleRows) {
      if (seenColumns.has(row.column)) continue
      const values = previewRecords
        .map((record) => Number(record?.[row.column]))
        .filter((value) => Number.isFinite(value))
      if (!values.length) continue
      figures.push({
        column: row.column,
        figure: {
          data: [
            {
              type: "box",
              name: row.column,
              y: values,
              boxpoints: "all",
              jitter: 0.45,
              pointpos: 0,
              boxmean: "sd",
              whiskerwidth: 0.8,
              fillcolor: "rgba(59, 130, 246, 0.2)",
              marker: {
                color: "rgba(37, 99, 235, 0.5)",
                opacity: 0.85,
                size: 6,
                line: { color: "rgba(29, 78, 216, 0.5)", width: 0.7 },
              },
              line: { color: "#1d4ed8", width: 2 },
              hovertemplate: `${row.column}<br>값: %{y:,.3f}<extra></extra>`,
            },
            {
              type: "scatter",
              mode: "markers",
              x: [row.column],
              y: [values.reduce((sum, value) => sum + value, 0) / values.length],
              name: "평균",
              marker: {
                color: "#ef4444",
                size: 10,
                symbol: "diamond",
                line: { color: "white", width: 1.2 },
              },
              hovertemplate: `평균<br>${row.column}: %{y:,.3f}<extra></extra>`,
            },
          ],
          layout: {
            autosize: true,
            margin: { l: 56, r: 20, t: 20, b: 32 },
            yaxis: {
              title: row.column,
              automargin: true,
              gridcolor: "rgba(148, 163, 184, 0.25)",
              zeroline: false,
            },
            xaxis: {
              title: "",
              automargin: true,
              showticklabels: false,
              showgrid: false,
              zeroline: false,
            },
            paper_bgcolor: "transparent",
            plot_bgcolor: "rgba(148, 163, 184, 0.06)",
            showlegend: true,
            legend: {
              orientation: "h",
              x: 1,
              xanchor: "right",
              y: 1.05,
              yanchor: "bottom",
              font: { size: 11 },
            },
            hovermode: "closest",
          },
        },
      })
      seenColumns.add(row.column)
    }
    return figures
  }, [hasInsufficientRowsForVisualization, previewRecords, boxPlotEligibleRows])
  const selectedStatsBoxPlot = useMemo(() => {
    if (!statsBoxPlotFigures.length) return null
    return (
      statsBoxPlotFigures.find((item) => item.column === selectedStatsBoxColumn) ||
      statsBoxPlotFigures[0]
    )
  }, [statsBoxPlotFigures, selectedStatsBoxColumn])
  useEffect(() => {
    if (!statsBoxPlotFigures.length) {
      if (selectedStatsBoxColumn) setSelectedStatsBoxColumn("")
      if (showStatsBoxPlot) setShowStatsBoxPlot(false)
      return
    }
    const exists = statsBoxPlotFigures.some((item) => item.column === selectedStatsBoxColumn)
    if (!exists) setSelectedStatsBoxColumn(statsBoxPlotFigures[0].column)
  }, [statsBoxPlotFigures, selectedStatsBoxColumn, showStatsBoxPlot])
  useEffect(() => {
    const nextLabel = selectedStatsBoxPlot?.column || "박스플롯 컬럼 선택"
    setStatsBoxValueLabel((prev) => (prev === nextLabel ? prev : nextLabel))
  }, [selectedStatsBoxPlot?.column])
  useEffect(() => {
    const measure = () => {
      const measureEl = statsBoxMeasureRef.current
      if (!measureEl) return
      const textWidth = Math.ceil(measureEl.getBoundingClientRect().width)
      const nextWidth = Math.min(
        BOX_PLOT_TRIGGER_MAX_W,
        Math.max(BOX_PLOT_TRIGGER_MIN_W, textWidth + BOX_PLOT_TRIGGER_EXTRA_PX)
      )
      setStatsBoxTriggerWidth((prev) => (prev === nextWidth ? prev : nextWidth))
    }
    measure()
    const rafId = window.requestAnimationFrame(measure)
    return () => window.cancelAnimationFrame(rafId)
  }, [statsBoxValueLabel])
  const resultInterpretation = useMemo(() => {
    if (summary) return summary
    if (!previewColumns.length) return "쿼리 결과가 없어 해석을 생성할 수 없습니다."
    const numericCols = statsRows.filter((row) => row.numericCount > 0)
    const topNumeric = numericCols
      .slice()
      .sort((a, b) => (b.avg ?? Number.NEGATIVE_INFINITY) - (a.avg ?? Number.NEGATIVE_INFINITY))[0]
    const base = `현재 결과는 ${previewColumns.length}개 컬럼, 미리보기 ${previewRowCount}행입니다.`
    if (!topNumeric || topNumeric.avg == null) return `${base} 수치형 요약 대상이 제한적입니다.`
    return `${base} 평균 기준으로 '${topNumeric.column}' 컬럼이 가장 큽니다(평균 ${formatStatNumber(topNumeric.avg)}).`
  }, [summary, previewColumns, previewRowCount, statsRows])
  const chartInterpretation = useMemo(() => {
    const normalizedType = normalizeChartType(recommendedAnalysis?.chart_spec?.chart_type)
    const pyramidGuide =
      "피라미드 차트는 일반 막대처럼 한쪽 방향 비교가 아니라, 0축 기준 좌/우 대칭으로 두 집단을 동시에 비교하는 차트입니다."
    if (recommendedAnalysis?.summary) {
      return normalizedType === "pyramid"
        ? `${recommendedAnalysis.summary} ${pyramidGuide}`
        : recommendedAnalysis.summary
    }
    if (recommendedAnalysis?.reason) {
      return normalizedType === "pyramid"
        ? `${recommendedAnalysis.reason} ${pyramidGuide}`
        : recommendedAnalysis.reason
    }
    if (normalizedType === "pyramid") return pyramidGuide
    if (recommendedChart) {
      return `차트 유형은 ${recommendedChart.type.toUpperCase()}이며, X축은 ${recommendedChart.xKey}, Y축은 ${recommendedChart.yKey} 기준입니다.`
    }
    if (survivalChartData?.length) {
      return `생존 곡선을 표시했습니다. 추정 중앙 생존시간은 약 ${medianSurvival.toFixed(2)}입니다.`
    }
    return "현재 결과에서는 자동 차트 추천 근거가 충분하지 않습니다."
  }, [recommendedAnalysis, recommendedChart, survivalChartData, medianSurvival])
  const statsInterpretation = useMemo(() => {
    if (!statsRows.length) return "통계표를 생성할 결과가 없습니다."
    const numeric = statsRows.filter((row) => row.numericCount > 0)
    const nullTotal = statsRows.reduce((sum, row) => sum + row.nullCount, 0)
    const missingTotal = statsRows.reduce((sum, row) => sum + row.missingCount, 0)
    if (!numeric.length) return `수치형 컬럼이 없어 결측/NULL 중심으로 확인됩니다(결측 ${missingTotal}, NULL ${nullTotal}).`
    const widest = numeric
      .slice()
      .sort((a, b) => ((b.max ?? 0) - (b.min ?? 0)) - ((a.max ?? 0) - (a.min ?? 0)))[0]
    const range = widest.max != null && widest.min != null ? widest.max - widest.min : null
    return `수치형 컬럼 ${numeric.length}개를 집계했습니다. 결측 ${missingTotal}, NULL ${nullTotal}이며, '${widest.column}'의 분산폭이 가장 큽니다${range != null ? ` (범위 ${formatStatNumber(range)})` : ""}.`
  }, [statsRows])
  const normalizeInsightText = (text: string) => {
    return text
      .replace(/Detected category \+ numeric for comparison\.?/gi, "범주형-수치형 조합 비교가 적합한 결과입니다.")
      .replace(/Result aliases indicate a time-series aggregate\.?/gi, "집계 결과가 시계열 추세 분석에 적합합니다.")
      .replace(/Result aliases indicate a grouped aggregate\.?/gi, "집계 결과가 그룹 비교 분석에 적합합니다.")
      .replace(/Detected time-like and numeric columns for a trend chart\.?/gi, "시간형-수치형 조합으로 추세 차트가 적합합니다.")
      .replace(/Detected multiple numeric columns for correlation\.?/gi, "수치형 컬럼 간 상관관계 분석이 적합합니다.")
      .replace(/Detected a single numeric column for distribution\.?/gi, "단일 수치형 컬럼 분포 분석이 적합합니다.")
      .replace(/^\s*Detected.*$/gim, "")
      .replace(/^Rows:\s*(\d+)$/gim, "결과 행 수: $1")
      .replace(/^source:\s*.+$/gim, "")
      .replace(/^\s*source:\s*.+$/gim, "")
      .replace(/^\s*recommended reason:\s*.+$/gim, "")
      .replace(/\bsource\s*:\s*llm\b/gi, "")
      .replace(/\n{3,}/g, "\n\n")
      .trim()
  }

  const isKoreanInsightText = (text: string) => {
    const normalized = String(text || "").replace(/\s+/g, " ").trim()
    if (!normalized) return false
    const hangulCount = (normalized.match(/[가-힣]/g) || []).length
    if (!hangulCount) return false
    const latinCount = (normalized.match(/[A-Za-z]/g) || []).length
    return hangulCount >= 8 || hangulCount * 2 >= Math.max(1, latinCount)
  }

  const splitLineIntoSentences = (line: string): string[] => {
    const sentences: string[] = []
    let buffer = ""

    for (let idx = 0; idx < line.length; idx += 1) {
      const ch = line[idx]
      buffer += ch

      if (!/[.!?]/.test(ch)) continue

      const prev = idx > 0 ? line[idx - 1] : ""
      const next = idx + 1 < line.length ? line[idx + 1] : ""

      // Keep decimal points inside numbers (e.g., 9.62) as part of the same sentence.
      if (/\d/.test(prev) && /\d/.test(next)) continue
      if (/[.!?]/.test(next)) continue
      if (next && !/[\s"')\]}]/.test(next)) continue

      const sentence = buffer.trim()
      if (sentence) sentences.push(sentence)
      buffer = ""
    }

    const tail = buffer.trim()
    if (tail) sentences.push(tail)
    return sentences
  }

  const splitInsightIntoPoints = (text: string): string[] => {
    const normalized = String(text || "")
      .replace(/\r/g, "")
      .replace(/[ \t]+/g, " ")
      .replace(/\n{2,}/g, "\n")
      .trim()
    if (!normalized) return []

    const rawLines = normalized
      .split("\n")
      .map((line) => line.replace(/^[\-\*\u2022\u25CF\u25E6]\s*/, "").trim())
      .filter(Boolean)

    const points: string[] = []
    const seen = new Set<string>()

    const pushPoint = (value: string) => {
      const cleaned = value.replace(/\s+/g, " ").trim()
      if (!cleaned) return
      if (/^[\)\]\}\.,;:!?]+$/.test(cleaned)) return
      const key = cleaned.toLowerCase()
      if (seen.has(key)) return
      seen.add(key)
      points.push(cleaned)
    }

    for (const line of rawLines) {
      const sentenceParts = splitLineIntoSentences(line)

      if (sentenceParts.length > 1) {
        sentenceParts.forEach(pushPoint)
        continue
      }

      if (line.length > 110 && line.includes(",")) {
        line
          .split(",")
          .map((part) => part.trim())
          .filter(Boolean)
          .forEach(pushPoint)
        continue
      }

      pushPoint(line)
    }

    return points.slice(0, 6)
  }

  const integratedInsight = useMemo(() => {
    const normalizedServerInsight = normalizeInsightText(
      String(visualizationResult?.insight || activeTab?.insight || "").trim()
    )
    const serverInsight = isKoreanInsightText(normalizedServerInsight) ? normalizedServerInsight : ""
    if (serverInsight) return serverInsight

    const localInsightParts: string[] = []
    const seenLocalInsight = new Set<string>()
    const pushLocalInsight = (value: string) => {
      const cleaned = String(value || "").replace(/\s+/g, " ").trim()
      if (!cleaned) return
      const key = cleaned.toLowerCase()
      if (seenLocalInsight.has(key)) return
      seenLocalInsight.add(key)
      localInsightParts.push(cleaned)
    }

    pushLocalInsight(resultInterpretation)
    pushLocalInsight(statsInterpretation)
    if (hasInsufficientRowsForVisualization) {
      pushLocalInsight(`시각화는 최소 ${VISUALIZE_MIN_ROWS}개 행에서 생성됩니다.`)
    } else {
      pushLocalInsight(chartInterpretation)
    }

    const localInsight = localInsightParts.join("\n")

    if (visualizationLoading && !hasInsufficientRowsForVisualization) {
      return "시각화 LLM이 SQL과 쿼리 결과를 기반으로 인사이트를 생성 중입니다."
    }
    if (visualizationError) {
      if (localInsight) {
        return `${localInsight}\n시각화 LLM 인사이트 생성에 실패했습니다. (${visualizationError})`
      }
      return `시각화 LLM 인사이트 생성에 실패했습니다. (${visualizationError})`
    }
    if (localInsight) {
      return localInsight
    }
    return "시각화 LLM 인사이트가 아직 없습니다. 쿼리를 다시 실행하거나 시각화를 새로고침해 주세요."
  }, [
    visualizationResult,
    activeTab?.insight,
    hasInsufficientRowsForVisualization,
    visualizationLoading,
    visualizationError,
    resultInterpretation,
    statsInterpretation,
    chartInterpretation,
  ])
  const insightPoints = useMemo(() => splitInsightIntoPoints(integratedInsight), [integratedInsight])
  const insightHeadline = insightPoints[0] || integratedInsight
  const formattedDisplaySql = useMemo(() => formatSqlForDisplay(displaySql), [displaySql])
  const highlightedDisplaySql = useMemo(() => highlightSqlForDisplay(displaySql), [displaySql])
  const cohortStarterQuestions = useMemo(
    () => (activeCohortContext ? buildCohortStarterQuestions(activeCohortContext) : []),
    [activeCohortContext]
  )
  const visibleBannerQuestions = cohortStarterQuestions.length > 0 ? cohortStarterQuestions : quickQuestions.slice(0, 3)
  const isPdfContextTableVisible =
    Boolean(activeCohortContext) &&
    Boolean(pdfContextBanner.context) &&
    !pdfContextBanner.hasShownTableOnce &&
    messages.length === 0
  const visibleQuickQuestions = quickQuestions.slice(0, 3)
  const latestVisibleTabs = useMemo(
    () => resultTabs.slice(0, visibleTabLimit),
    [resultTabs, visibleTabLimit]
  )
  const pastQueryTabs = useMemo(
    () => resultTabs.slice(visibleTabLimit),
    [resultTabs, visibleTabLimit]
  )
  const compactTabLabel = (text: string, maxChars = 10) => {
    const normalized = String(text || "").trim() || "새 질문"
    const chars = Array.from(normalized)
    if (chars.length <= maxChars) return normalized
    return `${chars.slice(0, maxChars).join("")}...`
  }
  const hasConversation =
    messages.length > 0 ||
    Boolean(response) ||
    Boolean(runResult) ||
    query.trim().length > 0 ||
    Boolean(activeCohortContext)
  const shouldShowResizablePanels = showResults && isDesktopLayout
  const chatPanelStyle = shouldShowResizablePanels ? { width: `${100 - resultsPanelWidth}%` } : undefined
  const resultsPanelStyle = shouldShowResizablePanels ? { width: `${resultsPanelWidth}%` } : undefined
  const appendSuggestions = (base: string, _suggestions?: string[]) => base
  const createResultTab = (questionText: string): ResultTabState => ({
    id: `tab-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    question: questionText,
    sql: "",
    resultData: null,
    visualization: null,
    statistics: [],
    insight: "",
    status: "pending",
    error: null,
    response: null,
    runResult: null,
    suggestedQuestions: [],
    showSqlPanel: false,
    showQueryResultPanel: false,
    editedSql: "",
    isEditing: false,
    preferredChartType: null,
  })

  const syncPanelsFromTab = (tab: ResultTabState | null) => {
    if (!tab) {
      setResponse(null)
      setRunResult(null)
      setVisualizationResult(null)
      setVisualizationError(null)
      setError(null)
      setEditedSql("")
      setIsEditing(false)
      setSuggestedQuestions([])
      setLastQuestion("")
      setShowSqlPanel(false)
      setShowQueryResultPanel(false)
      setShowResults(false)
      return
    }

    setResponse(tab.response)
    setRunResult(tab.runResult)
    setVisualizationResult(tab.visualization)
    setVisualizationError(tab.error || null)
    setError(tab.error || null)
    setEditedSql(tab.editedSql || tab.sql || "")
    setIsEditing(tab.isEditing)
    setSuggestedQuestions(tab.suggestedQuestions || [])
    setLastQuestion(tab.question || "")
    setShowSqlPanel(tab.showSqlPanel)
    setShowQueryResultPanel(tab.showQueryResultPanel)
    setShowResults(true)
  }

  const isTargetTabActive = (targetTabId?: string) =>
    !targetTabId || targetTabId === activeTabIdRef.current

  const updateTab = (tabId: string, patch: Partial<ResultTabState>) => {
    setResultTabs((prev) =>
      prev.map((tab) => (tab.id === tabId ? { ...tab, ...patch } : tab))
    )
  }

  const updateActiveTab = (patch: Partial<ResultTabState>) => {
    if (!activeTabId) return
    updateTab(activeTabId, patch)
  }

  const fetchVisualizationPlan = async (
    sqlText: string,
    questionText: string,
    previewData: PreviewData | null,
    targetTabId?: string
  ) => {
    const previewRowCountForViz = Number(previewData?.row_count ?? previewData?.rows?.length ?? 0)
    if (previewRowCountForViz < VISUALIZE_MIN_ROWS) {
      if (isTargetTabActive(targetTabId)) {
        setVisualizationLoading(false)
        setVisualizationResult(null)
        setVisualizationError(null)
      }
      if (targetTabId) {
        updateTab(targetTabId, { visualization: null, insight: "" })
      }
      return null
    }
    const visualizationUserQuery = composeVisualizationUserQuery(questionText)
    if (
      !sqlText?.trim() ||
      !visualizationUserQuery ||
      !previewData?.columns?.length ||
      !previewData?.rows?.length
    ) {
      if (isTargetTabActive(targetTabId)) {
        setVisualizationResult(null)
        setVisualizationError(null)
      }
      if (targetTabId) {
        updateTab(targetTabId, { visualization: null })
      }
      return null
    }
    const shouldTrackLoading = isTargetTabActive(targetTabId)
    // Always fetch latest visualization/insight from server (disable local cache)
    if (shouldTrackLoading) {
      setVisualizationLoading(true)
      setVisualizationError(null)
    }
    try {
      const records = previewData.rows.map((row) =>
        Object.fromEntries(previewData.columns.map((col, idx) => [col, row?.[idx]]))
      )
      const res = await fetchWithTimeout(
        vizUrl("/visualize"),
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            user_query: visualizationUserQuery,
            sql: sqlText,
            rows: records,
          }),
        },
        VISUALIZE_TIMEOUT_MS
      )
      if (!res.ok) throw new Error(await readError(res))
      const data: VisualizationResponsePayload = await res.json()
      const normalizedInsight = normalizeInsightText(String(data?.insight || ""))
      const tabInsight = isKoreanInsightText(normalizedInsight) ? normalizedInsight : ""
      if (isTargetTabActive(targetTabId)) {
        setVisualizationResult(data)
      }
      if (targetTabId) {
        updateTab(targetTabId, { visualization: data, insight: tabInsight })
      }
      return data
    } catch (err: any) {
      const message = err?.message || "시각화 추천 플랜 조회에 실패했습니다."
      if (isTargetTabActive(targetTabId)) {
        setVisualizationResult(null)
        setVisualizationError(message)
      }
      if (targetTabId) {
        updateTab(targetTabId, { visualization: null, error: message })
      }
      return null
    } finally {
      if (shouldTrackLoading) {
        setVisualizationLoading(false)
      }
    }
  }

  const handlePanelResizeMouseDown = (event: React.MouseEvent<HTMLDivElement>) => {
    if (event.button !== 0 || !shouldShowResizablePanels) return
    const container = mainContentRef.current
    if (!container) return
    const rect = container.getBoundingClientRect()
    if (rect.width <= 0) return

    panelResizeRef.current = {
      active: true,
      startX: event.clientX,
      startRightWidth: resultsPanelWidth,
      containerWidth: rect.width,
    }
    setIsPanelResizing(true)
    document.body.style.cursor = "col-resize"
    document.body.style.userSelect = "none"
    event.preventDefault()
  }

  const handleSqlMouseDown = (event: React.MouseEvent<HTMLDivElement>) => {
    if (event.button !== 0) return
    const el = sqlScrollRef.current
    if (!el) return

    sqlDragRef.current = {
      active: true,
      startX: event.clientX,
      startY: event.clientY,
      scrollLeft: el.scrollLeft,
      scrollTop: el.scrollTop,
    }
    setIsSqlDragging(true)
    event.preventDefault()
  }

  useEffect(() => {
    const handleMouseMove = (event: MouseEvent) => {
      if (!sqlDragRef.current.active) return
      const el = sqlScrollRef.current
      if (!el) return
      const dx = event.clientX - sqlDragRef.current.startX
      const dy = event.clientY - sqlDragRef.current.startY
      el.scrollLeft = sqlDragRef.current.scrollLeft - dx
      el.scrollTop = sqlDragRef.current.scrollTop - dy
    }

    const stopDragging = () => {
      if (!sqlDragRef.current.active) return
      sqlDragRef.current.active = false
      setIsSqlDragging(false)
    }

    window.addEventListener("mousemove", handleMouseMove)
    window.addEventListener("mouseup", stopDragging)
    return () => {
      window.removeEventListener("mousemove", handleMouseMove)
      window.removeEventListener("mouseup", stopDragging)
    }
  }, [])

  useEffect(() => {
    activeTabIdRef.current = activeTabId
  }, [activeTabId])

  useEffect(() => {
    const syncDesktopLayout = () => {
      setIsDesktopLayout(window.innerWidth >= 1024)
    }
    syncDesktopLayout()
    window.addEventListener("resize", syncDesktopLayout)
    return () => window.removeEventListener("resize", syncDesktopLayout)
  }, [])

  useEffect(() => {
    if (!messages.length) return
    const container = chatScrollRef.current
    if (!container) return

    const last = messages[messages.length - 1]
    const isNewMessage = last.id !== lastAutoScrolledMessageIdRef.current
    const shouldScroll = isNewMessage && (last.role === "assistant" || isLoading)
    if (!shouldScroll) return

    lastAutoScrolledMessageIdRef.current = last.id
    const rafId = window.requestAnimationFrame(() => {
      container.scrollTo({ top: container.scrollHeight, behavior: "smooth" })
    })
    return () => window.cancelAnimationFrame(rafId)
  }, [messages, isLoading])

  useEffect(() => {
    if (typeof window === "undefined") return
    const target = tabHeaderRef.current
    if (!target) return

    const recalcVisibleTabLimit = () => {
      const width = target.clientWidth || 0
      if (width <= 0) return
      const tabWidthEstimate = 118
      const historyButtonReserve = 112
      let next = Math.floor(width / tabWidthEstimate)
      next = Math.max(1, Math.min(8, next))
      if (resultTabs.length > next) {
        next = Math.floor((width - historyButtonReserve) / tabWidthEstimate)
        next = Math.max(1, Math.min(8, next))
      }
      setVisibleTabLimit(next)
    }

    recalcVisibleTabLimit()
    window.addEventListener("resize", recalcVisibleTabLimit)

    let observer: ResizeObserver | null = null
    if (typeof ResizeObserver !== "undefined") {
      observer = new ResizeObserver(() => recalcVisibleTabLimit())
      observer.observe(target)
    }
    return () => {
      window.removeEventListener("resize", recalcVisibleTabLimit)
      observer?.disconnect()
    }
  }, [resultTabs.length])

  useEffect(() => {
    const handleMouseMove = (event: MouseEvent) => {
      if (!panelResizeRef.current.active) return
      const { startX, startRightWidth, containerWidth } = panelResizeRef.current
      if (containerWidth <= 0) return
      const deltaPercent = ((event.clientX - startX) / containerWidth) * 100
      const nextRightWidth = Math.min(70, Math.max(30, startRightWidth - deltaPercent))
      setResultsPanelWidth(nextRightWidth)
    }

    const stopResizing = () => {
      if (!panelResizeRef.current.active) return
      panelResizeRef.current.active = false
      setIsPanelResizing(false)
      document.body.style.cursor = ""
      document.body.style.userSelect = ""
    }

    window.addEventListener("mousemove", handleMouseMove)
    window.addEventListener("mouseup", stopResizing)
    return () => {
      window.removeEventListener("mousemove", handleMouseMove)
      window.removeEventListener("mouseup", stopResizing)
      document.body.style.cursor = ""
      document.body.style.userSelect = ""
    }
  }, [])

  function normalizeColumn(value: string) {
    return value.toLowerCase().replace(/[^a-z0-9]/g, "")
  }

  function findColumnIndex(columns: string[], candidates: string[]) {
    const normalized = columns.map((col) => normalizeColumn(col))
    for (const candidate of candidates) {
      const idx = normalized.indexOf(normalizeColumn(candidate))
      if (idx >= 0) return idx
    }
    return -1
  }

  function toNumber(value: unknown) {
    if (value == null) return null
    const num = Number(value)
    return Number.isFinite(num) ? num : null
  }

  function buildSurvivalFromPreview(columns: string[], rows: any[][]) {
    if (!columns.length || !rows.length) return null
    const timeIdx = findColumnIndex(columns, ["time", "days", "day", "week", "weeks", "month", "months"])
    const survivalIdx = findColumnIndex(columns, ["survival", "survivalrate", "rate", "prob", "probability"])
    if (timeIdx < 0 || survivalIdx < 0) return null
    const lowerIdx = findColumnIndex(columns, ["lowerci", "ci_lower", "lcl", "lower"])
    const upperIdx = findColumnIndex(columns, ["upperci", "ci_upper", "ucl", "upper"])
    const atRiskIdx = findColumnIndex(columns, ["atrisk", "at_risk", "risk"])
    const eventsIdx = findColumnIndex(columns, ["events", "event", "death", "deaths"])

    const data = rows
      .map((row) => {
        const time = toNumber(row[timeIdx])
        const survival = toNumber(row[survivalIdx])
        if (time == null || survival == null) return null
        const lowerCI = toNumber(row[lowerIdx]) ?? survival
        const upperCI = toNumber(row[upperIdx]) ?? survival
        const atRisk = toNumber(row[atRiskIdx]) ?? 0
        const events = toNumber(row[eventsIdx]) ?? 0
        return { time, survival, lowerCI, upperCI, atRisk, events }
      })
      .filter(Boolean) as {
        time: number
        survival: number
        lowerCI: number
        upperCI: number
        atRisk: number
        events: number
      }[]

    return data.length ? data : null
  }

  const buildSuggestions = (questionText: string, columns?: string[]) => {
    const suggestions: string[] = []
    const normalized = questionText.toLowerCase()
    const cols = (columns || []).map((col) => col.toLowerCase())

    const pushUnique = (text: string) => {
      if (!text || suggestions.includes(text)) return
      suggestions.push(text)
    }

    if (normalized.includes("diagnos") || normalized.includes("진단") || cols.some((c) => c.includes("icd"))) {
      pushUnique("상위 10개 진단 보기")
      pushUnique("성별/연령별 진단 분포")
      pushUnique("진단 추세 보기")
    } else if (normalized.includes("icu") || normalized.includes("재원") || cols.some((c) => c.includes("stay"))) {
      pushUnique("ICU 평균 재원일수")
      pushUnique("ICU 재원일수 분포")
      pushUnique("ICU 재원 상위 10명")
    } else if (normalized.includes("입원") || normalized.includes("admission")) {
      pushUnique("입원 추이 보기")
      pushUnique("진단별 입원 건수")
      pushUnique("평균 입원기간")
    }

    if (cols.some((c) => c.includes("date") || c.includes("time"))) {
      pushUnique("기간별 추이")
    }
    if (cols.some((c) => c.includes("gender"))) {
      pushUnique("성별 통계 보기")
    }
    if (cols.some((c) => c.includes("age"))) {
      pushUnique("연령대별 보기")
    }

    if (suggestions.length === 0) {
      pushUnique("상위 10개 보기")
      pushUnique("최근 6개월")
      pushUnique("성별 통계 보기")
    }

    return suggestions.slice(0, 3)
  }

  const buildClarificationSuggestions = (payload?: OneShotPayload) => {
    if (!payload?.clarification) return []
    const options = Array.isArray(payload.clarification.options)
      ? payload.clarification.options.map((item) => String(item).trim()).filter(Boolean)
      : []
    const examples = Array.isArray(payload.clarification.example_inputs)
      ? payload.clarification.example_inputs.map((item) => String(item).trim()).filter(Boolean)
      : []
    const merged = [...options, ...examples]
    return Array.from(new Set(merged)).slice(0, 3)
  }

  const readError = async (res: Response) => {
    const text = await res.text()
    try {
      const json = JSON.parse(text)
      const detail = json?.detail
      if (typeof detail === "string" && detail.trim()) {
        return normalizeRequestErrorMessage({ message: detail.trim() }, detail.trim())
      }
      if (detail && typeof detail === "object") {
        const message = String((detail as any).message || "").trim()
        if (message) return normalizeRequestErrorMessage({ message }, message)
      }
      const message = String(json?.message || "").trim()
      if (message) return normalizeRequestErrorMessage({ message }, message)
    } catch {}
    const fallback = text || `${res.status} ${res.statusText}`
    return normalizeRequestErrorMessage({ message: fallback }, fallback)
  }

  const buildAssistantMessage = (data: OneShotResponse) => {
    const llmAssistantMessage = String(data.payload.assistant_message || "").trim()
    if (llmAssistantMessage) {
      return llmAssistantMessage
    }
    if (data.payload.mode === "clarify") {
      const clarify = data.payload.clarification
      const prompt = clarify?.question?.trim() || "질문 범위를 조금 더 좁혀주세요."
      const options = Array.isArray(clarify?.options) ? clarify.options.filter(Boolean) : []
      const examples = Array.isArray(clarify?.example_inputs) ? clarify.example_inputs.filter(Boolean) : []
      const reason = clarify?.reason?.trim()
      const lines = [prompt]
      if (reason) {
        lines.push(`이유: ${reason}`)
      }
      if (options.length) {
        lines.push(`선택 예시: ${options.slice(0, 4).join(", ")}`)
      }
      if (examples.length) {
        lines.push(`입력 예: ${examples.slice(0, 2).join(" / ")}`)
      }
      return lines.join("\n")
    }
    if (data.payload.mode === "demo") {
      const parts: string[] = []
      const summaryText = data.payload.result?.summary
      if (summaryText) {
        parts.push(summaryText.endsWith(".") ? summaryText : `${summaryText}.`)
      } else {
        parts.push("데모 캐시 결과를 가져왔어요.")
      }
      const rowCount = data.payload.result?.preview?.row_count
      if (rowCount != null) parts.push(`미리보기로 ${rowCount}행을 보여드렸어요.`)
      if (data.payload.result?.source) parts.push(`데모 캐시(source: ${data.payload.result.source}) 기반입니다.`)
      return parts.join(" ")
    }
    const base = "요청하신 내용을 바탕으로 SQL을 준비했어요. 실행하면 결과를 가져올게요."
    const payload = data.payload
    const localRiskScore = payload?.final?.risk_score ?? payload?.risk?.risk
    const localRiskIntent = payload?.risk?.intent
    const riskLabel =
      localRiskScore != null ? `위험도 ${localRiskScore}${localRiskIntent ? ` (${localRiskIntent})` : ""}로 평가되었어요.` : ""
    return [base, riskLabel].filter(Boolean).join(" ")
  }

  const applyPdfContextBanner = (context: ActiveCohortContext | null) => {
    if (!context) {
      setPdfContextBanner({ context: null, hasShownTableOnce: true })
      return
    }
    let hasShownTableOnce = false
    if (typeof window !== "undefined") {
      const key = buildPdfContextTableOnceKey(chatHistoryUser, context)
      hasShownTableOnce = window.sessionStorage.getItem(key) === "1"
    }
    setPdfContextBanner({
      context: toPdfCohortContext(context),
      hasShownTableOnce,
    })
  }

  const markPdfContextTableAsShown = (context: ActiveCohortContext | null) => {
    if (!context) return
    if (typeof window !== "undefined") {
      const key = buildPdfContextTableOnceKey(chatHistoryUser, context)
      window.sessionStorage.setItem(key, "1")
    }
    setPdfContextBanner((prev) =>
      prev.context ? { ...prev, hasShownTableOnce: true } : prev
    )
  }

  const clearActiveCohortContext = () => {
    requestTokenRef.current += 1
    setIsLoading(false)
    applyActiveCohort(null, { regenerateDefaultQuestions: true })
    clearPendingActiveCohortContext(chatHistoryUser)
  }

  useEffect(() => {
    if (!isAuthHydrated || !chatHistoryUser) return
    const loadChatState = async () => {
      setQuery("")
      setMessages([])
      setResponse(null)
      setRunResult(null)
      setVisualizationResult(null)
      setShowResults(false)
      setShowSqlPanel(false)
      setShowQueryResultPanel(false)
      setEditedSql("")
      setIsEditing(false)
      setLastQuestion("")
      setSuggestedQuestions([])
      setQuickQuestions(DEFAULT_QUICK_QUESTIONS)
      activeCohortContextRef.current = null
      setActiveCohortContext(null)
      applyPdfContextBanner(null)
      clearPendingActiveCohortContext(chatHistoryUser)
      fetch(apiUrl("/chat/history"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user: chatHistoryUser, state: null })
      }).catch(() => {})
      setIsHydrated(true)
    }
    loadChatState()
  }, [apiBaseUrl, isAuthHydrated, chatHistoryUser])

  useEffect(() => {
    if (!isAuthHydrated || !chatHistoryUser) return
    let nextModel = DEFAULT_CHAT_MODEL
    try {
      const saved = localStorage.getItem(modelStorageKey)
      if (saved) nextModel = saved
    } catch {}
    if (!modelOptions.includes(nextModel)) {
      nextModel = modelOptions[0] || DEFAULT_CHAT_MODEL
    }
    setSelectedModel(nextModel)
  }, [isAuthHydrated, chatHistoryUser, modelStorageKey, modelOptions])

  useEffect(() => {
    if (!isAuthHydrated || !chatHistoryUser) return
    const normalized = modelOptions.includes(selectedModel)
      ? selectedModel
      : modelOptions[0] || DEFAULT_CHAT_MODEL
    try {
      localStorage.setItem(modelStorageKey, normalized)
    } catch {}
    if (normalized !== selectedModel) {
      setSelectedModel(normalized)
    }
  }, [isAuthHydrated, chatHistoryUser, modelStorageKey, modelOptions, selectedModel])

  useEffect(() => {
    if (typeof window === "undefined") return
    const supported = Boolean(
      window.navigator?.mediaDevices?.getUserMedia &&
      typeof window.MediaRecorder !== "undefined"
    )
    setIsSpeechSupported(supported)
    return () => {
      clearVoiceTimer()
      stopVoiceWaveMonitor()
      const recorder = mediaRecorderRef.current
      if (recorder && recorder.state !== "inactive") {
        try {
          recorder.stop()
        } catch {}
      }
      mediaRecorderRef.current = null
      voiceStopActionRef.current = "cancel"
      const stream = mediaStreamRef.current
      if (stream) {
        stream.getTracks().forEach((track) => track.stop())
      }
      mediaStreamRef.current = null
      recordedChunksRef.current = []
      if (messageCopyTimerRef.current !== null) {
        window.clearTimeout(messageCopyTimerRef.current)
        messageCopyTimerRef.current = null
      }
      if (sqlCopyTimerRef.current !== null) {
        window.clearTimeout(sqlCopyTimerRef.current)
        sqlCopyTimerRef.current = null
      }
      if (boardMessageTimerRef.current !== null) {
        window.clearTimeout(boardMessageTimerRef.current)
        boardMessageTimerRef.current = null
      }
    }
  }, [])

  useEffect(() => {
    if (!isHydrated || !chatHistoryUser) return
    if (saveTimerRef.current) {
      window.clearTimeout(saveTimerRef.current)
    }
    const state: PersistedQueryState = {
      query,
      lastQuestion,
      messages: serializeMessages(messages),
      response: sanitizeResponse(response),
      runResult: sanitizeRunResult(runResult),
      visualizationResult: sanitizeVisualizationResult(visualizationResult),
      activeCohortContext,
      suggestedQuestions,
      showResults,
      showSqlPanel,
      showQueryResultPanel,
      editedSql,
      isEditing,
    }
    saveTimerRef.current = window.setTimeout(() => {
      fetch(apiUrl("/chat/history"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user: chatHistoryUser, state })
      }).catch(() => {})
    }, 600)
    return () => {
      if (saveTimerRef.current) {
        window.clearTimeout(saveTimerRef.current)
      }
    }
  }, [
    isHydrated,
    query,
    lastQuestion,
    messages,
    response,
    runResult,
    visualizationResult,
    activeCohortContext,
    suggestedQuestions,
    showResults,
    showSqlPanel,
    showQueryResultPanel,
    editedSql,
    isEditing,
    apiBaseUrl,
    chatHistoryUser,
  ])

  const loadDefaultQuestions = async (syncSuggested = false) => {
    try {
      const res = await fetchWithTimeout(apiUrlWithUser("/query/demo/questions"), {}, 15000)
      if (!res.ok) {
        setQuickQuestions(DEFAULT_QUICK_QUESTIONS)
        if (syncSuggested) setSuggestedQuestions(DEFAULT_QUICK_QUESTIONS.slice(0, 3))
        return DEFAULT_QUICK_QUESTIONS
      }
      const data = await res.json()
      const next = Array.isArray(data?.questions) && data.questions.length
        ? data.questions.slice(0, 3)
        : DEFAULT_QUICK_QUESTIONS
      setQuickQuestions(next)
      if (syncSuggested) setSuggestedQuestions(next.slice(0, 3))
      return next
    } catch {
      setQuickQuestions(DEFAULT_QUICK_QUESTIONS)
      if (syncSuggested) setSuggestedQuestions(DEFAULT_QUICK_QUESTIONS.slice(0, 3))
      return DEFAULT_QUICK_QUESTIONS
    }
  }

  useEffect(() => {
    void loadDefaultQuestions(false)
  }, [chatHistoryUser])

  const loadSavedCohortLibrary = async () => {
    setIsCohortLibraryLoading(true)
    try {
      const res = await fetchWithTimeout(apiUrlWithUser("/cohort/library?limit=200"), {}, 15000)
      if (!res.ok) {
        setSavedCohorts([])
        return
      }
      const payload = await res.json()
      const rawItems = Array.isArray(payload?.items)
        ? payload.items
        : Array.isArray(payload?.cohorts)
          ? payload.cohorts
          : []
      const parsed = rawItems
        .map((item: unknown) => toSavedCohort(item))
        .filter((item: SavedCohort | null): item is SavedCohort => item !== null)
      setSavedCohorts(parsed)
    } catch {
      setSavedCohorts([])
    } finally {
      setIsCohortLibraryLoading(false)
    }
  }

  useEffect(() => {
    void loadSavedCohortLibrary()
  }, [apiBaseUrl, chatHistoryUser])

  const applyActiveCohort = (context: ActiveCohortContext | null, options?: { regenerateDefaultQuestions?: boolean }) => {
    activeCohortContextRef.current = context
    setActiveCohortContext(context)
    applyPdfContextBanner(context)
    if (!context) {
      if (options?.regenerateDefaultQuestions) {
        void loadDefaultQuestions(true)
      } else {
        setQuickQuestions(DEFAULT_QUICK_QUESTIONS)
        setSuggestedQuestions(DEFAULT_QUICK_QUESTIONS.slice(0, 3))
      }
      return
    }
    const starterQuestions = buildCohortStarterQuestions(context)
    if (starterQuestions.length) {
      setQuickQuestions(starterQuestions)
      setSuggestedQuestions((prev) => (prev.length ? prev : starterQuestions))
    }
  }

  const handleApplySavedCohortForQuery = (cohort: SavedCohort) => {
    const context = toActiveCohortContext(cohort, "query-selector")
    applyActiveCohort(context)
    setIsCohortLibraryOpen(false)
  }

  const runQuery = async (questionText: string) => {
    const trimmed = questionText.trim()
    if (!trimmed || isLoading || runQueryInFlightRef.current) return
    runQueryInFlightRef.current = true
    const currentActiveContext = activeCohortContextRef.current

    if (currentActiveContext) {
      markPdfContextTableAsShown(currentActiveContext)
    }

    const tab = createResultTab(trimmed)
    setResultTabs((prev) => [tab, ...prev])
    activeTabIdRef.current = tab.id
    setActiveTabId(tab.id)
    setShowResults(true)
    const requestToken = ++requestTokenRef.current

    setIsLoading(true)
    setError(null)
    setBoardMessage(null)
    setResponse(null)
    setRunResult(null)
    setVisualizationResult(null)
    setVisualizationError(null)
    setEditedSql("")
    // Keep split layout while running next question to avoid chat panel jumping to full width.
    // Results content is refreshed below when response arrives.
    setShowSqlPanel(false)
    setShowQueryResultPanel(false)
    setLastQuestion(trimmed)
    setSuggestedQuestions([])
    const newMessage: ChatMessage = {
      id: Date.now().toString(),
      role: "user",
      content: trimmed,
      timestamp: new Date()
    }
    // Preserve recent conversational context so follow-up questions
    // ("그 결과에서...", "그 조건으로...") are interpreted correctly.
    const conversationSeed = [...messages, newMessage]
    const baseConversation = conversationSeed
      .slice(-10)
      .map((item) => ({ role: item.role, content: item.content }))
    const conversation = baseConversation
    setMessages(prev => [...prev, newMessage])
    setQuery("")

    try {
      const oneshotBody = {
        question: trimmed,
        conversation,
        model: selectedModel || undefined,
        user_id: chatHistoryUser,
        user_name: chatUser,
        user_role: chatUserRole,
        cohort_apply: Boolean(currentActiveContext),
        cohort_id: currentActiveContext?.cohortId || undefined,
        cohort_name: currentActiveContext?.cohortName || undefined,
        cohort_type: currentActiveContext?.type || undefined,
        cohort_sql: currentActiveContext?.cohortSql || undefined,
      }
      const maxAttempts = 3
      let res: Response | null = null
      let lastMessage = ""
      for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
        try {
          res = await fetchWithTimeout(apiUrl("/query/oneshot"), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(oneshotBody),
          }, ONESHOT_TIMEOUT_MS)
        } catch (fetchErr: any) {
          lastMessage = normalizeRequestErrorMessage(fetchErr, "요청이 실패했습니다.")
          const retryable = isNetworkFetchError(fetchErr) || isRetryableServerErrorMessage(lastMessage)
          if (!retryable || attempt >= maxAttempts) {
            throw new Error(lastMessage)
          }
          await sleepMs(250 * attempt)
          continue
        }
        if (res.ok) break
        lastMessage = await readError(res)
        const retryable = res.status >= 500 || isRetryableServerErrorMessage(lastMessage)
        if (!retryable || attempt >= maxAttempts) {
          throw new Error(lastMessage)
        }
        await sleepMs(250 * attempt)
      }
      if (!res || !res.ok) {
        throw new Error(lastMessage || "요청이 실패했습니다.")
      }
      if (requestToken !== requestTokenRef.current) return
      const data: OneShotResponse = await res.json()
      const shouldSyncActivePanels = activeTabIdRef.current === tab.id
      if (shouldSyncActivePanels) {
        setResponse(data)
      }
      updateTab(tab.id, {
        response: data,
        status: "success",
      })
      if (data.payload.mode === "clarify") {
        if (shouldSyncActivePanels) {
          setShowSqlPanel(false)
          setShowQueryResultPanel(false)
          setEditedSql("")
          setIsEditing(false)
        }
        const clarificationSuggestions = buildClarificationSuggestions(data.payload)
        if (shouldSyncActivePanels) {
          setSuggestedQuestions(clarificationSuggestions)
        }
        updateTab(tab.id, {
          suggestedQuestions: clarificationSuggestions,
          response: data,
          status: "success",
        })
        const responseMessage: ChatMessage = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: appendSuggestions(buildAssistantMessage(data), clarificationSuggestions),
          timestamp: new Date()
        }
        setMessages(prev => [...prev, responseMessage])
        return
      }

      if (shouldSyncActivePanels) {
        setShowResults(true)
        setShowSqlPanel(false)
        setShowQueryResultPanel(false)
      }
      const generatedSql =
        (data.payload.mode === "demo"
          ? data.payload.result?.sql
          : data.payload.final?.final_sql || data.payload.draft?.final_sql) || ""
      if (shouldSyncActivePanels) {
        setEditedSql(generatedSql)
        setIsEditing(false)
      }
      const suggestions: string[] = []
      if (shouldSyncActivePanels) {
        setSuggestedQuestions(suggestions)
      }
      updateTab(tab.id, {
        question: trimmed,
        sql: generatedSql,
        response: data,
        suggestedQuestions: suggestions,
        editedSql: generatedSql,
        status: "success",
      })

      // Advanced 모드에서는 SQL 생성 직후 자동 실행
      if (data.payload.mode === "advanced" && generatedSql.trim()) {
        await executeAdvancedSql({
          qid: data.qid,
          sql: generatedSql,
          questionForSuggestions: trimmed,
          addAssistantMessage: true,
          tabId: tab.id,
        })
      } else if (data.payload.mode === "demo" && generatedSql.trim()) {
        const preview = data.payload.result?.preview || null
        const fetchedRows = Number(preview?.row_count ?? 0)
        const totalRows =
          typeof preview?.total_count === "number" && Number.isFinite(preview.total_count)
            ? preview.total_count
            : null
        const answerPayload = await requestQueryAnswerMessage({
          questionText: trimmed,
          sqlText: generatedSql.trim(),
          previewData: preview,
          totalRows,
          fetchedRows,
        })
        const llmSuggestions = answerPayload.suggestedQuestions
        if (activeTabIdRef.current === tab.id) {
          setSuggestedQuestions(llmSuggestions)
        }
        updateTab(tab.id, {
          resultData: preview,
          suggestedQuestions: llmSuggestions,
          visualization: null,
          insight: "",
        })
        void fetchVisualizationPlan(
          generatedSql.trim(),
          trimmed,
          preview,
          tab.id
        )
          const responseMessage: ChatMessage = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: appendSuggestions(
            answerPayload.answerText,
            llmSuggestions
          ),
          timestamp: new Date()
        }
        setMessages(prev => [...prev, responseMessage])
      } else {
        const responseMessage: ChatMessage = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: appendSuggestions(buildAssistantMessage(data), suggestions),
          timestamp: new Date()
        }
        setMessages(prev => [...prev, responseMessage])
      }
    } catch (err: any) {
      const message =
        err?.name === "AbortError"
          ? "요청 시간이 초과되었습니다. 잠시 후 다시 시도해주세요."
          : normalizeRequestErrorMessage(err, "요청이 실패했습니다.")
      if (activeTabIdRef.current === tab.id) {
        setError(message)
      }
      updateTab(tab.id, {
        status: "error",
        error: message,
      })
      setMessages(prev => [
        ...prev,
        {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: `오류: ${message}`,
          timestamp: new Date()
        }
      ])
    } finally {
      runQueryInFlightRef.current = false
      if (requestToken === requestTokenRef.current) {
        setIsLoading(false)
      }
    }
  }

  const handleSubmit = async () => {
    if (isLoading) return
    await runQuery(query)
  }

  const stopVoiceMediaStream = (stream: MediaStream | null) => {
    if (!stream) return
    stream.getTracks().forEach((track) => track.stop())
  }

  const clearVoiceTimer = () => {
    if (voiceTimerRef.current) {
      window.clearInterval(voiceTimerRef.current)
      voiceTimerRef.current = null
    }
    voiceStartedAtRef.current = null
  }

  const resetVoiceWaveLevels = () => {
    setVoiceWaveLevels(buildVoiceWaveLevels())
  }

  const stopVoiceWaveMonitor = () => {
    if (voiceWaveRafRef.current) {
      window.cancelAnimationFrame(voiceWaveRafRef.current)
      voiceWaveRafRef.current = null
    }
    voiceWaveLastUpdateRef.current = 0
    if (voiceSourceRef.current) {
      try {
        voiceSourceRef.current.disconnect()
      } catch {}
      voiceSourceRef.current = null
    }
    if (voiceAnalyserRef.current) {
      try {
        voiceAnalyserRef.current.disconnect()
      } catch {}
      voiceAnalyserRef.current = null
    }
    voiceWaveBytesRef.current = null
    const ctx = voiceAudioContextRef.current
    voiceAudioContextRef.current = null
    if (ctx) {
      void ctx.close().catch(() => {})
    }
    resetVoiceWaveLevels()
  }

  const startVoiceWaveMonitor = (stream: MediaStream) => {
    if (typeof window === "undefined") return
    stopVoiceWaveMonitor()
    const audioWindow = window as Window & { webkitAudioContext?: typeof AudioContext }
    const AudioCtor = audioWindow.AudioContext || audioWindow.webkitAudioContext
    if (!AudioCtor) return
    try {
      const audioContext = new AudioCtor()
      const source = audioContext.createMediaStreamSource(stream)
      const analyser = audioContext.createAnalyser()
      analyser.fftSize = 1024
      analyser.smoothingTimeConstant = 0.78
      source.connect(analyser)

      const bytes = new Uint8Array(analyser.frequencyBinCount)
      voiceAudioContextRef.current = audioContext
      voiceSourceRef.current = source
      voiceAnalyserRef.current = analyser
      voiceWaveBytesRef.current = bytes
      voiceWaveLastUpdateRef.current = 0

      void audioContext.resume().catch(() => {})

      const render = (timestamp: number) => {
        const activeAnalyser = voiceAnalyserRef.current
        const activeBytes = voiceWaveBytesRef.current
        if (!activeAnalyser || !activeBytes) return
        activeAnalyser.getByteFrequencyData(activeBytes)
        if (timestamp - voiceWaveLastUpdateRef.current >= 45) {
          const bucketSize = Math.max(1, Math.floor(activeBytes.length / VOICE_WAVE_BAR_COUNT))
          const nextLevels: number[] = []
          for (let i = 0; i < VOICE_WAVE_BAR_COUNT; i += 1) {
            const start = i * bucketSize
            const end = i === VOICE_WAVE_BAR_COUNT - 1 ? activeBytes.length : Math.min(activeBytes.length, start + bucketSize)
            if (start >= activeBytes.length) {
              nextLevels.push(VOICE_WAVE_BASELINE)
              continue
            }
            let sum = 0
            for (let j = start; j < end; j += 1) {
              sum += activeBytes[j]
            }
            const count = Math.max(1, end - start)
            const mean = sum / count / 255
            const normalized = Math.max(VOICE_WAVE_BASELINE, Math.pow(mean, 0.85))
            nextLevels.push(Math.min(1, normalized))
          }
          setVoiceWaveLevels(nextLevels)
          voiceWaveLastUpdateRef.current = timestamp
        }
        voiceWaveRafRef.current = window.requestAnimationFrame(render)
      }

      voiceWaveRafRef.current = window.requestAnimationFrame(render)
    } catch {
      stopVoiceWaveMonitor()
    }
  }

  const startVoiceTimer = () => {
    clearVoiceTimer()
    const startedAt = Date.now()
    voiceStartedAtRef.current = startedAt
    setVoiceElapsedMs(0)
    voiceTimerRef.current = window.setInterval(() => {
      const base = voiceStartedAtRef.current || startedAt
      setVoiceElapsedMs(Math.max(0, Date.now() - base))
    }, 180)
  }

  const transcribeVoiceBlob = async (audioBlob: Blob) => {
    const browserLanguage =
      typeof window !== "undefined" ? String(window.navigator?.language || "").trim() : ""
    const language = browserLanguage.split("-")[0]?.toLowerCase() || "ko"
    const audioDataUrl = await blobToDataUrl(audioBlob)
    const res = await fetchWithTimeout(
      apiUrl("/query/transcribe"),
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          audio_data_url: audioDataUrl,
          language,
        }),
      },
      90_000
    )
    if (!res.ok) {
      const message = await readError(res)
      throw new Error(message || "음성 인식 요청에 실패했습니다.")
    }
    const data: QueryTranscribeResponse = await res.json()
    const text = String(data?.text || "").trim()
    if (!text) {
      throw new Error("음성 인식 결과가 비어 있습니다.")
    }
    return text
  }

  const stopVoiceRecorder = (action: "cancel" | "transcribe") => {
    const recorder = mediaRecorderRef.current
    if (!recorder) return
    voiceStopActionRef.current = action
    setIsVoiceModeOn(false)
    setIsListening(false)
    clearVoiceTimer()
    stopVoiceWaveMonitor()
    setVoiceInterimText(action === "transcribe" ? "음성을 텍스트로 변환 중..." : "")
    try {
      if (recorder.state !== "inactive") {
        recorder.stop()
      }
    } catch {
      mediaRecorderRef.current = null
      stopVoiceMediaStream(mediaStreamRef.current)
      mediaStreamRef.current = null
      recordedChunksRef.current = []
      voiceStopActionRef.current = "cancel"
      setVoiceElapsedMs(0)
      setVoiceInterimText("")
      setBoardMessage("음성 녹음을 종료하지 못했습니다. 다시 시도해 주세요.")
    }
  }

  const handleCancelVoiceInput = () => {
    if (!isVoiceModeOn) return
    stopVoiceRecorder("cancel")
  }

  const handleConfirmVoiceInput = () => {
    if (!isVoiceModeOn) return
    stopVoiceRecorder("transcribe")
  }

  const handleToggleVoiceInput = () => {
    if (!isSpeechSupported) {
      setBoardMessage("현재 브라우저는 음성 녹음을 지원하지 않습니다.")
      return
    }
    if (isVoiceTranscribing || isVoiceModeOn) {
      return
    }

    setBoardMessage(null)
    setIsVoiceTranscribing(false)
    setVoiceInterimText("")
    void (async () => {
      try {
        if (!navigator.mediaDevices?.getUserMedia || typeof window.MediaRecorder === "undefined") {
          throw new Error("unsupported")
        }
        const stream = await navigator.mediaDevices.getUserMedia({
          audio: {
            echoCancellation: true,
            noiseSuppression: true,
            autoGainControl: true,
          },
        })
        stopVoiceMediaStream(mediaStreamRef.current)
        mediaStreamRef.current = stream

        const mimeType = pickSupportedRecordingMimeType()
        const recorder = mimeType
          ? new window.MediaRecorder(stream, { mimeType })
          : new window.MediaRecorder(stream)
        voiceStopActionRef.current = "cancel"
        recordedChunksRef.current = []

        recorder.ondataavailable = (event: BlobEvent) => {
          if (event.data && event.data.size > 0) {
            recordedChunksRef.current.push(event.data)
          }
        }

        recorder.onerror = () => {
          clearVoiceTimer()
          stopVoiceWaveMonitor()
          voiceStopActionRef.current = "cancel"
          setIsVoiceModeOn(false)
          setIsListening(false)
          setVoiceElapsedMs(0)
          setVoiceInterimText("")
          setBoardMessage("녹음 중 오류가 발생했습니다. 다시 시도해 주세요.")
        }

        recorder.onstop = () => {
          clearVoiceTimer()
          stopVoiceWaveMonitor()
          const stopAction = voiceStopActionRef.current
          voiceStopActionRef.current = "cancel"
          const chunks = [...recordedChunksRef.current]
          recordedChunksRef.current = []
          mediaRecorderRef.current = null
          stopVoiceMediaStream(mediaStreamRef.current)
          mediaStreamRef.current = null
          setIsVoiceModeOn(false)
          setIsListening(false)
          setVoiceElapsedMs(0)

          if (stopAction !== "transcribe") {
            setVoiceInterimText("")
            return
          }

          const recordedBlob = new Blob(chunks, { type: recorder.mimeType || mimeType || "audio/webm" })
          if (!recordedBlob.size) {
            setVoiceInterimText("")
            setBoardMessage("녹음된 음성이 없습니다. 다시 시도해 주세요.")
            return
          }

          setIsVoiceTranscribing(true)
          setVoiceInterimText("음성을 텍스트로 변환 중...")
          void (async () => {
            try {
              const transcribedText = await transcribeVoiceBlob(recordedBlob)
              setQuery((prev) =>
                prev.trim().length > 0 ? `${prev.trimEnd()} ${transcribedText}` : transcribedText
              )
              setBoardMessage(null)
            } catch (err: unknown) {
              const message = normalizeRequestErrorMessage(err, "음성 인식에 실패했습니다.")
              setBoardMessage(message)
            } finally {
              setIsVoiceTranscribing(false)
              setVoiceInterimText("")
            }
          })()
        }

        mediaRecorderRef.current = recorder
        recorder.start(250)
        startVoiceWaveMonitor(stream)
        setIsVoiceModeOn(true)
        setIsListening(true)
        setVoiceInterimText("듣는 중...")
        startVoiceTimer()
      } catch (err: unknown) {
        clearVoiceTimer()
        stopVoiceWaveMonitor()
        stopVoiceMediaStream(mediaStreamRef.current)
        mediaStreamRef.current = null
        mediaRecorderRef.current = null
        recordedChunksRef.current = []
        voiceStopActionRef.current = "cancel"
        setIsVoiceModeOn(false)
        setIsListening(false)
        setVoiceElapsedMs(0)
        const name = String((err as any)?.name || "").toLowerCase()
        if (name === "notallowederror" || name === "securityerror") {
          setBoardMessage("마이크 권한이 필요합니다. 브라우저 설정에서 허용해 주세요.")
        } else {
          setBoardMessage("음성 녹음을 시작하지 못했습니다. 마이크를 확인해 주세요.")
        }
        setVoiceInterimText("")
      }
    })()
  }

  useEffect(() => {
    if (isLoading) {
      const recorder = mediaRecorderRef.current
      if (recorder && recorder.state !== "inactive") {
        voiceStopActionRef.current = "cancel"
        try {
          recorder.stop()
        } catch {}
      }
      clearVoiceTimer()
      stopVoiceWaveMonitor()
      setIsVoiceTranscribing(false)
      setIsVoiceModeOn(false)
      setIsListening(false)
      setVoiceElapsedMs(0)
      setVoiceInterimText("")
    }
  }, [isLoading])

  const writeTextToClipboard = async (text: string) => {
    const value = String(text || "")
    if (!value) return false

    try {
      if (typeof navigator !== "undefined" && navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(value)
        return true
      }
    } catch {
      // Fallback below for environments where Clipboard API is blocked.
    }

    if (typeof document === "undefined") return false

    let textarea: HTMLTextAreaElement | null = null
    try {
      textarea = document.createElement("textarea")
      textarea.value = value
      textarea.setAttribute("readonly", "")
      textarea.style.position = "fixed"
      textarea.style.top = "-9999px"
      textarea.style.left = "-9999px"
      textarea.style.opacity = "0"
      document.body.appendChild(textarea)
      textarea.focus()
      textarea.select()
      textarea.setSelectionRange(0, textarea.value.length)
      return document.execCommand("copy")
    } catch {
      return false
    } finally {
      if (textarea && textarea.parentNode) {
        textarea.parentNode.removeChild(textarea)
      }
    }
  }

  const handleCopyMessage = async (messageKey: string, text: string) => {
    const trimmed = text.trim()
    if (!trimmed) return
    try {
      const copied = await writeTextToClipboard(text)
      if (!copied) throw new Error("clipboard copy failed")
      setCopiedMessageId(messageKey)
      if (messageCopyTimerRef.current !== null) {
        window.clearTimeout(messageCopyTimerRef.current)
      }
      messageCopyTimerRef.current = window.setTimeout(() => {
        setCopiedMessageId((current) => (current === messageKey ? null : current))
        messageCopyTimerRef.current = null
      }, 1600)
    } catch {
      setBoardMessage("클립보드 복사에 실패했습니다.")
    }
  }

  const handleRerunMessage = async (text: string) => {
    const trimmed = text.trim()
    if (!trimmed || isLoading) return
    await runQuery(text)
  }

  const handleQuickQuestion = async (text: string) => {
    await runQuery(text)
  }

  const deriveDashboardCategory = (text: string) => {
    const normalized = text.toLowerCase()
    if (normalized.includes("생존") || normalized.includes("survival")) return "생존분석"
    if (normalized.includes("재입원") || normalized.includes("readmission")) return "재입원"
    if (normalized.includes("icu")) return "ICU"
    if (normalized.includes("응급") || normalized.includes("emergency")) return "응급실"
    if (normalized.includes("사망") || normalized.includes("mortality")) return "사망률"
    return "전체"
  }

  const saveQueryToFolder = async ({
    entry,
    folderForCache,
    newFolderForDisk,
  }: {
    entry: Record<string, unknown>
    folderForCache?: { id: string; name: string; createdAt?: string | null } | null
    newFolderForDisk?: { id: string; name: string; createdAt?: string | null } | null
  }) => {
    const previousCache = readDashboardCache<Record<string, unknown>, Record<string, unknown>>(chatHistoryUser)
    const normalizedEntryId = String(entry.id || "").trim()

    updateDashboardCache<Record<string, unknown>, Record<string, unknown>>(chatHistoryUser, (snapshot) => {
      const nextQueries = snapshot.queries.filter((item) => {
        if (!item || typeof item !== "object") return true
        return String((item as Record<string, unknown>).id || "").trim() !== normalizedEntryId
      })
      nextQueries.unshift(entry)

      let nextFolders = [...snapshot.folders]
      const normalizedFolderId = String(folderForCache?.id || "").trim()
      const normalizedFolderName = String(folderForCache?.name || "").trim()
      if (normalizedFolderId && normalizedFolderName) {
        nextFolders = snapshot.folders.filter((item) => {
          if (!item || typeof item !== "object") return true
          return String((item as Record<string, unknown>).id || "").trim() !== normalizedFolderId
        })
        nextFolders.push({
          id: normalizedFolderId,
          name: normalizedFolderName,
          createdAt: folderForCache?.createdAt || null,
        })
      }

      return { queries: nextQueries, folders: nextFolders }
    })

    try {
      const saveRes = await fetchWithTimeout(apiUrl("/dashboard/saveQuery"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          user: chatHistoryUser || null,
          question: String(entry.title || "").trim() || "저장된 쿼리",
          sql: String(entry.query || "").trim(),
          metadata: {
            row_count: effectiveTotalRows ?? 0,
            column_count: previewColumns.length,
            row_cap: previewRowCap ?? null,
            total_count: previewTotalCount,
            summary: summary || "",
            insight: String(entry.insight || "").trim(),
            llm_summary: String(entry.llmSummary || "").trim(),
            cohort: entry.cohort || null,
            pdf_analysis: entry.pdfAnalysis || null,
            stats: Array.isArray(entry.stats) ? entry.stats : [],
            recommended_charts: Array.isArray(entry.recommendedCharts) ? entry.recommendedCharts : [],
            primary_chart:
              entry.primaryChart && typeof entry.primaryChart === "object" ? entry.primaryChart : null,
            mode: mode || "",
            entry,
            new_folder: newFolderForDisk || null,
          },
        }),
      }, 15000)
      if (!saveRes.ok) {
        throw new Error("save failed")
      }
      return saveRes
    } catch (saveError) {
      if (previousCache) {
        writeDashboardCache(chatHistoryUser, {
          queries: previousCache.queries,
          folders: previousCache.folders,
        })
      } else {
        clearDashboardCache(chatHistoryUser)
      }
      throw saveError
    }
  }

  const loadDashboardFolders = async () => {
    setSaveFoldersLoading(true)
    try {
      const res = await fetchWithTimeout(apiUrlWithUser("/dashboard/queries"), {}, 12000)
      if (!res.ok) throw new Error("failed to load folders")
      const data = await res.json()
      const folders = Array.isArray(data?.folders) ? data.folders : []
      const queries = Array.isArray(data?.queries) ? data.queries : []

      const folderMap = new Map<string, DashboardFolderOption>()
      const nameKeyMap = new Map<string, DashboardFolderOption>()

      const pushFolder = (idRaw: unknown, nameRaw: unknown) => {
        const id = String(idRaw || "").trim()
        const name = String(nameRaw || "").trim()
        if (!id || !name) return
        if (!folderMap.has(id)) {
          const item = { id, name }
          folderMap.set(id, item)
          nameKeyMap.set(name.toLowerCase(), item)
        }
      }

      folders.forEach((item: any) => {
        pushFolder(item?.id, item?.name)
      })

      queries.forEach((item: any) => {
        const folderId = String(item?.folderId || "").trim()
        const category = String(item?.category || "").trim()
        if (!category) return
        if (folderId) {
          pushFolder(folderId, category)
          return
        }
        const key = category.toLowerCase()
        const existed = nameKeyMap.get(key)
        if (existed) return
        const syntheticId = `category:${encodeURIComponent(category)}`
        pushFolder(syntheticId, category)
      })

      const mapped = Array.from(folderMap.values()).sort((a, b) => a.name.localeCompare(b.name, "ko"))
      setSaveFolderOptions(mapped)
      if (!mapped.length) {
        setSaveFolderId("")
      } else if (!mapped.some((item) => item.id === saveFolderId)) {
        setSaveFolderId(mapped[0].id)
      }
    } catch {
      setSaveFolderOptions([])
      setSaveFolderId("")
    } finally {
      setSaveFoldersLoading(false)
    }
  }

  const openSaveDialog = async () => {
    if (!displaySql && !currentSql) {
      clearBoardMessageTimer()
      setBoardMessage("저장할 SQL이 없습니다.")
      return
    }
    const title = (lastQuestion || query || "저장된 쿼리").trim() || "저장된 쿼리"
    setSaveTitle(title)
    setSaveFolderMode("existing")
    setSaveNewFolderName("")
    clearBoardMessageTimer()
    setBoardMessage(null)
    setIsSaveDialogOpen(true)
    await loadDashboardFolders()
  }

  const handleSaveToDashboard = async () => {
    const finalTitle = (saveTitle || lastQuestion || query || "저장된 쿼리").trim()
    if (!finalTitle) {
      setBoardMessage("저장 이름을 입력해주세요.")
      return
    }

    const newFolderName = saveNewFolderName.trim()
    if (saveFolderMode === "new" && !newFolderName) {
      setBoardMessage("새 폴더 이름을 입력해주세요.")
      return
    }

    const selectedFolder = saveFolderOptions.find((item) => item.id === saveFolderId) || null
    const folderId =
      saveFolderMode === "new"
        ? `folder-${Date.now()}`
        : selectedFolder?.id || ""
    const folderName =
      saveFolderMode === "new"
        ? newFolderName
        : selectedFolder?.name || deriveDashboardCategory(finalTitle)

    const category = folderName || deriveDashboardCategory(finalTitle)
    const nowIso = new Date().toISOString()
    const saveTs = Date.now()
    const metrics = [
      { label: "행 수", value: String(effectiveTotalRows ?? 0) },
      { label: "전체 행 수", value: previewTotalCount != null ? String(previewTotalCount) : "-" },
      { label: "컬럼 수", value: String(previewColumns.length) },
      { label: "ROW CAP", value: previewRowCap != null ? String(previewRowCap) : "-" },
    ]
    const previewPayload =
      previewColumns.length && previewRows.length
        ? {
            columns: previewColumns,
            rows: previewRows.slice(0, 50),
            row_count: previewRowCount ?? previewRows.length,
            row_cap: previewRowCap ?? null,
            total_count: previewTotalCount,
          }
        : undefined
    const resolvedChartType: "line" | "bar" | "pie" = (() => {
      const specType = normalizeChartType(recommendedAnalysis?.chart_spec?.chart_type || "")
      if (specType === "line") return "line"
      if (specType === "pie" || specType === "nested_pie" || specType === "sunburst") return "pie"
      if (chartForRender?.type === "line") return "line"
      return "bar"
    })()

    const normalizedSavedInsight = normalizeInsightText(integratedInsight)
    const savedInsight = isKoreanInsightText(normalizedSavedInsight) ? normalizedSavedInsight : ""
    const cohortProvenance = buildDashboardCohortProvenance(activeCohortContextRef.current)
    const pdfAnalysisPayload = buildDashboardPdfAnalysisSnapshot(activeCohortContextRef.current)
    const statsPayload = toDashboardStatsRows(statsRows)

    const analysisRecommendedCharts: DashboardChartSpec[] = topRecommendedAnalyses
      .slice(0, 3)
      .map((analysis, index) => {
        const chartType = normalizeChartType(analysis?.chart_spec?.chart_type || "chart").toUpperCase() || "CHART"
        const imageDataUrl = String(analysis?.image_data_url || "").trim()
        const imagePayload = imageDataUrl.startsWith("data:image/") ? imageDataUrl : undefined
        return {
          id: `recommended-${saveTs}-${index + 1}`,
          type: chartType,
          x: analysis?.chart_spec?.x || undefined,
          y: analysis?.chart_spec?.y || undefined,
          config: {
            chartSpec: analysis?.chart_spec || null,
            figureJson: analysis?.figure_json || null,
            reason: analysis?.reason || "",
            summary: analysis?.summary || "",
            renderEngine: analysis?.render_engine || "",
          },
          thumbnailUrl: imagePayload,
          pngUrl: imagePayload,
        }
      })

    const recommendedChartsPayload = [...analysisRecommendedCharts]

    const primaryChartPayload: DashboardChartSpec | undefined = analysisRecommendedCharts[0]

    const newEntry = {
      id: `dashboard-${Date.now()}`,
      title: finalTitle,
      description: summary || "쿼리 결과 요약",
      insight: savedInsight || undefined,
      llmSummary: savedInsight || undefined,
      query: displaySql || currentSql,
      lastRun: "방금 전",
      executedAt: nowIso,
      isPinned: true,
      category,
      folderId: folderId || undefined,
      preview: previewPayload,
      cohort: cohortProvenance,
      pdfAnalysis: pdfAnalysisPayload,
      stats: statsPayload,
      recommendedCharts: recommendedChartsPayload,
      primaryChart: primaryChartPayload,
      metrics,
      chartType: resolvedChartType,
    }
    const folderPayload =
      folderId && folderName
        ? {
            id: folderId,
            name: folderName,
            createdAt: saveFolderMode === "new" ? nowIso : null,
          }
        : null
    const newFolderPayload =
      saveFolderMode === "new" && folderPayload
        ? {
            id: folderPayload.id,
            name: folderPayload.name,
            createdAt: folderPayload.createdAt,
          }
        : null

    setBoardSaving(true)
    clearBoardMessageTimer()
    setBoardMessage(null)
    try {
      await saveQueryToFolder({
        entry: newEntry as Record<string, unknown>,
        folderForCache: folderPayload,
        newFolderForDisk: newFolderPayload,
      })
      updateActiveTab({ question: finalTitle })
      setLastQuestion(finalTitle)
      showTransientBoardMessage("결과 보드에 저장했습니다.", 1400, { closeSaveDialog: true })
    } catch {
      clearBoardMessageTimer()
      setBoardMessage("결과 보드 저장에 실패했습니다.")
    } finally {
      setBoardSaving(false)
    }
  }

  const executeAdvancedSql = async ({
    qid,
    sql,
    questionForSuggestions,
    addAssistantMessage = true,
    tabId,
  }: {
    qid?: string
    sql?: string
    questionForSuggestions?: string
    addAssistantMessage?: boolean
    tabId?: string
  }) => {
    const currentActiveContext = activeCohortContextRef.current
    const clientRequestId = createClientRequestId()
    const body: Record<string, any> = {
      user_ack: true,
      client_request_id: clientRequestId,
      model: selectedModel || undefined,
      user_id: chatHistoryUser,
      user_name: chatUser,
      user_role: chatUserRole,
      cohort_apply: Boolean(currentActiveContext),
      cohort_id: currentActiveContext?.cohortId || undefined,
      cohort_name: currentActiveContext?.cohortName || undefined,
      cohort_type: currentActiveContext?.type || undefined,
      cohort_sql: currentActiveContext?.cohortSql || undefined,
    }
    if (qid) {
      body.qid = qid
    }
    if (sql?.trim()) {
      body.sql = sql.trim()
    }
    if (questionForSuggestions?.trim()) {
      body.question = questionForSuggestions.trim()
    }

    const maxAttempts = 3
    let res: Response | null = null
    let lastMessage = ""
    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      try {
        res = await fetchWithTimeout(apiUrl("/query/run"), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body)
        }, RUN_TIMEOUT_MS)
      } catch (fetchErr: any) {
        lastMessage = normalizeRequestErrorMessage(fetchErr, "실행이 실패했습니다.")
        const retryable = isNetworkFetchError(fetchErr) || isRetryableServerErrorMessage(lastMessage)
        if (!retryable || attempt >= maxAttempts) {
          throw new Error(lastMessage)
        }
        await sleepMs(250 * attempt)
        continue
      }
      if (res.ok) break
      lastMessage = await readError(res)
      const retryable = res.status >= 500 || isRetryableServerErrorMessage(lastMessage)
      if (!retryable || attempt >= maxAttempts) {
        throw new Error(lastMessage)
      }
      await sleepMs(250 * attempt)
    }
    if (!res || !res.ok) {
      throw new Error(lastMessage || "실행이 실패했습니다.")
    }

    const data: RunResponse = await res.json()
    const targetTabId = tabId || activeTabIdRef.current
    if (isTargetTabActive(targetTabId)) {
      setRunResult(data)
      setShowResults(true)
      setIsEditing(false)
    }
    const fetchedRows = Number(data.result?.row_count ?? 0)
    const totalRows =
      typeof data.result?.total_count === "number" && Number.isFinite(data.result.total_count)
        ? data.result.total_count
        : null
    const answerPromise = requestQueryAnswerMessage({
      questionText: questionForSuggestions || lastQuestion || "",
      sqlText: (data.sql || sql || "").trim(),
      previewData: data.result || null,
      totalRows,
      fetchedRows,
    })
    void fetchVisualizationPlan(
      (data.sql || sql || "").trim(),
      questionForSuggestions || lastQuestion || "",
      data.result || null,
      targetTabId
    )

    if (isTargetTabActive(targetTabId)) {
      setSuggestedQuestions([])
    }
    if (targetTabId) {
      updateTab(targetTabId, {
        sql: data.sql || sql || "",
        runResult: data,
        resultData: data.result || null,
        visualization: null,
        suggestedQuestions: [],
        insight: "",
        status: "success",
        error: null,
      })
    }

    const answerPayload = await answerPromise
    const suggestions = answerPayload.suggestedQuestions
    if (isTargetTabActive(targetTabId)) {
      setSuggestedQuestions(suggestions)
    }
    if (targetTabId) {
      updateTab(targetTabId, {
        suggestedQuestions: suggestions,
      })
    }

    if (addAssistantMessage) {
      setMessages(prev => [
        ...prev,
        {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: appendSuggestions(
            answerPayload.answerText,
            suggestions
          ),
          timestamp: new Date()
        }
      ])
    }
  }

  const handleExecuteEdited = async (overrideSql?: string) => {
    if (!response || mode !== "advanced") return
    setIsLoading(true)
    setError(null)
    setBoardMessage(null)
    updateActiveTab({ status: "pending", error: null })
    try {
      const sqlToRun = (overrideSql || editedSql || currentSql).trim()
      await executeAdvancedSql({
        qid: response.qid,
        sql: sqlToRun,
        questionForSuggestions: lastQuestion,
        addAssistantMessage: true,
        tabId: activeTabId,
      })
    } catch (err: any) {
      const message =
        err?.name === "AbortError"
          ? "요청 시간이 초과되었습니다. 잠시 후 다시 시도해주세요."
          : normalizeRequestErrorMessage(err, "실행이 실패했습니다.")
      setError(message)
      updateActiveTab({ status: "error", error: message })
      setMessages(prev => [
        ...prev,
        {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: `오류: ${message}`,
          timestamp: new Date()
        }
      ])
    } finally {
      setIsLoading(false)
    }
  }

  useEffect(() => {
    const applyPendingCohortContext = () => {
      if (typeof window === "undefined") return false
      const raw =
        localStorage.getItem(pendingActiveCohortContextKey) ||
        localStorage.getItem(PENDING_ACTIVE_COHORT_CONTEXT_KEY) ||
        localStorage.getItem(pendingLegacyPdfCohortContextKey) ||
        localStorage.getItem(LEGACY_PENDING_PDF_COHORT_CONTEXT_KEY)
      if (!raw) return false
      clearPendingActiveCohortContext(chatHistoryUser)

      let parsed: unknown = null
      try {
        parsed = JSON.parse(raw)
      } catch {
        return false
      }
      const context = normalizePendingPdfCohortContext(parsed)
      if (!context) return false

      const starterQuestions = buildCohortStarterQuestions(context)
      requestTokenRef.current += 1
      setIsLoading(false)
      setError(null)
      setBoardMessage(null)
      setMessages([])
      setResponse(null)
      setRunResult(null)
      setVisualizationResult(null)
      setVisualizationError(null)
      setShowResults(false)
      setShowSqlPanel(false)
      setShowQueryResultPanel(false)
      setEditedSql("")
      setIsEditing(false)
      setQuery("")
      setLastQuestion("")
      setSuggestedQuestions(starterQuestions)
      setQuickQuestions(starterQuestions)
      setResultTabs([])
      setActiveTabId("")
      activeTabIdRef.current = ""
      applyActiveCohort(context)
      return true
    }

    const runPendingDashboardQuery = async () => {
      if (typeof window === "undefined") return false
      const raw =
        localStorage.getItem(pendingDashboardQueryKey) ||
        localStorage.getItem("ql_pending_dashboard_query") ||
        sessionStorage.getItem(pendingDashboardQueryKey) ||
        sessionStorage.getItem("ql_pending_dashboard_query")
      if (!raw) return false
      localStorage.removeItem(pendingDashboardQueryKey)
      localStorage.removeItem("ql_pending_dashboard_query")
      sessionStorage.removeItem(pendingDashboardQueryKey)
      sessionStorage.removeItem("ql_pending_dashboard_query")

      let parsed: { question?: string; sql?: string; chartType?: "line" | "bar" | "pie" } | null = null
      try {
        parsed = JSON.parse(raw)
      } catch {
        return false
      }
      const sqlText = String(parsed?.sql || "").trim()
      if (!sqlText) return false

      const questionText = String(parsed?.question || "").trim() || "대시보드 저장 쿼리"
      const preferredChartType =
        parsed?.chartType === "line" || parsed?.chartType === "bar" || parsed?.chartType === "pie"
          ? parsed.chartType
          : null
      applyActiveCohort(null)
      const tab = createResultTab(questionText)
      tab.sql = sqlText
      tab.editedSql = sqlText
      tab.showSqlPanel = true
      tab.preferredChartType = preferredChartType
      syncPanelsFromTab(tab)
      setResultTabs((prev) => [tab, ...prev])
      activeTabIdRef.current = tab.id
      setActiveTabId(tab.id)
      setShowResults(true)
      setIsLoading(true)
      setError(null)
      setBoardMessage(null)
      setLastQuestion(questionText)
      setSuggestedQuestions([])

      try {
        await executeAdvancedSql({
          sql: sqlText,
          questionForSuggestions: questionText,
          addAssistantMessage: true,
          tabId: tab.id,
        })
      } catch (err: any) {
        const message =
          err?.name === "AbortError"
            ? "요청 시간이 초과되었습니다. 잠시 후 다시 시도해주세요."
            : normalizeRequestErrorMessage(err, "실행이 실패했습니다.")
        setError(message)
        updateTab(tab.id, { status: "error", error: message })
      } finally {
        setIsLoading(false)
      }
      return true
    }

    const runPendingEntries = async () => {
      const dashboardHandled = await runPendingDashboardQuery()
      if (dashboardHandled) return
      applyPendingCohortContext()
    }

    void runPendingEntries()
    const onOpenQueryView = () => {
      void runPendingEntries()
    }
    window.addEventListener("ql-open-query-view", onOpenQueryView)
    return () => {
      window.removeEventListener("ql-open-query-view", onOpenQueryView)
    }
  }, [chatHistoryUser, pendingDashboardQueryKey, pendingActiveCohortContextKey, pendingLegacyPdfCohortContextKey])

  const handleCopySql = async () => {
    if (!displaySql) return
    try {
      const copied = await writeTextToClipboard(displaySql)
      if (!copied) throw new Error("clipboard copy failed")
      setIsSqlCopied(true)
      if (sqlCopyTimerRef.current !== null) {
        window.clearTimeout(sqlCopyTimerRef.current)
      }
      sqlCopyTimerRef.current = window.setTimeout(() => {
        setIsSqlCopied(false)
        sqlCopyTimerRef.current = null
      }, 1600)
    } catch {
      setBoardMessage("클립보드 복사에 실패했습니다.")
    }
  }

  const handleDownloadCsv = () => {
    if (!previewColumns.length || !previewRows.length) return
    const header = previewColumns.join(",")
    const body = previewRows
      .map((row) =>
        previewColumns
          .map((_, idx) => {
            const cell = row[idx]
            const text = cell == null ? "" : String(cell)
            if (/[\",\\n]/.test(text)) {
              return `"${text.replace(/\"/g, '""')}"`
            }
            return text
          })
          .join(",")
      )
      .join("\r\n")
    // Use UTF-8 BOM + CRLF so Excel opens Korean text and row breaks correctly.
    const blob = new Blob([`\uFEFF${header}\r\n${body}`], { type: "text/csv;charset=utf-8;" })
    const url = URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = "results.csv"
    a.click()
    URL.revokeObjectURL(url)
  }

  const handleResetConversation = () => {
    setMessages([])
    setResponse(null)
    setRunResult(null)
    setVisualizationResult(null)
    setVisualizationError(null)
    setShowResults(false)
    setShowSqlPanel(false)
    setShowQueryResultPanel(false)
    setQuery("")
    setEditedSql("")
    setIsEditing(false)
    setLastQuestion("")
    setSuggestedQuestions([])
    applyActiveCohort(null)
    setError(null)
    setResultTabs([])
    activeTabIdRef.current = ""
    setActiveTabId("")
    setCopiedMessageId(null)
    setIsSqlCopied(false)
    clearPendingActiveCohortContext(chatHistoryUser)
    fetch(apiUrl("/chat/history"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ user: chatHistoryUser, state: null })
    }).catch(() => {})
  }

  const handleCloseTab = (tabId: string) => {
    setResultTabs((prev) => {
      const next = prev.filter((tab) => tab.id !== tabId)
      if (activeTabId === tabId) {
        const fallback = next[0]
        syncPanelsFromTab(fallback || null)
        activeTabIdRef.current = fallback?.id || ""
        setActiveTabId(fallback?.id || "")
      }
      return next
    })
  }

  const handleActivateTab = (tabId: string, promote = false) => {
    const targetTab = resultTabs.find((item) => item.id === tabId)
    if (!targetTab) return
    syncPanelsFromTab(targetTab)
    activeTabIdRef.current = tabId
    setActiveTabId(tabId)
    if (!promote) return
    setResultTabs((prev) => {
      const idx = prev.findIndex((item) => item.id === tabId)
      if (idx <= 0) return prev
      const target = prev[idx]
      return [target, ...prev.slice(0, idx), ...prev.slice(idx + 1)]
    })
  }

  return (
    <div className="flex flex-col h-[calc(100vh-56px)] sm:h-[calc(100vh-64px)]">
      {/* Main Content */}
      <div ref={mainContentRef} className="flex-1 min-h-0 flex flex-col lg:flex-row overflow-hidden">
        {/* Chat Panel */}
        <div
          className={cn(
            "min-h-0 flex flex-col border-border",
            shouldShowResizablePanels ? "lg:flex-none" : "flex-1"
          )}
          style={chatPanelStyle}
        >
          {isPdfContextTableVisible && activeCohortContext && pdfContextBanner.context && (
            <div className="shrink-0 border-b border-border bg-background/95 p-4">
              <div className="rounded-lg border border-primary/25 bg-primary/5 p-3">
                <div className="mb-2 flex items-center justify-between gap-2">
                  <div className="min-w-0">
                    <div className="flex items-center gap-1.5 text-[12px] font-semibold text-primary">
                      <FileText className="h-3.5 w-3.5" />
                      코호트 컨텍스트 적용됨
                    </div>
                    <p className="truncate text-[12px] text-muted-foreground">{pdfContextBanner.context.pdfName}</p>
                  </div>
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    className="h-7 px-2 text-[11px]"
                    onClick={clearActiveCohortContext}
                  >
                    해제
                  </Button>
                </div>
                <div className="overflow-x-auto rounded-md border border-border/70 bg-background/90">
                  <table className="w-full min-w-[460px] text-xs">
                    <tbody>
                      <tr className="border-b border-border/70">
                        <th className="w-36 bg-muted/40 px-3 py-2 text-left font-medium text-muted-foreground">대상 환자 수</th>
                        <td className="px-3 py-2 text-foreground">
                          {pdfContextBanner.context.cohortSize != null
                            ? `${pdfContextBanner.context.cohortSize.toLocaleString()}명`
                            : "-"}
                        </td>
                      </tr>
                      <tr className="border-b border-border/70">
                        <th className="bg-muted/40 px-3 py-2 text-left font-medium text-muted-foreground">핵심 변수</th>
                        <td className="px-3 py-2 text-foreground">
                          {(() => {
                            const vars = Array.isArray(pdfContextBanner.context?.keyVariables)
                              ? pdfContextBanner.context.keyVariables
                              : []
                            if (!vars.length) return "-"
                            const preview = vars.slice(0, 3).join(", ")
                            const remains = Math.max(0, vars.length - 3)
                            return remains > 0 ? `${preview} +${remains} more` : preview
                          })()}
                        </td>
                      </tr>
                      <tr className="border-b border-border/70">
                        <th className="bg-muted/40 px-3 py-2 text-left font-medium text-muted-foreground">코호트 기준 요약</th>
                        <td className="px-3 py-2 text-foreground">
                          {activeCohortContext.criteriaSummaryKo || activeCohortContext.summaryKo || "-"}
                        </td>
                      </tr>
                      <tr>
                        <th className="bg-muted/40 px-3 py-2 text-left font-medium text-muted-foreground">적용 시각</th>
                        <td className="px-3 py-2 text-foreground">
                          {new Date(pdfContextBanner.context.appliedAt).toLocaleString()}
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>
                {visibleBannerQuestions.length > 0 && (
                  <div className="mt-3">
                    <div className="mb-2 flex items-center gap-1 text-[11px] text-muted-foreground">
                      <Sparkles className="h-3 w-3 text-primary" />
                      추천 질문
                    </div>
                    <div className="flex flex-wrap gap-2">
                      {visibleBannerQuestions.slice(0, 3).map((item) => (
                        <Button
                          key={`banner-cohort-question-${item}`}
                          type="button"
                          variant="outline"
                          size="sm"
                          className="h-auto min-h-7 max-w-full rounded-full bg-background/80 px-2.5 py-1.5 text-left text-[11px] leading-4 whitespace-normal break-words"
                          onClick={() => handleQuickQuestion(item)}
                          disabled={isLoading}
                        >
                          {item}
                        </Button>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}
          {/* Messages */}
          <div ref={chatScrollRef} className="flex-1 overflow-y-auto p-4 space-y-4">
            {messages.length === 0 ? (
              <div className="flex flex-col items-center justify-center h-full text-center">
                <div className="w-12 h-12 rounded-full bg-primary/10 flex items-center justify-center mb-4">
                  <Send className="w-6 h-6 text-primary" />
                </div>
                <h3 className="font-medium text-foreground mb-2">질문을 입력하세요</h3>
                <p className="text-sm text-muted-foreground max-w-sm">
                  {activeCohortContext
                    ? "상단 코호트 요약 표를 확인한 뒤 바로 질문할 수 있습니다."
                    : '예: "65세 이상 환자 코호트를 만들고 생존 곡선을 보여줘"'}
                </p>
                {visibleQuickQuestions.length > 0 && (
                  <div className="mt-4 flex flex-col gap-2 w-full max-w-sm">
                    {visibleQuickQuestions.map((item) => (
                      <Button
                        key={item}
                        variant="secondary"
                        size="sm"
                        onClick={() => handleQuickQuestion(item)}
                        disabled={isLoading}
                        className="h-auto min-h-8 w-full justify-start whitespace-normal break-words py-2 text-left text-xs"
                      >
                        {item}
                      </Button>
                    ))}
                  </div>
                )}
              </div>
            ) : (
              messages.map((message, idx) => {
                const messageCopyKey = `${message.id}-${message.role}-${idx}`
                const isUser = message.role === "user"
                const isAssistant = message.role === "assistant"
                const isLastMessage = idx === messages.length - 1
                const showSuggestions = isAssistant && isLastMessage && suggestedQuestions.length > 0
                const canActOnMessage = Boolean(message.content.trim())
                return (
                  <div key={messageCopyKey} className={cn(
                    "flex flex-col",
                    isUser ? "items-end" : "items-start"
                  )}>
                    {isUser ? (
                      <div className="group flex max-w-[85%] items-end gap-1">
                        <div className="flex shrink-0 items-center gap-0.5 pb-0.5 opacity-100 transition-opacity md:pointer-events-none md:opacity-0 md:group-hover:pointer-events-auto md:group-hover:opacity-100 md:group-focus-within:pointer-events-auto md:group-focus-within:opacity-100">
                          <Button
                            type="button"
                            variant="ghost"
                            size="icon-sm"
                            className="h-5 w-5 text-muted-foreground hover:text-foreground"
                            title={copiedMessageId === messageCopyKey ? "복사됨" : "복사"}
                            aria-label="질문 복사"
                            onClick={() => {
                              void handleCopyMessage(messageCopyKey, message.content)
                            }}
                            disabled={!canActOnMessage}
                          >
                            {copiedMessageId === messageCopyKey ? (
                              <Check className="h-2.5 w-2.5 text-emerald-500" />
                            ) : (
                              <Copy className="h-2.5 w-2.5" />
                            )}
                          </Button>
                          <Button
                            type="button"
                            variant="ghost"
                            size="icon-sm"
                            className="h-5 w-5 text-muted-foreground hover:text-foreground"
                            title="재실행"
                            aria-label="질문 재실행"
                            onClick={() => {
                              void handleRerunMessage(message.content)
                            }}
                            disabled={isLoading || !canActOnMessage}
                          >
                            <RefreshCw className="h-2.5 w-2.5" />
                          </Button>
                        </div>
                        <div className="rounded-lg bg-primary p-3 text-primary-foreground">
                          <p className="text-sm whitespace-pre-line break-words">{message.content}</p>
                          <span className="mt-1 block text-[10px] opacity-70">
                            {message.timestamp.toLocaleTimeString()}
                          </span>
                        </div>
                      </div>
                    ) : (
                      <div className="group flex max-w-[80%] items-end gap-1">
                        <div className="rounded-lg bg-secondary p-3">
                          <p className="text-sm whitespace-pre-line break-words">{message.content}</p>
                          <span className="mt-1 block text-[10px] opacity-70">
                            {message.timestamp.toLocaleTimeString()}
                          </span>
                        </div>
                        <div className="flex shrink-0 items-center gap-0.5 pb-0.5 opacity-100 transition-opacity md:pointer-events-none md:opacity-0 md:group-hover:pointer-events-auto md:group-hover:opacity-100 md:group-focus-within:pointer-events-auto md:group-focus-within:opacity-100">
                          <Button
                            type="button"
                            variant="ghost"
                            size="icon-sm"
                            className="h-5 w-5 text-muted-foreground hover:text-foreground"
                            title={copiedMessageId === messageCopyKey ? "복사됨" : "복사"}
                            aria-label="답변 복사"
                            onClick={() => {
                              void handleCopyMessage(messageCopyKey, message.content)
                            }}
                            disabled={!canActOnMessage}
                          >
                            {copiedMessageId === messageCopyKey ? (
                              <Check className="h-2.5 w-2.5 text-emerald-500" />
                            ) : (
                              <Copy className="h-2.5 w-2.5" />
                            )}
                          </Button>
                        </div>
                      </div>
                    )}
                    {showSuggestions && (
                      <div className="mt-2 max-w-[80%] rounded-lg border border-border/60 bg-secondary/40 p-2">
                        <div className="mb-2 flex items-center gap-1 text-[10px] text-muted-foreground">
                          <Sparkles className="h-3 w-3 text-primary" />
                          추천 질문
                        </div>
                        <div className="flex flex-wrap gap-2">
                          {suggestedQuestions.slice(0, 3).map((item) => (
                            <Button
                              key={item}
                              variant="outline"
                              size="sm"
                              onClick={() => handleQuickQuestion(item)}
                              disabled={isLoading}
                              className="h-auto min-h-7 max-w-full rounded-full bg-background/80 px-2.5 py-1.5 text-left text-[10px] leading-4 whitespace-normal break-words shadow-xs"
                            >
                              {item}
                            </Button>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                )
              })
            )}
            {isLoading && (
              <div className="flex justify-start">
                <div className="w-full max-w-[320px] rounded-lg bg-secondary p-3">
                  <div className="mb-2 flex items-center justify-between gap-2">
                    <span className="text-sm">분석 중...</span>
                    <span className="text-[11px] text-muted-foreground">{Math.round(loadingProgress)}%</span>
                  </div>
                  <Progress value={loadingProgress} className="h-1.5" />
                </div>
              </div>
            )}
          </div>

          {/* Input */}
          <div className="p-4 border-t border-border">
            <div className="rounded-2xl border border-border bg-card px-2.5 py-2 shadow-xs">
              {activeCohortContext && (
                <div className="mb-2 flex items-center justify-between gap-2 rounded-xl border border-primary/25 bg-primary/5 px-2.5 py-2">
                  <div className="min-w-0">
                    <div className="flex items-center gap-1.5 text-[11px] font-medium text-primary">
                      <FileText className="h-3.5 w-3.5" />
                      코호트 컨텍스트 적용됨
                    </div>
                    <p className="truncate text-[11px] text-muted-foreground">
                      {activeCohortContext.filename || activeCohortContext.cohortName}
                    </p>
                  </div>
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    className="h-7 px-2 text-[11px]"
                    onClick={clearActiveCohortContext}
                  >
                    해제
                  </Button>
                </div>
              )}
              <Textarea
                placeholder="자연어로 질문하세요..."
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                className="min-h-[64px] resize-none border-0 bg-transparent px-1 py-1.5 text-sm shadow-none focus-visible:ring-0"
                onKeyDown={(e) => {
                  if (e.key === "Enter" && !e.shiftKey) {
                    e.preventDefault()
                    handleSubmit()
                  }
                }}
              />
              <div className="mt-2 flex items-center justify-between gap-2">
                <div className="flex min-w-0 items-center gap-1">
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon-sm"
                    className="h-8 w-8 rounded-full"
                    title="코호트 라이브러리 열기"
                    aria-label="코호트 라이브러리 열기"
                    onClick={() => {
                      void loadSavedCohortLibrary()
                      setIsCohortLibraryOpen(true)
                    }}
                  >
                    <Plus className="h-4 w-4" />
                  </Button>
                  <Select value={selectedModel} onValueChange={setSelectedModel}>
                    <SelectTrigger className="h-8 w-[152px] rounded-full border-border/70 bg-background text-xs">
                      <SelectValue placeholder="모델 선택" />
                    </SelectTrigger>
                    <SelectContent>
                      {modelOptions.map((model) => (
                        <SelectItem key={model} value={model}>
                          {model}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <div className="relative">
                    {isVoiceModeOn && (
                      <span className="pointer-events-none absolute inset-0 rounded-full border border-primary/70 animate-ping" />
                    )}
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon-sm"
                      className={cn(
                        "relative h-8 w-8 rounded-full transition-colors",
                        isVoiceModeOn && "bg-primary/10 text-primary",
                        isVoiceTranscribing && "bg-primary/10 text-primary"
                      )}
                      title={
                        isVoiceTranscribing
                          ? "음성 변환 중"
                          : isVoiceModeOn
                            ? "음성 입력 중지"
                            : "음성 입력 시작"
                      }
                      aria-label={
                        isVoiceTranscribing
                          ? "음성 변환 중"
                          : isVoiceModeOn
                            ? "음성 입력 중지"
                            : "음성 입력 시작"
                      }
                      onClick={handleToggleVoiceInput}
                      disabled={!isSpeechSupported || isVoiceTranscribing || isVoiceModeOn}
                    >
                      {isVoiceTranscribing ? (
                        <Loader2 className="h-4 w-4 animate-spin" />
                      ) : (
                        <Mic className="h-4 w-4" />
                      )}
                    </Button>
                  </div>
                </div>
                <Button
                  onClick={handleSubmit}
                  disabled={isLoading || isVoiceModeOn || isVoiceTranscribing || !query.trim()}
                  className="h-9 w-9 rounded-full p-0"
                  title="전송"
                  aria-label="전송"
                >
                  <Send className="h-4 w-4" />
                </Button>
              </div>
              {isVoiceModeOn && (
                <div className="mt-2 flex items-center gap-2 rounded-full border border-border bg-card/95 px-2 py-1.5 text-foreground shadow-sm">
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon-sm"
                    onClick={handleCancelVoiceInput}
                    className="h-9 w-9 rounded-full border border-border bg-secondary text-secondary-foreground hover:bg-secondary/80"
                    title="녹음 취소"
                    aria-label="녹음 취소"
                  >
                    <X className="h-4 w-4" />
                  </Button>
                  <div className="min-w-0 flex-1 px-1">
                    <div className="h-7 overflow-hidden rounded-full border border-border/70 bg-muted/30 px-2">
                      <div className="flex h-full items-center gap-[3px]">
                        {voiceWaveLevels.map((level, idx) => (
                          <span
                            key={`voice-wave-${idx}`}
                            className="w-[2px] rounded-full bg-primary transition-[height,opacity] duration-75 ease-out"
                            style={{
                              height: `${4 + Math.round(level * 18)}px`,
                              opacity: 0.2 + Math.min(0.8, level),
                            }}
                          />
                        ))}
                      </div>
                    </div>
                  </div>
                  <span className="w-11 text-right text-lg font-semibold tabular-nums tracking-tight text-foreground">
                    {formatVoiceElapsed(voiceElapsedMs)}
                  </span>
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon-sm"
                    onClick={handleConfirmVoiceInput}
                    className="h-9 w-9 rounded-full bg-primary text-primary-foreground hover:bg-primary/90"
                    title="녹음 확정"
                    aria-label="녹음 확정"
                    disabled={!isListening}
                  >
                    <Check className="h-4 w-4" />
                  </Button>
                </div>
              )}
              {isVoiceTranscribing && (
                <div className="mt-2 flex items-center gap-2 rounded-lg border border-primary/30 bg-primary/5 px-2 py-1.5 text-[11px] text-primary">
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  <span className="font-medium">음성 변환 중...</span>
                  <span className="min-w-0 truncate text-muted-foreground">
                    {voiceInterimText || "녹음된 음성을 텍스트로 변환하고 있습니다."}
                  </span>
                </div>
              )}
              {!isSpeechSupported && (
                <div className="mt-2 px-1 text-[11px] text-muted-foreground">
                  이 브라우저는 음성 녹음을 지원하지 않습니다.
                </div>
              )}
            </div>
          </div>
        </div>

        {shouldShowResizablePanels && (
          <div
            role="separator"
            aria-orientation="vertical"
            aria-label="패널 크기 조정"
            aria-valuemin={30}
            aria-valuemax={70}
            aria-valuenow={Math.round(resultsPanelWidth)}
            onMouseDown={handlePanelResizeMouseDown}
            className={cn(
              "hidden lg:flex w-3 shrink-0 items-center justify-center border-x border-border/50 bg-card/30 cursor-col-resize select-none transition-colors",
              isPanelResizing && "bg-secondary/60"
            )}
          >
            <div className="h-16 w-1 rounded-full bg-border/80" />
          </div>
        )}

        {/* Results Panel */}
        {showResults && (
          <div
            className={cn(
              "min-h-0 flex flex-col overflow-hidden border-t lg:border-t-0 border-border max-h-[50vh] lg:max-h-none",
              shouldShowResizablePanels && "lg:flex-none"
            )}
            style={resultsPanelStyle}
          >
            <div className="flex-1 overflow-y-auto p-4 pb-6 space-y-4">
              {resultTabs.length > 0 && (
                <div ref={tabHeaderRef} className="flex items-center gap-2 overflow-hidden pb-1">
                  {latestVisibleTabs.map((tab) => (
                    <button
                      key={tab.id}
                      type="button"
                      onClick={() => handleActivateTab(tab.id)}
                      title={tab.question || "새 질문"}
                      className={cn(
                        "inline-flex items-center gap-1.5 rounded-md border px-2 py-1 text-[11px]",
                        activeTabId === tab.id ? "bg-secondary border-primary/30" : "bg-background"
                      )}
                    >
                      <span className="max-w-[110px] truncate">{compactTabLabel(tab.question, 10)}</span>
                      <span
                        className={cn(
                          "inline-block h-2 w-2 rounded-full",
                          tab.status === "pending" && "bg-yellow-500",
                          tab.status === "success" && "bg-primary",
                          tab.status === "error" && "bg-destructive"
                        )}
                      />
                      <span
                        onClick={(e) => {
                          e.stopPropagation()
                          handleCloseTab(tab.id)
                        }}
                        className="text-muted-foreground hover:text-foreground"
                        role="button"
                        aria-label="탭 닫기"
                      >
                        ×
                      </span>
                    </button>
                  ))}
                  {pastQueryTabs.length > 0 && (
                    <Button
                      variant="outline"
                      size="sm"
                      className="h-7 shrink-0"
                      onClick={() => setIsPastQueriesDialogOpen(true)}
                    >
                      이전 쿼리 {pastQueryTabs.length}개
                    </Button>
                  )}
                </div>
              )}
              <div className="flex flex-wrap items-center gap-2">
                <Button
                  variant={showSqlPanel ? "secondary" : "outline"}
                  size="sm"
                  className="h-7"
                  onClick={() => {
                    const next = !showSqlPanel
                    setShowSqlPanel(next)
                    updateActiveTab({ showSqlPanel: next })
                  }}
                >
                  {showSqlPanel ? "SQL 숨기기" : "SQL 보기"}
                </Button>
                <Button
                  variant={showQueryResultPanel ? "secondary" : "outline"}
                  size="sm"
                  className="h-7"
                  onClick={() => {
                    const next = !showQueryResultPanel
                    setShowQueryResultPanel(next)
                    updateActiveTab({ showQueryResultPanel: next })
                  }}
                >
                  {showQueryResultPanel ? "쿼리 결과 숨기기" : "쿼리 결과 보기"}
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-7 gap-1"
                  onClick={openSaveDialog}
                  disabled={boardSaving || (!displaySql && !currentSql)}
                >
                  <BookmarkPlus className="w-3 h-3" />
                  저장하기
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-7 gap-1 ml-auto"
                  onClick={handleResetConversation}
                  disabled={isLoading || !hasConversation}
                >
                  <Trash2 className="w-3 h-3" />
                  대화 초기화
                </Button>
              </div>

              {showSqlPanel && (
              <Card>
                <CardHeader className="pb-2">
                  <div className="flex items-center justify-between">
                    <CardTitle className="text-sm">생성된 SQL</CardTitle>
                    <div className="flex items-center gap-2">
                      {mode === "advanced" && (
                        <Button
                          variant="ghost"
                          size="sm"
                          className="h-7 gap-1"
                          onClick={() => handleExecuteEdited()}
                          disabled={isLoading || !displaySql}
                        >
                          {isLoading ? <Loader2 className="w-3 h-3 animate-spin" /> : <Play className="w-3 h-3" />}
                          실행
                        </Button>
                      )}
                      <Button variant="ghost" size="sm" className="h-7 gap-1" onClick={handleCopySql}>
                        {isSqlCopied ? <Check className="w-3 h-3 text-emerald-500" /> : <Copy className="w-3 h-3" />}
                        {isSqlCopied ? "복사됨" : "복사"}
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-7 gap-1"
                        onClick={() => {
                          const next = !isEditing
                          setIsEditing(next)
                          updateActiveTab({ isEditing: next })
                        }}
                      >
                        {isEditing ? <Eye className="w-3 h-3" /> : <Pencil className="w-3 h-3" />}
                        {isEditing ? "미리보기" : "편집"}
                      </Button>
                    </div>
                  </div>
                </CardHeader>
                <CardContent>
                  {isEditing ? (
                    <div className="space-y-3">
                      <Textarea
                        value={editedSql}
                        onChange={(e) => {
                          setEditedSql(e.target.value)
                          updateActiveTab({ editedSql: e.target.value })
                        }}
                        className="font-mono text-xs min-h-[200px] bg-secondary/50"
                      />
                      <div className="flex items-center gap-2">
                        <Button size="sm" onClick={() => handleExecuteEdited(editedSql)} disabled={isLoading || !editedSql.trim()} className="gap-1">
                          {isLoading ? <Loader2 className="w-3 h-3 animate-spin" /> : <Play className="w-3 h-3" />}
                          검증 후 실행
                        </Button>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => {
                            setEditedSql(currentSql)
                            updateActiveTab({ editedSql: currentSql })
                          }}
                        >
                          <RefreshCw className="w-3 h-3 mr-1" />
                          초기화
                        </Button>
                      </div>
                      <p className="text-[10px] text-muted-foreground flex items-center gap-1">
                        <AlertTriangle className="w-3 h-3" />
                        수정한 SQL은 검증을 통과해야 실행됩니다.
                      </p>
                    </div>
                  ) : (
                    <div
                      ref={sqlScrollRef}
                      onMouseDown={handleSqlMouseDown}
                      className={cn(
                        "p-4 pb-6 pr-6 rounded-xl bg-secondary/50 border border-border/60 text-[13px] font-mono leading-7 text-foreground overflow-x-auto overflow-y-visible [scrollbar-gutter:stable]",
                        "cursor-grab select-none",
                        isSqlDragging && "cursor-grabbing"
                      )}
                    >
                      {formattedDisplaySql ? (
                        <pre className="w-max min-w-full whitespace-pre pb-1">
                          <code dangerouslySetInnerHTML={{ __html: highlightedDisplaySql }} />
                        </pre>
                      ) : (
                        "SQL이 아직 생성되지 않았습니다."
                      )}
                    </div>
                  )}
                </CardContent>
              </Card>
              )}

              {showQueryResultPanel && (
              <Card>
                <CardHeader className="pb-2">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <CardTitle className="text-sm">쿼리 결과</CardTitle>
                      {mode && (
                        <Badge variant="outline" className="text-[10px] uppercase">
                          {mode}
                        </Badge>
                      )}
                    </div>
                    <div className="flex items-center gap-2">
                      <Badge variant="secondary" className="text-xs">
                        {previewColumns.length ? `${effectiveTotalRows} total` : "no results"}
                      </Badge>
                      <Button
                        variant="ghost"
                        size="sm"
                        className="h-7 gap-1"
                        onClick={handleDownloadCsv}
                        disabled={!previewColumns.length}
                      >
                        <Download className="w-3 h-3" />
                        CSV
                      </Button>
                    </div>
                  </div>
                </CardHeader>
                <CardContent>
                  {boardMessage && (
                    <div className="mb-3 rounded-lg border border-border bg-secondary/40 px-3 py-2 text-xs text-muted-foreground">
                      {boardMessage}
                    </div>
                  )}
                  {error && (
                    <div className="mb-3 rounded-lg border border-destructive/40 bg-destructive/10 px-3 py-2 text-xs text-destructive">
                      {error}
                    </div>
                  )}
                  {previewColumns.length ? (
                    <div className="rounded-lg border border-border overflow-hidden">
                      <table className="w-full text-xs">
                        <thead className="bg-secondary/50">
                          <tr>
                            {previewColumns.map((col) => (
                              <th key={col} className="text-left p-2 font-medium">
                                {col}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {previewRows.slice(0, 10).map((row, idx) => (
                            <tr key={idx} className="border-t border-border hover:bg-secondary/30">
                              {previewColumns.map((_, colIdx) => {
                                const cell = row[colIdx]
                                const text = cell == null ? "" : String(cell)
                                return (
                                  <td key={`${idx}-${colIdx}`} className="p-2 font-mono">
                                    {text}
                                  </td>
                                )
                              })}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <div className="flex flex-col items-center justify-center gap-3 rounded-lg border border-dashed border-border p-6 text-xs text-muted-foreground">
                      <div>결과가 없습니다.</div>
                      {mode === "advanced" && displaySql && (
                        <Button size="sm" onClick={() => handleExecuteEdited()} disabled={isLoading}>
                          {isLoading ? <Loader2 className="w-3 h-3 mr-1 animate-spin" /> : <Play className="w-3 h-3 mr-1" />}
                          실행
                        </Button>
                      )}
                    </div>
                  )}
                </CardContent>
              </Card>
              )}

              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm">통계 자료</CardTitle>
                  <CardDescription className="text-xs">컬럼별 MIN, Q1, 중앙값, Q3, MAX, 평균, 결측치, NULL 개수</CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  {previewColumns.length ? (
                    <div className="space-y-4">
                      <div className="rounded-lg border border-border overflow-x-auto">
                        <table className="w-full text-xs">
                          <thead className="bg-secondary/50">
                            <tr>
                              <th className="text-left p-2 font-medium">컬럼</th>
                              <th className="text-right p-2 font-medium">N</th>
                              <th className="text-right p-2 font-medium">결측치</th>
                              <th className="text-right p-2 font-medium">NULL</th>
                              <th className="text-right p-2 font-medium">MIN</th>
                              <th className="text-right p-2 font-medium">Q1</th>
                              <th className="text-right p-2 font-medium">중앙값</th>
                              <th className="text-right p-2 font-medium">Q3</th>
                              <th className="text-right p-2 font-medium">MAX</th>
                              <th className="text-right p-2 font-medium">평균</th>
                            </tr>
                          </thead>
                          <tbody>
                            {statsRows.map((row) => (
                              <tr key={row.column} className="border-t border-border">
                                <td className="p-2 font-medium">{row.column}</td>
                                <td className="p-2 text-right">{row.count}</td>
                                <td className="p-2 text-right">{row.missingCount}</td>
                                <td className="p-2 text-right">{row.nullCount}</td>
                                <td className="p-2 text-right">{formatStatNumber(row.min)}</td>
                                <td className="p-2 text-right">{formatStatNumber(row.q1)}</td>
                                <td className="p-2 text-right">{formatStatNumber(row.median)}</td>
                                <td className="p-2 text-right">{formatStatNumber(row.q3)}</td>
                                <td className="p-2 text-right">{formatStatNumber(row.max)}</td>
                                <td className="p-2 text-right">{formatStatNumber(row.avg)}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>

                      {hasInsufficientRowsForVisualization ? (
                        <div className="rounded-lg border border-dashed border-border p-4 text-xs text-muted-foreground">
                          결과 행이 {previewRowCount}개라 박스플롯을 생성하지 않습니다. 최소 2개 행이 필요합니다.
                        </div>
                      ) : statsBoxPlotFigures.length ? (
                        <div className="space-y-3">
                          <div className="grid grid-cols-1 items-center gap-2 sm:grid-cols-3">
                            <div className="justify-self-start text-xs text-muted-foreground">
                              박스플롯 (컬럼별 개별 분포)
                            </div>
                            <div className="relative justify-self-center">
                              <span
                                ref={statsBoxMeasureRef}
                                className="pointer-events-none absolute -z-10 opacity-0 whitespace-nowrap text-sm font-normal"
                              >
                                {statsBoxValueLabel}
                              </span>
                              {showStatsBoxPlot && statsBoxPlotFigures.length > 1 ? (
                                <Select
                                  value={selectedStatsBoxPlot?.column || undefined}
                                  onValueChange={(value) => {
                                    setSelectedStatsBoxColumn(value)
                                    setStatsBoxValueLabel(value)
                                  }}
                                >
                                  <SelectTrigger
                                    className="relative h-8 pr-8 [&_svg]:absolute [&_svg]:right-3"
                                    style={{ width: statsBoxTriggerWidth }}
                                  >
                                    <SelectValue
                                      className="pointer-events-none absolute left-1/2 max-w-[calc(100%-2.5rem)] -translate-x-1/2 truncate text-center text-sm font-normal"
                                      placeholder="박스플롯 컬럼 선택"
                                    />
                                  </SelectTrigger>
                                  <SelectContent>
                                    {statsBoxPlotFigures.map((item) => (
                                      <SelectItem key={item.column} value={item.column}>
                                        {item.column}
                                      </SelectItem>
                                    ))}
                                  </SelectContent>
                                </Select>
                              ) : (
                                <div className="h-8" style={{ width: statsBoxTriggerWidth }} />
                              )}
                            </div>
                            <div className="justify-self-end">
                              <Button
                                variant="outline"
                                size="sm"
                                className="h-8"
                                onClick={() => setShowStatsBoxPlot((prev) => !prev)}
                              >
                                {showStatsBoxPlot ? "박스플롯 숨기기" : "박스플롯 보기"}
                              </Button>
                            </div>
                          </div>
                          {showStatsBoxPlot && selectedStatsBoxPlot ? (
                            <div className="rounded-lg border border-border p-3">
                              <div className="mb-2 text-xs text-muted-foreground">{selectedStatsBoxPlot.column}</div>
                              <div className="h-[320px] w-full">
                                <Plot
                                  data={Array.isArray(selectedStatsBoxPlot.figure.data) ? selectedStatsBoxPlot.figure.data : []}
                                  layout={selectedStatsBoxPlot.figure.layout || {}}
                                  config={{ responsive: true, displaylogo: false, editable: false }}
                                  style={{ width: "100%", height: "100%" }}
                                />
                              </div>
                            </div>
                          ) : (
                            <div className="rounded-lg border border-dashed border-border p-4 text-xs text-muted-foreground">
                              박스플롯 보기를 눌러 컬럼 분포를 확인하세요.
                            </div>
                          )}
                        </div>
                      ) : (
                        <div className="rounded-lg border border-dashed border-border p-4 text-xs text-muted-foreground">
                          MIN, Q1, 중앙값, Q3, MAX, 평균이 모두 있는 수치형 컬럼이 없어 박스플롯을 생성할 수 없습니다.
                        </div>
                      )}
                    </div>
                  ) : (
                    <div className="rounded-lg border border-dashed border-border p-6 text-xs text-muted-foreground">
                      결과가 없어 통계 자료를 표시할 수 없습니다.
                    </div>
                  )}
                </CardContent>
              </Card>

              <Card>
                <CardHeader className="pb-2">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <CardTitle className="text-sm">시각화 차트</CardTitle>
                      <CardDescription className="text-xs">결과 테이블 기반 시각화</CardDescription>
                    </div>
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      className="h-7 gap-1 text-xs"
                      onClick={handleOpenVisualizationZoom}
                      disabled={!hasZoomChart}
                    >
                      <Maximize2 className="h-3 w-3" />
                      확대 보기
                    </Button>
                  </div>
                </CardHeader>
                <CardContent>
                  {visualizationLoading ? (
                    <div className="rounded-lg border border-border p-6 text-xs text-muted-foreground">
                      시각화 추천 플랜을 생성 중입니다...
                    </div>
                  ) : hasInsufficientRowsForVisualization ? (
                    <div className="rounded-lg border border-dashed border-border p-6 text-xs text-muted-foreground">
                      결과 행이 {previewRowCount}개라 시각화를 생성하지 않습니다. 최소 2개 행이 필요합니다.
                    </div>
                  ) : previewColumns.length < 2 ? (
                    <div className="rounded-lg border border-dashed border-border p-6 text-xs text-muted-foreground">
                      결과 컬럼이 1개라 시각화를 표시할 수 없습니다. 최소 2개 컬럼이 필요합니다.
                    </div>
                  ) : (hasRecommendedSeabornImage || hasRecommendedPlotlyFigure) ? (
                    <div className="space-y-3">
                      {topRecommendedAnalyses.length > 0 && (
                        <div className="flex flex-wrap items-center gap-2">
                          {topRecommendedAnalyses.map((item, index) => {
                            const active = index === selectedAnalysisIndex
                            return (
                              <Button
                                key={`analysis-option-${index}-${String(item?.chart_spec?.chart_type || "plotly")}`}
                                type="button"
                                variant={active ? "default" : "outline"}
                                size="sm"
                                className="h-8 text-xs"
                                onClick={() => setSelectedAnalysisIndex(index)}
                              >
                                추천 {index + 1}: {formatChartTypeLabel(item?.chart_spec?.chart_type)}
                              </Button>
                            )
                          })}
                        </div>
                      )}
                      <div className="flex items-center gap-2 text-xs text-muted-foreground">
                        <Badge variant="outline">
                          {formatChartTypeLabel(recommendedAnalysis?.chart_spec?.chart_type || "plotly")}
                        </Badge>
                        {recommendedRenderEngine && (
                          <Badge variant="secondary">ENGINE: {recommendedRenderEngine}</Badge>
                        )}
                        {recommendedAnalysis?.chart_spec?.x && (
                          <Badge variant="secondary">X: {recommendedAnalysis.chart_spec.x}</Badge>
                        )}
                        {recommendedAnalysis?.chart_spec?.y && (
                          <Badge variant="secondary">Y: {recommendedAnalysis.chart_spec.y}</Badge>
                        )}
                      </div>
                      {canUseChartCategoryFilter && (
                        <div className="flex flex-col gap-2 rounded-lg border border-border/60 bg-secondary/20 p-2 md:flex-row md:items-center md:justify-between">
                          <div className="text-xs text-muted-foreground">
                            X축 카테고리 {chartCategories.length}개 / 선택 {(effectiveSelectedChartCategories?.length ?? chartCategories.length)}개
                          </div>
                          <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            className="h-8 w-fit"
                            onClick={() => setIsChartCategoryPickerOpen(true)}
                          >
                            추가 필터
                          </Button>
                        </div>
                      )}
                      <div
                        className="h-[380px] w-full rounded-lg border border-border p-2 overflow-hidden cursor-zoom-in"
                        onClick={handleVisualizationPanelClick}
                      >
                        {showRecommendedSeaborn ? (
                          <img
                            src={recommendedImageDataUrl || ""}
                            alt="seaborn chart"
                            className="h-full w-full object-contain"
                          />
                        ) : (
                          <Plot
                            data={Array.isArray(recommendedFigureForRender?.data) ? recommendedFigureForRender.data : []}
                            layout={recommendedFigureForRender?.layout || {}}
                            config={{ responsive: true, displaylogo: false, editable: false }}
                            style={{ width: "100%", height: "100%" }}
                            useResizeHandler
                          />
                        )}
                      </div>
                      {(recommendedAnalysis?.reason || recommendedAnalysis?.summary) && (
                        <div className="rounded-lg border border-border bg-secondary/30 p-3 text-xs text-muted-foreground space-y-1">
                          {recommendedAnalysis?.reason && <p>추천 이유: {normalizeInsightText(recommendedAnalysis.reason)}</p>}
                          {recommendedAnalysis?.summary && <p>요약: {normalizeInsightText(recommendedAnalysis.summary)}</p>}
                        </div>
                      )}
                    </div>
                  ) : localFallbackFigureForRender && Array.isArray(localFallbackFigureForRender.data) && localFallbackFigureForRender.data.length ? (
                    <div className="space-y-3">
                      {topRecommendedAnalyses.length > 0 && (
                        <div className="flex flex-wrap items-center gap-2">
                          {topRecommendedAnalyses.map((item, index) => {
                            const active = index === selectedAnalysisIndex
                            return (
                              <Button
                                key={`analysis-fallback-option-${index}-${String(item?.chart_spec?.chart_type || "plotly")}`}
                                type="button"
                                variant={active ? "default" : "outline"}
                                size="sm"
                                className="h-8 text-xs"
                                onClick={() => setSelectedAnalysisIndex(index)}
                              >
                                추천 {index + 1}: {formatChartTypeLabel(item?.chart_spec?.chart_type)}
                              </Button>
                            )
                          })}
                        </div>
                      )}
                      {visualizationError && (
                        <div className="rounded-lg border border-amber-300/40 bg-amber-500/10 p-3 text-xs text-amber-700 dark:text-amber-300">
                          시각화 API 응답이 없어 로컬 폴백 차트를 표시합니다: {visualizationError}
                        </div>
                      )}
                      <div className="flex items-center gap-2 text-xs text-muted-foreground">
                        <Badge variant="outline">
                          {formatChartTypeLabel(chartForRender?.type || recommendedAnalysis?.chart_spec?.chart_type || "plotly")}
                        </Badge>
                        {chartForRender?.xKey && <Badge variant="secondary">X: {chartForRender.xKey}</Badge>}
                        {chartForRender?.yKey && <Badge variant="secondary">Y: {chartForRender.yKey}</Badge>}
                        {!recommendedChart && <Badge variant="secondary">AUTO</Badge>}
                      </div>
                      {isChartCategoryPickerEnabled && (
                        <div className="flex flex-col gap-2 rounded-lg border border-border/60 bg-secondary/20 p-2 md:flex-row md:items-center md:justify-between">
                          <div className="text-xs text-muted-foreground">
                            X축 카테고리 {chartCategories.length}개 / 선택 {(effectiveSelectedChartCategories?.length ?? chartCategories.length)}개
                          </div>
                          <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            className="h-8 w-fit"
                            onClick={() => setIsChartCategoryPickerOpen(true)}
                          >
                            추가 필터
                          </Button>
                        </div>
                      )}
                      <div
                        className="h-[340px] w-full rounded-lg border border-border p-3 overflow-hidden cursor-zoom-in"
                        onClick={handleVisualizationPanelClick}
                      >
                        <Plot
                          data={Array.isArray(localFallbackFigureForRender?.data) ? localFallbackFigureForRender.data : []}
                          layout={localFallbackFigureForRender?.layout || {}}
                          config={{ responsive: true, displaylogo: false, editable: false }}
                          style={{ width: "100%", height: "100%" }}
                          useResizeHandler
                        />
                      </div>
                      {(recommendedAnalysis?.reason || recommendedAnalysis?.summary) && (
                        <div className="rounded-lg border border-border bg-secondary/30 p-3 text-xs text-muted-foreground space-y-1">
                          {recommendedAnalysis?.reason && <p>추천 이유: {recommendedAnalysis.reason}</p>}
                          {recommendedAnalysis?.summary && <p>요약: {recommendedAnalysis.summary}</p>}
                        </div>
                      )}
                    </div>
                  ) : survivalFigureForRender && Array.isArray(survivalFigureForRender.data) && survivalFigureForRender.data.length ? (
                    <div className="space-y-3">
                      <div className="flex items-center gap-2 text-xs text-muted-foreground">
                        <Badge variant="outline">SURVIVAL (PLOTLY)</Badge>
                        <Badge variant="secondary">Median: {medianSurvival.toFixed(2)}</Badge>
                      </div>
                      <div
                        className="h-[360px] w-full rounded-lg border border-border p-2 overflow-hidden cursor-zoom-in"
                        onClick={handleVisualizationPanelClick}
                      >
                        <Plot
                          data={Array.isArray(survivalFigureForRender?.data) ? survivalFigureForRender.data : []}
                          layout={survivalFigureForRender?.layout || {}}
                          config={{ responsive: true, displaylogo: false, editable: false }}
                          style={{ width: "100%", height: "100%" }}
                          useResizeHandler
                        />
                      </div>
                    </div>
                  ) : (
                    <div className="rounded-lg border border-dashed border-border p-6 text-xs text-muted-foreground">
                      현재 결과로 생성 가능한 차트가 없습니다. 시간/이벤트 컬럼이 포함되면 생존 차트를 표시합니다.
                    </div>
                  )}
                </CardContent>
              </Card>

              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-sm">해석</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="rounded-lg border border-primary/30 bg-primary/10 p-4">
                    <div className="space-y-3">
                      <div className="rounded-md border border-primary/20 bg-background/70 px-3 py-2 text-sm font-medium leading-relaxed text-foreground">
                        {insightHeadline}
                      </div>
                      {insightPoints.length > 1 ? (
                        <ul className="space-y-2 text-sm text-muted-foreground">
                          {insightPoints.slice(1).map((point, idx) => (
                            <li key={`insight-${idx}-${point.slice(0, 24)}`} className="flex items-start gap-2">
                              <span className="mt-0.5 inline-flex h-5 min-w-5 items-center justify-center rounded-full bg-primary/20 text-[11px] font-semibold text-primary">
                                {idx + 1}
                              </span>
                              <span className="leading-relaxed">{point}</span>
                            </li>
                          ))}
                        </ul>
                      ) : null}
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>
          </div>
        )}
      </div>

      <Dialog open={isChartCategoryPickerOpen} onOpenChange={setIsChartCategoryPickerOpen}>
        <DialogContent className="w-[min(96vw,980px)] p-0 sm:max-w-[980px]">
          <div className="grid max-h-[90dvh] grid-rows-[auto_minmax(0,1fr)_auto]">
            <DialogHeader className="border-b border-border/70 px-6 py-4 pr-12">
              <DialogTitle>X축 카테고리 추가 필터</DialogTitle>
              <DialogDescription>
                X축 카테고리가 많을 때 표시할 개수와 종류를 선택합니다.
              </DialogDescription>
            </DialogHeader>

            {isChartCategoryPickerEnabled ? (
              <div className="min-h-0 flex-1 overflow-y-auto px-6 py-4">
                <div className="space-y-3">
                  <div className="rounded-lg border border-border/60 bg-secondary/20 p-3">
                    <div className="text-xs text-muted-foreground">
                      총 {chartCategories.length}개 / 선택 {(effectiveDraftChartCategories?.length ?? chartCategories.length)}개
                    </div>
                    <div className="mt-2 grid grid-cols-1 gap-2 md:grid-cols-[auto_180px_minmax(0,1fr)] md:items-center">
                      <span className="text-xs text-muted-foreground">표시 개수</span>
                      <div className="w-full md:w-[180px]">
                        <Select value={draftChartCategoryCount} onValueChange={applyChartCategoryCountSelection}>
                          <SelectTrigger className="h-8">
                            <SelectValue placeholder="표시 개수 선택" />
                          </SelectTrigger>
                          <SelectContent>
                            {chartCategoryCountOptions.map((option) => (
                              <SelectItem key={option.value} value={option.value}>
                                {option.label}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                      <div className="flex min-w-0 flex-wrap items-center gap-2 md:justify-end">
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          className="h-8 whitespace-nowrap"
                          onClick={() =>
                            applyChartCategoryCountSelection(
                              String(Math.min(CHART_CATEGORY_DEFAULT_COUNT, chartCategories.length))
                            )
                          }
                        >
                          기본 {Math.min(CHART_CATEGORY_DEFAULT_COUNT, chartCategories.length)}개
                        </Button>
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          className="h-8 whitespace-nowrap"
                          onClick={() => applyChartCategoryCountSelection("all")}
                        >
                          전체 선택
                        </Button>
                      </div>
                    </div>
                  </div>

                  <div className="overflow-hidden rounded-lg border border-border">
                    {chartCategorySummaries.map((item, idx) => (
                      <label
                        key={`${item.value}-${idx}`}
                        className={cn(
                          "flex cursor-pointer items-center gap-3 px-3 py-2 text-sm hover:bg-secondary/40",
                          idx > 0 && "border-t border-border/60"
                        )}
                      >
                        <Checkbox
                          checked={selectedChartCategorySet.has(item.value)}
                          onCheckedChange={(checked) => toggleChartCategoryValue(item.value, checked === true)}
                        />
                        <span className="min-w-0 flex-1 truncate">{item.value}</span>
                        <Badge variant="outline" className="text-[10px]">
                          {item.occurrences}
                        </Badge>
                      </label>
                    ))}
                  </div>
                </div>
              </div>
            ) : (
              <div className="px-6 py-4">
                <div className="rounded-lg border border-dashed border-border p-4 text-sm text-muted-foreground">
                  X축 카테고리가 10개 이하라서 추가 필터 팝업이 필요하지 않습니다.
                </div>
              </div>
            )}

            <DialogFooter className="border-t border-border/70 px-6 py-4 sm:justify-end">
              <Button onClick={handleApplyChartCategorySelection}>
                적용
              </Button>
            </DialogFooter>
          </div>
        </DialogContent>
      </Dialog>

      <Dialog open={isVisualizationZoomOpen} onOpenChange={setIsVisualizationZoomOpen}>
        <DialogContent className="w-[min(98vw,1400px)] max-h-[95dvh] overflow-hidden p-0 sm:max-w-[1400px]">
          <div className="flex h-full max-h-[95dvh] flex-col">
            <DialogHeader className="border-b border-border/70 px-6 py-4 pr-12">
              <DialogTitle>{zoomChartPayload?.title || "시각화 확대 보기"}</DialogTitle>
              <DialogDescription>
                현재 선택된 시각화를 확대해 확인할 수 있습니다.
              </DialogDescription>
            </DialogHeader>
            <div className="min-h-0 flex-1 px-6 py-4">
              <div className="h-[72vh] min-h-[440px] w-full rounded-lg border border-border p-2 overflow-hidden">
                {String((zoomChartPayload as any)?.imageDataUrl || "").startsWith("data:image/") ? (
                  <img
                    src={String((zoomChartPayload as any)?.imageDataUrl)}
                    alt="zoomed seaborn chart"
                    className="h-full w-full object-contain"
                  />
                ) : (
                  <Plot
                    data={Array.isArray((zoomChartPayload as any)?.data) ? (zoomChartPayload as any).data : []}
                    layout={(zoomChartPayload as any)?.layout || {}}
                    config={{ responsive: true, displaylogo: false, editable: false }}
                    style={{ width: "100%", height: "100%" }}
                    useResizeHandler
                  />
                )}
              </div>
            </div>
          </div>
        </DialogContent>
      </Dialog>

      <Dialog open={isSaveDialogOpen} onOpenChange={setIsSaveDialogOpen}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>결과 보드에 저장</DialogTitle>
            <DialogDescription>저장 이름과 폴더를 선택하세요.</DialogDescription>
          </DialogHeader>

          <div className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">저장 이름</label>
              <Input
                value={saveTitle}
                onChange={(e) => setSaveTitle(e.target.value)}
                placeholder="예: 성별 입원 건수 비교"
              />
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">저장 폴더</label>
              <Select
                value={saveFolderMode}
                onValueChange={(value) => setSaveFolderMode(value as "existing" | "new")}
              >
                <SelectTrigger>
                  <SelectValue placeholder="폴더 방식 선택" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="existing">기존 폴더 선택</SelectItem>
                  <SelectItem value="new">새 폴더 만들기</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {saveFolderMode === "existing" ? (
              <div className="space-y-2">
                <label className="text-sm font-medium">기존 폴더</label>
                <Select value={saveFolderId || undefined} onValueChange={setSaveFolderId} disabled={saveFoldersLoading}>
                  <SelectTrigger>
                    <SelectValue placeholder={saveFoldersLoading ? "폴더 불러오는 중..." : "폴더 선택"} />
                  </SelectTrigger>
                  <SelectContent>
                    {saveFolderOptions.length ? (
                      saveFolderOptions.map((folder) => (
                        <SelectItem key={folder.id} value={folder.id}>
                          {folder.name}
                        </SelectItem>
                      ))
                    ) : (
                      <SelectItem value="__none__" disabled>
                        폴더가 없습니다
                      </SelectItem>
                    )}
                  </SelectContent>
                </Select>
              </div>
            ) : (
              <div className="space-y-2">
                <label className="text-sm font-medium">새 폴더 이름</label>
                <Input
                  value={saveNewFolderName}
                  onChange={(e) => setSaveNewFolderName(e.target.value)}
                  placeholder="예: 응급실 분석"
                />
              </div>
            )}

            {boardMessage && (
              <div className="text-xs text-muted-foreground">{boardMessage}</div>
            )}
          </div>

          <DialogFooter>
            <Button variant="ghost" onClick={() => setIsSaveDialogOpen(false)} disabled={boardSaving}>
              취소
            </Button>
            <Button onClick={handleSaveToDashboard} disabled={boardSaving}>
              {boardSaving ? <Loader2 className="w-3 h-3 mr-1 animate-spin" /> : null}
              저장
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={isPastQueriesDialogOpen} onOpenChange={setIsPastQueriesDialogOpen}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle>이전 쿼리 목록</DialogTitle>
            <DialogDescription> 최근 쿼리를 제외한 이전 쿼리입니다. 선택하면 상단 탭으로 이동합니다.</DialogDescription>
          </DialogHeader>

          <div className="max-h-[55vh] space-y-2 overflow-y-auto">
            {pastQueryTabs.length > 0 ? (
              pastQueryTabs.map((tab) => (
                <button
                  key={`past-${tab.id}`}
                  type="button"
                  onClick={() => {
                    handleActivateTab(tab.id, true)
                    setIsPastQueriesDialogOpen(false)
                  }}
                  className="flex w-full items-center gap-2 rounded-md border px-3 py-2 text-left text-sm hover:bg-secondary/40"
                >
                  <span
                    className={cn(
                      "inline-block h-2 w-2 rounded-full shrink-0",
                      tab.status === "pending" && "bg-yellow-500",
                      tab.status === "success" && "bg-primary",
                      tab.status === "error" && "bg-destructive"
                    )}
                  />
                  <span className="truncate">{tab.question || "새 질문"}</span>
                </button>
              ))
            ) : (
              <div className="rounded-md border border-dashed p-4 text-center text-sm text-muted-foreground">
                표시할 이전 쿼리가 없습니다.
              </div>
            )}
          </div>

          <DialogFooter>
            <Button variant="ghost" onClick={() => setIsPastQueriesDialogOpen(false)}>
              닫기
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <CohortLibraryDialog
        open={isCohortLibraryOpen}
        onOpenChange={setIsCohortLibraryOpen}
        cohorts={savedCohorts}
        loading={isCohortLibraryLoading}
        onRefresh={() => {
          void loadSavedCohortLibrary()
        }}
        onSelectForQuery={handleApplySavedCohortForQuery}
      />
    </div>
  )
}

interface SimpleStatsRow {
  column: string
  count: number
  numericCount: number
  nullCount: number
  missingCount: number
  min: number | null
  q1: number | null
  median: number | null
  q3: number | null
  max: number | null
  avg: number | null
}

function buildSimpleStats(columns: string[], rows: any[][]): SimpleStatsRow[] {
  const totalCount = rows.length
  return columns.map((column, colIdx) => {
    const numbers: number[] = []
    let nullCount = 0
    let missingCount = 0

    for (const row of rows) {
      const value = row?.[colIdx]
      const isNull = value == null
      const isBlank = typeof value === "string" && value.trim() === ""
      if (isNull) {
        nullCount += 1
        missingCount += 1
        continue
      }
      if (isBlank) {
        missingCount += 1
        continue
      }
      const num = Number(value)
      if (Number.isFinite(num)) {
        numbers.push(num)
      }
    }

    if (!numbers.length) {
      return {
        column,
        count: totalCount,
        numericCount: 0,
        nullCount,
        missingCount,
        min: null,
        q1: null,
        median: null,
        q3: null,
        max: null,
        avg: null,
      }
    }

    const sorted = [...numbers].sort((a, b) => a - b)
    const q1 = quantile(sorted, 0.25)
    const median = quantile(sorted, 0.5)
    const q3 = quantile(sorted, 0.75)
    const min = Math.min(...numbers)
    const max = Math.max(...numbers)
    const avg = numbers.reduce((sum, value) => sum + value, 0) / numbers.length

    return {
      column,
      count: totalCount,
      numericCount: numbers.length,
      nullCount,
      missingCount,
      min: Number(min.toFixed(4)),
      q1: Number(q1.toFixed(4)),
      median: Number(median.toFixed(4)),
      q3: Number(q3.toFixed(4)),
      max: Number(max.toFixed(4)),
      avg: Number(avg.toFixed(4)),
    }
  })
}

function quantile(sorted: number[], q: number) {
  if (!sorted.length) return 0
  const pos = (sorted.length - 1) * q
  const base = Math.floor(pos)
  const rest = pos - base
  const next = sorted[base + 1]
  if (next === undefined) return sorted[base]
  return sorted[base] + rest * (next - sorted[base])
}

function formatStatNumber(value: number | null) {
  if (value == null || !Number.isFinite(value)) return "-"
  return Number(value.toFixed(4)).toLocaleString()
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
