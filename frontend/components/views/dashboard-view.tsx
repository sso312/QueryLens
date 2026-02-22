"use client"

import { type ReactNode, useEffect, useMemo, useRef, useState } from "react"
import dynamic from "next/dynamic"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Checkbox } from "@/components/ui/checkbox"
import { Skeleton } from "@/components/ui/skeleton"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  Pin,
  Clock,
  MoreHorizontal,
  Play,
  Calendar,
  Search,
  Plus,
  Star,
  StarOff,
  Copy,
  Trash2,
  BarChart3,
  PieChart,
  Activity,
  FolderOpen,
  Folder,
  FolderPlus,
  Pencil,
  Check,
  ChevronDown,
  Scale,
  X,
  ArrowRight,
  ChevronRight,
  Maximize2,
} from "lucide-react"
import { cn } from "@/lib/utils"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuSub,
  DropdownMenuSubContent,
  DropdownMenuSubTrigger,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { useAuth } from "@/components/auth-provider"
import {
  readDashboardCache,
  writeDashboardCache,
} from "@/lib/dashboard-cache"

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false }) as any

interface SavedQuery {
  id: string
  title: string
  description: string
  insight?: string
  llmSummary?: string
  query: string
  lastRun: string
  schedule?: string
  isPinned: boolean
  category: string
  folderId?: string
  cohort?: SavedQueryCohortProvenance
  pdfAnalysis?: SavedQueryPdfAnalysis
  preview?: {
    columns: string[]
    rows: any[][]
    row_count: number
    row_cap?: number | null
  }
  stats?: Array<{
    column: string
    n?: number
    missing?: number
    nulls?: number
    min?: unknown
    q1?: unknown
    median?: unknown
    q3?: unknown
    max?: unknown
    mean?: unknown
  }>
  metrics: { label: string; value: string; trend?: "up" | "down" }[]
  chartType: "line" | "bar" | "pie"
  recommendedCharts?: DashboardChartSpec[]
  primaryChart?: DashboardChartSpec
}

interface SavedQueryCohortProvenance {
  source: "NONE" | "LIBRARY" | "PDF" | string
  libraryCohortId?: string
  libraryCohortName?: string
  pdfCohortId?: string
  pdfPaperTitle?: string
  libraryUsed?: boolean
}

interface SavedQueryPdfAnalysisInclusion {
  id: string
  title: string
  operationalDefinition: string
  evidence?: string
}

interface SavedQueryPdfAnalysis {
  pdfHash?: string
  pdfName?: string
  summaryKo?: string
  criteriaSummaryKo?: string
  variables?: string[]
  inclusionExclusion?: SavedQueryPdfAnalysisInclusion[]
  source?: string
  analyzedAt?: string
  libraryUsed?: boolean
  libraryCohortId?: string
  libraryCohortName?: string
}

interface SavedFolder {
  id: string
  name: string
  tone?: string
  createdAt?: string
}

interface FolderCardInfo {
  id: string
  name: string
  count: number
  pinnedCount: number
  tone?: string
  editable: boolean
}

interface DialogStatRow {
  column: string
  n: number
  missingCount: number
  nullCount: number
  min: number | null
  q1: number | null
  median: number | null
  q3: number | null
  max: number | null
  avg: number | null
}

interface DashboardChartSpec {
  id: string
  type: string
  x?: string
  y?: string
  config?: Record<string, unknown>
  thumbnailUrl?: string
  pngUrl?: string
}

interface PopupChartPayload {
  title: string
  imageUrl?: string
  figure?: {
    data: unknown[]
    layout?: Record<string, unknown>
  }
}

const ALL_FOLDER_ID = "__all__"
const DEFAULT_FOLDER_ID = "folder-general"

const FOLDER_TONES = ["emerald", "sky", "amber", "rose", "violet"] as const

const savedQueries: SavedQuery[] = [
  {
    id: "1",
    title: "65세 이상 심부전 생존 분석",
    description: "65세 이상 심부전 환자의 Kaplan-Meier 생존 곡선",
    query: "SELECT ... FROM patients WHERE age >= 65",
    lastRun: "2시간 전",
    schedule: "매일 09:00",
    isPinned: true,
    category: "생존분석",
    metrics: [
      { label: "환자 수", value: "1,247" },
      { label: "30일 생존율", value: "75.8%", trend: "down" },
      { label: "중앙 생존", value: "82일" },
    ],
    chartType: "line",
  },
  {
    id: "2",
    title: "월별 재입원율 추이",
    description: "30일 내 재입원율 월별 트렌드 분석",
    query: "SELECT ... FROM admissions",
    lastRun: "1일 전",
    schedule: "매주 월요일",
    isPinned: true,
    category: "재입원",
    metrics: [
      { label: "이번 달", value: "12.4%", trend: "up" },
      { label: "전월 대비", value: "+1.2%", trend: "up" },
      { label: "목표", value: "10%" },
    ],
    chartType: "bar",
  },
  {
    id: "3",
    title: "진단별 ICU 입실률",
    description: "주요 진단 코드별 ICU 입실 비율",
    query: "SELECT ... FROM diagnoses_icd",
    lastRun: "3일 전",
    isPinned: false,
    category: "ICU",
    metrics: [
      { label: "심부전", value: "34.2%" },
      { label: "패혈증", value: "52.1%" },
      { label: "뇌졸중", value: "28.7%" },
    ],
    chartType: "pie",
  },
  {
    id: "4",
    title: "응급실 평균 대기시간",
    description: "시간대별 응급실 대기시간 분석",
    query: "SELECT ... FROM edstays",
    lastRun: "12시간 전",
    schedule: "매일 18:00",
    isPinned: false,
    category: "응급실",
    metrics: [
      { label: "평균", value: "4.2시간" },
      { label: "피크시간", value: "6.8시간", trend: "up" },
      { label: "최소", value: "1.5시간" },
    ],
    chartType: "bar",
  },
]

const seedFolders: SavedFolder[] = [
  { id: "folder-survival", name: "생존분석", tone: "emerald" },
  { id: "folder-readmission", name: "재입원", tone: "sky" },
  { id: "folder-icu", name: "ICU", tone: "amber" },
  { id: "folder-er", name: "응급실", tone: "rose" },
]

const isLegacyDemoSeedQuery = (item: any) => {
  const id = String(item?.id || "").trim()
  const sql = String(item?.query || "").trim()
  if (!id || !sql) return false
  return ["1", "2", "3", "4"].includes(id) && /SELECT\s+\.\.\.\s+FROM/i.test(sql)
}

const makeFolderId = () => `folder-${Date.now()}`

const normalizeName = (value: string) => value.trim().replace(/\s+/g, " ")

const nextTone = (index: number) => FOLDER_TONES[index % FOLDER_TONES.length]

const isBrokenDashboardText = (value: string) => {
  const text = String(value || "").trim()
  if (!text) return true
  if (/\?{2,}/.test(text)) return true
  const qCount = (text.match(/\?/g) || []).length
  return qCount >= 3
}

const formatDashboardTitle = (query: SavedQuery) => {
  if (!isBrokenDashboardText(query.title)) return query.title
  return "저장된 쿼리"
}

const formatDashboardLastRun = (value: string) => {
  if (!isBrokenDashboardText(value)) return value
  return "방금 전"
}

const formatDashboardMetricLabel = (label: string, index: number) => {
  if (!isBrokenDashboardText(label)) return label
  if (index === 0) return "행 수"
  if (index === 1) return "컬럼 수"
  if (index === 2) return "ROW CAP"
  return "지표"
}

const isNumericValue = (value: unknown) => {
  const n = Number(value)
  return Number.isFinite(n)
}

const toFiniteNumber = (value: unknown): number | null => {
  const n = Number(value)
  return Number.isFinite(n) ? n : null
}

const toText = (value: unknown) => String(value ?? "").trim()

const toOptionalText = (value: unknown) => {
  const text = toText(value)
  return text || undefined
}

const quantile = (sortedValues: number[], p: number): number | null => {
  if (!sortedValues.length) return null
  if (sortedValues.length === 1) return sortedValues[0]
  const pos = (sortedValues.length - 1) * p
  const lower = Math.floor(pos)
  const upper = Math.ceil(pos)
  if (lower === upper) return sortedValues[lower]
  const weight = pos - lower
  return sortedValues[lower] * (1 - weight) + sortedValues[upper] * weight
}

const formatStatNumber = (value: number | null) => {
  if (value == null || Number.isNaN(value)) return "-"
  return value.toLocaleString(undefined, { maximumFractionDigits: 4 })
}

const normalizeChartType = (value: unknown) => String(value || "").trim().toUpperCase()

const isRecordLike = (value: unknown): value is Record<string, unknown> =>
  value != null && typeof value === "object" && !Array.isArray(value)

const readBooleanLike = (value: unknown): boolean | undefined => {
  if (typeof value === "boolean") return value
  if (typeof value === "number") return value !== 0
  const text = String(value ?? "").trim().toLowerCase()
  if (!text) return undefined
  if (["true", "1", "y", "yes"].includes(text)) return true
  if (["false", "0", "n", "no"].includes(text)) return false
  return undefined
}

const normalizeCohortProvenance = (raw: unknown): SavedQueryCohortProvenance | undefined => {
  if (!isRecordLike(raw)) return undefined
  const sourceRaw = toText(raw.source).toUpperCase()
  const source: SavedQueryCohortProvenance["source"] =
    sourceRaw === "PDF" || sourceRaw === "LIBRARY" || sourceRaw === "NONE"
      ? sourceRaw
      : "NONE"
  const libraryUsed = readBooleanLike(raw.libraryUsed ?? raw.library_used)
  const normalized: SavedQueryCohortProvenance = {
    source,
    libraryCohortId: toOptionalText(raw.libraryCohortId ?? raw.library_cohort_id),
    libraryCohortName: toOptionalText(raw.libraryCohortName ?? raw.library_cohort_name),
    pdfCohortId: toOptionalText(raw.pdfCohortId ?? raw.pdf_cohort_id),
    pdfPaperTitle: toOptionalText(raw.pdfPaperTitle ?? raw.pdf_paper_title),
    libraryUsed,
  }
  return normalized
}

const normalizePdfAnalysisSnapshot = (raw: unknown): SavedQueryPdfAnalysis | undefined => {
  if (!isRecordLike(raw)) return undefined
  const variables = Array.isArray(raw.variables)
    ? raw.variables
        .map((item) => toText(item))
        .filter(Boolean)
        .slice(0, 24)
    : []
  const inclusionCandidate = raw.inclusionExclusion ?? raw.inclusion_exclusion
  const inclusionExclusion: SavedQueryPdfAnalysisInclusion[] = []
  if (Array.isArray(inclusionCandidate)) {
    for (let idx = 0; idx < inclusionCandidate.length; idx += 1) {
      const item = inclusionCandidate[idx]
      if (!isRecordLike(item)) continue
      const operationalDefinition = toText(item.operationalDefinition ?? item.operational_definition)
      if (!operationalDefinition) continue
      inclusionExclusion.push({
        id: toText(item.id) || `ie-${idx + 1}`,
        title: toText(item.title) || `조건 ${idx + 1}`,
        operationalDefinition,
        evidence: toOptionalText(item.evidence),
      })
      if (inclusionExclusion.length >= 20) break
    }
  }
  const normalized: SavedQueryPdfAnalysis = {
    pdfHash: toOptionalText(raw.pdfHash ?? raw.pdf_hash),
    pdfName: toOptionalText(raw.pdfName ?? raw.pdf_name),
    summaryKo: toOptionalText(raw.summaryKo ?? raw.summary_ko),
    criteriaSummaryKo: toOptionalText(raw.criteriaSummaryKo ?? raw.criteria_summary_ko),
    variables: variables.length ? variables : undefined,
    inclusionExclusion: inclusionExclusion.length ? inclusionExclusion : undefined,
    source: toOptionalText(raw.source),
    analyzedAt: toOptionalText(raw.analyzedAt ?? raw.analyzed_at),
    libraryUsed: readBooleanLike(raw.libraryUsed ?? raw.library_used),
    libraryCohortId: toOptionalText(raw.libraryCohortId ?? raw.library_cohort_id),
    libraryCohortName: toOptionalText(raw.libraryCohortName ?? raw.library_cohort_name),
  }
  if (
    !normalized.pdfHash &&
    !normalized.pdfName &&
    !normalized.summaryKo &&
    !normalized.criteriaSummaryKo &&
    !normalized.variables?.length &&
    !normalized.inclusionExclusion?.length
  ) {
    return undefined
  }
  return normalized
}

const formatCohortSourceText = (query: SavedQuery) => {
  const cohort = query.cohort
  const pdfAnalysis = query.pdfAnalysis
  const source = String(cohort?.source || "NONE").toUpperCase()
  const libraryRef =
    cohort?.libraryCohortName ||
    cohort?.libraryCohortId ||
    pdfAnalysis?.libraryCohortName ||
    pdfAnalysis?.libraryCohortId ||
    ""
  if (!cohort || source === "NONE") {
    return "코호트: 선택 안 함"
  }
  if (source === "PDF") {
    const pdfTitle =
      cohort.pdfPaperTitle || pdfAnalysis?.pdfName || cohort.pdfCohortId || pdfAnalysis?.pdfHash || "미지정 PDF"
    const provenance = libraryRef || cohort.pdfCohortId || pdfAnalysis?.pdfHash || "출처 정보 없음"
    return `코호트: PDF / ${pdfTitle} / ${provenance}`
  }
  return `코호트: 라이브러리 / ${libraryRef || "미지정"}`
}

const cohortSourceBadgeText = (query: SavedQuery) => {
  const source = String(query.cohort?.source || "NONE").toUpperCase()
  if (source === "PDF") return "PDF 코호트"
  if (source === "LIBRARY") return "라이브러리 코호트"
  return "코호트 없음"
}

const isLibraryUsed = (query: SavedQuery) => {
  if (typeof query.cohort?.libraryUsed === "boolean") return query.cohort.libraryUsed
  if (typeof query.pdfAnalysis?.libraryUsed === "boolean") return query.pdfAnalysis.libraryUsed
  return Boolean(
    query.cohort?.libraryCohortId ||
      query.cohort?.libraryCohortName ||
      query.pdfAnalysis?.libraryCohortId ||
      query.pdfAnalysis?.libraryCohortName
  )
}

const buildAxisTitleConfig = (axis: Record<string, unknown>, defaultSize: number) => {
  const rawTitle = axis.title
  if (typeof rawTitle === "string") {
    return {
      text: rawTitle,
      standoff: 16,
      font: { size: defaultSize },
    }
  }
  if (isRecordLike(rawTitle)) {
    const rawFont = isRecordLike(rawTitle.font) ? rawTitle.font : {}
    return {
      ...rawTitle,
      standoff: Number(rawTitle.standoff ?? 16),
      font: {
        ...rawFont,
        size: Number(rawFont.size ?? defaultSize),
      },
    }
  }
  return {
    standoff: 16,
    font: { size: defaultSize },
  }
}

const enhancePopupPlotLayout = (layoutInput: unknown) => {
  const layout = isRecordLike(layoutInput) ? layoutInput : {}
  const margin = isRecordLike(layout.margin) ? layout.margin : {}
  const xaxis = isRecordLike(layout.xaxis) ? layout.xaxis : {}
  const yaxis = isRecordLike(layout.yaxis) ? layout.yaxis : {}
  const xTickFont = isRecordLike(xaxis.tickfont) ? xaxis.tickfont : {}
  const yTickFont = isRecordLike(yaxis.tickfont) ? yaxis.tickfont : {}

  return {
    ...layout,
    margin: {
      ...margin,
      l: Math.max(44, Number(margin.l ?? 0)),
      r: Math.max(20, Number(margin.r ?? 0)),
      t: Math.max(34, Number(margin.t ?? 0)),
      b: Math.max(72, Number(margin.b ?? 0)),
    },
    xaxis: {
      ...xaxis,
      automargin: true,
      tickangle: typeof xaxis.tickangle === "number" ? xaxis.tickangle : -20,
      tickfont: {
        ...xTickFont,
        size: Number(xTickFont.size ?? 13),
      },
      title: buildAxisTitleConfig(xaxis, 14),
    },
    yaxis: {
      ...yaxis,
      automargin: true,
      tickfont: {
        ...yTickFont,
        size: Number(yTickFont.size ?? 13),
      },
      title: buildAxisTitleConfig(yaxis, 13),
    },
  }
}

const sanitizeRecommendedCharts = (raw: unknown, queryId: string): DashboardChartSpec[] => {
  if (!Array.isArray(raw)) return []
  const charts: DashboardChartSpec[] = []
  const seen = new Set<string>()
  raw.forEach((item, idx) => {
    const source = (item || {}) as Record<string, unknown>
    const type = normalizeChartType(source.type || source.chart_type)
    if (!type) return
    const id = String(source.id || `${queryId}-rec-${idx + 1}`).trim() || `${queryId}-rec-${idx + 1}`
    if (seen.has(id)) return
    const x = String(source.x || "").trim() || undefined
    const y = String(source.y || "").trim() || undefined
    const thumbnailUrl = String(source.thumbnailUrl || source.thumbnail_url || source.image_data_url || "").trim() || undefined
    const pngUrl = String(source.pngUrl || source.png_url || "").trim() || undefined
    const config =
      source.config && typeof source.config === "object" && !Array.isArray(source.config)
        ? (source.config as Record<string, unknown>)
        : undefined
    const sourceTag = String(config?.source || "").trim().toLowerCase()
    if (sourceTag === "fallback") return
    charts.push({ id, type, x, y, config, thumbnailUrl, pngUrl })
    seen.add(id)
  })
  return charts.slice(0, 3)
}

const getRecommendedChartsForQuery = (
  query: SavedQuery,
  scope: string
) => {
  return sanitizeRecommendedCharts(query.recommendedCharts, `${scope}-${query.id}`).slice(0, 3)
}

const buildFigureFromStoredChart = (chart: DashboardChartSpec | null | undefined) => {
  if (!chart?.config || typeof chart.config !== "object" || Array.isArray(chart.config)) return null
  const config = chart.config as Record<string, unknown>
  const rawFigure = config.figureJson || config.figure_json
  if (!rawFigure || typeof rawFigure !== "object" || Array.isArray(rawFigure)) return null
  const figure = rawFigure as Record<string, unknown>
  const data = Array.isArray(figure.data) ? figure.data : []
  if (!data.length) return null
  const layout =
    figure.layout && typeof figure.layout === "object" && !Array.isArray(figure.layout)
      ? (figure.layout as Record<string, unknown>)
      : {}
  return { data, layout }
}

const buildFigureByChartSpec = (
  chartType: string,
  columns: string[],
  records: Array<Record<string, unknown>>,
  title: string,
  preferredX?: string,
  preferredY?: string
) => {
  if (!columns.length || !records.length) return null
  const numericCols = columns.filter((col) => records.some((r) => isNumericValue(r[col])))
  const categoryCols = columns.filter((col) => !numericCols.includes(col))
  const xCol = preferredX && columns.includes(preferredX) ? preferredX : categoryCols[0] || columns[0]
  const yCol =
    preferredY && columns.includes(preferredY)
      ? preferredY
      : numericCols[0] || columns.find((col) => col !== xCol)
  if (!xCol || !yCol) return null

  const normalized = normalizeChartType(chartType)
  if (normalized === "PIE") {
    const sums = new Map<string, number>()
    for (const row of records) {
      const key = String(row[xCol] ?? "")
      const value = Number(row[yCol])
      if (!Number.isFinite(value)) continue
      sums.set(key, (sums.get(key) || 0) + value)
    }
    if (!sums.size) return null
    return {
      data: [{ type: "pie", labels: Array.from(sums.keys()), values: Array.from(sums.values()), textinfo: "label+percent" }],
      layout: { margin: { l: 24, r: 24, t: 36, b: 24 }, title },
    }
  }

  if (normalized === "LINE") {
    return {
      data: [{ type: "scatter", mode: "lines+markers", x: records.map((r) => r[xCol]), y: records.map((r) => Number(r[yCol])) }],
      layout: { margin: { l: 40, r: 16, t: 36, b: 40 }, xaxis: { title: xCol }, yaxis: { title: yCol }, title },
    }
  }

  if (normalized === "SCATTER") {
    return {
      data: [{ type: "scatter", mode: "markers", x: records.map((r) => r[xCol]), y: records.map((r) => Number(r[yCol])) }],
      layout: { margin: { l: 40, r: 16, t: 36, b: 40 }, xaxis: { title: xCol }, yaxis: { title: yCol }, title },
    }
  }

  return {
    data: [{ type: "bar", x: records.map((r) => r[xCol]), y: records.map((r) => Number(r[yCol])) }],
    layout: { margin: { l: 40, r: 16, t: 36, b: 40 }, xaxis: { title: xCol }, yaxis: { title: yCol }, title },
  }
}

const previewRowsToRecords = (query: SavedQuery | null) => {
  if (!query?.preview?.columns?.length || !Array.isArray(query.preview.rows)) return []
  const columns = query.preview.columns
  return query.preview.rows.map((row) => {
    const cells = Array.isArray(row) ? row : []
    const record: Record<string, unknown> = {}
    for (let i = 0; i < columns.length; i += 1) {
      record[columns[i]] = cells[i]
    }
    return record
  })
}

const buildStatsRowsFromQuery = (query: SavedQuery | null): DialogStatRow[] => {
  if (!query) return []
  if (Array.isArray(query.stats) && query.stats.length) {
    return query.stats.map((row) => ({
      column: String(row.column || ""),
      n: Number(row.n || 0),
      missingCount: Number(row.missing || 0),
      nullCount: Number(row.nulls || 0),
      min: toFiniteNumber(row.min),
      q1: toFiniteNumber(row.q1),
      median: toFiniteNumber(row.median),
      q3: toFiniteNumber(row.q3),
      max: toFiniteNumber(row.max),
      avg: toFiniteNumber(row.mean),
    }))
  }

  const preview = query.preview
  if (!preview?.columns?.length || !Array.isArray(preview.rows)) return []

  return preview.columns.map((column, colIndex) => {
    let n = 0
    let missingCount = 0
    let nullCount = 0
    const numericValues: number[] = []

    for (const row of preview.rows) {
      const cell = Array.isArray(row) ? row[colIndex] : undefined
      if (cell === null) {
        nullCount += 1
        missingCount += 1
        continue
      }
      if (cell === undefined || (typeof cell === "string" && cell.trim() === "")) {
        missingCount += 1
        continue
      }
      n += 1
      const numeric = toFiniteNumber(cell)
      if (numeric != null) numericValues.push(numeric)
    }

    numericValues.sort((a, b) => a - b)
    const min = numericValues.length ? numericValues[0] : null
    const max = numericValues.length ? numericValues[numericValues.length - 1] : null
    const avg = numericValues.length
      ? numericValues.reduce((acc, value) => acc + value, 0) / numericValues.length
      : null

    return {
      column,
      n,
      missingCount,
      nullCount,
      min,
      q1: quantile(numericValues, 0.25),
      median: quantile(numericValues, 0.5),
      q3: quantile(numericValues, 0.75),
      max,
      avg,
    }
  })
}

const detectBarVariant = (query: SavedQuery | null) => {
  const t = String(query?.title || "").toLowerCase()
  const d = String(query?.description || "").toLowerCase()
  const text = `${t} ${d}`
  if (text.includes("100%") || text.includes("100 percent")) return "hpercent"
  if (text.includes("horizontal stacked")) return "hstack"
  if (text.includes("stacked")) return "stacked"
  if (text.includes("grouped")) return "grouped"
  return "basic"
}

function normalizeDashboardData(rawQueries: SavedQuery[], rawFolders: SavedFolder[]) {
  const folderById = new Map<string, SavedFolder>()
  const folderByName = new Map<string, SavedFolder>()
  const folders: SavedFolder[] = []
  let changed = false

  for (const raw of rawFolders) {
    const id = String(raw?.id || "").trim()
    const name = normalizeName(String(raw?.name || ""))
    if (!id || !name || folderById.has(id)) {
      changed = true
      continue
    }
    const folder: SavedFolder = {
      id,
      name,
      tone: raw?.tone ? String(raw.tone) : undefined,
      createdAt: raw?.createdAt ? String(raw.createdAt) : undefined,
    }
    folderById.set(id, folder)
    folderByName.set(name.toLowerCase(), folder)
    folders.push(folder)
  }

  const getOrCreateFolderByName = (nameInput: string) => {
    const normalized = normalizeName(nameInput || "") || "기타"
    const key = normalized.toLowerCase()
    const existing = folderByName.get(key)
    if (existing) return existing
    const created: SavedFolder = {
      id: makeFolderId(),
      name: normalized,
      tone: nextTone(folders.length),
    }
    folders.push(created)
    folderById.set(created.id, created)
    folderByName.set(key, created)
    changed = true
    return created
  }

  if (folders.length === 0) {
    for (const item of seedFolders) {
      folders.push(item)
      folderById.set(item.id, item)
      folderByName.set(item.name.toLowerCase(), item)
    }
    changed = true
  }

  const queries = rawQueries.map((raw, index) => {
    const rawQuery = (raw || {}) as unknown as Record<string, unknown>
    const currentFolderId = typeof rawQuery.folderId === "string" ? rawQuery.folderId.trim() : ""
    const currentFolder = currentFolderId ? folderById.get(currentFolderId) : undefined
    let targetFolder = currentFolder

    if (!targetFolder) {
      const category = normalizeName(String(rawQuery.category || ""))
      targetFolder = getOrCreateFolderByName(category && category !== "전체" ? category : "기타")
    }

    const previewCandidate = (rawQuery.preview || {}) as Record<string, unknown>
    const previewColumns = Array.isArray(previewCandidate.columns)
      ? previewCandidate.columns.map((col) => String(col ?? "")).filter(Boolean)
      : []
    const previewRows = Array.isArray(previewCandidate.rows)
      ? previewCandidate.rows.map((row) => (Array.isArray(row) ? row : []))
      : []
    const normalizedPreview = previewColumns.length
      ? {
          columns: previewColumns,
          rows: previewRows as any[][],
          row_count: Number.isFinite(Number(previewCandidate.row_count))
            ? Number(previewCandidate.row_count)
            : previewRows.length,
          row_cap: Number.isFinite(Number(previewCandidate.row_cap))
            ? Number(previewCandidate.row_cap)
            : previewCandidate.row_cap == null
              ? null
              : undefined,
        }
      : undefined

    const normalizedMetrics = Array.isArray(rawQuery.metrics)
      ? rawQuery.metrics
          .map((metric) => {
            const item = (metric || {}) as Record<string, unknown>
            const label = String(item.label || "").trim()
            const value = String(item.value || "-").trim()
            let trend: "up" | "down" | undefined
            if (item.trend === "up" || item.trend === "down") {
              trend = item.trend
            }
            return { label, value: value || "-", trend }
          })
          .filter((metric) => metric.label || metric.value)
      : []
    const fallbackMetrics =
      normalizedMetrics.length > 0
        ? normalizedMetrics
        : [
            { label: "행 수", value: String(normalizedPreview?.row_count ?? "-") },
            { label: "컬럼 수", value: String(normalizedPreview?.columns.length ?? "-") },
            {
              label: "ROW CAP",
              value: normalizedPreview?.row_cap == null ? "-" : String(normalizedPreview.row_cap),
            },
          ]

    const normalizedChartType =
      rawQuery.chartType === "line" || rawQuery.chartType === "bar" || rawQuery.chartType === "pie"
        ? rawQuery.chartType
        : "bar"
    const normalizedId = String(rawQuery.id || rawQuery.queryId || `query-${index + 1}`).trim() || `query-${index + 1}`
    const normalizedRecommendedCharts = sanitizeRecommendedCharts(rawQuery.recommendedCharts, normalizedId)
    const normalizedPrimaryChart =
      sanitizeRecommendedCharts([rawQuery.primaryChart], `${normalizedId}-primary`)[0] || normalizedRecommendedCharts[0]
    const normalizedStats = Array.isArray(rawQuery.stats)
      ? (rawQuery.stats as SavedQuery["stats"])
      : undefined
    const normalizedCohort = normalizeCohortProvenance(rawQuery.cohort ?? rawQuery.cohort_provenance)
    const normalizedPdfAnalysis = normalizePdfAnalysisSnapshot(rawQuery.pdfAnalysis ?? rawQuery.pdf_analysis)
    const nextCategory = targetFolder.name
    const nextQuery: SavedQuery = {
      ...(raw as SavedQuery),
      id: normalizedId,
      title: String(rawQuery.title || rawQuery.question || "저장된 쿼리"),
      description: String(rawQuery.description || ""),
      insight: typeof rawQuery.insight === "string" ? rawQuery.insight : undefined,
      llmSummary: typeof rawQuery.llmSummary === "string" ? rawQuery.llmSummary : undefined,
      query: String(rawQuery.query || rawQuery.sql || ""),
      lastRun: String(rawQuery.lastRun || rawQuery.executedAt || "방금 전"),
      schedule: typeof rawQuery.schedule === "string" ? rawQuery.schedule : undefined,
      isPinned: Boolean(rawQuery.isPinned),
      folderId: targetFolder.id,
      category: nextCategory,
      cohort: normalizedCohort,
      pdfAnalysis: normalizedPdfAnalysis,
      preview: normalizedPreview,
      stats: normalizedStats,
      metrics: fallbackMetrics,
      chartType: normalizedChartType,
      recommendedCharts: normalizedRecommendedCharts,
      primaryChart: normalizedPrimaryChart,
    }

    if (rawQuery.folderId !== nextQuery.folderId || rawQuery.category !== nextQuery.category) {
      changed = true
    }

    return nextQuery
  })

  if (folders.length === 0) {
    folders.push({ id: DEFAULT_FOLDER_ID, name: "기타", tone: nextTone(0) })
    changed = true
  }

  return { queries, folders, changed }
}

export function DashboardView() {
  const { user } = useAuth()
  const apiBaseUrl = (process.env.NEXT_PUBLIC_API_BASE_URL || "").replace(/\/$/, "")
  const apiUrl = (path: string) => (apiBaseUrl ? `${apiBaseUrl}${path}` : path)
  const dashboardUser = (user?.id || user?.username || user?.name || "").trim()
  const apiUrlWithUser = (path: string) => {
    const base = apiUrl(path)
    if (!dashboardUser) return base
    const separator = base.includes("?") ? "&" : "?"
    return `${base}${separator}user=${encodeURIComponent(dashboardUser)}`
  }
  const pendingDashboardQueryKey = dashboardUser
    ? `ql_pending_dashboard_query:${dashboardUser}`
    : "ql_pending_dashboard_query"
  const [queries, setQueries] = useState<SavedQuery[]>([])
  const [folders, setFolders] = useState<SavedFolder[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [activeFolderId, setActiveFolderId] = useState(ALL_FOLDER_ID)
  const [openedFolderId, setOpenedFolderId] = useState<string | null>(null)
  const [isFolderDialogOpen, setIsFolderDialogOpen] = useState(false)
  const [dialogQueryId, setDialogQueryId] = useState<string | null>(null)
  const [isCreateFolderOpen, setIsCreateFolderOpen] = useState(false)
  const [createFolderName, setCreateFolderName] = useState("")
  const [isDeleteFolderOpen, setIsDeleteFolderOpen] = useState(false)
  const [deleteFolderTargetId, setDeleteFolderTargetId] = useState<string | null>(null)
  const [isRenameFolderOpen, setIsRenameFolderOpen] = useState(false)
  const [renameFolderTargetId, setRenameFolderTargetId] = useState<string | null>(null)
  const [renameFolderName, setRenameFolderName] = useState("")
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)

  // Comparison State
  const [selectedQueryIds, setSelectedQueryIds] = useState<Set<string>>(new Set())
  const [isCompareOpen, setIsCompareOpen] = useState(false)
  const [comparisonResults, setComparisonResults] = useState<Record<string, { loading: boolean; error?: string; data?: any }>>({})
  const [visibleComparisonIds, setVisibleComparisonIds] = useState<Set<string>>(new Set())
  const [comparisonOrder, setComparisonOrder] = useState<string[]>([])
  const [isCompareSelectionMode, setIsCompareSelectionMode] = useState(false)
  const [isDeleteQueriesOpen, setIsDeleteQueriesOpen] = useState(false)
  const [deleteQueryIds, setDeleteQueryIds] = useState<string[]>([])
  const [deletingQueries, setDeletingQueries] = useState(false)
  const [compareChartSelectionByQuery, setCompareChartSelectionByQuery] = useState<Record<string, string>>({})
  const [detailChartSelectionByQuery, setDetailChartSelectionByQuery] = useState<Record<string, string>>({})
  const [popupChartPayload, setPopupChartPayload] = useState<PopupChartPayload | null>(null)

  const saveTimer = useRef<number | null>(null)
  const listSectionRef = useRef<HTMLDivElement | null>(null)

  const folderMap = useMemo(() => new Map(folders.map((folder) => [folder.id, folder])), [folders])

  const applyDashboardSnapshot = (nextQueries: SavedQuery[], nextFolders: SavedFolder[]) => {
    setQueries(nextQueries)
    setFolders(nextFolders)
    writeDashboardCache<SavedQuery, SavedFolder>(dashboardUser, {
      queries: nextQueries,
      folders: nextFolders,
    })
  }

  const persistDashboard = async (nextQueries: SavedQuery[], nextFolders: SavedFolder[], silent = false) => {
    if (!silent) {
      setSaving(true)
    }
    try {
      const res = await fetch(apiUrlWithUser("/dashboard/queries"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user: dashboardUser || null, queries: nextQueries, folders: nextFolders }),
      })
      if (!res.ok) {
        throw new Error("dashboard_persist_failed")
      }
    } catch (persistError) {
      if (!silent) {
        setError("결과 보드 저장에 실패했습니다.")
      }
      throw persistError
    } finally {
      if (!silent) {
        setSaving(false)
      }
    }
  }

  const clearScheduledPersist = () => {
    if (saveTimer.current) {
      window.clearTimeout(saveTimer.current)
      saveTimer.current = null
    }
  }

  const schedulePersist = (nextQueries: SavedQuery[], nextFolders: SavedFolder[]) => {
    clearScheduledPersist()
    saveTimer.current = window.setTimeout(() => {
      void persistDashboard(nextQueries, nextFolders, true).catch(() => {
        setError("결과 보드 저장에 실패했습니다.")
      })
    }, 400)
  }

  const fetchFreshDashboard = async () => {
    const res = await fetch(apiUrlWithUser("/dashboard/queries"))
    if (!res.ok) {
      throw new Error("Failed to fetch dashboard queries.")
    }
    const payload = await res.json()
    const remoteQueries = Array.isArray(payload?.queries) ? payload.queries : []
    const remoteFolders = Array.isArray(payload?.folders) ? payload.folders : []
    const cleanedRemoteQueries = remoteQueries.filter((item: any) => !isLegacyDemoSeedQuery(item))
    const removedLegacyDemo = cleanedRemoteQueries.length !== remoteQueries.length

    const cleanedRemoteFolders = (() => {
      if (!removedLegacyDemo) return remoteFolders
      const usedFolderIds = new Set(
        cleanedRemoteQueries
          .map((item: any) => String(item?.folderId || "").trim())
          .filter(Boolean)
      )
      return remoteFolders.filter((item: any) => {
        const id = String(item?.id || "").trim()
        const name = String(item?.name || "").trim()
        if (!id) return false
        if (usedFolderIds.has(id)) return true
        const isSeedFolder = seedFolders.some((seed) => seed.id === id || seed.name === name)
        return !isSeedFolder
      })
    })()

    const useDemoFallback = cleanedRemoteQueries.length === 0 && Boolean(payload?.detail)
    const baseQueries = useDemoFallback ? savedQueries : cleanedRemoteQueries
    const baseFolders = useDemoFallback ? seedFolders : cleanedRemoteFolders

    return {
      normalized: normalizeDashboardData(baseQueries, baseFolders),
      removedLegacyDemo,
      useDemoFallback,
    }
  }

  const restoreFreshAndCache = async () => {
    const fresh = await fetchFreshDashboard()
    applyDashboardSnapshot(fresh.normalized.queries, fresh.normalized.folders)
    return fresh
  }

  const loadQueries = async () => {
    setError(null)
    const cached = readDashboardCache<SavedQuery, SavedFolder>(dashboardUser)
    if (cached) {
      const normalizedCached = normalizeDashboardData(cached.queries, cached.folders)
      applyDashboardSnapshot(normalizedCached.queries, normalizedCached.folders)
      setLoading(false)
    } else {
      setLoading(true)
    }

    try {
      const fresh = await restoreFreshAndCache()
      if (!fresh.useDemoFallback && (fresh.normalized.changed || fresh.removedLegacyDemo)) {
        void persistDashboard(fresh.normalized.queries, fresh.normalized.folders, true).catch(() => {
          setError("결과 보드 저장에 실패했습니다.")
        })
      }
      setError(null)
    } catch {
      if (!cached) {
        const normalized = normalizeDashboardData(savedQueries, seedFolders)
        applyDashboardSnapshot(normalized.queries, normalized.folders)
        setError("결과 보드를 불러오지 못했습니다.")
      }
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void loadQueries()
    return () => {
      clearScheduledPersist()
    }
  }, [dashboardUser])

  useEffect(() => {
    const existsActive =
      activeFolderId === ALL_FOLDER_ID || folders.some((folder) => folder.id === activeFolderId)
    if (!existsActive) {
      setActiveFolderId(ALL_FOLDER_ID)
    }
    if (openedFolderId) {
      const existsOpened = folders.some((folder) => folder.id === openedFolderId)
      if (!existsOpened) {
        setOpenedFolderId(null)
        setIsFolderDialogOpen(false)
      }
    }
  }, [folders, activeFolderId, openedFolderId])

  useEffect(() => {
    const validIds = new Set(queries.map((query) => query.id))
    setSelectedQueryIds((prev) => {
      const next = new Set(Array.from(prev).filter((id) => validIds.has(id)))
      if (next.size === prev.size) {
        let same = true
        prev.forEach((id) => {
          if (!next.has(id)) same = false
        })
        if (same) return prev
      }
      return next
    })
    setVisibleComparisonIds((prev) => {
      const next = new Set(Array.from(prev).filter((id) => validIds.has(id)))
      if (next.size === prev.size) {
        let same = true
        prev.forEach((id) => {
          if (!next.has(id)) same = false
        })
        if (same) return prev
      }
      return next
    })
    setComparisonOrder((prev) => prev.filter((id) => validIds.has(id)))
    setComparisonResults((prev) => {
      const next: Record<string, { loading: boolean; error?: string; data?: any }> = {}
      Object.keys(prev).forEach((id) => {
        if (validIds.has(id)) {
          next[id] = prev[id]
        }
      })
      return next
    })
    setCompareChartSelectionByQuery((prev) => {
      const next: Record<string, string> = {}
      Object.keys(prev).forEach((id) => {
        if (validIds.has(id)) next[id] = prev[id]
      })
      return next
    })
  }, [queries])

  const updateDashboardState = (nextQueries: SavedQuery[], nextFolders: SavedFolder[]) => {
    applyDashboardSnapshot(nextQueries, nextFolders)
    schedulePersist(nextQueries, nextFolders)
  }

  const mergeBundleIntoQuery = (query: SavedQuery, rawBundle: unknown): SavedQuery => {
    if (!rawBundle || typeof rawBundle !== "object") return query
    const bundle = rawBundle as Record<string, unknown>
    const merged: SavedQuery = { ...query }

    if (typeof bundle.title === "string" && bundle.title.trim()) merged.title = bundle.title
    if (typeof bundle.description === "string") merged.description = bundle.description
    if (typeof bundle.query === "string") merged.query = bundle.query
    if (typeof bundle.insight === "string") merged.insight = bundle.insight
    if (typeof bundle.llmSummary === "string") merged.llmSummary = bundle.llmSummary
    if (typeof bundle.lastRun === "string" && bundle.lastRun.trim()) merged.lastRun = bundle.lastRun
    if (typeof bundle.category === "string" && bundle.category.trim()) merged.category = bundle.category
    if (typeof bundle.folderId === "string" && bundle.folderId.trim()) merged.folderId = bundle.folderId

    const normalizedBundleCohort = normalizeCohortProvenance(bundle.cohort ?? bundle.cohort_provenance)
    if (normalizedBundleCohort) {
      merged.cohort = normalizedBundleCohort
    }
    const normalizedBundlePdfAnalysis = normalizePdfAnalysisSnapshot(bundle.pdfAnalysis ?? bundle.pdf_analysis)
    if (normalizedBundlePdfAnalysis) {
      merged.pdfAnalysis = normalizedBundlePdfAnalysis
    }

    if (bundle.chartType === "line" || bundle.chartType === "bar" || bundle.chartType === "pie") {
      merged.chartType = bundle.chartType
    } else if (typeof bundle.chartType === "string") {
      const lowered = bundle.chartType.toLowerCase()
      if (lowered === "line" || lowered === "bar" || lowered === "pie") merged.chartType = lowered
    }

    if (Array.isArray(bundle.recommendedCharts)) {
      merged.recommendedCharts = sanitizeRecommendedCharts(bundle.recommendedCharts, query.id)
    }

    if (bundle.primaryChart != null) {
      const normalizedPrimaryChart =
        sanitizeRecommendedCharts([bundle.primaryChart], `${query.id}-primary`)[0] || undefined
      if (normalizedPrimaryChart) merged.primaryChart = normalizedPrimaryChart
    }

    if (Array.isArray(bundle.stats)) {
      merged.stats = bundle.stats as SavedQuery["stats"]
    }

    if (Array.isArray(bundle.metrics)) {
      const metrics: SavedQuery["metrics"] = []
      bundle.metrics.forEach((metric) => {
        const item = (metric || {}) as Record<string, unknown>
        const label = String(item.label || "").trim()
        const value = String(item.value || "-").trim()
        let trend: "up" | "down" | undefined
        if (item.trend === "up" || item.trend === "down") trend = item.trend
        if (!label && !value) return
        metrics.push({ label: label || "지표", value: value || "-", trend })
      })
      if (metrics.length) merged.metrics = metrics
    }

    if (bundle.preview && typeof bundle.preview === "object" && !Array.isArray(bundle.preview)) {
      const preview = bundle.preview as Record<string, unknown>
      const columns = Array.isArray(preview.columns)
        ? preview.columns.map((col) => String(col ?? "")).filter(Boolean)
        : []
      const rows = Array.isArray(preview.rows)
        ? preview.rows.map((row) => (Array.isArray(row) ? row : []))
        : []
      if (columns.length) {
        merged.preview = {
          columns,
          rows,
          row_count: Number.isFinite(Number(preview.row_count)) ? Number(preview.row_count) : rows.length,
          row_cap: Number.isFinite(Number(preview.row_cap))
            ? Number(preview.row_cap)
            : preview.row_cap == null
              ? null
              : undefined,
        }
      }
    }

    return merged
  }

  const fetchQueryBundles = async (queryIds: string[]) => {
    const ids = Array.from(new Set(queryIds.map((id) => String(id || "").trim()).filter(Boolean)))
    if (!ids.length) return {} as Record<string, unknown>
    try {
      const res = await fetch(apiUrl("/dashboard/queryBundles"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user: dashboardUser || null, queryIds: ids }),
      })
      if (!res.ok) return {} as Record<string, unknown>
      const payload = await res.json()
      const bundles =
        payload && typeof payload === "object" && payload.bundles && typeof payload.bundles === "object"
          ? (payload.bundles as Record<string, unknown>)
          : {}
      if (Object.keys(bundles).length > 0) {
        setQueries((prev) => {
          const mergedQueries = prev.map((query) =>
            bundles[query.id] ? mergeBundleIntoQuery(query, bundles[query.id]) : query
          )
          writeDashboardCache<SavedQuery, SavedFolder>(dashboardUser, {
            queries: mergedQueries,
            folders,
          })
          return mergedQueries
        })
      }
      return bundles
    } catch {
      return {} as Record<string, unknown>
    }
  }

  const togglePin = (id: string) => {
    const nextQueries = queries.map((q) => (q.id === id ? { ...q, isPinned: !q.isPinned } : q))
    updateDashboardState(nextQueries, folders)
  }

  const requestDeleteQueries = (ids: string[]) => {
    const nextIds = Array.from(new Set(ids.map((id) => String(id || "").trim()).filter(Boolean)))
    if (!nextIds.length) return
    setDeleteQueryIds(nextIds)
    setIsDeleteQueriesOpen(true)
  }

  const handleDelete = (id: string) => {
    requestDeleteQueries([id])
  }

  const handleSelectionModeToggle = () => {
    if (isCompareSelectionMode) {
      setIsCompareSelectionMode(false)
      setSelectedQueryIds(new Set())
      return
    }
    setIsCompareSelectionMode(true)
  }

  const deleteSelectedQueries = async (ids: string[]) => {
    const nextIds = Array.from(new Set(ids.map((id) => String(id || "").trim()).filter(Boolean)))
    if (!nextIds.length) return

    const removingIdSet = new Set(nextIds)
    const previousQueries = queries
    const previousFolders = folders
    const nextQueries = previousQueries.filter((query) => !removingIdSet.has(query.id))

    clearScheduledPersist()
    applyDashboardSnapshot(nextQueries, previousFolders)
    setSelectedQueryIds((prev) => new Set(Array.from(prev).filter((id) => !removingIdSet.has(id))))
    setComparisonOrder((prev) => prev.filter((id) => !removingIdSet.has(id)))
    setVisibleComparisonIds((prev) => new Set(Array.from(prev).filter((id) => !removingIdSet.has(id))))
    setComparisonResults((prev) => {
      const next: Record<string, { loading: boolean; error?: string; data?: any }> = {}
      Object.keys(prev).forEach((id) => {
        if (!removingIdSet.has(id)) {
          next[id] = prev[id]
        }
      })
      return next
    })
    setCompareChartSelectionByQuery((prev) => {
      const next: Record<string, string> = {}
      Object.keys(prev).forEach((id) => {
        if (!removingIdSet.has(id)) {
          next[id] = prev[id]
        }
      })
      return next
    })

    try {
      await persistDashboard(nextQueries, previousFolders)
    } catch {
      const restored = await restoreFreshAndCache()
        .then(() => true)
        .catch(() => false)
      if (!restored) {
        applyDashboardSnapshot(previousQueries, previousFolders)
      }
      throw new Error("delete_selected_failed")
    }
  }

  const confirmDeleteQueries = async () => {
    if (!deleteQueryIds.length || deletingQueries) return
    setDeletingQueries(true)
    setError(null)
    try {
      await deleteSelectedQueries(deleteQueryIds)
      setDeleteQueryIds([])
      setIsDeleteQueriesOpen(false)
      setIsCompareSelectionMode(false)
      setSelectedQueryIds(new Set())
    } catch {
      setError("선택한 쿼리 삭제에 실패했습니다. 최신 데이터로 복구했습니다.")
    } finally {
      setDeletingQueries(false)
    }
  }

  const handleDuplicate = (id: string) => {
    const target = queries.find((q) => q.id === id)
    if (!target) return
    const nextQueries = [
      {
        ...target,
        id: `copy-${Date.now()}`,
        title: `${target.title} (복제)`,
        isPinned: false,
        lastRun: "방금 생성",
      },
      ...queries,
    ]
    updateDashboardState(nextQueries, folders)
  }

  const handleAddQuery = () => {
    const targetFolderId = activeFolderId || folders[0]?.id
    const targetFolder = folders.find((folder) => folder.id === targetFolderId) || folders[0]
    if (!targetFolder) {
      setError("폴더를 먼저 생성해주세요.")
      return
    }
    const nextQueries = [
      {
        id: `new-${Date.now()}`,
        title: "새 쿼리",
        description: "설명을 입력하세요",
        query: "",
        lastRun: "방금 생성",
        isPinned: true,
        category: targetFolder.name,
        folderId: targetFolder.id,
        metrics: [
          { label: "지표 1", value: "-" },
          { label: "지표 2", value: "-" },
          { label: "지표 3", value: "-" },
        ],
        chartType: "bar" as const,
      },
      ...queries,
    ]
    updateDashboardState(nextQueries, folders)
  }

  const handleCreateFolder = (rawName: string) => {
    const name = normalizeName(rawName)
    if (!name) {
      setError("폴더 이름을 입력해주세요.")
      return
    }
    const duplicated = folders.some((folder) => folder.name.toLowerCase() === name.toLowerCase())
    if (duplicated) {
      setError("같은 이름의 폴더가 이미 있습니다.")
      return
    }

    const nextFolder: SavedFolder = {
      id: makeFolderId(),
      name,
      tone: nextTone(folders.length),
      createdAt: new Date().toISOString(),
    }
    const nextFolders = [...folders, nextFolder]
    updateDashboardState(queries, nextFolders)
    setActiveFolderId(nextFolder.id)
  }

  const handleRenameFolder = (folderId: string) => {
    const target = folders.find((folder) => folder.id === folderId)
    if (!target) return
    setRenameFolderTargetId(folderId)
    setRenameFolderName(target.name)
    setIsRenameFolderOpen(true)
  }

  const confirmRenameFolder = () => {
    const targetId = renameFolderTargetId
    if (!targetId) return
    const name = normalizeName(renameFolderName)
    if (!name) {
      setError("폴더 이름을 입력해주세요.")
      return
    }
    const duplicated = folders.some((folder) => folder.id !== targetId && folder.name.toLowerCase() === name.toLowerCase())
    if (duplicated) {
      setError("같은 이름의 폴더가 이미 있습니다.")
      return
    }

    const nextFolders = folders.map((folder) =>
      folder.id === targetId ? { ...folder, name } : folder
    )
    const nextQueries = queries.map((query) =>
      query.folderId === targetId ? { ...query, category: name } : query
    )
    updateDashboardState(nextQueries, nextFolders)
    setIsRenameFolderOpen(false)
    setRenameFolderTargetId(null)
    setRenameFolderName("")
  }

  const handleDeleteFolder = (folderId: string) => {
    const target = folders.find((folder) => folder.id === folderId)
    if (!target) return
    if (folders.length <= 1) {
      setError("마지막 폴더는 삭제할 수 없습니다.")
      return
    }
    setDeleteFolderTargetId(folderId)
    setIsDeleteFolderOpen(true)
  }

  const confirmDeleteFolder = () => {
    const targetId = deleteFolderTargetId
    if (!targetId) return
    const target = folders.find((folder) => folder.id === targetId)
    if (!target) return
    const fallbackFolder = folders.find((folder) => folder.id !== targetId) || folders[0]
    if (!fallbackFolder) {
      setError("이동할 폴더가 없습니다.")
      return
    }
    const nextFolders = folders.filter((folder) => folder.id !== targetId)
    const nextQueries = queries.map((query) =>
      query.folderId === targetId
        ? { ...query, folderId: fallbackFolder.id, category: fallbackFolder.name }
        : query
    )
    updateDashboardState(nextQueries, nextFolders)
    if (activeFolderId === targetId) {
      setActiveFolderId(fallbackFolder.id)
    }
    setIsDeleteFolderOpen(false)
    setDeleteFolderTargetId(null)
  }

  const moveQueryToFolder = (queryId: string, folderId: string) => {
    const folder = folderMap.get(folderId)
    if (!folder) return
    const nextQueries = queries.map((query) =>
      query.id === queryId
        ? {
          ...query,
          folderId,
          category: folder.name,
        }
        : query
    )
    updateDashboardState(nextQueries, folders)
  }

  const getChartIcon = (type: string) => {
    switch (type) {
      case "line":
        return <Activity className="w-4 h-4" />
      case "bar":
        return <BarChart3 className="w-4 h-4" />
      case "pie":
        return <PieChart className="w-4 h-4" />
      default:
        return <BarChart3 className="w-4 h-4" />
    }
  }

  const folderCards = useMemo<FolderCardInfo[]>(() => {
    const counts = new Map<string, { count: number; pinnedCount: number }>()
    for (const query of queries) {
      const folderId = query.folderId || DEFAULT_FOLDER_ID
      const current = counts.get(folderId) || { count: 0, pinnedCount: 0 }
      current.count += 1
      if (query.isPinned) {
        current.pinnedCount += 1
      }
      counts.set(folderId, current)
    }

    const allPinned = queries.filter((query) => query.isPinned).length
    const cards: FolderCardInfo[] = [
      {
        id: ALL_FOLDER_ID,
        name: "전체",
        count: queries.length,
        pinnedCount: allPinned,
        tone: undefined,
        editable: false,
      },
    ]
    const orderedFolders = [...folders].sort((a, b) => a.name.localeCompare(b.name, "ko"))
    for (const folder of orderedFolders) {
      const stats = counts.get(folder.id) || { count: 0, pinnedCount: 0 }
      cards.push({
        id: folder.id,
        name: folder.name,
        count: stats.count,
        pinnedCount: stats.pinnedCount,
        tone: folder.tone,
        editable: true,
      })
    }

    return cards
  }, [folders, queries])

  const normalizedSearch = searchTerm.trim().toLowerCase()
  const filteredQueries = useMemo(() => {
    return queries
      .filter((query) => {
        const matchesSearch =
          query.title.toLowerCase().includes(normalizedSearch) ||
          query.description.toLowerCase().includes(normalizedSearch)
        const matchesFolder =
          activeFolderId === ALL_FOLDER_ID || query.folderId === activeFolderId
        return matchesSearch && matchesFolder
      })
      .sort((a, b) => {
        if (a.isPinned !== b.isPinned) {
          return a.isPinned ? -1 : 1
        }
        return a.title.localeCompare(b.title, "ko")
      })
  }, [normalizedSearch, queries, activeFolderId])

  const selectedFolderName =
    activeFolderId === ALL_FOLDER_ID
      ? "전체 쿼리"
      : `${folderMap.get(activeFolderId)?.name || "알 수 없는 폴더"} 쿼리`

  const openedFolderName = openedFolderId
    ? folderMap.get(openedFolderId)?.name || "알 수 없는 폴더"
    : ""

  const dialogQueries = useMemo(() => {
    if (!openedFolderId) return []
    return queries.filter((query) => query.folderId === openedFolderId)
  }, [openedFolderId, queries])

  useEffect(() => {
    if (!isFolderDialogOpen) return
    if (!dialogQueries.length) {
      setDialogQueryId(null)
      return
    }
    if (!dialogQueryId || !dialogQueries.some((item) => item.id === dialogQueryId)) {
      setDialogQueryId(dialogQueries[0].id)
    }
  }, [dialogQueries, dialogQueryId, isFolderDialogOpen])

  const selectedDialogQuery = dialogQueries.find((item) => item.id === dialogQueryId) || null
  const selectedDialogRecords = useMemo(
    () => previewRowsToRecords(selectedDialogQuery),
    [selectedDialogQuery]
  )
  const selectedDialogColumns = useMemo(
    () => selectedDialogQuery?.preview?.columns || [],
    [selectedDialogQuery]
  )
  const selectedDialogRecommendedCharts = useMemo(() => {
    if (!selectedDialogQuery) return []
    return getRecommendedChartsForQuery(selectedDialogQuery, "detail")
  }, [selectedDialogQuery])
  const selectedDialogRecommendedChart = useMemo(() => {
    if (!selectedDialogQuery) return null
    const selectedId = detailChartSelectionByQuery[selectedDialogQuery.id]
    return selectedDialogRecommendedCharts.find((item) => item.id === selectedId) || selectedDialogRecommendedCharts[0] || null
  }, [detailChartSelectionByQuery, selectedDialogQuery, selectedDialogRecommendedCharts])
  const selectedDialogStoredFigure = useMemo(
    () => buildFigureFromStoredChart(selectedDialogRecommendedChart),
    [selectedDialogRecommendedChart]
  )

  const selectedDialogFigure = useMemo(() => {
    const q = selectedDialogQuery
    if (!q) return null
    const records = selectedDialogRecords
    if (!records.length) return null

    const columns = selectedDialogColumns
    const numericCols = columns.filter((col) => records.some((r) => isNumericValue(r[col])))
    const categoryCols = columns.filter((col) => !numericCols.includes(col))
    const title = q.title || "Saved Visualization"

    if (q.chartType === "pie") {
      const labelCol = categoryCols[0] || columns[0]
      const valueCol = numericCols[0] || columns[1]
      if (!labelCol || !valueCol) return null
      const sums = new Map<string, number>()
      for (const r of records) {
        const label = String(r[labelCol] ?? "")
        const value = Number(r[valueCol])
        if (!Number.isFinite(value)) continue
        sums.set(label, (sums.get(label) || 0) + value)
      }
      return {
        data: [
          {
            type: "pie",
            labels: Array.from(sums.keys()),
            values: Array.from(sums.values()),
            textinfo: "label+percent",
          },
        ],
        layout: { margin: { l: 24, r: 24, t: 36, b: 24 }, title },
      }
    }

    if (q.chartType === "line") {
      const xCol = categoryCols[0] || columns[0]
      const yCol = numericCols[0] || columns[1]
      if (!xCol || !yCol) return null
      return {
        data: [
          {
            type: "scatter",
            mode: "lines+markers",
            x: records.map((r) => r[xCol]),
            y: records.map((r) => Number(r[yCol])),
            name: yCol,
          },
        ],
        layout: {
          margin: { l: 40, r: 16, t: 36, b: 40 },
          xaxis: { title: xCol },
          yaxis: { title: yCol },
          title,
        },
      }
    }

    // bar
    const variant = detectBarVariant(q)
    if (numericCols.length >= 2 && categoryCols.length >= 1) {
      const xCol = categoryCols[0]
      const traces = numericCols.slice(0, 6).map((col) => ({
        type: "bar",
        name: col,
        x: records.map((r) => String(r[xCol] ?? "")),
        y: records.map((r) => Number(r[col])),
      }))
      return {
        data: traces,
        layout: {
          margin: { l: 40, r: 16, t: 36, b: 40 },
          barmode: variant === "stacked" ? "stack" : "group",
          xaxis: { title: xCol },
          yaxis: { title: "value" },
          title,
        },
      }
    }

    if (numericCols.length >= 1 && categoryCols.length >= 2) {
      const xCol = categoryCols[0]
      const gCol = categoryCols[1]
      const yCol = numericCols[0]
      const categories = Array.from(new Set(records.map((r) => String(r[xCol] ?? ""))))
      const groups = Array.from(new Set(records.map((r) => String(r[gCol] ?? ""))))
      const index = new Map<string, number>()
      categories.forEach((c, i) => index.set(c, i))
      const traces = groups.map((g) => {
        const values = new Array(categories.length).fill(0)
        for (const r of records) {
          if (String(r[gCol] ?? "") !== g) continue
          const key = String(r[xCol] ?? "")
          const idx = index.get(key)
          if (idx == null) continue
          const v = Number(r[yCol])
          if (Number.isFinite(v)) values[idx] += v
        }
        if (variant === "hstack" || variant === "hpercent") {
          return { type: "bar", name: g, orientation: "h", y: categories, x: values }
        }
        return { type: "bar", name: g, x: categories, y: values }
      })
      const barmode = variant === "stacked" || variant === "hstack" || variant === "hpercent" ? "stack" : "group"
      const barnorm = variant === "hpercent" ? "percent" : undefined
      const horizontal = variant === "hstack" || variant === "hpercent"
      return {
        data: traces,
        layout: {
          margin: { l: 56, r: 16, t: 36, b: 40 },
          barmode,
          barnorm,
          xaxis: { title: horizontal ? yCol : xCol },
          yaxis: { title: horizontal ? xCol : yCol },
          title,
        },
      }
    }

    if (numericCols.length >= 1 && columns.length >= 2) {
      const xCol = columns[0]
      const yCol = numericCols[0]
      return {
        data: [{ type: "bar", x: records.map((r) => r[xCol]), y: records.map((r) => Number(r[yCol])) }],
        layout: {
          margin: { l: 40, r: 16, t: 36, b: 40 },
          xaxis: { title: xCol },
          yaxis: { title: yCol },
          title,
        },
      }
    }

    return null
  }, [selectedDialogColumns, selectedDialogQuery, selectedDialogRecords])

  const selectedDialogStatsRows = useMemo<DialogStatRow[]>(() => buildStatsRowsFromQuery(selectedDialogQuery), [selectedDialogQuery])

  const selectedDialogInsight = useMemo(() => {
    if (!selectedDialogQuery) return "선택된 쿼리가 없습니다."
    const savedInsight = String(selectedDialogQuery.llmSummary || selectedDialogQuery.insight || "").trim()
    if (savedInsight) return savedInsight
    const fallback = String(selectedDialogQuery.description || "").trim()
    return fallback || "저장된 해석 데이터가 없습니다."
  }, [selectedDialogQuery])
  const selectedDialogCohortText = useMemo(
    () => (selectedDialogQuery ? formatCohortSourceText(selectedDialogQuery) : "코호트: 선택 안 함"),
    [selectedDialogQuery]
  )
  const selectedDialogLibraryUsed = useMemo(
    () => (selectedDialogQuery ? isLibraryUsed(selectedDialogQuery) : false),
    [selectedDialogQuery]
  )
  const selectedDialogPdfAnalysis = selectedDialogQuery?.pdfAnalysis
  const selectedDialogPdfVariables = selectedDialogPdfAnalysis?.variables || []
  const selectedDialogPdfInclusions = selectedDialogPdfAnalysis?.inclusionExclusion || []
  const selectedDialogAnalyzedAt = (() => {
    const value = selectedDialogPdfAnalysis?.analyzedAt
    if (!value) return null
    const date = new Date(value)
    return Number.isNaN(date.getTime()) ? value : date.toLocaleString("ko-KR")
  })()
  const selectedDialogRecommendedImage =
    selectedDialogRecommendedChart?.pngUrl || selectedDialogRecommendedChart?.thumbnailUrl || ""
  const selectedDialogChartTitle = `${selectedDialogQuery?.title || "쿼리"} · ${
    selectedDialogRecommendedChart?.type || selectedDialogQuery?.chartType || "chart"
  }`

  const openPopupChart = (
    title: string,
    imageUrl?: string,
    figure?: { data?: unknown[]; layout?: Record<string, unknown> }
  ) => {
    const normalizedImage = String(imageUrl || "").trim()
    const hasFigure = Array.isArray(figure?.data) && figure.data.length > 0
    if (!normalizedImage && !hasFigure) return
    setPopupChartPayload({
      title,
      imageUrl: normalizedImage || undefined,
      figure: hasFigure ? { data: figure!.data as unknown[], layout: figure?.layout || {} } : undefined,
    })
  }

  const handleOpenFolder = (folderId: string) => {
    if (folderId === ALL_FOLDER_ID) return
    setActiveFolderId(folderId)
    setOpenedFolderId(folderId)
    setIsFolderDialogOpen(true)
    const targetIds = queries.filter((query) => query.folderId === folderId).map((query) => query.id)
    void fetchQueryBundles(targetIds)
  }

  const handleToggleCompare = (id: string) => {
    const next = new Set(selectedQueryIds)
    if (next.has(id)) {
      next.delete(id)
    } else {
      if (next.size >= 3) {
        // toast({
        //   title: "비교 불가",
        //   description: "비교는 최대 3개까지만 가능합니다.",
        //   variant: "destructive",
        // })
        alert("비교는 최대 3개까지만 가능합니다.")
        return
      }
      next.add(id)
    }
    setSelectedQueryIds(next)
  }

  const executeComparisonQueries = (ids: Set<string>, bundlesById?: Record<string, unknown>) => {
    const nextResults: Record<string, { loading: boolean; error?: string; data?: any }> = {}

    ids.forEach(id => {
      const baseQuery = queries.find(q => q.id === id)
      if (!baseQuery) return
      const query = bundlesById?.[id] ? mergeBundleIntoQuery(baseQuery, bundlesById[id]) : baseQuery

      if (query.preview && query.preview.columns && Array.isArray(query.preview.rows)) {
        // 저장된 프리뷰 데이터를 차트/테이블용 레코드 형식으로 변환
        const columns = query.preview.columns
        const rows = query.preview.rows
        const records = rows.map((row: any[]) => {
          const rec: any = {}
          columns.forEach((col: string, i: number) => {
            rec[col] = row[i]
          })
          return rec
        })

        nextResults[id] = {
          loading: false,
          data: { columns, records }
        }
      } else {
        // 프리뷰 데이터가 없는 경우 처리
        nextResults[id] = {
          loading: false,
          error: "저장된 결과 데이터가 없습니다. (쿼리 탭에서 실행 후 저장해주세요)"
        }
      }
    })

    setComparisonResults(prev => ({ ...prev, ...nextResults }))
  }

  const handleStartCompare = async () => {
    const ids = Array.from(selectedQueryIds)
    const bundlesById = await fetchQueryBundles(ids)
    setIsCompareOpen(true)
    setIsCompareSelectionMode(false)
    setCompareChartSelectionByQuery({})
    setVisibleComparisonIds(new Set(selectedQueryIds))
    setComparisonOrder(Array.from(selectedQueryIds))
    executeComparisonQueries(new Set(ids), bundlesById)
  }

  const handleRunQuery = (query: SavedQuery) => {
    if (typeof window === "undefined") return
    const payload = {
      question: (query.title || "").trim(),
      sql: (query.query || "").trim(),
      description: (query.description || "").trim(),
      chartType: query.chartType,
      ts: Date.now(),
    }
    const serialized = JSON.stringify(payload)
    let stored = false
    try {
      localStorage.setItem(pendingDashboardQueryKey, serialized)
      stored = true
    } catch {}
    if (!stored) {
      try {
        sessionStorage.setItem(pendingDashboardQueryKey, serialized)
        stored = true
      } catch {}
    }
    if (!stored) {
      setError("브라우저 저장소에 실행 정보를 저장하지 못했습니다. 저장소를 정리한 뒤 다시 시도해주세요.")
      return
    }
    window.dispatchEvent(new Event("ql-open-query-view"))
  }

  const isInitialLoading = loading && queries.length === 0 && folders.length === 0

  return (
    <div className="p-4 sm:p-6 space-y-4 sm:space-y-6 w-full max-w-none">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <div>
          <h2 className="text-xl sm:text-2xl font-bold text-foreground">결과 보드</h2>
          <p className="text-sm text-muted-foreground mt-1">폴더를 만들고 쿼리를 이동해 체계적으로 관리합니다</p>
        </div>
      </div>

      {error && <div className="text-sm text-destructive">{error}</div>}
      {saving && <div className="text-xs text-muted-foreground">저장 중...</div>}

      <div className="relative w-full">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
        <Input
          placeholder="쿼리 검색..."
          className="pl-9"
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
        />
      </div>

      {isInitialLoading ? (
        <div className="space-y-4">
          <div className="grid sm:grid-cols-2 xl:grid-cols-3 gap-3 sm:gap-4">
            {Array.from({ length: 3 }).map((_, index) => (
              <Skeleton key={`folder-skeleton-${index}`} className="h-24 rounded-2xl" />
            ))}
          </div>
          <Card>
            <CardHeader className="pb-3 space-y-2">
              <Skeleton className="h-5 w-40" />
              <Skeleton className="h-4 w-72" />
            </CardHeader>
            <CardContent className="space-y-3">
              <Skeleton className="h-24 w-full rounded-xl" />
              <Skeleton className="h-16 w-full rounded-xl" />
              <Skeleton className="h-16 w-full rounded-xl" />
            </CardContent>
          </Card>
        </div>
      ) : (
        <>
          <div>
            <div className="mb-3 flex items-center justify-between gap-2">
              <h3 className="text-sm font-medium text-muted-foreground">폴더</h3>
              <Button variant="outline" size="sm" className="h-8 gap-1" onClick={() => setIsCreateFolderOpen(true)}>
                <FolderPlus className="w-3.5 h-3.5" />
                폴더 생성
              </Button>
            </div>
            <div className="grid sm:grid-cols-2 xl:grid-cols-3 gap-3 sm:gap-4">
              {folderCards.map((folder) => (
                <FolderCard
                  key={folder.id}
                  folder={folder}
                  active={activeFolderId === folder.id}
                  onSelect={() => setActiveFolderId(folder.id)}
                  onOpen={() => handleOpenFolder(folder.id)}
                  onRename={folder.editable ? () => handleRenameFolder(folder.id) : undefined}
                  onDelete={folder.editable ? () => handleDeleteFolder(folder.id) : undefined}
                />
              ))}
            </div>
          </div>

          <Card ref={listSectionRef}>
            <CardHeader className="pb-3">
              <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2">
                <div>
                  <CardTitle className="text-base">{selectedFolderName}</CardTitle>
                  <CardDescription>
                    {activeFolderId === ALL_FOLDER_ID
                      ? "전체 쿼리를 리스트 형식으로 확인하고 관리합니다."
                      : "선택한 폴더의 쿼리를 리스트 형식으로 확인하고 관리합니다."}
                  </CardDescription>
                </div>
                <Badge variant="secondary" className="w-fit">
                  {filteredQueries.length}개 쿼리
                </Badge>
              </div>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="rounded-xl border border-border overflow-hidden bg-secondary/10 p-4 transition-all duration-300">
                <div className="space-y-3">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <h3 className="text-sm font-semibold flex items-center gap-2 text-foreground">
                      <Scale className="w-4 h-4 text-primary" />
                      비교할 쿼리 ({selectedQueryIds.size}/3)
                    </h3>
                    <div className="flex items-center gap-2">
                      <Button
                        variant={isCompareSelectionMode ? "default" : "outline"}
                        size="sm"
                        className="h-7 text-xs"
                        onClick={handleSelectionModeToggle}
                      >
                        {isCompareSelectionMode ? "선택 취소" : "선택 모드"}
                      </Button>
                      {isCompareSelectionMode && (
                        <Button
                          variant="destructive"
                          size="sm"
                          className="h-7 text-xs gap-1"
                          disabled={selectedQueryIds.size === 0 || deletingQueries}
                          onClick={() => requestDeleteQueries(Array.from(selectedQueryIds))}
                        >
                          <Trash2 className="w-3.5 h-3.5" />
                          삭제
                        </Button>
                      )}
                      <Button
                        size="sm"
                        className="h-7 text-xs gap-1"
                        disabled={selectedQueryIds.size < 2}
                        onClick={handleStartCompare}
                      >
                        비교하기
                        <ArrowRight className="w-3.5 h-3.5" />
                      </Button>
                    </div>
                  </div>

                  {isCompareSelectionMode && (
                    <div className="rounded-md border border-primary/30 bg-primary/5 px-3 py-2 text-xs text-muted-foreground">
                      리스트의 체크박스를 눌러 최대 3개까지 빠르게 선택할 수 있습니다.
                    </div>
                  )}

                  {selectedQueryIds.size > 0 ? (
                    <div className="flex flex-wrap gap-2 animate-in fade-in slide-in-from-top-2">
                      {queries
                        .filter((q) => selectedQueryIds.has(q.id))
                        .map((q) => (
                          <Badge
                            key={q.id}
                            variant="secondary"
                            className="pl-2 pr-1 py-1 flex items-center gap-1 bg-background border shadow-sm"
                          >
                            {q.title}
                            <button
                              onClick={() => handleToggleCompare(q.id)}
                              className="ml-1 hover:bg-muted-foreground/20 rounded-full p-0.5 transition-colors"
                            >
                              <X className="w-3 h-3" />
                            </button>
                          </Badge>
                        ))}
                </div>
                  ) : (
                    <div className="flex items-center text-sm text-muted-foreground gap-2 py-1">
                      <Scale className="w-4 h-4 opacity-50" />
                      <span>
                        {isCompareSelectionMode
                          ? "체크박스로 비교 대상을 선택하세요."
                          : "선택 모드 버튼을 눌러 체크박스로 빠르게 선택하세요."}
                      </span>
                    </div>
                  )}
                </div>
              </div>
          {filteredQueries.length > 0 ? (
            <div className="rounded-xl border border-border overflow-hidden">
              <div className="hidden lg:grid grid-cols-[minmax(0,2fr)_minmax(0,1.2fr)_minmax(0,1fr)_130px_110px] gap-3 px-4 py-2 bg-secondary/40 text-[11px] font-medium text-muted-foreground">
                <span>쿼리</span>
                <span>주요 지표</span>
                <span>실행 정보</span>
                <span>폴더</span>
                <span className="text-right">작업</span>
              </div>
              <div className="divide-y divide-border">
                {filteredQueries.map((query) => (
                  <DashboardQueryRow
                    key={query.id}
                    query={query}
                    folderName={folderMap.get(query.folderId || "")?.name || query.category || "기타"}
                    folders={folders}
                    onMoveToFolder={moveQueryToFolder}
                    onRun={handleRunQuery}
                    onTogglePin={togglePin}
                    onDelete={handleDelete}
                    onDuplicate={handleDuplicate}
                    getChartIcon={getChartIcon}
                    selected={selectedQueryIds.has(query.id)}
                    onToggleCompare={handleToggleCompare}
                    selectionMode={isCompareSelectionMode}
                  />
                ))}
              </div>
            </div>
          ) : (
            <div className="text-center py-12">
              <div className="w-12 h-12 rounded-full bg-secondary flex items-center justify-center mx-auto mb-4">
                <Search className="w-6 h-6 text-muted-foreground" />
              </div>
              <p className="text-muted-foreground">폴더 또는 검색 조건에 맞는 쿼리가 없습니다</p>
            </div>
          )}
        </CardContent>
          </Card>
        </>
      )}

      <Dialog
        open={isCompareOpen}
        onOpenChange={(open: boolean) => {
          setIsCompareOpen(open)
          if (!open) {
            setVisibleComparisonIds(new Set())
            setComparisonOrder([])
            setCompareChartSelectionByQuery({})
          }
        }}
      >
        <DialogContent className="!w-screen !max-w-screen h-screen !max-h-screen rounded-none border-0 p-0 gap-0 flex flex-col overflow-hidden">
          <DialogHeader className="px-6 pt-6 pb-4 border-b border-border shrink-0">
            <DialogTitle>상세 비교 분석</DialogTitle>
            <DialogDescription>선택한 항목의 실행 결과를 비교합니다.</DialogDescription>
          </DialogHeader>
          <div className="flex-1 min-h-0 flex flex-col px-6 pb-6 pt-4 overflow-hidden">
            <div className="flex items-center gap-2 mb-3">
              <Badge variant="secondary">{comparisonOrder.length}개 쿼리</Badge>
            </div>

            {comparisonOrder.length > 0 ? (
              <>
                <div className="flex items-center gap-2 p-2 bg-secondary/10 border border-border rounded-lg overflow-x-auto no-scrollbar">
                  {comparisonOrder
                    .map((id) => queries.find((q) => q.id === id))
                    .filter((q): q is SavedQuery => !!q)
                    .map((q) => {
                      const result = comparisonResults[q.id] || { loading: false }
                      const isVisible = visibleComparisonIds.has(q.id)
                      return (
                        <button
                          key={q.id}
                          draggable
                          onDragStart={(e) => {
                            e.dataTransfer.setData("text/plain", q.id)
                          }}
                          onDragOver={(e) => e.preventDefault()}
                          onDrop={(e) => {
                            e.preventDefault()
                            const draggedId = e.dataTransfer.getData("text/plain")
                            if (draggedId === q.id) return

                            const newOrder = [...comparisonOrder]
                            const fromIndex = newOrder.indexOf(draggedId)
                            const toIndex = newOrder.indexOf(q.id)

                            if (fromIndex !== -1 && toIndex !== -1) {
                              newOrder.splice(fromIndex, 1)
                              newOrder.splice(toIndex, 0, draggedId)
                              setComparisonOrder(newOrder)
                            }
                          }}
                          onClick={() => {
                            const next = new Set(visibleComparisonIds)
                            if (next.has(q.id)) next.delete(q.id)
                            else next.add(q.id)
                            setVisibleComparisonIds(next)
                          }}
                          className={cn(
                            "flex items-center gap-2 px-3 py-1.5 rounded-full text-xs font-medium transition-all whitespace-nowrap border shrink-0 cursor-move active:cursor-grabbing",
                            isVisible
                              ? "bg-primary text-primary-foreground border-primary shadow-sm"
                              : "bg-background text-muted-foreground border-border hover:bg-secondary"
                          )}
                        >
                          <span>{q.title}</span>
                          {result.loading ? (
                            <Activity className="w-3 h-3 animate-spin ml-1" />
                          ) : result.error ? (
                            <div className="w-1.5 h-1.5 rounded-full bg-red-400 ml-1" />
                          ) : result.data ? (
                            <div className="w-1.5 h-1.5 rounded-full bg-green-400 ml-1" />
                          ) : null}
                        </button>
                      )
                    })}
                </div>

                <div className="flex-1 min-h-0 mt-4 overflow-y-auto pr-1">
                  <div
                    className={cn(
                      "grid gap-4 overflow-x-auto",
                      visibleComparisonIds.size === 1 && "grid-cols-1",
                      visibleComparisonIds.size === 2 && "grid-cols-1 lg:grid-cols-2",
                      visibleComparisonIds.size >= 3 && "grid-cols-1 lg:grid-cols-2 xl:grid-cols-3"
                    )}
                  >
                    {comparisonOrder
                      .filter((id) => visibleComparisonIds.has(id))
                      .map((id) => queries.find((q) => q.id === id))
                      .filter((q): q is SavedQuery => !!q)
                      .map((q) => {
                      const result = comparisonResults[q.id] || { loading: false }
                      const records = result.data ? result.data.records : []
                      const normalizedRecords = Array.isArray(records) ? records : []
                      const visibleRecords = normalizedRecords.slice(0, 10)
                      const columns = result.data ? result.data.columns : []
                      const recommendedCharts = getRecommendedChartsForQuery(q, "compare")
                      const selectedRecommendedId = compareChartSelectionByQuery[q.id]
                      const selectedRecommendedChart =
                        recommendedCharts.find((item) => item.id === selectedRecommendedId) ||
                        recommendedCharts[0] ||
                        null
                      const selectedRecommendedImage =
                        selectedRecommendedChart?.pngUrl || selectedRecommendedChart?.thumbnailUrl || ""
                      const compareChartTitle = `${q.title} · ${selectedRecommendedChart?.type || q.chartType}`
                      const storedRecommendedFigure = buildFigureFromStoredChart(selectedRecommendedChart)
                      const chartFigure = selectedRecommendedChart ? storedRecommendedFigure : null
                      const compareSummary = String(q.llmSummary || q.insight || q.description || "").trim() || "요약 정보가 없습니다."
                      const compareStatsRows = buildStatsRowsFromQuery(q)
                      const compareCohortText = formatCohortSourceText(q)
                      const compareLibraryUsed = isLibraryUsed(q)
                      const comparePdfAnalysis = q.pdfAnalysis
                      const comparePdfVariables = comparePdfAnalysis?.variables || []
                      const comparePdfInclusions = comparePdfAnalysis?.inclusionExclusion || []
                      const compareAnalyzedAt = (() => {
                        const value = comparePdfAnalysis?.analyzedAt
                        if (!value) return null
                        const date = new Date(value)
                        return Number.isNaN(date.getTime()) ? value : date.toLocaleString("ko-KR")
                      })()

                      return (
                        <Card key={q.id} className="border border-border/60 shadow-md overflow-hidden flex flex-col h-full min-h-[500px]">
                          <CardHeader className="bg-secondary/20 py-3 px-4 flex flex-row items-center justify-between shrink-0">
                            <div className="flex items-center gap-2 min-w-0">
                              <Badge variant="outline" className="shrink-0">{q.category}</Badge>
                              <Badge variant="outline" className="shrink-0 text-[10px]">
                                {cohortSourceBadgeText(q)}
                              </Badge>
                              {compareLibraryUsed && (
                                <Badge variant="secondary" className="shrink-0 text-[10px]">
                                  라이브러리 사용
                                </Badge>
                              )}
                              <span className="font-semibold text-sm truncate" title={q.title}>{q.title}</span>
                            </div>
                            <div className="flex items-center gap-2 shrink-0">
                              {result.loading && <span className="text-xs text-muted-foreground animate-pulse">실행 중...</span>}
                              {result.error && <span className="text-xs text-destructive">{result.error}</span>}
                              {result.data && <span className="text-xs text-green-600 flex items-center"><Check className="w-3 h-3 mr-1" /> 완료</span>}
                            </div>
                          </CardHeader>
                          <CardContent className="p-0 flex flex-col flex-1 min-h-0">
                            {result.loading ? (
                              <div className="flex-1 flex items-center justify-center">
                                <div className="flex flex-col items-center gap-2 text-muted-foreground">
                                  <Activity className="w-6 h-6 animate-spin" />
                                  <span className="text-xs">데이터 불러오는 중...</span>
                                </div>
                              </div>
                            ) : result.error ? (
                              <div className="flex-1 flex items-center justify-center text-destructive text-sm opacity-80 p-4 text-center">
                                데이터를 불러오지 못했습니다. <br /> {result.error}
                              </div>
                            ) : result.data ? (
                              <div className="flex flex-col h-full min-h-0 divide-y divide-border">
                                <div className="h-[380px] md:h-[420px] p-2 bg-white flex flex-col gap-2">
                                  {recommendedCharts.length > 0 && (
                                    <div className="flex flex-wrap gap-1.5">
                                      {recommendedCharts.map((chart, index) => {
                                        const selected = selectedRecommendedChart?.id === chart.id
                                        return (
                                          <Button
                                            key={chart.id}
                                            size="sm"
                                            variant={selected ? "default" : "outline"}
                                            className="h-6 px-2 text-[10px]"
                                            onClick={() =>
                                              setCompareChartSelectionByQuery((prev) => ({
                                                ...prev,
                                                [q.id]: chart.id,
                                              }))
                                            }
                                          >
                                            추천 {index + 1} · {chart.type}
                                          </Button>
                                        )
                                      })}
                                    </div>
                                  )}
                                  <div className="flex-1 min-h-0 relative">
                                    {(selectedRecommendedImage || chartFigure) && (
                                      <Button
                                        type="button"
                                        size="icon"
                                        variant="secondary"
                                        className="absolute top-1.5 right-1.5 z-10 h-7 w-7 shadow-sm"
                                        onClick={() => openPopupChart(compareChartTitle, selectedRecommendedImage, chartFigure || undefined)}
                                        aria-label="차트 확대 보기"
                                      >
                                        <Maximize2 className="w-3.5 h-3.5" />
                                      </Button>
                                    )}
                                    {selectedRecommendedImage ? (
                                      <button
                                        type="button"
                                        className="w-full h-full cursor-zoom-in rounded border border-border/60 overflow-hidden"
                                        onClick={() => openPopupChart(compareChartTitle, selectedRecommendedImage)}
                                        aria-label={`${q.title} 추천 차트 확대`}
                                      >
                                        <img
                                          src={selectedRecommendedImage}
                                          alt={`${q.title} 추천 차트`}
                                          className="w-full h-full object-contain"
                                        />
                                      </button>
                                    ) : chartFigure ? (
                                      <div
                                        role="button"
                                        tabIndex={0}
                                        className="h-full rounded border border-border/60 p-0.5 cursor-zoom-in"
                                        onClick={() => openPopupChart(compareChartTitle, "", chartFigure || undefined)}
                                        onKeyDown={(event) => {
                                          if (event.key === "Enter" || event.key === " ") {
                                            event.preventDefault()
                                            openPopupChart(compareChartTitle, "", chartFigure || undefined)
                                          }
                                        }}
                                      >
                                        <Plot
                                          data={Array.isArray(chartFigure.data) ? chartFigure.data : []}
                                          layout={enhancePopupPlotLayout(
                                            chartFigure.layout || { autosize: true, margin: { l: 30, r: 10, t: 10, b: 30 } }
                                          )}
                                          config={{ responsive: true, displayModeBar: false }}
                                          style={{ width: "100%", height: "100%" }}
                                          useResizeHandler
                                        />
                                      </div>
                                    ) : selectedRecommendedChart ? (
                                      <div className="h-full flex items-center justify-center text-xs text-muted-foreground text-center px-3">
                                        저장 당시 추천 시각화 이미지/figure가 없습니다.
                                      </div>
                                    ) : (
                                      <div className="h-full flex items-center justify-center text-xs text-muted-foreground">
                                        시각화 가능한 데이터가 없습니다.
                                      </div>
                                    )}
                                  </div>
                                </div>

                                <div className="bg-secondary/10 border-b border-border">
                                  <details className="group">
                                    <summary className="flex items-center gap-2 p-2 cursor-pointer text-[10px] font-semibold text-muted-foreground hover:text-foreground transition-colors select-none">
                                      <ChevronRight className="w-3 h-3 transition-transform group-open:rotate-90" />
                                      SQL 보기
                                    </summary>
                                    <div className="px-2 pb-2">
                                      <pre className="text-[10px] bg-secondary/50 p-2 rounded overflow-x-auto font-mono text-muted-foreground max-h-[100px]">
                                        {q.query}
                                      </pre>
                                    </div>
                                  </details>
                                </div>

                                <div className="bg-secondary/10 border-b border-border">
                                  <details className="group">
                                    <summary className="flex items-center gap-2 p-2 cursor-pointer text-[10px] font-semibold text-muted-foreground hover:text-foreground transition-colors select-none">
                                      <ChevronRight className="w-3 h-3 transition-transform group-open:rotate-90" />
                                      요약 보기
                                    </summary>
                                    <div className="px-2 pb-2">
                                      <div className="text-xs rounded bg-secondary/50 p-2 whitespace-pre-wrap leading-relaxed text-muted-foreground">
                                        {compareSummary}
                                      </div>
                                    </div>
                                  </details>
                                </div>

                                <div className="bg-secondary/10 border-b border-border">
                                  <details className="group">
                                    <summary className="flex items-center gap-2 p-2 cursor-pointer text-[10px] font-semibold text-muted-foreground hover:text-foreground transition-colors select-none">
                                      <ChevronRight className="w-3 h-3 transition-transform group-open:rotate-90" />
                                      코호트/PDF 분석 보기
                                    </summary>
                                    <div className="px-2 pb-2 space-y-2">
                                      <div className="rounded border border-border/60 bg-secondary/40 p-2 text-[11px] text-muted-foreground">
                                        {compareCohortText}
                                      </div>
                                      {comparePdfAnalysis ? (
                                        <>
                                          {compareAnalyzedAt && (
                                            <div className="text-[11px] text-muted-foreground">
                                              분석 시각: {compareAnalyzedAt}
                                            </div>
                                          )}
                                          <div className="rounded border border-border/60 bg-secondary/30 p-2 text-[11px] whitespace-pre-wrap leading-relaxed">
                                            {comparePdfAnalysis.summaryKo || "저장된 논문 요약이 없습니다."}
                                          </div>
                                          <div className="rounded border border-border/60 bg-secondary/30 p-2 text-[11px] whitespace-pre-wrap leading-relaxed">
                                            {comparePdfAnalysis.criteriaSummaryKo || "저장된 코호트 기준 요약이 없습니다."}
                                          </div>
                                          {comparePdfVariables.length > 0 && (
                                            <div className="flex flex-wrap gap-1">
                                              {comparePdfVariables.map((item) => (
                                                <Badge key={item} variant="outline" className="text-[10px]">
                                                  {item}
                                                </Badge>
                                              ))}
                                            </div>
                                          )}
                                          {comparePdfInclusions.length > 0 && (
                                            <div className="max-h-32 overflow-auto rounded border border-border/60">
                                              <table className="w-full text-[10px]">
                                                <thead className="bg-secondary/40">
                                                  <tr>
                                                    <th className="text-left p-1.5 font-medium">항목</th>
                                                    <th className="text-left p-1.5 font-medium">정의</th>
                                                  </tr>
                                                </thead>
                                                <tbody>
                                                  {comparePdfInclusions.map((item) => (
                                                    <tr key={item.id} className="border-t border-border/60 align-top">
                                                      <td className="p-1.5 whitespace-nowrap">{item.title}</td>
                                                      <td className="p-1.5 whitespace-pre-wrap break-words">{item.operationalDefinition}</td>
                                                    </tr>
                                                  ))}
                                                </tbody>
                                              </table>
                                            </div>
                                          )}
                                        </>
                                      ) : (
                                        <div className="text-[10px] rounded border border-dashed border-border p-2 text-muted-foreground">
                                          저장된 PDF 분석 데이터가 없습니다.
                                        </div>
                                      )}
                                    </div>
                                  </details>
                                </div>

                                <div className="bg-secondary/10 border-b border-border">
                                  <details className="group">
                                    <summary className="flex items-center gap-2 p-2 cursor-pointer text-[10px] font-semibold text-muted-foreground hover:text-foreground transition-colors select-none">
                                      <ChevronRight className="w-3 h-3 transition-transform group-open:rotate-90" />
                                      통계 보기
                                    </summary>
                                    <div className="px-2 pb-2">
                                      {compareStatsRows.length ? (
                                        <div className="rounded border border-border overflow-auto max-h-[180px]">
                                          <table className="w-full text-[10px]">
                                            <thead className="bg-secondary/40">
                                              <tr>
                                                <th className="text-left p-1.5 font-medium">컬럼</th>
                                                <th className="text-right p-1.5 font-medium">N</th>
                                                <th className="text-right p-1.5 font-medium">결측치</th>
                                                <th className="text-right p-1.5 font-medium">NULL</th>
                                                <th className="text-right p-1.5 font-medium">MIN</th>
                                                <th className="text-right p-1.5 font-medium">Q1</th>
                                                <th className="text-right p-1.5 font-medium">중앙값</th>
                                                <th className="text-right p-1.5 font-medium">Q3</th>
                                                <th className="text-right p-1.5 font-medium">MAX</th>
                                                <th className="text-right p-1.5 font-medium">평균</th>
                                              </tr>
                                            </thead>
                                            <tbody>
                                              {compareStatsRows.map((row) => (
                                                <tr key={row.column} className="border-t border-border/60">
                                                  <td className="p-1.5">{row.column}</td>
                                                  <td className="p-1.5 text-right">{row.n.toLocaleString()}</td>
                                                  <td className="p-1.5 text-right">{row.missingCount.toLocaleString()}</td>
                                                  <td className="p-1.5 text-right">{row.nullCount.toLocaleString()}</td>
                                                  <td className="p-1.5 text-right">{formatStatNumber(row.min)}</td>
                                                  <td className="p-1.5 text-right">{formatStatNumber(row.q1)}</td>
                                                  <td className="p-1.5 text-right">{formatStatNumber(row.median)}</td>
                                                  <td className="p-1.5 text-right">{formatStatNumber(row.q3)}</td>
                                                  <td className="p-1.5 text-right">{formatStatNumber(row.max)}</td>
                                                  <td className="p-1.5 text-right">{formatStatNumber(row.avg)}</td>
                                                </tr>
                                              ))}
                                            </tbody>
                                          </table>
                                        </div>
                                      ) : (
                                        <div className="text-[10px] rounded border border-dashed border-border p-2 text-muted-foreground">
                                          통계 정보가 없습니다.
                                        </div>
                                      )}
                                    </div>
                                  </details>
                                </div>

                                <div className="flex-1 min-h-0 overflow-auto bg-white relative border-t border-border/50">
                                  <div className="border-b border-border/60 bg-secondary/30 px-2 py-1 text-[10px] text-muted-foreground">
                                    {`쿼리 결과 상위 ${visibleRecords.length.toLocaleString()}개 표시 / 전체 ${normalizedRecords.length.toLocaleString()}개`}
                                  </div>
                                  <table className="w-full text-xs text-left border-separate border-spacing-0">
                                    <thead className="bg-white">
                                      <tr>
                                        {columns.map((col: string) => (
                                          <th
                                            key={col}
                                            className="sticky top-0 z-30 bg-white p-2 font-medium border-b border-border whitespace-nowrap text-muted-foreground shadow-[inset_0_-1px_0_hsl(var(--border))]"
                                          >
                                            {col}
                                          </th>
                                        ))}
                                      </tr>
                                    </thead>
                                    <tbody>
                                      {visibleRecords.map((row: any, idx: number) => (
                                        <tr key={idx} className="hover:bg-secondary/5">
                                          {columns.map((col: string) => (
                                            <td key={`${idx}-${col}`} className="p-2 truncate max-w-[120px] border-b border-border/50">
                                              {String(row[col])}
                                            </td>
                                          ))}
                                        </tr>
                                      ))}
                                    </tbody>
                                  </table>
                                </div>
                              </div>
                            ) : (
                              <div className="flex-1 flex items-center justify-center text-sm text-muted-foreground">
                                실행 대기 중
                              </div>
                            )}
                          </CardContent>
                        </Card>
                      )
                    })}
                  </div>
                </div>
              </>
            ) : (
              <div className="flex-1 flex items-center justify-center text-sm text-muted-foreground">
                비교할 쿼리를 먼저 선택해주세요.
              </div>
            )}
          </div>
        </DialogContent>
      </Dialog>

      <Dialog open={isFolderDialogOpen} onOpenChange={(open: boolean) => {
        setIsFolderDialogOpen(open)
        if (!open) setOpenedFolderId(null)
      }}>
        <DialogContent className="!w-screen !max-w-screen h-screen !max-h-screen rounded-none border-0 p-0 gap-0 flex flex-col overflow-hidden">
          <DialogHeader className="px-6 pt-6 pb-4 border-b border-border shrink-0">
            <DialogTitle>{openedFolderName || "폴더"}</DialogTitle>
            <DialogDescription>폴더 안 쿼리를 한눈에 확인합니다.</DialogDescription>
          </DialogHeader>
          <div className="flex-1 min-h-0 flex flex-col px-6 pb-6 pt-4 overflow-hidden">
            <div className="flex items-center justify-between mb-3">
              <Badge variant="secondary">
                {dialogQueries.length}개 쿼리
              </Badge>
            </div>
            {dialogQueries.length > 0 && (
              <div className="mb-3 flex gap-2 overflow-x-auto pb-1">
                {dialogQueries.map((q) => (
                  <Button
                    key={q.id}
                    variant={q.id === dialogQueryId ? "default" : "outline"}
                    size="sm"
                    className="h-7 px-2 text-[11px] whitespace-nowrap"
                    onClick={() => setDialogQueryId(q.id)}
                  >
                    {formatDashboardTitle(q)}
                  </Button>
                ))}
              </div>
            )}
            <div className="flex-1 min-h-0 rounded-xl border border-border bg-card/60 p-4 overflow-y-auto">
              {selectedDialogQuery ? (
                <div className="space-y-4">
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <div className="text-sm font-semibold text-foreground">{selectedDialogQuery.title}</div>
                      <div className="text-xs text-muted-foreground mt-1">{selectedDialogQuery.description}</div>
                    </div>
                    <Button size="sm" className="h-8" onClick={() => handleRunQuery(selectedDialogQuery)}>
                      <Play className="w-3.5 h-3.5 mr-1" />
                      실행
                    </Button>
                  </div>

                  <Card className="border border-border/60">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm">코호트 출처</CardTitle>
                      <CardDescription className="text-xs">PDF/라이브러리 provenance 및 저장된 PDF 분석 정보를 확인합니다.</CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-2">
                      <div className="flex flex-wrap items-center gap-1.5">
                        <Badge variant="outline" className="text-[11px]">
                          {cohortSourceBadgeText(selectedDialogQuery)}
                        </Badge>
                        {selectedDialogLibraryUsed && (
                          <Badge variant="secondary" className="text-[11px]">
                            라이브러리 사용
                          </Badge>
                        )}
                        {selectedDialogPdfAnalysis?.pdfName && (
                          <Badge variant="outline" className="text-[11px] max-w-full truncate">
                            PDF: {selectedDialogPdfAnalysis.pdfName}
                          </Badge>
                        )}
                      </div>
                      <div className="rounded border border-border/60 bg-secondary/20 p-2 text-xs text-muted-foreground">
                        {selectedDialogCohortText}
                      </div>
                      <details className="group rounded border border-border/60 bg-background/70">
                        <summary className="flex items-center gap-2 px-2 py-1.5 cursor-pointer text-[11px] font-medium text-muted-foreground hover:text-foreground select-none">
                          <ChevronRight className="w-3 h-3 transition-transform group-open:rotate-90" />
                          PDF LLM 분석 보기
                        </summary>
                        <div className="px-2 pb-2 space-y-2">
                          {selectedDialogPdfAnalysis ? (
                            <>
                              {selectedDialogAnalyzedAt && (
                                <div className="text-[11px] text-muted-foreground">
                                  분석 시각: {selectedDialogAnalyzedAt}
                                </div>
                              )}
                              <div className="text-xs rounded border border-border/60 bg-secondary/10 p-2 whitespace-pre-wrap leading-relaxed">
                                {selectedDialogPdfAnalysis.summaryKo || "저장된 논문 요약이 없습니다."}
                              </div>
                              <div className="text-xs rounded border border-border/60 bg-secondary/10 p-2 whitespace-pre-wrap leading-relaxed">
                                {selectedDialogPdfAnalysis.criteriaSummaryKo || "저장된 코호트 기준 요약이 없습니다."}
                              </div>
                              {selectedDialogPdfVariables.length > 0 && (
                                <div className="flex flex-wrap gap-1">
                                  {selectedDialogPdfVariables.map((item) => (
                                    <Badge key={item} variant="outline" className="text-[10px]">
                                      {item}
                                    </Badge>
                                  ))}
                                </div>
                              )}
                              {selectedDialogPdfInclusions.length > 0 && (
                                <div className="max-h-40 overflow-auto rounded border border-border/60">
                                  <table className="w-full text-[11px]">
                                    <thead className="bg-secondary/30">
                                      <tr>
                                        <th className="text-left p-1.5 font-medium">항목</th>
                                        <th className="text-left p-1.5 font-medium">정의</th>
                                      </tr>
                                    </thead>
                                    <tbody>
                                      {selectedDialogPdfInclusions.map((item) => (
                                        <tr key={item.id} className="border-t border-border/60 align-top">
                                          <td className="p-1.5 whitespace-nowrap">{item.title}</td>
                                          <td className="p-1.5 whitespace-pre-wrap break-words">{item.operationalDefinition}</td>
                                        </tr>
                                      ))}
                                    </tbody>
                                  </table>
                                </div>
                              )}
                            </>
                          ) : (
                            <div className="text-[11px] text-muted-foreground">저장된 PDF 분석 데이터가 없습니다.</div>
                          )}
                        </div>
                      </details>
                    </CardContent>
                  </Card>

                  <Card className="border border-border/60">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm">통계 자료</CardTitle>
                      <CardDescription className="text-xs">
                        컬럼별 N, 결측치, NULL, MIN, Q1, 중앙값, Q3, MAX, 평균
                      </CardDescription>
                    </CardHeader>
                    <CardContent>
                      {selectedDialogStatsRows.length ? (
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
                              {selectedDialogStatsRows.map((row) => (
                                <tr key={row.column} className="border-t border-border hover:bg-secondary/30">
                                  <td className="p-2 font-medium">{row.column}</td>
                                  <td className="p-2 text-right">{row.n.toLocaleString()}</td>
                                  <td className="p-2 text-right">{row.missingCount.toLocaleString()}</td>
                                  <td className="p-2 text-right">{row.nullCount.toLocaleString()}</td>
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
                      ) : (
                        <div className="rounded-lg border border-dashed border-border p-6 text-xs text-muted-foreground">
                          통계 자료를 계산할 수 있는 결과가 없습니다.
                        </div>
                      )}
                    </CardContent>
                  </Card>

                  <Card className="border border-border/60">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm">시각화 차트</CardTitle>
                      <CardDescription className="text-xs">저장된 결과 기반 Plotly 차트</CardDescription>
                    </CardHeader>
                    <CardContent>
                      {selectedDialogRecommendedCharts.length > 0 && selectedDialogQuery && (
                        <div className="mb-3 flex flex-wrap gap-1.5">
                          {selectedDialogRecommendedCharts.map((chart, index) => {
                            const selected = selectedDialogRecommendedChart?.id === chart.id
                            return (
                              <Button
                                key={chart.id}
                                size="sm"
                                variant={selected ? "default" : "outline"}
                                className="h-7 px-2 text-[11px]"
                                onClick={() =>
                                  setDetailChartSelectionByQuery((prev) => ({
                                    ...prev,
                                    [selectedDialogQuery.id]: chart.id,
                                  }))
                                }
                              >
                                추천 {index + 1} · {chart.type}
                              </Button>
                            )
                          })}
                        </div>
                      )}

                      {selectedDialogRecommendedImage ? (
                        <div className="h-[340px] rounded-lg border border-border bg-white overflow-hidden relative">
                          <Button
                            type="button"
                            size="icon"
                            variant="secondary"
                            className="absolute top-2 right-2 z-10 h-7 w-7 shadow-sm"
                            onClick={() =>
                              openPopupChart(
                                selectedDialogChartTitle,
                                selectedDialogRecommendedImage
                              )
                            }
                            aria-label="차트 확대 보기"
                          >
                            <Maximize2 className="w-3.5 h-3.5" />
                          </Button>
                          <button
                            type="button"
                            className="w-full h-full cursor-zoom-in"
                            onClick={() =>
                              openPopupChart(
                                selectedDialogChartTitle,
                                selectedDialogRecommendedImage
                              )
                            }
                            aria-label="추천 차트 확대"
                          >
                            <img
                              src={selectedDialogRecommendedImage}
                              alt="추천 차트"
                              className="w-full h-full object-contain"
                            />
                          </button>
                        </div>
                      ) : selectedDialogStoredFigure ? (
                        <div
                          role="button"
                          tabIndex={0}
                          className="h-[340px] rounded-lg border border-border bg-white p-0.5 cursor-zoom-in relative"
                          onClick={() =>
                            openPopupChart(
                              selectedDialogChartTitle,
                              "",
                              selectedDialogStoredFigure
                            )
                          }
                          onKeyDown={(event) => {
                            if (event.key === "Enter" || event.key === " ") {
                              event.preventDefault()
                              openPopupChart(
                                selectedDialogChartTitle,
                                "",
                                selectedDialogStoredFigure
                              )
                            }
                          }}
                        >
                          <Button
                            type="button"
                            size="icon"
                            variant="secondary"
                            className="absolute top-2 right-2 z-10 h-7 w-7 shadow-sm"
                            onClick={(event) => {
                              event.stopPropagation()
                              openPopupChart(
                                selectedDialogChartTitle,
                                "",
                                selectedDialogStoredFigure
                              )
                            }}
                            aria-label="차트 확대 보기"
                          >
                            <Maximize2 className="w-3.5 h-3.5" />
                          </Button>
                          <Plot
                            data={Array.isArray(selectedDialogStoredFigure.data) ? selectedDialogStoredFigure.data : []}
                            layout={enhancePopupPlotLayout(selectedDialogStoredFigure.layout || {})}
                            config={{ responsive: true, displaylogo: false, editable: true }}
                            style={{ width: "100%", height: "100%" }}
                            useResizeHandler
                          />
                        </div>
                      ) : selectedDialogRecommendedChart ? (
                        <div className="rounded-lg border border-dashed border-border p-6 text-xs text-muted-foreground">
                          저장 당시 추천 시각화 이미지/figure가 없습니다.
                        </div>
                      ) : (
                        <div className="rounded-lg border border-dashed border-border p-6 text-xs text-muted-foreground">
                          표시할 시각화 데이터가 없습니다.
                        </div>
                      )}
                    </CardContent>
                  </Card>

                  <Card className="border border-border/60">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-sm">해석</CardTitle>
                      <CardDescription className="text-xs">저장된 해석 원문</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <div className="rounded-lg border border-border/60 bg-secondary/20 p-3 text-sm leading-relaxed whitespace-pre-wrap">
                        {selectedDialogInsight}
                      </div>
                    </CardContent>
                  </Card>
                </div>
              ) : (
                <div className="h-full flex items-center justify-center text-sm text-muted-foreground">
                  쿼리를 선택하면 상세가 표시됩니다.
                </div>
              )}
            </div>
          </div>
        </DialogContent>
      </Dialog>

      <Dialog
        open={Boolean(popupChartPayload)}
        onOpenChange={(open: boolean) => {
          if (!open) setPopupChartPayload(null)
        }}
      >
        <DialogContent className="!w-[96vw] !max-w-[96vw] h-[92vh] p-0 gap-0 flex flex-col overflow-hidden">
          <DialogHeader className="px-6 pt-6 pb-4 border-b border-border shrink-0">
            <DialogTitle>{popupChartPayload?.title || "차트 확대 보기"}</DialogTitle>
            <DialogDescription>선택한 시각화를 크게 확인할 수 있습니다.</DialogDescription>
          </DialogHeader>
          <div className="flex-1 min-h-0 p-4 bg-card/30">
            {popupChartPayload?.imageUrl ? (
              <div className="w-full h-full rounded-lg border border-border bg-white overflow-hidden">
                <img
                  src={popupChartPayload.imageUrl}
                  alt={popupChartPayload.title}
                  className="w-full h-full object-contain"
                />
              </div>
            ) : popupChartPayload?.figure ? (
              <div className="w-full h-full rounded-lg border border-border bg-white p-2">
                <Plot
                  data={Array.isArray(popupChartPayload.figure.data) ? popupChartPayload.figure.data : []}
                  layout={enhancePopupPlotLayout(popupChartPayload.figure.layout || {})}
                  config={{ responsive: true, displayModeBar: true, displaylogo: false }}
                  style={{ width: "100%", height: "100%" }}
                  useResizeHandler
                />
              </div>
            ) : (
              <div className="w-full h-full flex items-center justify-center text-sm text-muted-foreground">
                확대할 시각화가 없습니다.
              </div>
            )}
          </div>
        </DialogContent>
      </Dialog>

      <Dialog
        open={isDeleteQueriesOpen}
        onOpenChange={(open: boolean) => {
          if (deletingQueries) return
          setIsDeleteQueriesOpen(open)
          if (!open) {
            setDeleteQueryIds([])
          }
        }}
      >
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>쿼리 삭제</DialogTitle>
            <DialogDescription>
              {deleteQueryIds.length > 0
                ? `선택한 쿼리를 삭제할까요? 총 ${deleteQueryIds.length}개 쿼리가 폴더에서 제거되며 복구할 수 없습니다.`
                : "선택한 쿼리를 삭제할까요?"}
            </DialogDescription>
          </DialogHeader>
          <div className="flex items-center justify-end gap-2">
            <Button
              variant="ghost"
              disabled={deletingQueries}
              onClick={() => {
                setIsDeleteQueriesOpen(false)
                setDeleteQueryIds([])
              }}
            >
              취소
            </Button>
            <Button variant="destructive" disabled={deletingQueries} onClick={confirmDeleteQueries}>
              삭제
            </Button>
          </div>
        </DialogContent>
      </Dialog>

      <Dialog open={isCreateFolderOpen} onOpenChange={(open: boolean) => {
        setIsCreateFolderOpen(open)
        if (!open) {
          setCreateFolderName("")
        }
      }}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>새 폴더 생성</DialogTitle>
            <DialogDescription>새 폴더 이름을 입력하세요.</DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            <Input
              placeholder="폴더 이름"
              value={createFolderName}
              onChange={(e) => setCreateFolderName(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  e.preventDefault()
                  handleCreateFolder(createFolderName)
                  setIsCreateFolderOpen(false)
                  setCreateFolderName("")
                }
              }}
            />
            <div className="flex items-center justify-end gap-2">
              <Button variant="ghost" onClick={() => setIsCreateFolderOpen(false)}>
                취소
              </Button>
              <Button
                onClick={() => {
                  handleCreateFolder(createFolderName)
                  setIsCreateFolderOpen(false)
                  setCreateFolderName("")
                }}
              >
                확인
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>

      <Dialog open={isRenameFolderOpen} onOpenChange={(open: boolean) => {
        setIsRenameFolderOpen(open)
        if (!open) {
          setRenameFolderTargetId(null)
          setRenameFolderName("")
        }
      }}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>폴더 이름 변경</DialogTitle>
            <DialogDescription>새 폴더 이름을 입력하세요.</DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            <Input
              placeholder="폴더 이름"
              value={renameFolderName}
              onChange={(e) => setRenameFolderName(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  e.preventDefault()
                  confirmRenameFolder()
                }
              }}
            />
            <div className="flex items-center justify-end gap-2">
              <Button variant="ghost" onClick={() => setIsRenameFolderOpen(false)}>
                취소
              </Button>
              <Button onClick={confirmRenameFolder}>
                확인
              </Button>
            </div>
          </div>
        </DialogContent>
      </Dialog>

      <Dialog open={isDeleteFolderOpen} onOpenChange={(open: boolean) => {
        setIsDeleteFolderOpen(open)
        if (!open) {
          setDeleteFolderTargetId(null)
        }
      }}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>폴더 삭제</DialogTitle>
            <DialogDescription>
              {deleteFolderTargetId
                ? `폴더 "${folders.find((folder) => folder.id === deleteFolderTargetId)?.name || ""}"를 삭제할까요?`
                : "선택한 폴더를 삭제할까요?"}
            </DialogDescription>
          </DialogHeader>
          <div className="flex items-center justify-end gap-2">
            <Button variant="ghost" onClick={() => setIsDeleteFolderOpen(false)}>
              취소
            </Button>
            <Button variant="destructive" onClick={confirmDeleteFolder}>
              삭제
            </Button>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  )
}

interface FolderCardProps {
  folder: FolderCardInfo
  active: boolean
  onSelect: () => void
  onOpen: () => void
  onRename?: () => void
  onDelete?: () => void
}

function FolderCard({ folder, active, onSelect, onOpen, onRename, onDelete }: FolderCardProps) {
  return (
    <Card
      className={cn(
        "relative overflow-hidden rounded-2xl border bg-card aspect-[3.5/1] w-full gap-0 py-0 transform-gpu transition-[transform,box-shadow,border-color,background-color] duration-200 ease-out",
        active
          ? "border-primary/50 bg-primary/5 hover:scale-[1.015] hover:shadow-md"
          : "hover:scale-[1.015] hover:shadow-md"
      )}
    >
      <CardContent className="p-4 h-full flex items-center">
        <button
          type="button"
          className="absolute inset-0 z-10 cursor-pointer"
          aria-label={`${folder.name} 폴더 선택`}
          onClick={onSelect}
          onDoubleClick={onOpen}
        />
        <div className="relative z-20 flex items-start gap-2 w-full pointer-events-none">
          <div className="flex-1 text-left min-w-0 select-none">
            <div className="flex items-center gap-2 min-w-0">
              <div className="w-9 h-9 rounded-lg bg-secondary flex items-center justify-center shrink-0">
                {folder.id === ALL_FOLDER_ID ? (
                  <FolderOpen className="w-4 h-4 text-primary" />
                ) : (
                  <Folder className="w-4 h-4 text-primary" />
                )}
              </div>
              <div className="min-w-0">
                <div className="text-sm font-semibold text-foreground truncate">{folder.name}</div>
                <div className="text-xs text-muted-foreground">{folder.count}개 쿼리</div>
              </div>
            </div>
          </div>
          <div
            className="relative z-20 ml-auto flex items-center gap-2 shrink-0 pointer-events-auto"
          >
            {folder.pinnedCount > 0 && (
              <Badge variant="outline" className="text-[10px] shrink-0">
                <Pin className="w-3 h-3 mr-1" />
                {folder.pinnedCount}
              </Badge>
            )}
            {onRename && (
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant="ghost" size="icon" className="h-8 w-8 shrink-0">
                    <MoreHorizontal className="w-4 h-4" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end">
                  <DropdownMenuItem onClick={onRename}>
                    <Pencil className="w-4 h-4 mr-2" />
                    이름 변경
                  </DropdownMenuItem>
                  {onDelete && (
                    <>
                      <DropdownMenuSeparator />
                      <DropdownMenuItem className="text-destructive" onClick={onDelete}>
                        <Trash2 className="w-4 h-4 mr-2" />
                        폴더 삭제
                      </DropdownMenuItem>
                    </>
                  )}
                </DropdownMenuContent>
              </DropdownMenu>
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

interface DashboardQueryRowProps {
  query: SavedQuery
  folderName: string
  folders: SavedFolder[]
  onMoveToFolder: (queryId: string, folderId: string) => void
  onRun: (query: SavedQuery) => void
  onTogglePin: (id: string) => void
  onDelete: (id: string) => void
  onDuplicate: (id: string) => void
  getChartIcon: (type: string) => ReactNode
  selected: boolean
  onToggleCompare: (id: string) => void
  selectionMode: boolean
}

function DashboardQueryRow({
  query,
  folderName,
  folders,
  onMoveToFolder,
  onRun,
  onTogglePin,
  onDelete,
  onDuplicate,
  getChartIcon,
  selected,
  onToggleCompare,
  selectionMode,
}: DashboardQueryRowProps) {
  const displayTitle = formatDashboardTitle(query)
  const displayLastRun = formatDashboardLastRun(query.lastRun)

  return (
    <div
      className={cn(
        "grid grid-cols-1 lg:grid-cols-[minmax(0,2fr)_minmax(0,1.2fr)_minmax(0,1fr)_130px_110px] gap-3 px-4 py-3 hover:bg-secondary/20 transition-colors",
        selected && "bg-primary/5 hover:bg-primary/10 -ml-[2px] border-l-2 border-primary"
      )}
    >
      <div className="min-w-0">
        <div className="flex items-center gap-2 min-w-0">
          {selectionMode && (
            <Checkbox
              checked={selected}
              onCheckedChange={() => onToggleCompare(query.id)}
              aria-label={`${displayTitle} 비교 선택`}
              className="shrink-0"
            />
          )}
          <div className="w-8 h-8 rounded-lg bg-secondary flex items-center justify-center shrink-0">
            {getChartIcon(query.chartType)}
          </div>
          <div className="min-w-0">
            <div className="text-sm font-medium text-foreground truncate">{displayTitle}</div>
            <div className="text-xs text-muted-foreground line-clamp-1">{query.description}</div>
          </div>
          {query.isPinned && <Pin className="w-3.5 h-3.5 text-primary shrink-0" />}
        </div>
      </div>

      <div className="flex flex-wrap gap-1">
        {query.metrics.slice(0, 3).map((metric, idx) => (
          <Badge key={idx} variant="secondary" className="text-[10px] font-normal">
            {formatDashboardMetricLabel(metric.label, idx)}: {metric.value}
          </Badge>
        ))}
      </div>

      <div className="text-xs text-muted-foreground space-y-1">
        <div className="flex items-center gap-1">
          <Clock className="w-3 h-3" />
          <span>{displayLastRun}</span>
        </div>
        {query.schedule && (
          <div className="flex items-center gap-1">
            <Calendar className="w-3 h-3" />
            <span>{query.schedule}</span>
          </div>
        )}
      </div>

      <div className="flex items-center">
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="outline" size="sm" className="h-7 px-2 text-[10px] gap-1">
              {folderName}
              <ChevronDown className="w-3 h-3" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start">
            {folders.map((folder) => (
              <DropdownMenuItem key={folder.id} onClick={() => onMoveToFolder(query.id, folder.id)}>
                {query.folderId === folder.id ? (
                  <Check className="w-4 h-4 mr-2 text-primary" />
                ) : (
                  <span className="w-4 h-4 mr-2" />
                )}
                {folder.name}
              </DropdownMenuItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
      </div>

      <div className="flex items-center justify-end gap-1">
        <Button size="sm" variant="ghost" className="h-8 px-2 text-xs" onClick={() => onRun(query)}>
          <Play className="w-3 h-3 mr-1" />
          실행
        </Button>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="ghost" size="icon" className="h-8 w-8">
              <MoreHorizontal className="w-4 h-4" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuItem onClick={() => onTogglePin(query.id)}>
              {query.isPinned ? <StarOff className="w-4 h-4 mr-2" /> : <Star className="w-4 h-4 mr-2" />}
              {query.isPinned ? "고정 해제" : "고정"}
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => onToggleCompare(query.id)}>
              <Scale className="w-4 h-4 mr-2" />
              {selected ? "비교 목록에서 제외" : "비교 목록에 추가"}
            </DropdownMenuItem>
            <DropdownMenuSub>
              <DropdownMenuSubTrigger>폴더 이동</DropdownMenuSubTrigger>
              <DropdownMenuSubContent>
                {folders.map((folder) => (
                  <DropdownMenuItem key={folder.id} onClick={() => onMoveToFolder(query.id, folder.id)}>
                    {query.folderId === folder.id ? (
                      <Check className="w-4 h-4 mr-2 text-primary" />
                    ) : (
                      <span className="w-4 h-4 mr-2" />
                    )}
                    {folder.name}
                  </DropdownMenuItem>
                ))}
              </DropdownMenuSubContent>
            </DropdownMenuSub>
            <DropdownMenuItem onClick={() => onDuplicate(query.id)}>
              <Copy className="w-4 h-4 mr-2" />
              복제
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem className="text-destructive" onClick={() => onDelete(query.id)}>
              <Trash2 className="w-4 h-4 mr-2" />
              삭제
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
    </div>
  )
}
