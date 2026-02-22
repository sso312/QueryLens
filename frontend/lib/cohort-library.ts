export type CohortType = "CROSS_SECTIONAL" | "PDF_DERIVED"

export type SavedCohortSource = {
  createdFrom: "CROSS_SECTIONAL_PAGE" | "PDF_ANALYSIS_PAGE" | "IMPORT"
  pdfName?: string
  pdfAnalysisId?: string
}

export type SavedCohortPdfDetail = {
  paperSummary?: string
  inclusionExclusion?: Array<{
    id: string
    title: string
    operationalDefinition: string
    evidence?: string
  }>
  variables?: Array<{
    key: string
    label: string
    table?: string
    mappingId?: string
  }>
}

export type CohortInclusionItem = {
  id: string
  title: string
  operationalDefinition: string
  evidence?: string
}

export type SavedCohort = {
  id: string
  type: CohortType
  name: string
  description?: string
  cohortSql: string
  count?: number
  sqlFilterSummary?: string
  humanSummary?: string
  source: SavedCohortSource
  pdfDetails?: SavedCohortPdfDetail
  createdAt: string
  updatedAt: string
  status?: "active" | "archived"
  params?: Record<string, unknown> | null
  metrics?: Record<string, unknown> | null
}

export type ActiveCohortContext = {
  cohortId: string
  cohortName: string
  type: CohortType
  cohortSql: string
  patientCount: number | null
  sqlFilterSummary: string
  summaryKo: string
  criteriaSummaryKo: string
  variables: string[]
  badgeLabel: string
  source: string
  ts: number
  filename: string
  pdfHash: string
  paperSummary?: string
  inclusionExclusion?: CohortInclusionItem[]
}

export const PENDING_ACTIVE_COHORT_CONTEXT_KEY = "ql_pending_active_cohort_context"
export const LEGACY_PENDING_PDF_COHORT_CONTEXT_KEY = "ql_pending_pdf_cohort_context"

const COHORT_CONTEXT_SQL_LIMIT = 3200

const isRecord = (value: unknown): value is Record<string, unknown> =>
  !!value && typeof value === "object" && !Array.isArray(value)

const readFiniteNumber = (value: unknown): number | null => {
  const num = typeof value === "number" ? value : Number(value)
  return Number.isFinite(num) ? num : null
}

const toStr = (value: unknown): string => String(value ?? "").trim()

export const normalizeCohortType = (value: unknown): CohortType | null => {
  const text = toStr(value).toUpperCase()
  if (text === "CROSS_SECTIONAL" || text === "PDF_DERIVED") {
    return text
  }
  return null
}

const normalizeStringList = (value: unknown, maxCount = 24): string[] => {
  if (!Array.isArray(value)) return []
  const normalized: string[] = []
  for (const item of value) {
    const text = toStr(item)
    if (!text || normalized.includes(text)) continue
    normalized.push(text)
    if (normalized.length >= maxCount) break
  }
  return normalized
}

const normalizeInclusionExclusion = (
  value: unknown
): Array<{
  id: string
  title: string
  operationalDefinition: string
  evidence?: string
}> => {
  if (!Array.isArray(value)) return []
  return value
    .map((item, idx) => {
      if (!isRecord(item)) return null
      const id = toStr(item.id) || `ie-${idx + 1}`
      const title = toStr(item.title) || `조건 ${idx + 1}`
      const operationalDefinition =
        toStr(item.operationalDefinition) || toStr(item.operational_definition) || ""
      const evidence = toStr(item.evidence) || undefined
      if (!operationalDefinition) return null
      return { id, title, operationalDefinition, evidence }
    })
    .filter((item): item is NonNullable<typeof item> => item !== null)
}

const normalizeVariableDetails = (
  value: unknown
): Array<{
  key: string
  label: string
  table?: string
  mappingId?: string
}> => {
  if (!Array.isArray(value)) return []
  return value
    .map((item) => {
      if (!isRecord(item)) return null
      const key = toStr(item.key)
      const label = toStr(item.label)
      if (!key && !label) return null
      return {
        key: key || label,
        label: label || key,
        table: toStr(item.table) || undefined,
        mappingId: toStr(item.mappingId) || toStr(item.mapping_id) || undefined,
      }
    })
    .filter((item): item is NonNullable<typeof item> => item !== null)
}

const normalizeSource = (value: unknown): SavedCohortSource => {
  const fallback: SavedCohortSource = { createdFrom: "IMPORT" }
  if (!isRecord(value)) return fallback
  const createdFromRaw = toStr(value.createdFrom) || toStr(value.created_from)
  const createdFrom =
    createdFromRaw === "CROSS_SECTIONAL_PAGE" ||
    createdFromRaw === "PDF_ANALYSIS_PAGE" ||
    createdFromRaw === "IMPORT"
      ? createdFromRaw
      : "IMPORT"
  const pdfName = toStr(value.pdfName) || toStr(value.pdf_name) || undefined
  const pdfAnalysisId =
    toStr(value.pdfAnalysisId) || toStr(value.pdf_analysis_id) || undefined
  return { createdFrom, pdfName, pdfAnalysisId }
}

const normalizePdfDetails = (value: unknown): SavedCohortPdfDetail | undefined => {
  if (!isRecord(value)) return undefined
  const paperSummary = toStr(value.paperSummary) || toStr(value.paper_summary) || undefined
  const inclusionExclusion = normalizeInclusionExclusion(
    value.inclusionExclusion ?? value.inclusion_exclusion
  )
  const variables = normalizeVariableDetails(value.variables)
  if (!paperSummary && !inclusionExclusion.length && !variables.length) return undefined
  return {
    paperSummary,
    inclusionExclusion: inclusionExclusion.length ? inclusionExclusion : undefined,
    variables: variables.length ? variables : undefined,
  }
}

export const toSavedCohort = (value: unknown): SavedCohort | null => {
  if (!isRecord(value)) return null
  const id = toStr(value.id)
  const type = normalizeCohortType(value.type)
  const name = toStr(value.name)
  if (!id || !type || !name) return null

  const cohortSql =
    toStr(value.cohortSql) || toStr(value.cohort_sql) || toStr(value.sql) || ""
  const count = readFiniteNumber(value.count)
  const createdAt =
    toStr(value.createdAt) || toStr(value.created_at) || new Date().toISOString()
  const updatedAt =
    toStr(value.updatedAt) || toStr(value.updated_at) || createdAt

  const statusText = toStr(value.status).toLowerCase()
  const status =
    statusText === "active" || statusText === "archived"
      ? (statusText as "active" | "archived")
      : undefined

  return {
    id,
    type,
    name,
    description: toStr(value.description) || undefined,
    cohortSql,
    count: count != null ? Math.max(0, Math.round(count)) : undefined,
    sqlFilterSummary: toStr(value.sqlFilterSummary) || toStr(value.sql_filter_summary) || undefined,
    humanSummary: toStr(value.humanSummary) || toStr(value.human_summary) || undefined,
    source: normalizeSource(value.source),
    pdfDetails: normalizePdfDetails(value.pdfDetails ?? value.pdf_details),
    createdAt,
    updatedAt,
    status,
    params: isRecord(value.params) ? value.params : null,
    metrics: isRecord(value.metrics) ? value.metrics : null,
  }
}

const normalizeContextVariables = (value: unknown): string[] => {
  const direct = normalizeStringList(value)
  if (direct.length) return direct
  if (!Array.isArray(value)) return []
  const labels: string[] = []
  for (const item of value) {
    if (!isRecord(item)) continue
    const label = toStr(item.label) || toStr(item.key)
    if (!label || labels.includes(label)) continue
    labels.push(label)
    if (labels.length >= 24) break
  }
  return labels
}

export const toActiveCohortContext = (
  cohort: SavedCohort,
  source = "cohort-library"
): ActiveCohortContext => {
  const variablesFromDetails = normalizeContextVariables(cohort.pdfDetails?.variables)
  const variables =
    variablesFromDetails.length > 0
      ? variablesFromDetails
      : normalizeStringList((cohort as any).variables)

  return {
    cohortId: cohort.id,
    cohortName: cohort.name,
    type: cohort.type,
    cohortSql: (cohort.cohortSql || "").slice(0, COHORT_CONTEXT_SQL_LIMIT),
    patientCount: cohort.count != null ? Math.max(0, Math.round(cohort.count)) : null,
    sqlFilterSummary: cohort.sqlFilterSummary || "",
    summaryKo: cohort.humanSummary || "",
    criteriaSummaryKo: cohort.sqlFilterSummary || "",
    variables,
    badgeLabel: cohort.type === "PDF_DERIVED" ? "PDF 코호트" : "저장 코호트",
    source,
    ts: Date.now(),
    filename: cohort.source.pdfName || cohort.name,
    pdfHash: cohort.source.pdfAnalysisId || cohort.id,
    paperSummary: cohort.pdfDetails?.paperSummary || cohort.humanSummary || undefined,
    inclusionExclusion: cohort.pdfDetails?.inclusionExclusion || undefined,
  }
}

export const normalizeActiveCohortContext = (value: unknown): ActiveCohortContext | null => {
  if (!isRecord(value)) return null

  const savedCandidate = toSavedCohort(value)
  if (savedCandidate) {
    return toActiveCohortContext(savedCandidate)
  }

  const cohortSql = toStr(value.cohortSql) || toStr(value.cohort_sql)
  const summaryKo = toStr(value.summaryKo) || toStr(value.summary_ko)
  const criteriaSummaryKo =
    toStr(value.criteriaSummaryKo) ||
    toStr(value.criteria_summary_ko) ||
    toStr(value.sqlFilterSummary) ||
    toStr(value.sql_filter_summary)
  const variables = normalizeContextVariables(value.variables)
  const inclusionExclusion = normalizeInclusionExclusion(
    value.inclusionExclusion ?? value.inclusion_exclusion
  )

  if (!cohortSql && !summaryKo && !criteriaSummaryKo && !variables.length) {
    return null
  }

  const type = normalizeCohortType(value.type) || "PDF_DERIVED"
  const cohortId = toStr(value.cohortId) || toStr(value.id) || toStr(value.pdfHash)
  const cohortName = toStr(value.cohortName) || toStr(value.name) || "저장 코호트"
  const filename =
    toStr(value.filename) || toStr(value.pdfName) || cohortName
  const patientCountRaw = readFiniteNumber(value.patientCount ?? value.count)
  const tsRaw = readFiniteNumber(value.ts)

  return {
    cohortId: cohortId || filename,
    cohortName,
    type,
    cohortSql: cohortSql.slice(0, COHORT_CONTEXT_SQL_LIMIT),
    patientCount: patientCountRaw != null ? Math.max(0, Math.round(patientCountRaw)) : null,
    sqlFilterSummary: toStr(value.sqlFilterSummary) || toStr(value.sql_filter_summary),
    summaryKo,
    criteriaSummaryKo,
    variables,
    badgeLabel:
      toStr(value.badgeLabel) || (type === "PDF_DERIVED" ? "PDF 코호트" : "저장 코호트"),
    source: toStr(value.source) || "cohort-context",
    ts: tsRaw != null ? tsRaw : Date.now(),
    filename,
    pdfHash: toStr(value.pdfHash) || cohortId || filename,
    paperSummary: toStr(value.paperSummary) || toStr(value.paper_summary) || undefined,
    inclusionExclusion: inclusionExclusion.length ? inclusionExclusion : undefined,
  }
}

export const scopedStorageKey = (baseKey: string, userId: string) => {
  const owner = (userId || "").trim()
  return owner ? `${baseKey}:${owner}` : baseKey
}

export const persistPendingActiveCohortContext = (
  context: ActiveCohortContext,
  userId: string
) => {
  if (typeof window === "undefined") return
  const payload = JSON.stringify(context)
  const scopedKey = scopedStorageKey(PENDING_ACTIVE_COHORT_CONTEXT_KEY, userId)
  window.localStorage.setItem(scopedKey, payload)
  window.localStorage.setItem(PENDING_ACTIVE_COHORT_CONTEXT_KEY, payload)
  if (context.type === "PDF_DERIVED") {
    const legacyScopedKey = scopedStorageKey(LEGACY_PENDING_PDF_COHORT_CONTEXT_KEY, userId)
    window.localStorage.setItem(legacyScopedKey, payload)
    window.localStorage.setItem(LEGACY_PENDING_PDF_COHORT_CONTEXT_KEY, payload)
  }
}

export const clearPendingActiveCohortContext = (userId: string) => {
  if (typeof window === "undefined") return
  window.localStorage.removeItem(scopedStorageKey(PENDING_ACTIVE_COHORT_CONTEXT_KEY, userId))
  window.localStorage.removeItem(PENDING_ACTIVE_COHORT_CONTEXT_KEY)
  window.localStorage.removeItem(scopedStorageKey(LEGACY_PENDING_PDF_COHORT_CONTEXT_KEY, userId))
  window.localStorage.removeItem(LEGACY_PENDING_PDF_COHORT_CONTEXT_KEY)
}

export const cohortTypeLabel = (type: CohortType) =>
  type === "PDF_DERIVED" ? "PDF" : "단면"
