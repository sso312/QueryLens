export type PdfCohortHistoryStatus = "DONE" | "RUNNING" | "ERROR"

export type PdfCohortHistoryItem = {
  id: string
  createdAt: string
  updatedAt?: string
  fileName?: string
  paperTitle?: string
  authors?: string
  year?: number
  journal?: string
  status: PdfCohortHistoryStatus
  errorMessage?: string
  cohortSaved?: boolean
  linkedCohortId?: string
  criteriaSummary?: string
  mappedVarsCount?: number
  sqlReady?: boolean
}

export type PdfCohortHistoryListResponse = {
  items: PdfCohortHistoryItem[]
  page: number
  pageSize: number
  total: number
}

export type PdfCohortHistoryDetail = {
  id: string
  createdAt: string
  updatedAt?: string
  status: PdfCohortHistoryStatus
  paperMeta: {
    fileName?: string
    paperTitle?: string
    authors?: string
    year?: number
    journal?: string
  }
  pdfExtract: {
    methodsText?: string
    extractedCriteria?: {
      inclusion: string[]
      exclusion: string[]
    }
  }
  mapping: {
    variables: Array<{
      raw: string
      mappedTo?: string
      confidence?: number
    }>
  }
  sql: {
    generatedSql?: string
    engine?: string
    lastRun?: {
      ranAt: string
      rowCount?: number
      ok: boolean
      error?: string
    }
  }
  llm: {
    summary?: string
    notes?: string
  }
  linkedCohort?: {
    cohortId: string
    cohortName: string
  }
  rawData?: Record<string, unknown>
}

export type PdfCohortHistoryListParams = {
  query?: string
  status?: "" | PdfCohortHistoryStatus
  from?: string
  to?: string
  cohortSaved?: "" | "saved" | "unsaved"
  sort?: "newest" | "oldest"
  page?: number
  pageSize?: number
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  !!value && typeof value === "object" && !Array.isArray(value)

const toText = (value: unknown) => String(value ?? "").trim()

const toOptionalText = (value: unknown) => {
  const text = toText(value)
  return text || undefined
}

const toBool = (value: unknown) => {
  if (typeof value === "boolean") return value
  if (typeof value === "number") return value !== 0
  const text = toText(value).toLowerCase()
  if (!text) return false
  return ["true", "1", "y", "yes", "saved"].includes(text)
}

const toOptionalNumber = (value: unknown) => {
  const num = typeof value === "number" ? value : Number(value)
  return Number.isFinite(num) ? num : undefined
}

const normalizeStatus = (value: unknown): PdfCohortHistoryStatus => {
  const text = toText(value).toUpperCase()
  if (text === "RUNNING" || text === "ERROR") return text
  return "DONE"
}

const appendQueryParams = (path: string, params: Record<string, unknown>) => {
  const query = new URLSearchParams()
  for (const [key, rawValue] of Object.entries(params)) {
    if (rawValue == null) continue
    const text = String(rawValue).trim()
    if (!text) continue
    query.set(key, text)
  }
  if (!query.toString()) return path
  const sep = path.includes("?") ? "&" : "?"
  return `${path}${sep}${query.toString()}`
}

const withUser = (path: string, userId: string) => {
  const user = toText(userId)
  if (!user) return path
  return appendQueryParams(path, { user })
}

const parseJsonOrThrow = async (res: Response) => {
  if (!res.ok) {
    const detail = toText(await res.text())
    throw new Error(detail || "PDF 히스토리 요청에 실패했습니다.")
  }
  return res.json()
}

const normalizeHistoryListItem = (raw: unknown): PdfCohortHistoryItem | null => {
  if (!isRecord(raw)) return null
  const id = toText(raw.id)
  if (!id) return null
  return {
    id,
    createdAt: toText(raw.createdAt || raw.created_at),
    updatedAt: toOptionalText(raw.updatedAt || raw.updated_at),
    fileName: toOptionalText(raw.fileName || raw.file_name),
    paperTitle: toOptionalText(raw.paperTitle || raw.paper_title),
    authors: toOptionalText(raw.authors),
    year: toOptionalNumber(raw.year),
    journal: toOptionalText(raw.journal),
    status: normalizeStatus(raw.status),
    errorMessage: toOptionalText(raw.errorMessage || raw.error_message),
    cohortSaved: toBool(raw.cohortSaved || raw.cohort_saved),
    linkedCohortId: toOptionalText(raw.linkedCohortId || raw.linked_cohort_id),
    criteriaSummary: toOptionalText(raw.criteriaSummary || raw.criteria_summary),
    mappedVarsCount: toOptionalNumber(raw.mappedVarsCount || raw.mapped_vars_count),
    sqlReady: toBool(raw.sqlReady || raw.sql_ready),
  }
}

export const fetchPdfCohortHistoryList = async (
  params: PdfCohortHistoryListParams,
  userId: string,
  signal?: AbortSignal
): Promise<PdfCohortHistoryListResponse> => {
  const path = appendQueryParams(withUser("/pdf/history", userId), {
    query: params.query,
    status: params.status,
    from: params.from,
    to: params.to,
    cohortSaved: params.cohortSaved,
    sort: params.sort,
    page: params.page,
    pageSize: params.pageSize,
  })
  const payload = await parseJsonOrThrow(await fetch(path, { signal }))
  const items = Array.isArray(payload?.items)
    ? payload.items.map(normalizeHistoryListItem).filter((item): item is PdfCohortHistoryItem => item !== null)
    : []
  return {
    items,
    page: Number(payload?.page || 1) || 1,
    pageSize: Number(payload?.pageSize || payload?.page_size || params.pageSize || 10) || 10,
    total: Number(payload?.total || 0) || 0,
  }
}

export const fetchPdfCohortHistoryDetail = async (
  id: string,
  userId: string,
  signal?: AbortSignal
): Promise<PdfCohortHistoryDetail> => {
  const path = withUser(`/pdf/history/${encodeURIComponent(id)}`, userId)
  const payload = await parseJsonOrThrow(await fetch(path, { signal }))
  const paperMeta = isRecord(payload?.paperMeta)
    ? payload.paperMeta
    : isRecord(payload?.paper_meta)
      ? payload.paper_meta
      : {}
  const extractedCriteria = isRecord(payload?.pdfExtract?.extractedCriteria)
    ? payload.pdfExtract.extractedCriteria
    : isRecord(payload?.pdfExtract?.extracted_criteria)
      ? payload.pdfExtract.extracted_criteria
      : isRecord(payload?.pdf_extract?.extracted_criteria)
        ? payload.pdf_extract.extracted_criteria
        : {}
  const mappingVariablesRaw = Array.isArray(payload?.mapping?.variables)
    ? payload.mapping.variables
    : Array.isArray(payload?.mapping?.mapped_variables)
      ? payload.mapping.mapped_variables
      : []
  const mappingVariables = mappingVariablesRaw
    .map((item) => {
      if (!isRecord(item)) return null
      const raw = toText(item.raw || item.name || item.variable)
      if (!raw) return null
      return {
        raw,
        mappedTo: toOptionalText(item.mappedTo || item.mapped_to),
        confidence: toOptionalNumber(item.confidence),
      }
    })
    .filter((item): item is NonNullable<typeof item> => item !== null)
  const lastRun = isRecord(payload?.sql?.lastRun)
    ? payload.sql.lastRun
    : isRecord(payload?.sql?.last_run)
      ? payload.sql.last_run
      : {}
  const linkedCohortRaw = isRecord(payload?.linkedCohort)
    ? payload.linkedCohort
    : isRecord(payload?.linked_cohort)
      ? payload.linked_cohort
      : null

  return {
    id: toText(payload?.id),
    createdAt: toText(payload?.createdAt || payload?.created_at),
    updatedAt: toOptionalText(payload?.updatedAt || payload?.updated_at),
    status: normalizeStatus(payload?.status),
    paperMeta: {
      fileName: toOptionalText(paperMeta.fileName || paperMeta.file_name),
      paperTitle: toOptionalText(paperMeta.paperTitle || paperMeta.paper_title),
      authors: toOptionalText(paperMeta.authors),
      year: toOptionalNumber(paperMeta.year),
      journal: toOptionalText(paperMeta.journal),
    },
    pdfExtract: {
      methodsText: toOptionalText(
        payload?.pdfExtract?.methodsText ||
          payload?.pdfExtract?.methods_text ||
          payload?.pdf_extract?.methods_text
      ),
      extractedCriteria: {
        inclusion: Array.isArray(extractedCriteria?.inclusion)
          ? extractedCriteria.inclusion.map((item) => toText(item)).filter(Boolean)
          : [],
        exclusion: Array.isArray(extractedCriteria?.exclusion)
          ? extractedCriteria.exclusion.map((item) => toText(item)).filter(Boolean)
          : [],
      },
    },
    mapping: {
      variables: mappingVariables,
    },
    sql: {
      generatedSql: toOptionalText(payload?.sql?.generatedSql || payload?.sql?.generated_sql),
      engine: toOptionalText(payload?.sql?.engine),
      lastRun: {
        ranAt: toText(lastRun.ranAt || lastRun.ran_at),
        rowCount: toOptionalNumber(lastRun.rowCount || lastRun.row_count),
        ok: toBool(lastRun.ok),
        error: toOptionalText(lastRun.error),
      },
    },
    llm: {
      summary: toOptionalText(payload?.llm?.summary),
      notes: toOptionalText(payload?.llm?.notes),
    },
    linkedCohort: linkedCohortRaw
      ? {
          cohortId: toText(linkedCohortRaw.cohortId || linkedCohortRaw.cohort_id),
          cohortName: toText(linkedCohortRaw.cohortName || linkedCohortRaw.cohort_name),
        }
      : undefined,
    rawData: isRecord(payload?.rawData)
      ? payload.rawData
      : isRecord(payload?.raw_data)
        ? payload.raw_data
        : undefined,
  }
}
