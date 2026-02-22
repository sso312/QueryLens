"use client"

import { useMemo, useState } from "react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Drawer,
  DrawerContent,
  DrawerDescription,
  DrawerFooter,
  DrawerHeader,
  DrawerTitle,
} from "@/components/ui/drawer"
import { Skeleton } from "@/components/ui/skeleton"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Copy, FileSearch, RefreshCcw } from "lucide-react"
import type { PdfCohortHistoryDetail } from "@/lib/pdf-cohort-history"
import { cn } from "@/lib/utils"

type PdfCohortHistoryDrawerProps = {
  open: boolean
  onOpenChange: (open: boolean) => void
  detail: PdfCohortHistoryDetail | null
  loading: boolean
  error: string | null
  onRetry: () => void
  onApply: (detail: PdfCohortHistoryDetail) => void
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

const DETAIL_BG = "bg-[#eceff3]"
const DETAIL_BG_SOFT = "bg-[#f3f5f8]"
const DETAIL_BORDER = "border-[#d3dae3]"

const SQL_KEYWORDS = new Set([
  "SELECT",
  "FROM",
  "WHERE",
  "JOIN",
  "LEFT",
  "RIGHT",
  "INNER",
  "OUTER",
  "FULL",
  "ON",
  "GROUP",
  "BY",
  "ORDER",
  "LIMIT",
  "FETCH",
  "FIRST",
  "ROWS",
  "ONLY",
  "AND",
  "OR",
  "NOT",
  "AS",
  "CASE",
  "WHEN",
  "THEN",
  "ELSE",
  "END",
  "WITH",
  "UNION",
  "ALL",
  "DISTINCT",
  "COUNT",
  "SUM",
  "AVG",
  "MIN",
  "MAX",
  "IN",
  "IS",
  "NULL",
  "LIKE",
  "BETWEEN",
  "OVER",
  "PARTITION",
  "HAVING",
  "EXISTS",
  "CAST",
  "COALESCE",
])

const SQL_TOKEN_REGEX = /('[^']*(?:''[^']*)*'|"(?:[^"]|"")*"|\b\d+(?:\.\d+)?\b|\b[A-Za-z_][A-Za-z0-9_]*\b)/g

const renderHighlightedSql = (sql: string) => {
  const lines = sql.split("\n")
  return lines.map((line, lineIndex) => {
    const commentStart = line.indexOf("--")
    const commentText = commentStart >= 0 ? line.slice(commentStart) : ""
    const codeText = commentStart >= 0 ? line.slice(0, commentStart) : line
    const segments = codeText.split(SQL_TOKEN_REGEX)
    return (
      <span key={`sql-line-${lineIndex}`} className="block whitespace-pre">
        {segments.map((segment, segmentIndex) => {
          if (!segment) return null
          const upper = segment.toUpperCase()
          const key = `sql-token-${lineIndex}-${segmentIndex}`
          if (segment.startsWith("'") || segment.startsWith('"')) {
            return (
              <span key={key} className="text-emerald-700">
                {segment}
              </span>
            )
          }
          if (/^\d+(?:\.\d+)?$/.test(segment)) {
            return (
              <span key={key} className="text-amber-700">
                {segment}
              </span>
            )
          }
          if (SQL_KEYWORDS.has(upper)) {
            return (
              <span key={key} className="text-blue-700 font-semibold">
                {segment}
              </span>
            )
          }
          return <span key={key}>{segment}</span>
        })}
        {commentText ? <span className="text-slate-500">{commentText}</span> : null}
      </span>
    )
  })
}

export function PdfCohortHistoryDrawer({
  open,
  onOpenChange,
  detail,
  loading,
  error,
  onRetry,
  onApply,
  onMoveToQuery,
  onOpenLinkedCohort,
}: PdfCohortHistoryDrawerProps) {
  const [isSqlCopied, setIsSqlCopied] = useState(false)
  const statusText = statusLabelMap[detail?.status || ""] || "완료"
  const statusClassName = statusClassMap[detail?.status || "DONE"] || statusClassMap.DONE
  const generatedSql = String(detail?.sql?.generatedSql || "").trim()
  const isActionDisabled = detail?.status !== "DONE"

  const title = useMemo(() => {
    if (!detail) return "히스토리 상세"
    return detail.paperMeta.paperTitle || detail.paperMeta.fileName || "PDF 산출물"
  }, [detail])

  const handleCopySql = async () => {
    if (!generatedSql) return
    try {
      await navigator.clipboard.writeText(generatedSql)
      setIsSqlCopied(true)
      window.setTimeout(() => setIsSqlCopied(false), 1200)
    } catch {
      setIsSqlCopied(false)
    }
  }

  return (
    <Drawer open={open} onOpenChange={onOpenChange} direction="right">
      <DrawerContent className="data-[vaul-drawer-direction=right]:w-[92vw] data-[vaul-drawer-direction=right]:sm:max-w-[860px]">
        <DrawerHeader className="border-b">
          <DrawerTitle className="text-base font-semibold">{title}</DrawerTitle>
          <DrawerDescription className="flex items-center gap-2">
            {detail && <Badge className={statusClassName}>{statusText}</Badge>}
            {detail?.createdAt ? <span>{new Date(detail.createdAt).toLocaleString("ko-KR")}</span> : <span>상세 정보</span>}
          </DrawerDescription>
        </DrawerHeader>

        <div className="flex-1 overflow-auto px-4 py-3 space-y-3">
          {loading && (
            <div className="space-y-2">
              <Skeleton className="h-6 w-2/3" />
              <Skeleton className="h-20 w-full" />
              <Skeleton className="h-20 w-full" />
              <Skeleton className="h-28 w-full" />
            </div>
          )}
          {!loading && error && (
            <div className="rounded-lg border border-destructive/40 bg-destructive/10 p-3 text-sm text-destructive space-y-2">
              <div>{error}</div>
              <Button type="button" variant="outline" size="sm" onClick={onRetry}>
                <RefreshCcw className="w-3.5 h-3.5 mr-1" />
                재시도
              </Button>
            </div>
          )}
          {!loading && !error && detail && (
            <>
              <section className={cn("rounded-lg border p-3", DETAIL_BG_SOFT, DETAIL_BORDER)}>
                <h4 className="text-sm font-semibold mb-2">논문 메타</h4>
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 text-xs">
                  <div>
                    <div className="text-muted-foreground">파일명</div>
                    <div className="font-medium break-all">{detail.paperMeta.fileName || "-"}</div>
                  </div>
                  <div>
                    <div className="text-muted-foreground">논문 제목</div>
                    <div className="font-medium">{detail.paperMeta.paperTitle || "-"}</div>
                  </div>
                  <div>
                    <div className="text-muted-foreground">저자</div>
                    <div className="font-medium">{detail.paperMeta.authors || "-"}</div>
                  </div>
                  <div>
                    <div className="text-muted-foreground">저널 / 연도</div>
                    <div className="font-medium">
                      {detail.paperMeta.journal || "-"}
                      {detail.paperMeta.year ? ` / ${detail.paperMeta.year}` : ""}
                    </div>
                  </div>
                </div>
              </section>

              <details open className={cn("rounded-lg border p-3", DETAIL_BG, DETAIL_BORDER)}>
                <summary className="cursor-pointer text-sm font-semibold">Inclusion / Exclusion</summary>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mt-2">
                  <div>
                    <div className="text-xs text-muted-foreground mb-1">Inclusion</div>
                    <ul className="text-xs space-y-1 list-disc pl-4">
                      {(detail.pdfExtract.extractedCriteria?.inclusion || []).slice(0, 40).map((item, index) => (
                        <li key={`inc-${index}`}>{item}</li>
                      ))}
                      {!(detail.pdfExtract.extractedCriteria?.inclusion || []).length && <li className="list-none text-muted-foreground">없음</li>}
                    </ul>
                  </div>
                  <div>
                    <div className="text-xs text-muted-foreground mb-1">Exclusion</div>
                    <ul className="text-xs space-y-1 list-disc pl-4">
                      {(detail.pdfExtract.extractedCriteria?.exclusion || []).slice(0, 40).map((item, index) => (
                        <li key={`exc-${index}`}>{item}</li>
                      ))}
                      {!(detail.pdfExtract.extractedCriteria?.exclusion || []).length && <li className="list-none text-muted-foreground">없음</li>}
                    </ul>
                  </div>
                </div>
              </details>

              <details className={cn("rounded-lg border p-3", DETAIL_BG, DETAIL_BORDER)}>
                <summary className="cursor-pointer text-sm font-semibold">
                  변수 매핑 ({detail.mapping.variables.length.toLocaleString()}개)
                </summary>
                <div className={cn("mt-2 max-h-[240px] overflow-auto rounded border", DETAIL_BG_SOFT, DETAIL_BORDER)}>
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="w-[45%]">논문 변수</TableHead>
                        <TableHead className="w-[40%]">매핑 대상</TableHead>
                        <TableHead className="text-right">신뢰도</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {detail.mapping.variables.map((item, index) => (
                        <TableRow key={`${item.raw}-${item.mappedTo || ""}-${index}`}>
                          <TableCell className="text-xs">{item.raw}</TableCell>
                          <TableCell className="text-xs">{item.mappedTo || "-"}</TableCell>
                          <TableCell className="text-xs text-right">
                            {typeof item.confidence === "number" ? item.confidence.toFixed(2) : "-"}
                          </TableCell>
                        </TableRow>
                      ))}
                      {!detail.mapping.variables.length && (
                        <TableRow>
                          <TableCell colSpan={3} className="text-xs text-muted-foreground">
                            매핑 정보가 없습니다.
                          </TableCell>
                        </TableRow>
                      )}
                    </TableBody>
                  </Table>
                </div>
              </details>

              <details className={cn("rounded-lg border p-3", DETAIL_BG, DETAIL_BORDER)} open>
                <summary className="cursor-pointer text-sm font-semibold">생성 SQL</summary>
                <div className="mt-2 space-y-2">
                  <div className="flex items-center justify-between text-xs">
                    <span className="text-muted-foreground">
                      엔진: {detail.sql.engine || "ORACLE"}
                      {detail.sql.lastRun?.rowCount != null ? ` / 결과 행: ${detail.sql.lastRun.rowCount.toLocaleString()}` : ""}
                    </span>
                    <Button type="button" variant="outline" size="sm" onClick={handleCopySql} disabled={!generatedSql}>
                      <Copy className="w-3.5 h-3.5 mr-1" />
                      {isSqlCopied ? "복사됨" : "SQL 복사"}
                    </Button>
                  </div>
                  <pre className={cn("max-h-[220px] overflow-auto rounded border p-3 text-[11px] leading-relaxed", DETAIL_BG_SOFT, DETAIL_BORDER)}>
                    {generatedSql ? (
                      <code className={cn("block rounded border px-3 py-2 text-slate-900", DETAIL_BG, DETAIL_BORDER)}>
                        {renderHighlightedSql(generatedSql)}
                      </code>
                    ) : (
                      "생성된 SQL이 없습니다."
                    )}
                  </pre>
                </div>
              </details>

              <details className={cn("rounded-lg border p-3", DETAIL_BG, DETAIL_BORDER)}>
                <summary className="cursor-pointer text-sm font-semibold">LLM 요약</summary>
                <div className="mt-2 space-y-2">
                  <div className={cn("rounded border p-3 text-xs whitespace-pre-wrap leading-relaxed", DETAIL_BG_SOFT, DETAIL_BORDER)}>
                    {detail.llm.summary || "요약 정보가 없습니다."}
                  </div>
                  {detail.llm.notes && (
                    <div className={cn("rounded border p-3 text-xs whitespace-pre-wrap leading-relaxed text-muted-foreground", DETAIL_BG_SOFT, DETAIL_BORDER)}>
                      {detail.llm.notes}
                    </div>
                  )}
                </div>
              </details>

              {detail.pdfExtract.methodsText && (
                <details className={cn("rounded-lg border p-3", DETAIL_BG, DETAIL_BORDER)}>
                  <summary className="cursor-pointer text-sm font-semibold">추출된 Methods</summary>
                  <pre className={cn("mt-2 max-h-[200px] overflow-auto rounded border p-3 text-[11px] whitespace-pre-wrap leading-relaxed", DETAIL_BG_SOFT, DETAIL_BORDER)}>
                    {detail.pdfExtract.methodsText}
                  </pre>
                </details>
              )}
            </>
          )}
        </div>

        <DrawerFooter className="border-t">
          {detail && (
            <div className="flex flex-wrap items-center gap-2">
              <Button type="button" disabled={isActionDisabled} onClick={() => onApply(detail)}>
                <FileSearch className="w-4 h-4 mr-1.5" />
                이 산출물로 화면 적용
              </Button>
              <Button type="button" variant="secondary" disabled={isActionDisabled} onClick={() => onMoveToQuery(detail)}>
                채팅
              </Button>
              {detail.linkedCohort?.cohortId ? (
                <Button
                  type="button"
                  variant="outline"
                  disabled={isActionDisabled}
                  onClick={() => onOpenLinkedCohort(detail.linkedCohort!.cohortId, detail)}
                >
                  코호트 열기
                </Button>
              ) : null}
            </div>
          )}
        </DrawerFooter>
      </DrawerContent>
    </Drawer>
  )
}
