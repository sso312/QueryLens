"use client"

import { useMemo, useState } from "react"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Search, RefreshCw, Play, Trash2, Database } from "lucide-react"
import {
  type SavedCohort,
  type CohortType,
  cohortTypeLabel,
} from "@/lib/cohort-library"
import { cn } from "@/lib/utils"

type CohortFilterType = "ALL" | CohortType

type CohortLibraryDialogProps = {
  open: boolean
  onOpenChange: (open: boolean) => void
  cohorts: SavedCohort[]
  loading?: boolean
  title?: string
  description?: string
  onRefresh?: () => void
  onSelectForQuery: (cohort: SavedCohort) => void
  onDeleteCohort?: (cohort: SavedCohort) => void
}

const formatDateTime = (value: string) => {
  const text = String(value || "").trim()
  if (!text) return "-"
  const parsed = new Date(text)
  if (Number.isNaN(parsed.getTime())) return text
  return parsed.toLocaleString()
}

export function CohortLibraryDialog({
  open,
  onOpenChange,
  cohorts,
  loading = false,
  title = "저장된 코호트 라이브러리",
  description = "이름 또는 PDF 파일명으로 검색하고 쿼리에 바로 적용할 수 있습니다.",
  onRefresh,
  onSelectForQuery,
  onDeleteCohort,
}: CohortLibraryDialogProps) {
  const [search, setSearch] = useState("")
  const [typeFilter, setTypeFilter] = useState<CohortFilterType>("ALL")

  const filtered = useMemo(() => {
    const needle = search.trim().toLowerCase()
    return cohorts
      .filter((item) => (typeFilter === "ALL" ? true : item.type === typeFilter))
      .filter((item) => {
        if (!needle) return true
        const corpus = [
          item.name,
          item.description || "",
          item.source.pdfName || "",
          item.humanSummary || "",
          item.sqlFilterSummary || "",
        ]
          .join(" ")
          .toLowerCase()
        return corpus.includes(needle)
      })
      .sort((a, b) => {
        const aTs = Date.parse(a.updatedAt || a.createdAt || "") || 0
        const bTs = Date.parse(b.updatedAt || b.createdAt || "") || 0
        return bTs - aTs
      })
  }, [cohorts, search, typeFilter])

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-4xl">
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          <DialogDescription>{description}</DialogDescription>
        </DialogHeader>

        <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
          <div className="relative flex-1">
            <Search className="pointer-events-none absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
            <Input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="코호트 이름, PDF 파일명, 요약 검색"
              className="pl-8"
            />
          </div>
          <Select value={typeFilter} onValueChange={(value) => setTypeFilter(value as CohortFilterType)}>
            <SelectTrigger className="w-full sm:w-[180px]">
              <SelectValue placeholder="타입 필터" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="ALL">전체</SelectItem>
              <SelectItem value="CROSS_SECTIONAL">단면</SelectItem>
              <SelectItem value="PDF_DERIVED">PDF</SelectItem>
            </SelectContent>
          </Select>
          <Button
            type="button"
            variant="outline"
            className="gap-1"
            onClick={onRefresh}
            disabled={!onRefresh || loading}
          >
            <RefreshCw className={cn("h-3.5 w-3.5", loading && "animate-spin")} />
            새로고침
          </Button>
        </div>

        <div className="max-h-[55vh] overflow-y-auto rounded-md border border-border bg-muted/20 p-2">
          {loading ? (
            <div className="py-8 text-center text-sm text-muted-foreground">불러오는 중...</div>
          ) : filtered.length === 0 ? (
            <div className="py-8 text-center text-sm text-muted-foreground">저장된 코호트가 없습니다.</div>
          ) : (
            <div className="space-y-2">
              {filtered.map((item) => (
                <div
                  key={item.id}
                  className="rounded-md border border-border bg-background p-3"
                >
                  <div className="mb-2 flex items-start justify-between gap-2">
                    <div className="min-w-0">
                      <div className="flex items-center gap-2">
                        <p className="truncate text-sm font-medium text-foreground">{item.name}</p>
                        <Badge variant="secondary" className="text-[10px]">
                          {cohortTypeLabel(item.type)}
                        </Badge>
                      </div>
                      <p className="text-[11px] text-muted-foreground">
                        수정: {formatDateTime(item.updatedAt || item.createdAt)}
                        {item.source.pdfName ? ` · ${item.source.pdfName}` : ""}
                      </p>
                    </div>
                    <div className="text-right text-xs text-muted-foreground">
                      <div className="inline-flex items-center gap-1">
                        <Database className="h-3 w-3" />
                        {item.count != null ? `${item.count.toLocaleString()}명` : "-"}
                      </div>
                    </div>
                  </div>

                  {(item.humanSummary || item.sqlFilterSummary) && (
                    <p className="mb-2 line-clamp-2 text-[11px] text-muted-foreground">
                      {item.humanSummary || item.sqlFilterSummary}
                    </p>
                  )}

                  <div className="flex items-center gap-1">
                    <Button
                      type="button"
                      size="sm"
                      className="h-7 gap-1 text-xs"
                      onClick={() => onSelectForQuery(item)}
                    >
                      <Play className="h-3 w-3" />
                      쿼리하기
                    </Button>
                    {onDeleteCohort && (
                      <Button
                        type="button"
                        size="sm"
                        variant="ghost"
                        className="h-7 gap-1 text-xs text-destructive"
                        onClick={() => onDeleteCohort(item)}
                      >
                        <Trash2 className="h-3 w-3" />
                        삭제
                      </Button>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}
