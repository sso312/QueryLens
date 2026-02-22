"use client"

import { useEffect, useMemo, useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Textarea } from "@/components/ui/textarea"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { 
  GitBranch, 
  Calculator, 
  BookOpen,
  Plus,
  Pencil,
  Check,
  Trash2,
  ArrowRight,
  Search,
  Table2,
  Hash
} from "lucide-react"

interface JoinTemplate {
  id: string
  name: string
  sql: string
  leftTable?: string
  rightTable?: string
  joinColumn?: string
}

interface MetricTemplate {
  id: string
  name: string
  sql: string
}

interface Term {
  id: string
  term: string
  aliases: string[]
  definition: string
}

export function ContextView() {
  const [joins, setJoins] = useState<JoinTemplate[]>([])
  const [metrics, setMetrics] = useState<MetricTemplate[]>([])
  const [terms, setTerms] = useState<Term[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const [saveMessage, setSaveMessage] = useState<string | null>(null)
  const [editingJoinId, setEditingJoinId] = useState<string | null>(null)
  const [editingMetricId, setEditingMetricId] = useState<string | null>(null)
  const [editingTermId, setEditingTermId] = useState<string | null>(null)

  const loadDocs = async () => {
    setLoading(true)
    setError(null)
    setSaveMessage(null)
    try {
        const [joinRes, sqlRes, glossaryRes] = await Promise.all([
          fetch("/admin/rag/docs?doc_type=template&kind=join&limit=1000"),
          fetch("/admin/rag/docs?doc_type=template&kind=sql&limit=1000"),
          fetch("/admin/rag/docs?doc_type=glossary&limit=1000"),
        ])

      if (!joinRes.ok || !sqlRes.ok || !glossaryRes.ok) {
        throw new Error("Failed to fetch RAG docs.")
      }

      const joinPayload = await joinRes.json()
      const sqlPayload = await sqlRes.json()
      const glossaryPayload = await glossaryRes.json()

      const joinDocs = Array.isArray(joinPayload?.docs) ? joinPayload.docs : []
      const sqlDocs = Array.isArray(sqlPayload?.docs) ? sqlPayload.docs : []
      const glossaryDocs = Array.isArray(glossaryPayload?.docs) ? glossaryPayload.docs : []

      const parsedJoins = joinDocs.map((doc: any, idx: number) => {
        const { name, sql } = parseTemplateDoc(doc)
        const parsed = parseJoinSql(sql)
        return {
          id: doc.id ?? `${name}-${idx}`,
          name: name || `join_template_${idx}`,
          sql,
          leftTable: parsed?.leftTable ?? "",
          rightTable: parsed?.rightTable ?? "",
          joinColumn: parsed?.leftColumn ?? "",
        }
      })

      const parsedMetrics = sqlDocs.map((doc: any, idx: number) => {
        const { name, sql } = parseTemplateDoc(doc)
        return {
          id: doc.id ?? `${name}-${idx}`,
          name: name || `template_${idx}`,
          sql,
        }
      })

      const parsedTerms = glossaryDocs.map((doc: any, idx: number) => {
        const { term, definition } = parseGlossaryDoc(doc)
        return {
          id: doc.id ?? `${term}-${idx}`,
          term: term || `term_${idx}`,
          aliases: [],
          definition: definition || "",
        }
      })

      setJoins(parsedJoins)
      setMetrics(parsedMetrics)
      setTerms(parsedTerms)
      setEditingJoinId(null)
      setEditingMetricId(null)
      setEditingTermId(null)
    } catch (err) {
      setError("RAG 문서를 불러오지 못했습니다.")
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadDocs()
  }, [])

  const tableChain = useMemo(() => {
    const chain: string[] = []
    joins.forEach((join) => {
      const parsed = parseJoinSql(join.sql)
      const leftTable = parsed?.leftTable ?? join.leftTable
      const rightTable = parsed?.rightTable ?? join.rightTable
      if (leftTable && !chain.includes(leftTable)) {
        chain.push(leftTable)
      }
      if (rightTable && !chain.includes(rightTable)) {
        chain.push(rightTable)
      }
    })
    return chain
  }, [joins])

  const knownTables = useMemo(() => {
    const tables = new Set<string>()
    joins.forEach((join) => {
      const parsed = parseJoinSql(join.sql)
      if (parsed?.leftTable) {
        tables.add(parsed.leftTable)
      } else if (join.leftTable) {
        tables.add(join.leftTable)
      }
      if (parsed?.rightTable) {
        tables.add(parsed.rightTable)
      } else if (join.rightTable) {
        tables.add(join.rightTable)
      }
    })
    return Array.from(tables).sort()
  }, [joins])

  const filteredTerms = terms.filter(t => 
    t.term.toLowerCase().includes(searchTerm.toLowerCase()) ||
    t.aliases.some(a => a.toLowerCase().includes(searchTerm.toLowerCase()))
  )

  const addJoin = () => {
    const id = `join-${Date.now()}`
    setJoins(prev => [{ id, name: "join_template_new", sql: "", leftTable: "", rightTable: "", joinColumn: "" }, ...prev])
    setEditingJoinId(id)
  }

  const updateJoinFromFields = (
    joinId: string,
    patch: { leftTable?: string; rightTable?: string; joinColumn?: string }
  ) => {
    setJoins((prev) =>
      prev.map((item) => {
        if (item.id !== joinId) {
          return item
        }

        const parsed = parseJoinSql(item.sql)
        const leftTable = patch.leftTable ?? item.leftTable ?? parsed?.leftTable ?? ""
        const rightTable = patch.rightTable ?? item.rightTable ?? parsed?.rightTable ?? ""
        const joinColumn = patch.joinColumn ?? item.joinColumn ?? parsed?.leftColumn ?? ""
        const joinType = parsed?.joinType ?? "INNER"
        const nextSql = buildJoinSqlFromFields({
          leftTable,
          rightTable,
          joinColumn,
          joinType,
          fallbackSql: item.sql,
        })

        return { ...item, leftTable, rightTable, joinColumn, sql: nextSql }
      })
    )
  }

  const addMetric = () => {
    const id = `metric-${Date.now()}`
    setMetrics(prev => [{ id, name: "template_new", sql: "" }, ...prev])
    setEditingMetricId(id)
  }

  const addTerm = () => {
    const id = `term-${Date.now()}`
    setTerms(prev => [{ id, term: "", aliases: [], definition: "" }, ...prev])
    setSearchTerm("")
    setEditingTermId(id)
  }

  const handleSave = async () => {
    setSaving(true)
    setSaveMessage(null)
    setError(null)
    try {
      const payload = {
        joins: joins
          .map(item => ({
            name: item.name.trim(),
            sql: item.sql.trim(),
          }))
          .filter(item => item.name && item.sql),
        metrics: metrics
          .map(item => ({
            name: item.name.trim(),
            sql: item.sql.trim(),
          }))
          .filter(item => item.name && item.sql),
        terms: terms
          .map(item => ({
            term: item.term.trim(),
            definition: item.definition.trim(),
          }))
          .filter(item => item.term),
      }

      const res = await fetch("/admin/rag/context", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      })

      if (!res.ok) {
        throw new Error("Failed to save context.")
      }

      setSaveMessage("저장이 완료되었습니다.")
      await loadDocs()
    } catch (err) {
      setError("저장에 실패했습니다.")
    } finally {
      setSaving(false)
    }
  }

  const handleCancel = async () => {
    setSaveMessage(null)
    await loadDocs()
  }

  return (
    <div className="p-4 sm:p-6 space-y-4 sm:space-y-6 w-full max-w-none">
      <div>
        <h2 className="text-xl sm:text-2xl font-bold text-foreground">컨텍스트 편집</h2>
        <p className="text-sm text-muted-foreground mt-1">NL2SQL 변환에 사용되는 조인, 지표, 용어를 관리합니다.</p>
        <Badge variant="outline" className="mt-2">관리자 전용</Badge>
      </div>
      {error && (
        <div className="text-sm text-destructive">{error}</div>
      )}
      {saveMessage && (
        <div className="text-sm text-emerald-600">{saveMessage}</div>
      )}

      <Tabs defaultValue="joins" className="space-y-4">
        <TabsList className="grid w-full grid-cols-3 h-auto">
          <TabsTrigger value="joins" className="gap-1 sm:gap-2 text-xs sm:text-sm py-2">
            <GitBranch className="w-3 h-3 sm:w-4 sm:h-4" />
            <span className="hidden sm:inline">조인 관계</span>
            <span className="sm:hidden">조인</span>
          </TabsTrigger>
          <TabsTrigger value="metrics" className="gap-1 sm:gap-2 text-xs sm:text-sm py-2">
            <Calculator className="w-3 h-3 sm:w-4 sm:h-4" />
            <span className="hidden sm:inline">지표 템플릿</span>
            <span className="sm:hidden">지표</span>
          </TabsTrigger>
          <TabsTrigger value="terms" className="gap-1 sm:gap-2 text-xs sm:text-sm py-2">
            <BookOpen className="w-3 h-3 sm:w-4 sm:h-4" />
            <span className="hidden sm:inline">용어 사전</span>
            <span className="sm:hidden">용어</span>
          </TabsTrigger>
        </TabsList>

        {/* Joins Tab */}
        <TabsContent value="joins" className="space-y-4">
          <datalist id="join-table-options">
            {knownTables.map((table) => (
              <option key={table} value={table} />
            ))}
          </datalist>
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="text-lg">테이블 조인 관계</CardTitle>
                  <CardDescription>테이블 간의 조인 관계를 정의합니다. NL2SQL이 자동으로 적절한 조인을 선택합니다.</CardDescription>
                </div>
                <Button size="sm" className="gap-2" onClick={addJoin} disabled={loading || saving}>
                  <Plus className="w-4 h-4" />
                  조인 추가
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              {/* Visual Join Graph */}
              <div className="mb-6 p-4 rounded-lg bg-secondary/30 border border-border">
                <div className="text-xs text-muted-foreground mb-3">테이블 관계도</div>
                {loading ? (
                  <div className="text-xs text-muted-foreground">불러오는 중...</div>
                ) : tableChain.length === 0 ? (
                  <div className="text-xs text-muted-foreground">연결된 조인 템플릿이 없습니다.</div>
                ) : (
                  <div className="flex items-center justify-center gap-4 flex-wrap">
                    {tableChain.map((table, idx) => (
                      <div key={`${table}-${idx}`} className="flex items-center gap-2">
                        <div className="px-3 py-2 rounded-lg bg-primary/20 border border-primary/30">
                          <span className="font-mono text-sm text-foreground">{table}</span>
                        </div>
                        {idx < tableChain.length - 1 && <ArrowRight className="w-4 h-4 text-muted-foreground" />}
                      </div>
                    ))}
                  </div>
                )}
              </div>

              {/* Join List */}
              <div className="space-y-3">
                {loading ? (
                  <div className="text-xs text-muted-foreground">불러오는 중...</div>
                ) : joins.length === 0 ? (
                  <div className="text-xs text-muted-foreground">표시할 조인 템플릿이 없습니다.</div>
                ) : joins.map((join) => {
                  const parsed = parseJoinSql(join.sql)
                  const isEditing = editingJoinId === join.id
                  const leftTable = parsed?.leftTable ?? join.leftTable ?? (join.name || "template")
                  const leftColumn = parsed?.leftColumn ?? join.joinColumn ?? "?"
                  const rightTable = parsed?.rightTable ?? join.rightTable ?? "unknown"
                  const rightColumn = parsed?.rightColumn ?? join.joinColumn ?? "?"
                  const joinType = parsed?.joinType ?? "TEMPLATE"

                  return (
                    <div key={join.id} className="p-3 rounded-lg border border-border hover:border-primary/30 transition-colors space-y-3">
                      <div className="flex items-center justify-between gap-3">
                        <div className="flex items-center gap-2">
                          <Badge variant="secondary" className="text-[10px]">Template</Badge>
                          <span className="text-xs font-mono text-muted-foreground">{join.name}</span>
                        </div>
                        <div className="flex items-center gap-1">
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-8 w-8"
                            onClick={() => setEditingJoinId(isEditing ? null : join.id)}
                            disabled={saving}
                          >
                            {isEditing ? <Check className="w-3 h-3" /> : <Pencil className="w-3 h-3" />}
                          </Button>
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-8 w-8 text-destructive"
                            onClick={() => setJoins(prev => prev.filter(item => item.id !== join.id))}
                            disabled={saving}
                          >
                            <Trash2 className="w-3 h-3" />
                          </Button>
                        </div>
                      </div>
                      {isEditing ? (
                        <div className="space-y-2">
                          <Input
                            value={join.name}
                            placeholder="Template name"
                            onChange={(e) =>
                              setJoins(prev =>
                                prev.map(item =>
                                  item.id === join.id ? { ...item, name: e.target.value } : item
                                )
                              )
                            }
                          />
                          <div className="grid md:grid-cols-3 gap-2">
                            <div className="space-y-1">
                              <p className="text-[11px] text-muted-foreground">왼쪽 테이블 (선택/직접입력)</p>
                              <Input
                                list="join-table-options"
                                value={join.leftTable ?? parsed?.leftTable ?? ""}
                                placeholder="e.g. admissions"
                                onChange={(e) =>
                                  updateJoinFromFields(join.id, { leftTable: e.target.value })
                                }
                              />
                            </div>
                            <div className="space-y-1">
                              <p className="text-[11px] text-muted-foreground">오른쪽 테이블 (선택/직접입력)</p>
                              <Input
                                list="join-table-options"
                                value={join.rightTable ?? parsed?.rightTable ?? ""}
                                placeholder="e.g. icustays"
                                onChange={(e) =>
                                  updateJoinFromFields(join.id, { rightTable: e.target.value })
                                }
                              />
                            </div>
                            <div className="space-y-1">
                              <p className="text-[11px] text-muted-foreground">연결 컬럼</p>
                              <Input
                                value={join.joinColumn ?? parsed?.leftColumn ?? ""}
                                placeholder="e.g. hadm_id"
                                onChange={(e) =>
                                  updateJoinFromFields(join.id, { joinColumn: e.target.value })
                                }
                              />
                            </div>
                          </div>
                          <Textarea
                            value={join.sql}
                            placeholder="SQL template"
                            rows={4}
                            onChange={(e) =>
                              setJoins(prev =>
                                prev.map(item => {
                                  if (item.id !== join.id) {
                                    return item
                                  }
                                  const nextSql = e.target.value
                                  const nextParsed = parseJoinSql(nextSql)
                                  return {
                                    ...item,
                                    sql: nextSql,
                                    leftTable: nextParsed?.leftTable ?? item.leftTable ?? "",
                                    rightTable: nextParsed?.rightTable ?? item.rightTable ?? "",
                                    joinColumn: nextParsed?.leftColumn ?? item.joinColumn ?? "",
                                  }
                                })
                              )
                            }
                          />
                        </div>
                      ) : (
                        <>
                          <div className="flex items-center gap-2 flex-wrap">
                            <div className="flex items-center gap-1 px-2 py-1 rounded bg-secondary">
                              <Table2 className="w-3 h-3 text-muted-foreground" />
                              <span className="font-mono text-xs">{leftTable}</span>
                            </div>
                            <span className="text-xs text-muted-foreground">.{leftColumn}</span>
                            <Badge variant="outline" className="text-[10px]">{joinType}</Badge>
                            <div className="flex items-center gap-1 px-2 py-1 rounded bg-secondary">
                              <Table2 className="w-3 h-3 text-muted-foreground" />
                              <span className="font-mono text-xs">{rightTable}</span>
                            </div>
                            <span className="text-xs text-muted-foreground">.{rightColumn}</span>
                          </div>
                          {join.sql && (
                            <div className="w-full rounded bg-secondary/50 font-mono text-[11px] text-foreground overflow-x-auto px-2 py-1">
                              {join.sql}
                            </div>
                          )}
                        </>
                      )}
                    </div>
                  )
                })}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Metrics Tab */}
        <TabsContent value="metrics" className="space-y-4">
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="text-lg">지표 템플릿</CardTitle>
                  <CardDescription>자주 사용하는 지표의 SQL 공식을 미리 정의합니다.</CardDescription>
                </div>
                <Button size="sm" className="gap-2" onClick={addMetric} disabled={loading || saving}>
                  <Plus className="w-4 h-4" />
                  지표 추가
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              <div className="grid md:grid-cols-2 gap-4">
                {loading ? (
                  <div className="text-xs text-muted-foreground">불러오는 중...</div>
                ) : metrics.length === 0 ? (
                  <div className="text-xs text-muted-foreground">표시할 지표 템플릿이 없습니다.</div>
                ) : metrics.map((metric) => {
                  const isEditing = editingMetricId === metric.id
                  const displayName = toDisplayName(metric.name) || metric.name
                  return (
                    <div key={metric.id} className="p-4 rounded-lg border border-border hover:border-primary/30 transition-colors space-y-3">
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <div className="flex items-center gap-2">
                            <span className="font-medium text-foreground">{displayName}</span>
                            <Badge variant="secondary" className="text-[10px]">Template</Badge>
                          </div>
                          <span className="text-xs text-muted-foreground font-mono">{metric.name}</span>
                        </div>
                        <div className="flex items-center gap-1">
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-7 w-7"
                            onClick={() => setEditingMetricId(isEditing ? null : metric.id)}
                            disabled={saving}
                          >
                            {isEditing ? <Check className="w-3 h-3" /> : <Pencil className="w-3 h-3" />}
                          </Button>
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-7 w-7 text-destructive"
                            onClick={() => setMetrics(prev => prev.filter(item => item.id !== metric.id))}
                            disabled={saving}
                          >
                            <Trash2 className="w-3 h-3" />
                          </Button>
                        </div>
                      </div>
                      {isEditing ? (
                        <div className="space-y-2">
                          <Input
                            value={metric.name}
                            placeholder="Template name"
                            onChange={(e) =>
                              setMetrics(prev =>
                                prev.map(item =>
                                  item.id === metric.id ? { ...item, name: e.target.value } : item
                                )
                              )
                            }
                          />
                          <Textarea
                            value={metric.sql}
                            placeholder="SQL template"
                            rows={4}
                            onChange={(e) =>
                              setMetrics(prev =>
                                prev.map(item =>
                                  item.id === metric.id ? { ...item, sql: e.target.value } : item
                                )
                              )
                            }
                          />
                        </div>
                      ) : (
                        <>
                          <p className="text-xs text-muted-foreground">RAG SQL template</p>
                          <div className="p-2 rounded bg-secondary/50 font-mono text-[11px] text-foreground overflow-x-auto">
                            {metric.sql}
                          </div>
                        </>
                      )}
                    </div>
                  )
                })}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Terms Tab */}
        <TabsContent value="terms" className="space-y-4">
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle className="text-lg">용어 사전 / 약어 등록</CardTitle>
                  <CardDescription>의료 용어와 약어를 SQL 조건으로 매핑합니다.</CardDescription>
                </div>
                <Button size="sm" className="gap-2" onClick={addTerm} disabled={loading || saving}>
                  <Plus className="w-4 h-4" />
                  용어 추가
                </Button>
              </div>
            </CardHeader>
            <CardContent className="space-y-4">
              {/* Search */}
              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
                <Input 
                  placeholder="용어 또는 약어 검색..." 
                  className="pl-9"
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                />
              </div>

              {/* Terms List */}
              <div className="space-y-3">
                {loading ? (
                  <div className="text-xs text-muted-foreground">불러오는 중...</div>
                ) : filteredTerms.length === 0 ? (
                  <div className="text-xs text-muted-foreground">표시할 용어가 없습니다.</div>
                ) : filteredTerms.map((term) => {
                  const isEditing = editingTermId === term.id
                  return (
                    <div key={term.id} className="p-4 rounded-lg border border-border hover:border-primary/30 transition-colors space-y-3">
                      <div className="flex items-start justify-between gap-3">
                        <div className="flex items-center gap-2">
                          <Hash className="w-4 h-4 text-primary" />
                          <span className="font-medium text-foreground">{term.term || "새 용어"}</span>
                        </div>
                        <div className="flex items-center gap-1">
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-7 w-7"
                            onClick={() => setEditingTermId(isEditing ? null : term.id)}
                            disabled={saving}
                          >
                            {isEditing ? <Check className="w-3 h-3" /> : <Pencil className="w-3 h-3" />}
                          </Button>
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-7 w-7 text-destructive"
                            onClick={() => setTerms(prev => prev.filter(item => item.id !== term.id))}
                            disabled={saving}
                          >
                            <Trash2 className="w-3 h-3" />
                          </Button>
                        </div>
                      </div>
                      {isEditing ? (
                        <div className="space-y-2">
                          <Input
                            value={term.term}
                            placeholder="용어"
                            onChange={(e) =>
                              setTerms(prev =>
                                prev.map(item =>
                                  item.id === term.id ? { ...item, term: e.target.value } : item
                                )
                              )
                            }
                          />
                          <Textarea
                            value={term.definition}
                            placeholder="정의"
                            rows={3}
                            onChange={(e) =>
                              setTerms(prev =>
                                prev.map(item =>
                                  item.id === term.id ? { ...item, definition: e.target.value } : item
                                )
                              )
                            }
                          />
                        </div>
                      ) : (
                        <>
                          {term.aliases.length > 0 && (
                            <div className="flex flex-wrap gap-1">
                              {term.aliases.map((alias) => (
                                <Badge key={alias} variant="outline" className="text-[10px]">{alias}</Badge>
                              ))}
                            </div>
                          )}
                          <p className="text-xs text-muted-foreground">{term.definition}</p>
                        </>
                      )}
                    </div>
                  )
                })}
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>

      {/* Save Button */}
      <div className="flex justify-end gap-3">
        <Button variant="outline" onClick={handleCancel} disabled={loading || saving}>변경 취소</Button>
        <Button onClick={handleSave} disabled={loading || saving}>
          {saving ? "저장 중..." : "모든 변경 저장"}
        </Button>
      </div>
    </div>
  )
}

function parseTemplateDoc(doc: any) {
  const text = typeof doc?.text === "string" ? doc.text : ""
  const fallbackName = doc?.metadata?.name || doc?.id || ""
  let name = fallbackName
  let sql = ""
  const parts = text.split(/\r?\nSQL:/)
  if (parts.length > 1) {
    const header = parts[0]
    sql = parts.slice(1).join("\nSQL:").trim()
    if (header.toLowerCase().startsWith("template:")) {
      name = header.slice("template:".length).trim() || name
    }
  } else if (text.toLowerCase().startsWith("template:")) {
    name = text.slice("template:".length).trim()
  }
  return { name, sql }
}

function parseGlossaryDoc(doc: any) {
  const text = typeof doc?.text === "string" ? doc.text : ""
  const fallbackTerm = doc?.metadata?.term || doc?.id || ""
  let term = fallbackTerm
  let definition = ""
  const cleaned = text.replace(/^Glossary:/i, "").trim()
  const eqIdx = cleaned.indexOf("=")
  if (eqIdx >= 0) {
    term = cleaned.slice(0, eqIdx).trim() || term
    definition = cleaned.slice(eqIdx + 1).trim()
  } else if (cleaned) {
    term = cleaned
  }
  return { term, definition }
}

function parseJoinSql(sql: string) {
  if (!sql) {
    return null
  }
  const match = sql.match(
    /from\s+([A-Z0-9_]+)\s+([A-Z0-9_]+)\s+(left|right|inner|full)?\s*join\s+([A-Z0-9_]+)\s+([A-Z0-9_]+)\s+on\s+([A-Z0-9_]+)\.([A-Z0-9_]+)\s*=\s*([A-Z0-9_]+)\.([A-Z0-9_]+)/i
  )
  if (!match) {
    return null
  }
  const leftTable = match[1]
  const leftAlias = match[2]
  const joinType = match[3] ? match[3].toUpperCase() : "INNER"
  const rightTable = match[4]
  const rightAlias = match[5]
  const alias1 = match[6]
  const col1 = match[7]
  const alias2 = match[8]
  const col2 = match[9]

  let leftColumn = col1
  let rightColumn = col2
  if (
    alias1.toLowerCase() === rightAlias.toLowerCase() &&
    alias2.toLowerCase() === leftAlias.toLowerCase()
  ) {
    leftColumn = col2
    rightColumn = col1
  }

  return {
    leftTable: leftTable.toLowerCase(),
    leftColumn: leftColumn.toLowerCase(),
    rightTable: rightTable.toLowerCase(),
    rightColumn: rightColumn.toLowerCase(),
    joinType,
  }
}

function toDisplayName(name: string) {
  if (!name) {
    return ""
  }
  const normalized = name.trim().toLowerCase()

  const koNameMap: Record<string, string> = {
    // Clinical research metric templates
    template_hospital_los_days: "병원 재원일수",
    template_readmission_30d_flag: "30일 재입원 여부",
    template_in_hospital_mortality_flag: "입원 중 사망 여부",
    template_in_hospital_mortality_rate: "입원 중 사망률",
    template_icu_los_days: "ICU 재원일수",
    template_icu_readmit_same_hadm_flag: "ICU 재입실 여부(동일 입원)",
    template_transfer_count_per_admission: "입원별 병동 이동 횟수",
    template_discharge_location_distribution: "퇴원 형태 분포",

    // Backward compatibility for previously used sample templates
    template_sample_rows_two_cols: "샘플 조회(2개 컬럼)",
    template_count_rows_sampled: "샘플 건수",
    template_distinct_sample: "중복 제거 샘플",
    template_count_by_gender: "성별 분포",
    template_count_by_admission_type: "입원 유형 분포",
    template_recent_admissions_30d: "최근 30일 입원",
    template_avg_icu_los: "평균 ICU 재원일수",
    template_top_n_ordered: "상위 N 집계",
    template_recent_admissions_7d: "최근 7일 입원",
  }

  const mapped = koNameMap[normalized]
  if (mapped) {
    return mapped
  }

  return name.replace(/^template_/, "").replace(/_/g, " ")
}

function buildJoinSqlFromFields({
  leftTable,
  rightTable,
  joinColumn,
  joinType,
  fallbackSql,
}: {
  leftTable: string
  rightTable: string
  joinColumn: string
  joinType: string
  fallbackSql: string
}) {
  const left = leftTable.trim()
  const right = rightTable.trim()
  const col = joinColumn.trim()

  if (!left || !right || !col) {
    return fallbackSql
  }

  const jt = (joinType || "INNER").toUpperCase()
  const leftUpper = left.toUpperCase()
  const rightUpper = right.toUpperCase()
  const colUpper = col.toUpperCase()

  return `SELECT l.${colUpper}, r.${colUpper} FROM ${leftUpper} l ${jt} JOIN ${rightUpper} r ON l.${colUpper} = r.${colUpper} WHERE l.${colUpper} IS NOT NULL AND ROWNUM <= 100`
}
