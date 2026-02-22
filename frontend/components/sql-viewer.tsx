"use client"

import { useState } from "react"
import { Copy, Check, Code2, ChevronDown, ChevronUp } from "lucide-react"
import { Button } from "@/components/ui/button"

interface SqlViewerProps {
  sql: string
  tables: string[]
}

export function SqlViewer({ sql, tables }: SqlViewerProps) {
  const [copied, setCopied] = useState(false)
  const [expanded, setExpanded] = useState(true)

  const copyToClipboard = async () => {
    await navigator.clipboard.writeText(sql)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const highlightSql = (code: string) => {
    const keywords = ["SELECT", "FROM", "WHERE", "JOIN", "LEFT", "INNER", "ON", "AND", "OR", "AS", "CASE", "WHEN", "THEN", "END", "GROUP BY", "ORDER BY", "HAVING", "LIMIT", "DISTINCT", "WITH", "COUNT", "MIN", "MAX", "AVG", "SUM", "COALESCE", "IS", "NOT", "NULL", "IN", "BETWEEN", "LIKE", "EXTRACT", "YEAR", "MONTH", "DAY"]
    
    let highlighted = code
    keywords.forEach(keyword => {
      const regex = new RegExp(`\\b${keyword}\\b`, "gi")
      highlighted = highlighted.replace(regex, `<span class="text-primary font-medium">${keyword}</span>`)
    })
    
    highlighted = highlighted.replace(/'[^']*'/g, '<span class="text-chart-2">$&</span>')
    highlighted = highlighted.replace(/--.*$/gm, '<span class="text-muted-foreground">$&</span>')
    
    return highlighted
  }

  return (
    <div className="rounded-xl border border-border bg-card overflow-hidden">
      <div 
        className="flex items-center justify-between px-4 py-3 border-b border-border cursor-pointer hover:bg-secondary/30 transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <div className="flex items-center gap-3">
          <div className="flex items-center justify-center w-8 h-8 rounded-lg bg-primary/10">
            <Code2 className="w-4 h-4 text-primary" />
          </div>
          <div>
            <h3 className="text-sm font-medium text-foreground">생성된 SQL 쿼리</h3>
            <div className="flex items-center gap-2 mt-1">
              {tables.map((table) => (
                <span key={table} className="text-xs px-2 py-0.5 rounded-full bg-secondary text-muted-foreground">
                  {table}
                </span>
              ))}
            </div>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="ghost"
            size="sm"
            onClick={(e) => {
              e.stopPropagation()
              copyToClipboard()
            }}
            className="h-8 px-2"
          >
            {copied ? <Check className="w-4 h-4 text-primary" /> : <Copy className="w-4 h-4" />}
          </Button>
          {expanded ? <ChevronUp className="w-4 h-4 text-muted-foreground" /> : <ChevronDown className="w-4 h-4 text-muted-foreground" />}
        </div>
      </div>
      
      {expanded && (
        <div className="p-4 bg-background/50 overflow-x-auto">
          <pre className="text-sm font-mono leading-relaxed">
            <code dangerouslySetInnerHTML={{ __html: highlightSql(sql) }} />
          </pre>
        </div>
      )}
    </div>
  )
}
