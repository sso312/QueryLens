"use client"

import React from "react"

import { useState } from "react"
import { Search, Sparkles, Loader2 } from "lucide-react"
import { Button } from "@/components/ui/button"

interface QueryInputProps {
  onSubmit: (query: string) => void
  isLoading: boolean
}

export function QueryInput({ onSubmit, isLoading }: QueryInputProps) {
  const [query, setQuery] = useState("65세 이상 심부전 환자 코호트 만들어줘, 생존 곡선 그려줘")

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    if (query.trim() && !isLoading) {
      onSubmit(query)
    }
  }

  const exampleQueries = [
    "65세 이상 심부전 환자",
    "당뇨병 환자 생존율",
    "폐렴 ICU 입원 환자",
  ]

  return (
    <div className="space-y-4">
      <form onSubmit={handleSubmit} className="relative">
        <div className="flex items-center gap-3 rounded-xl border border-border bg-card p-2 transition-all focus-within:ring-2 focus-within:ring-primary/50 focus-within:border-primary">
          <div className="flex items-center justify-center w-10 h-10 rounded-lg bg-primary/10">
            <Sparkles className="w-5 h-5 text-primary" />
          </div>
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="자연어로 연구 질문을 입력하세요..."
            className="flex-1 bg-transparent text-foreground placeholder:text-muted-foreground outline-none text-base"
            disabled={isLoading}
          />
          <Button 
            type="submit" 
            disabled={isLoading || !query.trim()}
            className="px-5 gap-2"
          >
            {isLoading ? (
              <>
                <Loader2 className="w-4 h-4 animate-spin" />
                분석 중
              </>
            ) : (
              <>
                <Search className="w-4 h-4" />
                분석하기
              </>
            )}
          </Button>
        </div>
      </form>
      
      <div className="flex flex-wrap gap-2">
        <span className="text-sm text-muted-foreground">예시:</span>
        {exampleQueries.map((example) => (
          <button
            key={example}
            onClick={() => setQuery(example)}
            className="text-sm px-3 py-1 rounded-full border border-border bg-secondary/50 text-muted-foreground hover:text-foreground hover:border-primary/50 transition-colors"
            disabled={isLoading}
          >
            {example}
          </button>
        ))}
      </div>
    </div>
  )
}
