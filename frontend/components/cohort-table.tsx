"use client"

import { Users, Download, Filter } from "lucide-react"
import { Button } from "@/components/ui/button"

interface CohortPatient {
  subject_id: number
  age: number
  gender: string
  admission_date: string
  los_days: number
  status: "생존" | "사망"
  death_date?: string
}

interface CohortTableProps {
  patients: CohortPatient[]
  totalCount: number
}

export function CohortTable({ patients, totalCount }: CohortTableProps) {
  return (
    <div className="rounded-xl border border-border bg-card overflow-hidden">
      <div className="flex items-center justify-between px-4 py-3 border-b border-border">
        <div className="flex items-center gap-3">
          <div className="flex items-center justify-center w-8 h-8 rounded-lg bg-primary/10">
            <Users className="w-4 h-4 text-primary" />
          </div>
          <div>
            <h3 className="text-sm font-medium text-foreground">환자 코호트</h3>
            <p className="text-xs text-muted-foreground">총 {totalCount.toLocaleString()}명의 환자</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" className="h-8 gap-2 bg-transparent">
            <Filter className="w-3 h-3" />
            필터
          </Button>
          <Button variant="outline" size="sm" className="h-8 gap-2 bg-transparent">
            <Download className="w-3 h-3" />
            CSV
          </Button>
        </div>
      </div>
      
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="bg-secondary/30">
            <tr>
              <th className="px-4 py-3 text-left font-medium text-muted-foreground">환자 ID</th>
              <th className="px-4 py-3 text-left font-medium text-muted-foreground">나이</th>
              <th className="px-4 py-3 text-left font-medium text-muted-foreground">성별</th>
              <th className="px-4 py-3 text-left font-medium text-muted-foreground">입원일</th>
              <th className="px-4 py-3 text-left font-medium text-muted-foreground">재원일수</th>
              <th className="px-4 py-3 text-left font-medium text-muted-foreground">상태</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {patients.map((patient) => (
              <tr key={patient.subject_id} className="hover:bg-secondary/20 transition-colors">
                <td className="px-4 py-3 font-mono text-foreground">{patient.subject_id}</td>
                <td className="px-4 py-3 text-foreground">{patient.age}세</td>
                <td className="px-4 py-3 text-foreground">{patient.gender}</td>
                <td className="px-4 py-3 text-foreground">{patient.admission_date}</td>
                <td className="px-4 py-3 text-foreground">{patient.los_days}일</td>
                <td className="px-4 py-3">
                  <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${
                    patient.status === "생존" 
                      ? "bg-primary/20 text-primary" 
                      : "bg-destructive/20 text-destructive"
                  }`}>
                    {patient.status}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      
      <div className="px-4 py-3 border-t border-border flex items-center justify-between text-sm text-muted-foreground">
        <span>1-{patients.length} of {totalCount.toLocaleString()} 환자</span>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" disabled>이전</Button>
          <Button variant="outline" size="sm">다음</Button>
        </div>
      </div>
    </div>
  )
}
