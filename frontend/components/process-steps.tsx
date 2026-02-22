"use client"

import React from "react"

import { Check, Loader2, MessageSquare, Database, BarChart3, Table } from "lucide-react"

interface Step {
  id: number
  label: string
  description: string
  icon: React.ElementType
  status: "pending" | "loading" | "complete"
}

interface ProcessStepsProps {
  currentStep: number
}

export function ProcessSteps({ currentStep }: ProcessStepsProps) {
  const steps: Step[] = [
    {
      id: 1,
      label: "자연어 분석",
      description: "NL2SQL 변환",
      icon: MessageSquare,
      status: currentStep > 1 ? "complete" : currentStep === 1 ? "loading" : "pending",
    },
    {
      id: 2,
      label: "데이터 추출",
      description: "MIMIC-IV 쿼리",
      icon: Database,
      status: currentStep > 2 ? "complete" : currentStep === 2 ? "loading" : "pending",
    },
    {
      id: 3,
      label: "코호트 생성",
      description: "환자 필터링",
      icon: Table,
      status: currentStep > 3 ? "complete" : currentStep === 3 ? "loading" : "pending",
    },
    {
      id: 4,
      label: "생존 분석",
      description: "Kaplan-Meier",
      icon: BarChart3,
      status: currentStep > 4 ? "complete" : currentStep === 4 ? "loading" : "pending",
    },
  ]

  return (
    <div className="flex items-center justify-between px-4">
      {steps.map((step, index) => (
        <div key={step.id} className="flex items-center">
          <div className="flex flex-col items-center">
            <div
              className={`flex items-center justify-center w-10 h-10 rounded-xl transition-all duration-300 ${
                step.status === "complete"
                  ? "bg-primary text-primary-foreground"
                  : step.status === "loading"
                  ? "bg-primary/20 text-primary ring-2 ring-primary ring-offset-2 ring-offset-background"
                  : "bg-secondary text-muted-foreground"
              }`}
            >
              {step.status === "complete" ? (
                <Check className="w-5 h-5" />
              ) : step.status === "loading" ? (
                <Loader2 className="w-5 h-5 animate-spin" />
              ) : (
                <step.icon className="w-5 h-5" />
              )}
            </div>
            <div className="mt-2 text-center">
              <p className={`text-xs font-medium ${
                step.status === "pending" ? "text-muted-foreground" : "text-foreground"
              }`}>
                {step.label}
              </p>
              <p className="text-xs text-muted-foreground">{step.description}</p>
            </div>
          </div>
          {index < steps.length - 1 && (
            <div className={`w-16 md:w-24 h-0.5 mx-2 mt-[-20px] transition-all duration-500 ${
              step.status === "complete" ? "bg-primary" : "bg-border"
            }`} />
          )}
        </div>
      ))}
    </div>
  )
}
