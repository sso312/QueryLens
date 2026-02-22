"use client"

import { useState, useMemo } from "react"
import dynamic from "next/dynamic"
import { Beaker, Play, RotateCcw, TrendingUp, TrendingDown, Minus, ChevronRight, Lightbulb } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Slider } from "@/components/ui/slider"

interface SimulationParams {
  readmissionDays: number
  ageThreshold: number
  losThreshold: number
}

interface MetricChange {
  label: string
  originalValue: number
  newValue: number
  unit: string
  description: string
}

const defaultParams: SimulationParams = {
  readmissionDays: 30,
  ageThreshold: 65,
  losThreshold: 7,
}

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false }) as any

// Generate survival data based on parameters
function generateSurvivalData(params: SimulationParams) {
  const baseData = [
    { time: 0, original: 100, simulated: 100 },
    { time: 7, original: 94.2, simulated: 0 },
    { time: 14, original: 88.5, simulated: 0 },
    { time: 21, original: 82.1, simulated: 0 },
    { time: 30, original: 75.8, simulated: 0 },
    { time: 45, original: 68.3, simulated: 0 },
    { time: 60, original: 61.2, simulated: 0 },
    { time: 75, original: 54.8, simulated: 0 },
    { time: 90, original: 48.5, simulated: 0 },
    { time: 120, original: 39.2, simulated: 0 },
    { time: 150, original: 31.5, simulated: 0 },
    { time: 180, original: 25.1, simulated: 0 },
  ]

  // Adjust simulation based on parameters
  const readmissionFactor = (30 - params.readmissionDays) * 0.3
  const ageFactor = (params.ageThreshold - 65) * 0.4
  const losFactor = (params.losThreshold - 7) * 0.25

  return baseData.map((point, index) => {
    if (index === 0) return point
    const adjustment = readmissionFactor + ageFactor - losFactor
    const decay = Math.pow(0.97, index)
    const simulated = Math.max(5, Math.min(100, point.original + adjustment * decay * (12 - index)))
    return {
      ...point,
      simulated: Number(simulated.toFixed(1)),
    }
  })
}

// Calculate metrics based on parameters
function calculateMetrics(params: SimulationParams, originalParams: SimulationParams): MetricChange[] {
  const readmissionChange = ((originalParams.readmissionDays - params.readmissionDays) / originalParams.readmissionDays) * 100
  const ageChange = params.ageThreshold - originalParams.ageThreshold
  const losChange = params.losThreshold - originalParams.losThreshold

  // Simulate metric changes
  const basePatients = 1247
  const baseReadmissionRate = 18.5
  const baseMortality = 12.3
  const baseLoS = 8.2

  const patientChange = Math.round(basePatients * (ageChange * 0.08 + readmissionChange * 0.002))
  const readmissionRateChange = readmissionChange * -0.4 + losChange * 0.3
  const mortalityChange = ageChange * 0.15 + losChange * -0.2

  return [
    {
      label: "대상 환자 수",
      originalValue: basePatients,
      newValue: Math.max(100, basePatients + patientChange),
      unit: "명",
      description: "코호트 기준 변경에 따른 대상 환자 수",
    },
    {
      label: "30일 재입원율",
      originalValue: baseReadmissionRate,
      newValue: Math.max(5, Math.min(40, baseReadmissionRate + readmissionRateChange)),
      unit: "%",
      description: "재입원 기준일 변경 시 예상 재입원율",
    },
    {
      label: "원내 사망률",
      originalValue: baseMortality,
      newValue: Math.max(3, Math.min(25, baseMortality + mortalityChange)),
      unit: "%",
      description: "코호트 조건 변경에 따른 예상 사망률",
    },
    {
      label: "평균 재원일수",
      originalValue: baseLoS,
      newValue: Math.max(3, Math.min(20, baseLoS + losChange * 0.5)),
      unit: "일",
      description: "재원일수 기준 변경 시 평균값",
    },
  ]
}

export function WhatIfAnalysis() {
  const [params, setParams] = useState<SimulationParams>(defaultParams)
  const [isSimulating, setIsSimulating] = useState(false)
  const [hasSimulated, setHasSimulated] = useState(false)

  const survivalData = useMemo(() => generateSurvivalData(params), [params])
  const metrics = useMemo(() => calculateMetrics(params, defaultParams), [params])

  const hasChanges = 
    params.readmissionDays !== defaultParams.readmissionDays ||
    params.ageThreshold !== defaultParams.ageThreshold ||
    params.losThreshold !== defaultParams.losThreshold

  const handleSimulate = async () => {
    setIsSimulating(true)
    await new Promise(resolve => setTimeout(resolve, 800))
    setHasSimulated(true)
    setIsSimulating(false)
  }

  const handleReset = () => {
    setParams(defaultParams)
    setHasSimulated(false)
  }

  const survivalFigure = useMemo(() => {
    if (!survivalData.length) return null
    return {
      data: [
        {
          type: "scatter",
          mode: "lines",
          x: survivalData.map((d) => d.time),
          y: survivalData.map((d) => d.original),
          name: "Baseline",
          line: { color: "#6b7280", width: 2, shape: "hv", dash: "dash" },
          hovertemplate: "day %{x}<br>%{y:.1f}%<extra></extra>",
        },
        {
          type: "scatter",
          mode: "lines",
          x: survivalData.map((d) => d.time),
          y: survivalData.map((d) => d.simulated),
          name: "Simulated",
          line: { color: "#3ecf8e", width: 2, shape: "hv" },
          hovertemplate: "day %{x}<br>%{y:.1f}%<extra></extra>",
        },
      ],
      layout: {
        margin: { l: 48, r: 24, t: 24, b: 42 },
        xaxis: { title: "Time (days)" },
        yaxis: { title: "Survival (%)", range: [0, 100] },
        legend: { orientation: "h", y: 1.14 },
      },
    }
  }, [survivalData])

  const renderChangeIndicator = (original: number, newVal: number) => {
    const diff = newVal - original
    const percentDiff = ((diff / original) * 100).toFixed(1)
    
    if (Math.abs(diff) < 0.1) {
      return (
        <span className="inline-flex items-center gap-1 text-muted-foreground text-xs">
          <Minus className="w-3 h-3" />
          변화없음
        </span>
      )
    }
    
    if (diff > 0) {
      return (
        <span className="inline-flex items-center gap-1 text-amber-400 text-xs">
          <TrendingUp className="w-3 h-3" />
          +{percentDiff}%
        </span>
      )
    }
    
    return (
      <span className="inline-flex items-center gap-1 text-primary text-xs">
        <TrendingDown className="w-3 h-3" />
        {percentDiff}%
      </span>
    )
  }

  return (
    <div className="rounded-xl border border-border bg-card overflow-hidden">
      <div className="flex items-center justify-between px-4 py-3 border-b border-border">
        <div className="flex items-center gap-3">
          <div className="flex items-center justify-center w-8 h-8 rounded-lg bg-amber-500/10">
            <Beaker className="w-4 h-4 text-amber-400" />
          </div>
          <div>
            <h3 className="text-sm font-medium text-foreground">What-if 시뮬레이션</h3>
            <p className="text-xs text-muted-foreground">가상 코호트 조건 변경 분석</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="ghost"
            size="sm"
            onClick={handleReset}
            disabled={!hasChanges}
            className="text-muted-foreground hover:text-foreground"
          >
            <RotateCcw className="w-4 h-4 mr-1" />
            초기화
          </Button>
          <Button
            size="sm"
            onClick={handleSimulate}
            disabled={!hasChanges || isSimulating}
            className="bg-amber-500 hover:bg-amber-600 text-background"
          >
            {isSimulating ? (
              <>
                <div className="w-4 h-4 mr-1 border-2 border-background/30 border-t-background rounded-full animate-spin" />
                분석 중...
              </>
            ) : (
              <>
                <Play className="w-4 h-4 mr-1" />
                시뮬레이션 실행
              </>
            )}
          </Button>
        </div>
      </div>

      <div className="p-4">
        {/* Example Question */}
        <div className="mb-6 p-3 rounded-lg bg-amber-500/5 border border-amber-500/20">
          <div className="flex items-start gap-2">
            <Lightbulb className="w-4 h-4 text-amber-400 mt-0.5 shrink-0" />
            <div>
              <p className="text-sm text-foreground font-medium">예시 질문</p>
              <p className="text-xs text-muted-foreground mt-1">
                "만약 재입원 기준을 30일에서 15일로 바꾸면 지표가 어떻게 변해?"
              </p>
            </div>
          </div>
        </div>

        {/* Parameter Controls */}
        <div className="grid md:grid-cols-3 gap-6 mb-6">
          {/* Readmission Days */}
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <label className="text-sm font-medium text-foreground">재입원 기준일</label>
              <span className="text-sm font-semibold text-primary">{params.readmissionDays}일</span>
            </div>
            <Slider
              value={[params.readmissionDays]}
              onValueChange={([value]) => setParams(prev => ({ ...prev, readmissionDays: value }))}
              min={7}
              max={60}
              step={1}
              className="[&_[role=slider]]:bg-primary"
            />
            <div className="flex justify-between text-xs text-muted-foreground">
              <span>7일</span>
              <span className="text-amber-400">{defaultParams.readmissionDays}일 (현재)</span>
              <span>60일</span>
            </div>
          </div>

          {/* Age Threshold */}
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <label className="text-sm font-medium text-foreground">연령 기준</label>
              <span className="text-sm font-semibold text-primary">{params.ageThreshold}세 이상</span>
            </div>
            <Slider
              value={[params.ageThreshold]}
              onValueChange={([value]) => setParams(prev => ({ ...prev, ageThreshold: value }))}
              min={50}
              max={80}
              step={5}
              className="[&_[role=slider]]:bg-primary"
            />
            <div className="flex justify-between text-xs text-muted-foreground">
              <span>50세</span>
              <span className="text-amber-400">{defaultParams.ageThreshold}세 (현재)</span>
              <span>80세</span>
            </div>
          </div>

          {/* LOS Threshold */}
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <label className="text-sm font-medium text-foreground">재원일수 기준</label>
              <span className="text-sm font-semibold text-primary">{params.losThreshold}일 이상</span>
            </div>
            <Slider
              value={[params.losThreshold]}
              onValueChange={([value]) => setParams(prev => ({ ...prev, losThreshold: value }))}
              min={1}
              max={30}
              step={1}
              className="[&_[role=slider]]:bg-primary"
            />
            <div className="flex justify-between text-xs text-muted-foreground">
              <span>1일</span>
              <span className="text-amber-400">{defaultParams.losThreshold}일 (현재)</span>
              <span>30일</span>
            </div>
          </div>
        </div>

        {/* Results Section */}
        {hasSimulated && (
          <div className="space-y-6 animate-in fade-in slide-in-from-bottom-2 duration-300">
            {/* Metrics Grid */}
            <div>
              <h4 className="text-sm font-medium text-foreground mb-3 flex items-center gap-2">
                <ChevronRight className="w-4 h-4 text-primary" />
                예상 지표 변화
              </h4>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                {metrics.map((metric) => (
                  <div
                    key={metric.label}
                    className="rounded-lg bg-secondary/50 p-3 hover:bg-secondary/70 transition-colors"
                  >
                    <p className="text-xs text-muted-foreground mb-1">{metric.label}</p>
                    <div className="flex items-baseline gap-2">
                      <span className="text-lg font-semibold text-foreground">
                        {metric.newValue.toLocaleString(undefined, { maximumFractionDigits: 1 })}
                      </span>
                      <span className="text-xs text-muted-foreground">{metric.unit}</span>
                    </div>
                    <div className="mt-1 flex items-center gap-2">
                      <span className="text-xs text-muted-foreground line-through">
                        {metric.originalValue.toLocaleString(undefined, { maximumFractionDigits: 1 })}
                      </span>
                      {renderChangeIndicator(metric.originalValue, metric.newValue)}
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* Comparison Chart */}
            <div>
              <h4 className="text-sm font-medium text-foreground mb-3 flex items-center gap-2">
                <ChevronRight className="w-4 h-4 text-primary" />
                생존 곡선 비교
              </h4>
              <div className="h-[280px]">
                {survivalFigure ? (
                  <Plot
                    data={Array.isArray(survivalFigure.data) ? survivalFigure.data : []}
                    layout={survivalFigure.layout || {}}
                    config={{ responsive: true, displaylogo: false, editable: true }}
                    style={{ width: "100%", height: "100%" }}
                    useResizeHandler
                  />
                ) : (
                  <div className="h-full rounded-lg border border-dashed border-border text-sm text-muted-foreground flex items-center justify-center">
                    No simulation chart data.
                  </div>
                )}
              </div>
            </div>

            {/* Insight Box */}
            <div className="rounded-lg bg-primary/5 border border-primary/20 p-4">
              <h4 className="text-sm font-medium text-foreground mb-2 flex items-center gap-2">
                <Lightbulb className="w-4 h-4 text-primary" />
                분석 인사이트
              </h4>
              <p className="text-sm text-muted-foreground">
                {params.readmissionDays < defaultParams.readmissionDays && (
                  <>재입원 기준을 {defaultParams.readmissionDays}일에서 {params.readmissionDays}일로 단축하면 재입원율이 {(((defaultParams.readmissionDays - params.readmissionDays) / defaultParams.readmissionDays) * 40).toFixed(1)}% 감소할 것으로 예상됩니다. </>
                )}
                {params.ageThreshold > defaultParams.ageThreshold && (
                  <>연령 기준을 {params.ageThreshold}세로 상향 조정하면 대상 환자 수가 감소하지만 더 고위험군에 집중할 수 있습니다. </>
                )}
                {params.losThreshold !== defaultParams.losThreshold && (
                  <>재원일수 기준 변경은 평균 재원일수와 사망률에 영향을 미칩니다.</>
                )}
                {!hasChanges && "조건을 변경하면 예상 결과를 확인할 수 있습니다."}
              </p>
            </div>
          </div>
        )}

        {/* Empty State */}
        {!hasSimulated && (
          <div className="text-center py-8">
            <div className="inline-flex items-center justify-center w-12 h-12 rounded-full bg-secondary/50 mb-4">
              <Beaker className="w-6 h-6 text-muted-foreground" />
            </div>
            <p className="text-sm text-muted-foreground">
              슬라이더를 조정하여 조건을 변경한 후<br />
              시뮬레이션을 실행하세요
            </p>
          </div>
        )}
      </div>
    </div>
  )
}
