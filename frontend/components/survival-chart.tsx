"use client"

import { useMemo } from "react"
import dynamic from "next/dynamic"
import { Activity, Info } from "lucide-react"

interface SurvivalDataPoint {
  time: number
  survival: number
  lowerCI: number
  upperCI: number
  atRisk: number
  events: number
}

interface SurvivalChartProps {
  data: SurvivalDataPoint[]
  medianSurvival: number
  totalPatients: number
  totalEvents: number
}

const Plot = dynamic(() => import("react-plotly.js"), { ssr: false }) as any

export function SurvivalChart({ data, medianSurvival, totalPatients, totalEvents }: SurvivalChartProps) {
  const figure = useMemo(() => {
    if (!data.length) return null
    const time = data.map((d) => d.time)
    const lower = data.map((d) => d.lowerCI)
    const upper = data.map((d) => d.upperCI)
    const survival = data.map((d) => d.survival)

    return {
      data: [
        {
          type: "scatter",
          mode: "lines",
          x: time,
          y: lower,
          line: { color: "rgba(62,207,142,0)" },
          hoverinfo: "skip",
          showlegend: false,
        },
        {
          type: "scatter",
          mode: "lines",
          x: time,
          y: upper,
          line: { color: "rgba(62,207,142,0)" },
          fill: "tonexty",
          fillcolor: "rgba(62,207,142,0.18)",
          name: "95% CI",
        },
        {
          type: "scatter",
          mode: "lines",
          x: time,
          y: survival,
          line: { color: "#3ecf8e", width: 2, shape: "hv" },
          name: "Survival",
          hovertemplate: "day %{x}<br>survival %{y:.1f}%<extra></extra>",
        },
      ],
      layout: {
        margin: { l: 48, r: 24, t: 24, b: 42 },
        xaxis: { title: "Time (days)" },
        yaxis: { title: "Survival (%)", range: [0, 100] },
        legend: { orientation: "h", y: 1.14 },
        shapes: [
          {
            type: "line",
            x0: Math.min(...time),
            x1: Math.max(...time),
            y0: 50,
            y1: 50,
            line: { color: "rgba(148,163,184,0.6)", width: 1, dash: "dash" },
          },
          {
            type: "line",
            x0: medianSurvival,
            x1: medianSurvival,
            y0: 0,
            y1: 100,
            line: { color: "rgba(62,207,142,0.8)", width: 1, dash: "dash" },
          },
        ],
      },
    }
  }, [data, medianSurvival])

  return (
    <div className="rounded-xl border border-border bg-card overflow-hidden">
      <div className="flex items-center justify-between px-4 py-3 border-b border-border">
        <div className="flex items-center gap-3">
          <div className="flex items-center justify-center w-8 h-8 rounded-lg bg-primary/10">
            <Activity className="w-4 h-4 text-primary" />
          </div>
          <div>
            <h3 className="text-sm font-medium text-foreground">Kaplan-Meier Survival Curve</h3>
            <p className="text-xs text-muted-foreground">Cohort outcome over time</p>
          </div>
        </div>
      </div>

      <div className="p-4">
        <div className="grid grid-cols-3 gap-4 mb-6">
          <div className="rounded-lg bg-secondary/50 p-3">
            <p className="text-xs text-muted-foreground">Total Patients</p>
            <p className="text-xl font-semibold text-foreground">{totalPatients.toLocaleString()}</p>
          </div>
          <div className="rounded-lg bg-secondary/50 p-3">
            <p className="text-xs text-muted-foreground">Events</p>
            <p className="text-xl font-semibold text-foreground">{totalEvents.toLocaleString()}</p>
          </div>
          <div className="rounded-lg bg-secondary/50 p-3">
            <p className="text-xs text-muted-foreground">Median Survival</p>
            <p className="text-xl font-semibold text-primary">{medianSurvival} days</p>
          </div>
        </div>

        <div className="h-[350px]">
          {figure ? (
            <Plot
              data={Array.isArray(figure.data) ? figure.data : []}
              layout={figure.layout || {}}
              config={{ responsive: true, displaylogo: false, editable: true }}
              style={{ width: "100%", height: "100%" }}
              useResizeHandler
            />
          ) : (
            <div className="h-full rounded-lg border border-dashed border-border text-sm text-muted-foreground flex items-center justify-center">
              No survival data available.
            </div>
          )}
        </div>

        <div className="mt-4 rounded-lg bg-secondary/30 p-3 flex items-start gap-2">
          <Info className="w-4 h-4 text-muted-foreground mt-0.5 shrink-0" />
          <div className="text-xs text-muted-foreground">
            <p>
              <strong className="text-foreground">Interpretation:</strong> Median survival is {medianSurvival} days.
            </p>
            <p className="mt-1">
              Day 30 survival {data.find((d) => d.time === 30)?.survival.toFixed(1) || "-"}% | Day 90 survival{" "}
              {data.find((d) => d.time === 90)?.survival.toFixed(1) || "-"}%
            </p>
          </div>
        </div>
      </div>
    </div>
  )
}
