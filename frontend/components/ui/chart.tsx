"use client"

import * as React from "react"

import { cn } from "@/lib/utils"

export type ChartConfig = {
  [k in string]: {
    label?: React.ReactNode
    icon?: React.ComponentType
  } & (
    | { color?: string; theme?: never }
    | { color?: never; theme: Record<"light" | "dark", string> }
  )
}

type ChartContextProps = {
  config: ChartConfig
}

const ChartContext = React.createContext<ChartContextProps | null>(null)

function useChart() {
  const context = React.useContext(ChartContext)
  if (!context) {
    throw new Error("useChart must be used within a <ChartContainer />")
  }
  return context
}

function ChartContainer({
  className,
  children,
  config,
  ...props
}: React.ComponentProps<"div"> & {
  config: ChartConfig
}) {
  return (
    <ChartContext.Provider value={{ config }}>
      <div data-slot="chart" className={cn("flex aspect-video justify-center text-xs", className)} {...props}>
        {children}
      </div>
    </ChartContext.Provider>
  )
}

function ChartTooltip() {
  return null
}

function ChartTooltipContent() {
  return null
}

function ChartLegend() {
  return null
}

function ChartLegendContent() {
  return null
}

export {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  ChartLegend,
  ChartLegendContent,
  useChart,
}
