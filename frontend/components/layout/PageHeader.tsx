import type { ReactNode } from "react"
import { cn } from "@/lib/utils"

type PageHeaderProps = {
  title: string
  subtitle?: string
  rightSlot?: ReactNode
  icon?: ReactNode
  compact?: boolean
  className?: string
}

export default function PageHeader({ title, subtitle, rightSlot, icon, compact = false, className }: PageHeaderProps) {
  if (compact) {
    return (
      <div className={cn("min-w-0", className)}>
        <div className="flex min-w-0 items-center gap-2">
          {icon ? (
            <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-md bg-secondary text-muted-foreground">
              {icon}
            </span>
          ) : null}
          <div className="min-w-0">
            <h1 className="m-0 truncate text-[15px] font-medium leading-tight text-foreground sm:text-[16px]">{title}</h1>
            {subtitle ? (
              <p className="truncate text-[11px] font-normal leading-[1.25] text-muted-foreground sm:text-[12px]">{subtitle}</p>
            ) : null}
          </div>
        </div>
      </div>
    )
  }

  return (
    <header className={cn("flex items-start justify-between gap-4 border-b border-border bg-card px-8 py-7", className)}>
      <div>
        <h1 className="m-0 text-[28px] font-bold tracking-[-0.3px] text-foreground">{title}</h1>
        {subtitle ? <p className="mt-1.5 text-[15px] font-normal leading-[1.45] text-muted-foreground">{subtitle}</p> : null}
      </div>
      {rightSlot ? <div>{rightSlot}</div> : null}
    </header>
  )
}
