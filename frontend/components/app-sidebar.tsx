"use client"

import { cn } from "@/lib/utils"
import Image from "next/image"
import {
  Database,
  // Settings2,
  MessageSquare,
  LayoutDashboard,
  FileText,
  ChevronLeft,
  ChevronRight,
  Users
} from "lucide-react"
import { Button } from "@/components/ui/button"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"

export type ViewType = "connection" | "query" | "dashboard" | "audit" | "cohort" | "pdf-cohort"

interface AppSidebarProps {
  currentView: ViewType
  onViewChange: (view: ViewType) => void
  collapsed: boolean
  onToggleCollapse: () => void
}

const navItems = [
  { id: "connection" as const, label: "DB 연결", icon: Database },
  // { id: "context" as const, label: "컨텍스트", icon: Settings2, description: "관리자 설정" },
  { id: "query" as const, label: "채팅", icon: MessageSquare },
  { id: "dashboard" as const, label: "대시보드", icon: LayoutDashboard },
  { id: "audit" as const, label: "감사 로그", icon: FileText },
  { id: "pdf-cohort" as const, label: "PDF 코호트 분석", icon: FileText },
  { id: "cohort" as const, label: "단면 연구 집단", icon: Users },
]

export function AppSidebar({ currentView, onViewChange, collapsed, onToggleCollapse }: AppSidebarProps) {
  return (
    <TooltipProvider delayDuration={0}>
      <aside className={cn(
        "flex flex-col h-screen bg-card border-r border-border transition-all duration-300",
        collapsed ? "w-16" : "w-56"
      )}>
        {/* Logo */}
        <div className={cn(
          "flex items-center h-16 border-b border-border px-4",
          collapsed ? "justify-center" : "gap-1"
        )}>
          <div className={cn("flex items-center justify-center shrink-0", collapsed ? "w-8 h-8" : "w-9 h-9")}>
            <Image
              src="/query-lens-logo-v2.svg"
              alt="Query LENs"
              width={collapsed ? 32 : 36}
              height={collapsed ? 32 : 36}
              priority
            />
          </div>
          {!collapsed && (
            <div className="overflow-hidden">
              <h1 className="text-sm font-semibold text-foreground truncate">Query LENs</h1>
              <p className="text-[10px] text-muted-foreground truncate">NL2SQL 플랫폼</p>
            </div>
          )}
        </div>

        {/* Main Navigation */}
        <nav className="flex-1 p-2 space-y-1">
          {navItems.map((item) => {
            const isActive = currentView === item.id
            const button = (
              <button
                key={item.id}
                onClick={() => onViewChange(item.id)}
                className={cn(
                  "flex items-center w-full rounded-lg transition-colors",
                  collapsed ? "justify-center p-3" : "gap-3 px-3 py-2.5",
                  isActive
                    ? "bg-primary text-primary-foreground"
                    : "text-muted-foreground hover:bg-secondary hover:text-foreground"
                )}
              >
                <item.icon className={cn("shrink-0", collapsed ? "w-5 h-5" : "w-4 h-4")} />
                {!collapsed && (
                  <span className="text-[15px] font-medium leading-[1.2] text-left truncate">{item.label}</span>
                )}
              </button>
            )

            if (collapsed) {
              return (
                <Tooltip key={item.id}>
                  <TooltipTrigger asChild>{button}</TooltipTrigger>
                  <TooltipContent side="right">
                    <span className="font-medium">{item.label}</span>
                  </TooltipContent>
                </Tooltip>
              )
            }
            return button
          })}
        </nav>

        {/* Collapse Toggle */}
        <div className="p-2 border-t border-border">
          <Button
            variant="ghost"
            size="sm"
            onClick={onToggleCollapse}
            className={cn("w-full", collapsed ? "justify-center" : "justify-start gap-2")}
          >
            {collapsed ? <ChevronRight className="w-4 h-4" /> : <ChevronLeft className="w-4 h-4" />}
            {!collapsed && <span className="text-xs">접기</span>}
          </Button>
        </div>
      </aside>
    </TooltipProvider>
  )
}
