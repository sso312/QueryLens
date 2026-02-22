import type { ViewType } from "@/components/app-sidebar"

export const VIEW_META: Record<ViewType, { title: string; subtitle?: string }> = {
  connection: {
    title: "DB 연결",
    subtitle: "데이터베이스 연결을 구성하고 접근 권한을 관리합니다.",
  },
  query: {
    title: "채팅",
    subtitle: "자연어 질문을 SQL로 변환하고 실행 결과를 확인합니다.",
  },
  dashboard: {
    title: "대시보드",
    subtitle: "쿼리 결과를 보드 형태로 시각화합니다.",
  },
  audit: {
    title: "감사 로그",
    subtitle: "의사결정 근거와 실행 이력을 추적합니다.",
  },
  cohort: {
    title: "단면 연구 집단",
    subtitle: "가상 코호트를 생성하고 조건 변경 결과를 시뮬레이션합니다.",
  },
  "pdf-cohort": {
    title: "PDF 코호트 분석",
    subtitle: "논문 기반 코호트를 추출해 분석 흐름을 구성합니다.",
  },
}
