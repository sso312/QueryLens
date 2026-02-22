# Text-to-SQL 서비스 UI 요구사항정의서 (현행 구현 기준)

## 1. 문서 정보
- 문서명: Text-to-SQL 서비스 UI 요구사항정의서
- 기준일: 2026-02-10
- 기준 UI 코드: `/home/min/final/ui`
- 기준 API 코드: `/home/min/final/text-to-sql/backend/app/api/routes`
- 작성 목적: 현재 구현된 UI를 기준으로 기능/비기능 요구사항을 명확히 정의

## 2. 목적 및 범위
- 자연어 질의를 SQL로 변환하여 의료 데이터 조회/분석을 수행하는 UI를 제공한다.
- 사용자 인증, DB 연결/권한 설정, 컨텍스트 관리, 쿼리 실행/검증, 결과 보드, 감사 로그, 코호트 시뮬레이션 기능을 포함한다.
- 본 문서는 "신규 기획안"이 아니라 "현행 구현"을 기준으로 한다.

## 3. 사용자 및 권한
- 연구원
  - 쿼리/분석, 결과 보드, 감사 로그 조회, 코호트 분석 수행
- 관리자
  - 연구원 기능 + 컨텍스트 편집, DB 연결/테이블 스코프 설정

참고:
- 로그인 계정은 프론트 데모 계정 기반이며 세션은 `localStorage`에 저장된다.

## 4. 화면 구성
- 로그인: `/login`
- 메인 앱: `/`
  - 사이드바 메뉴
    - DB 연결
    - 컨텍스트
    - 쿼리
    - 대시보드
    - 감사 로그
    - 코호트
  - 상단 헤더
    - 현재 뷰 타이틀
    - 테마 전환
    - 사용자 프로필/로그아웃
    - 모바일 메뉴 버튼

## 5. 기능 요구사항

| ID | 요구사항 | 수용 기준 |
|---|---|---|
| FR-001 | 로그인 시 아이디/비밀번호 인증을 수행한다. | 유효 계정은 `/` 이동, 실패 시 오류 메시지 노출 |
| FR-002 | 비로그인 상태에서 메인 접근 시 로그인 화면으로 이동한다. | `/` 접근 시 `/login` 리다이렉트 |
| FR-003 | 로그인 사용자 정보를 브라우저에 저장/복원한다. | 새로고침 후 로그인 상태 유지 |
| FR-004 | 테마(라이트/다크/시스템) 설정 및 영속 저장을 지원한다. | 재접속 후 기존 테마 유지 |
| FR-101 | DB 연결 상태를 조회한다. | `/admin/oracle/pool/status` 결과를 상태 카드에 표시 |
| FR-102 | 연결 설정(host/port/db/user/password) 조회/저장을 지원한다. | GET/POST `/admin/settings/connection` 성공 |
| FR-103 | 테이블 스코프 조회/저장을 지원한다. | GET/POST `/admin/settings/table-scope` 성공 |
| FR-104 | 메타데이터 기반 테이블 목록을 표시한다. | `/admin/metadata/tables` 응답 렌더링 |
| FR-105 | 테이블 선택은 개별/전체 선택과 페이지 이동을 지원한다. | 체크박스/페이지 버튼 동작 정상 |
| FR-201 | 조인/지표/용어 문서를 로드한다. | `/admin/rag/docs` 기반 목록 표시 |
| FR-202 | 조인 템플릿 추가/수정/삭제를 지원한다. | UI 조작 후 상태 반영 |
| FR-203 | 지표 템플릿 추가/수정/삭제를 지원한다. | UI 조작 후 상태 반영 |
| FR-204 | 용어 사전 추가/수정/삭제/검색을 지원한다. | 검색어 필터링 동작 |
| FR-205 | 컨텍스트 저장 시 RAG 재색인을 수행한다. | POST `/admin/rag/context` 성공 및 완료 메시지 |
| FR-301 | 사용자별 대화 이력을 복원한다. | GET `/chat/history`로 상태 복원 |
| FR-302 | 대화 상태를 저장한다. | POST `/chat/history`로 디바운스 저장 |
| FR-303 | 빠른 질문 목록을 로딩하고 즉시 실행한다. | GET `/query/demo/questions` 결과 사용 |
| FR-304 | 자연어 질문 One-shot 실행을 지원한다. | POST `/query/oneshot` 성공 시 모드별 처리 |
| FR-305 | Clarify 모드에서 추가 질문/추천 응답을 제시한다. | 대화창에 clarify 메시지와 제안 표시 |
| FR-306 | Demo 모드에서 SQL/미리보기/요약/소스를 표시한다. | 결과 패널에 표와 요약 표시 |
| FR-307 | Advanced 모드에서 SQL 실행 결과를 표시한다. | POST `/query/run` 성공 시 결과 표출 |
| FR-308 | 기술 모드에서 SQL 검증/편집/재실행을 지원한다. | 검증 카드, SQL 편집, 실행 버튼 동작 |
| FR-309 | 결과 CSV 다운로드를 지원한다. | 결과 테이블을 CSV로 저장 |
| FR-310 | 결과를 결과 보드에 저장한다. | GET/POST `/dashboard/queries`로 반영 |
| FR-311 | 대화 초기화 기능을 제공한다. | 화면 상태 및 서버 저장 상태 초기화 |
| FR-401 | 결과 보드 조회/표시를 지원한다. | GET `/dashboard/queries` 렌더링 |
| FR-402 | 결과 보드 검색/카테고리 필터를 지원한다. | 검색/카테고리 조건으로 필터링 |
| FR-403 | 고정/복제/삭제/공유를 지원한다. | 항목 액션 후 목록 반영 |
| FR-404 | 결과 보드 변경사항 자동 저장을 지원한다. | POST `/dashboard/queries` 디바운스 저장 |
| FR-501 | 감사 로그 목록/통계를 조회한다. | GET `/audit/logs` 데이터 표시 |
| FR-502 | 감사 로그 검색/기간/사용자 필터를 지원한다. | 조건 기반 필터링 결과 표시 |
| FR-503 | 감사 로그 CSV/JSONL 내보내기를 지원한다. | 필터 결과 파일 다운로드 |
| FR-504 | 감사 로그 상세(SQL/적용 항목/스냅샷)를 조회한다. | 로그 확장 시 상세 표시 |
| FR-601 | 코호트 시뮬레이션 조건 편집 및 실행을 지원한다. | POST `/cohort/simulate` 결과 반영 |
| FR-602 | 생존곡선/신뢰도/서브그룹 분석을 표시한다. | 차트/표/인사이트 렌더링 |
| FR-603 | 코호트 SQL 조회/복사를 지원한다. | POST `/cohort/sql` 결과 다이얼로그 표시 |
| FR-604 | 코호트 저장/조회/삭제를 지원한다. | GET/POST/DELETE `/cohort/saved` 동작 |
| FR-605 | 저장 코호트 재분석을 지원한다. | 저장 파라미터 재적용 후 시뮬레이션 수행 |
| FR-606 | 저장 코호트 JSON 내보내기를 지원한다. | 단일 코호트 JSON 다운로드 |

## 6. 비기능 요구사항
- NFR-001 반응형 UI
  - 데스크톱: 고정 사이드바
  - 모바일: 시트 기반 사이드 메뉴
- NFR-002 API 라우팅
  - Next.js rewrites로 `/query`, `/admin`, `/audit`, `/chat`, `/dashboard`, `/cohort` 프록시 지원
- NFR-003 오류 처리
  - API 실패/타임아웃 시 사용자 메시지 제공
  - 주요 화면은 오류 발생 시에도 사용 가능한 상태 유지
- NFR-004 상태 지속성
  - 인증/테마/대화상태/결과보드 상태를 재접속 시 복원 가능해야 함
- NFR-005 보안/정책 가시성
  - Read-Only 모드, 정책 검증, 위험 점수 정보를 UI에서 확인 가능해야 함

## 7. 외부 인터페이스 요구사항(API)
- Query
  - `POST /query/oneshot`
  - `POST /query/run`
  - `GET /query/demo/questions`
- Chat
  - `GET /chat/history`
  - `POST /chat/history`
- Dashboard
  - `GET /dashboard/queries`
  - `POST /dashboard/queries`
- Audit
  - `GET /audit/logs`
- Cohort
  - `POST /cohort/simulate`
  - `POST /cohort/sql`
  - `GET /cohort/saved`
  - `POST /cohort/saved`
  - `DELETE /cohort/saved/{cohort_id}`
- Admin
  - `GET /admin/oracle/pool/status`
  - `GET/POST /admin/settings/connection`
  - `GET/POST /admin/settings/table-scope`
  - `GET /admin/metadata/tables`
  - `GET /admin/rag/docs`
  - `POST /admin/rag/context`

## 8. 현행 기준 보완 필요사항
- 관리자 전용 표기는 존재하나 화면 레벨 강제 권한제어(RBAC)는 미구현.
- 결과 보드의 `실행`, `스케줄 설정`은 UI 항목 중심이며 실행 로직은 제한적.
- 쿼리 화면 `차트` 탭은 자동 시각화 연동이 제한됨.
- 감사 로그의 결과 스냅샷 다운로드 버튼은 연동 범위가 제한적일 수 있음.
- 로그인은 운영형 IAM 연동이 아닌 데모 계정 방식.
