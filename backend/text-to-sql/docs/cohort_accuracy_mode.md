# PDF Cohort Accuracy Mode

## 목적
- PDF 코호트 분석 시 속도보다 정확도를 우선합니다.
- SQL 직접 생성(LLM free-form)을 차단하고 `CohortSpec -> Intent -> Compiler` 경로를 강제합니다.

## 활성화
- `.env`에 `PDF_ACCURACY_MODE=true` 설정
- 또는 업로드 API 호출 시 `accuracy_mode=true` 쿼리 파라미터 전달

## 동작 요약
1. 스니펫 추출 강화 (표/방법론 키워드 확장, span/page 보존)
2. CohortSpec 생성 + evidence 보강
3. CohortSpec critic(LLM)로 누락/의미 붕괴 점검
4. evidence 없는 조건은 ambiguity로 승격
5. unresolved ambiguity가 있으면 SQL 컴파일 전 중단 (`needs_user_input`)
6. SchemaMap 필수 항목 검증 (미충족 시 중단)
7. CohortSpec 기반 Intent 생성 후 템플릿 컴파일
8. validator invariant 수행 후 통과 시에만 `completed`

## Timeout 정책
- `accuracy_mode=true` 실행 쿼리는 Oracle `call_timeout`을 최소 `180000ms`로 적용합니다.
- API 미들웨어 timeout은 `API_REQUEST_TIMEOUT_SEC`(최소 190초) 기준으로 동작합니다.
- 프록시/서버(gunicorn/nginx) timeout이 더 짧으면 먼저 끊기므로 동일하게 190초 이상으로 맞춰야 합니다.

## SQL 성능 가드레일
- `measurement_required`는 상관 `EXISTS` 대신 `meas_ok` CTE(사전 후보 축소 + `JOIN` + `GROUP BY/HAVING`)로 컴파일합니다.
- 정확도 모드 기본 population 정책:
  - `require_icu=true`
  - `episode_selector=first`
  - `episode_unit=per_subject`
  - measurement window: `outtime-24h ~ outtime`

## 주요 invariant
- `require_icu=true` -> 결과 `stay_id IS NULL` 금지
- `episode_selector=first` -> key 중복 금지
- `icu_los_min_days` 위반 금지
- `death_within_days`는 event-to-event 비교 검증
- `measurement_required`는 signal별 window 내 측정 존재 검증

## 응답 필드
- `result.accuracy_mode`
- `result.cohort_spec`
- `result.validation_report`
- `result.accuracy_report`
- `result.next_action` (모호성 해결/SchemaMap 보강 필요 시)
