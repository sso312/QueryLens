# Text-to-SQL 데모 기술 문서

이 문서는 Text-to-SQL 데모 스택(RAG + Oracle + UI)의 현재 구현, 아키텍처, 운영 절차를 요약합니다. 개발, 배포, QA에 바로 사용할 수 있도록 독립적으로 이해 가능한 형태로 작성되었습니다.

---

## 1) 시스템 개요

이 시스템은 자연어 질문을 Oracle SQL로 변환하며 다음을 사용합니다:
- RAG 컨텍스트(스키마, 예시, 템플릿, 용어집)
- PolicyGate 안전 검사
- 예산 추적 및 게이팅
- 저비용/즉시 응답을 위한 Demo 캐시
- LLM 생성 및 리뷰 흐름을 포함한 Advanced 모드

상위 흐름:
1) 사용자가 UI(`/ask`)에서 질문
2) Demo 모드: 캐시된 답변이 있으면 즉시 반환
3) Advanced 모드:
   - RAG 컨텍스트 -> LLM이 SQL 생성
   - 사용자 확인(Review 화면)
   - Oracle 실행 (row cap 및 timeout 적용)

---

## 2) 저장소 구조

루트: `text-to-sql/`

- `backend/`
  - FastAPI API, RAG, Oracle 서비스, 정책, 예산, 로깅
- `../ui/`
  - Next.js UI (repo root, 현재 연결 대상)
- `ui/`
  - 레거시 UI (text-to-sql 내부, Ask/Review/Admin)
- `scripts/`
  - 자산 검증, 데모 캐시, 평가 스크립트
- `deploy/`
  - Docker Compose 및 Dockerfiles
- `var/`
  - 런타임 데이터 (metadata, rag, cache, logs, mongo) (gitignored)
- `oracle/`
  - Instant Client (Thick 모드에 필요)

---

## 3) 핵심 서비스

### 3.1 설정(Config)
파일: `backend/app/core/config.py`
- `.env` 로드
- 모델/예산/Oracle/RAG 중앙 설정
- 주요 환경 변수:
  - `DEMO_MODE`, `BUDGET_LIMIT_KRW`, `COST_ALERT_THRESHOLD_KRW`
  - `LLM_COST_PER_1K_TOKENS_KRW`, `SQL_RUN_COST_KRW`
  - `ENGINEER_MODEL`, `EXPERT_MODEL`, `INTENT_MODEL`
  - `CONTEXT_TOKEN_BUDGET`, `EXAMPLES_PER_QUERY`, `TEMPLATES_PER_QUERY`
  - `MAX_RETRY_ATTEMPTS`, `EXPERT_SCORE_THRESHOLD`
  - `ROW_CAP`, `DB_TIMEOUT_SEC`, `MAX_DB_JOINS`
  - `SQL_AUTO_REPAIR_ENABLED`, `SQL_AUTO_REPAIR_MAX_ATTEMPTS`
  - `ORACLE_DSN`, `ORACLE_USER`, `ORACLE_PASSWORD`
  - `ORACLE_DEFAULT_SCHEMA`
  - `ORACLE_LIB_DIR`, `ORACLE_TNS_ADMIN`
  - `RAG_PERSIST_DIR`, `RAG_TOP_K`, `RAG_EMBEDDING_PROVIDER`, `RAG_EMBEDDING_MODEL`, `RAG_EMBEDDING_BATCH_SIZE`, `RAG_EMBEDDING_DIM`
  - `MONGO_URI`, `MONGO_DB`, `MONGO_COLLECTION`, `MONGO_VECTOR_INDEX`
  - `BUDGET_CONFIG_PATH`

### 3.2 Oracle 레이어
파일:
- `backend/app/services/oracle/connection.py`
- `backend/app/services/oracle/executor.py`
- `backend/app/services/oracle/metadata_extractor.py`

핵심 사항:
- 풀 초기화 및 타임아웃 설정
- `ORACLE_LIB_DIR`를 통한 Thick 모드 지원
- `ALL_*` 뷰에서 메타데이터 추출
- 실행 시 강제 사항:
  - `ROW_CAP` (ROWNUM)
  - `DB_TIMEOUT_SEC`
  - SELECT-only 정책
  - `ALTER SESSION SET CURRENT_SCHEMA = ...` 로 `ORACLE_DEFAULT_SCHEMA` 적용

### 3.3 RAG 파이프라인
파일:
- `backend/app/services/rag/mongo_store.py`
- `backend/app/services/rag/indexer.py`
- `backend/app/services/rag/retrieval.py`
- `backend/app/services/runtime/context_budget.py`

핵심 사항:
- 스키마, 용어집, SQL 예시, 조인 템플릿 인덱싱
- MongoDB 저장소 사용, `MONGO_VECTOR_INDEX`가 있으면 벡터 검색
- Mongo 설정이 없으면 SimpleStore로 폴백
- Top-K 검색 + 토큰 예산 기반 컨텍스트 트리밍

### 3.4 에이전트
파일:
- `backend/app/services/agents/sql_engineer.py`
- `backend/app/services/agents/sql_expert.py`
- `backend/app/services/agents/orchestrator.py`

핵심 사항:
- Engineer가 RAG 컨텍스트 기반 SQL 생성
- 위험 점수가 임계값 초과 시 Expert가 보정
- Demo 캐시 경로: `var/cache/demo_cache.json`
- Advanced 모드에서 PolicyGate 사전 검사 적용

### 3.5 정책 & 예산
파일:
- `backend/app/services/policy/gate.py`
- `backend/app/services/cost_tracker.py`
- `backend/app/services/budget_gate.py`

PolicyGate 차단 조건:
- SELECT가 아닌 쿼리
- WHERE 절 누락
- 과도한 JOIN
- 쓰기/변경 쿼리

예산:
- 예산 상태 저장: `var/logs/cost_state.json`
- 런타임 오버라이드 저장: `var/logs/budget_config.json`

---

## 4) API 엔드포인트

### Metadata & RAG
- `POST /admin/metadata/sync`
  - Body: `{ "owner": "SSO" }`
  - Output: schema_catalog + join_graph
- `POST /admin/rag/reindex`
- `GET /admin/rag/status`

### Query 흐름
- `POST /query/oneshot`
  - Body: `{ "question": "..." }`
  - `qid` 및 payload 반환
- `GET /query/get?qid=...`
  - 저장된 payload 반환
- `POST /query/run`
  - Body: `{ "qid": "...", "user_ack": true }`
  - SQL 실행 (PolicyGate 적용)

### 예산 & 시스템
- `GET /admin/budget/status`
- `POST /admin/budget/config`
- `GET /admin/oracle/pool/status`

### 리포트
- `POST /report/evidence`

---

## 5) UI 흐름

### Ask 페이지 (`/ask`)
- Demo 질문 목록은 `/query/demo/questions`에서 로드
- Demo 결과는 캐시된 미리보기 테이블 표시
- Advanced 결과는 "Review & Run" 링크 표시
- 예산 배너에서 사용량 및 임계값 표시

### Review 페이지 (`/review/{qid}`)
- SQL을 검토용으로 표시
- 사용자 정책 확인(`user_ack`) 필요
- SQL 실행 후 결과 테이블 표시
- PolicyGate 에러는 사용자 친화 메시지로 변환

### Admin 페이지 (`/admin`)
- 예산 설정(한도 + 알림)
- 예산 상태(raw)
- RAG 상태
- Oracle 풀 상태

---

## 6) 데이터 자산

`var/metadata`에 생성/관리:
- `schema_catalog.json`
- `join_graph.json`
- `sql_examples.jsonl` (50개 이상 권장)
- `join_templates.jsonl` (5개 이상 권장)
- `sql_templates.jsonl` (선택)
- `glossary_docs.jsonl`
- `demo_questions.jsonl`

캐시:
- `var/cache/demo_cache.json`

로그:
- `var/logs/events.jsonl`
- `var/logs/cost_state.json`
- `var/logs/budget_config.json`

---

## 7) 배포 (Docker Compose)

파일: `deploy/compose/docker-compose.yml`

포트:
- API: `8001` -> 컨테이너 `8000`
- UI: `3000`

Instant Client:
- `oracle/instantclient_23_26`를 `/opt/oracle/instantclient`로 마운트
- `ORACLE_LIB_DIR=/opt/oracle/instantclient`
- `LD_LIBRARY_PATH=/opt/oracle/instantclient`

실행:
```
docker compose -f deploy/compose/docker-compose.yml up -d --build
```

---

## 8) 로컬 개발

Backend:
```
python -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
export PYTHONPATH=$PWD/backend
uvicorn app.main:app --reload --port 8000
```

UI:
```
cd ui
npm install
API_BASE_URL=http://localhost:8001 npm run dev
```

---

## 9) Demo 캐시 생성

```
export PYTHONPATH=$PWD/backend
export LD_LIBRARY_PATH=$PWD/oracle/instantclient_23_26
python scripts/pregen_demo_cache.py
```

결과:
- `var/cache/demo_cache.json`

---

## 10) 평가 / 정확도

스크립트:
- `scripts/eval_questions.py` (질문 -> SQL -> 결과 비교)
- `scripts/eval_report_summary.py` (요약 + CSV)

예시:
```
export PYTHONPATH=$PWD/backend
export DEMO_MODE=false
export LD_LIBRARY_PATH=$PWD/oracle/instantclient_23_26

python scripts/eval_questions.py \
  --input var/metadata/sql_examples.jsonl \
  --output var/logs/eval_report.jsonl \
  --ignore-order \
  --max 50

python scripts/eval_report_summary.py \
  --input var/logs/eval_report.jsonl \
  --csv var/logs/eval_report.csv
```

---

## 11) 트러블슈팅

### DPY-4011
- Thin 모드가 차단된 경우(NNE 활성화)
- Instant Client로 Thick 모드 사용

### DPI-1047 / libaio.so.1
- 호스트 런타임 라이브러리 누락
- `libaio1` 설치 또는 `libaio.so.1t64`를 `libaio.so.1`로 링크

### ORA-00942
- 스키마/소유자 오류 또는 권한 부족
- `ORACLE_DEFAULT_SCHEMA` 및 권한 확인

### PolicyGate 오류
- WHERE 필요
- JOIN 과다
- SELECT only
- 쓰기 쿼리 차단

---

## 12) 보안 & 안전

- `.env`는 gitignored
- PolicyGate가 위험한 SQL 차단
- Row cap + timeout 적용
- 예산 게이트가 한도 초과 요청 차단

---

## 13) 다음 단계 (선택)

- 평가 스크립트용 CI 파이프라인 추가
- 용어집 및 예시 확장
- 고급 UI 필터 및 차트 요약 추가
