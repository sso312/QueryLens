# Query Visualization

`query-visualization`은 `text-to-sql`에서 생성한 SQL 실행 결과(`user_query`, `sql`, `rows`)를 입력받아,
시각화 카드(Plotly)와 분석 인사이트를 생성하는 FastAPI 서비스입니다.

현재 파이프라인은 다음 순서로 동작합니다.

1. 입력 `rows`를 DataFrame으로 변환
2. 시간 파생 컬럼(`elapsed_icu_days`, `elapsed_admit_days`) 자동 생성 시도
3. DataFrame 스키마 요약
4. MongoDB Vector Search 기반 RAG 조회 (실패 시 로컬 `data/*.jsonl` fallback)
5. 의도 추출 + 룰 기반 차트 계획
6. Plotly figure 생성
7. 실패 시 relaxed 재시도
8. LLM 인사이트 생성 (실패 시 fallback 문구)

## 1) 폴더 구성

```text
query-visualization/
  src/
    api/server.py                 # /health, /db-test, /visualize
    agent/                        # 분석 오케스트레이션, 룰 엔진, 코드 생성, RAG 조회
    db/                           # Oracle/Mongo 접근, 스키마 요약
    config/                       # Oracle/LLM/RAG 환경변수
    metrics/evaluator.py          # 파이프라인 평가 지표 계산
    models/chart_spec.py          # API 응답 모델
    utils/logging.py              # 구조화 로그 + Mongo(app_events) 적재
  data/
    schema_catalog.json(.jsonl)
    sql_examples.jsonl
    sql_templates.jsonl
    join_templates.jsonl
    glossary_docs.jsonl
    rag_seed.jsonl
    rag_examples.jsonl
    external_rag_docs.jsonl
    table_value_profiles.jsonl
    table_value_profile_summary.json
  scripts/
    build_rag_index.py            # data/*.jsonl -> Mongo 임베딩 적재
    evaluate_pipeline.py          # 평가 실행 스크립트(현재 파일 인코딩/문자열 깨짐 주의)
  deploy/
    docker/Dockerfile.api
    compose/docker-compose.yml
  tests/
    test_api_server.py
    test_analysis_agent.py
    test_pipeline.py
    test_rule_engine.py
    test_code_generator.py
```

## 2) 사전 준비

- Python 3.11+
- Oracle 접근 정보
- OpenAI API Key (의도 추출/인사이트/임베딩)
- MongoDB Atlas (RAG 벡터 검색)
- (옵션) Oracle Thick 모드용 Instant Client

## 3) 환경변수

아래 값은 `query-visualization/.env` 기준으로 준비하면 됩니다.

### 필수

- `ORACLE_USER`
- `ORACLE_PASSWORD`
- `ORACLE_DSN` 또는 (`ORACLE_HOST`, `ORACLE_PORT`, `ORACLE_SERVICE_NAME`)
- `OPENAI_API_KEY`
- `MONGODB_URI`

### 주요 선택값

- `OPENAI_MODEL` (기본: `gpt-4o-mini`)
- `OPENAI_EMBEDDING_MODEL` (기본: `text-embedding-3-small`)
- `OPENAI_EMBEDDING_DIM` (기본: `128`)
- `MONGODB_DB` (기본: `QueryLENs`)
- `MONGODB_COLLECTION` (기본: `sql-to-plot`)
- `MONGODB_VECTOR_INDEX` (기본: `rag_vector_index`)
- `MONGODB_EMBED_FIELD` (기본: `embedding`)
- `RAG_ENABLED` (기본: `true`)
- `RAG_TOP_K` (기본: `6`)
- `RAG_MIN_SCORE` (기본: `0.2`)
- `RAG_CONTEXT_MAX_CHARS` (기본: `4000`)
- `RAG_DOC_VERSION` (기본: `v1`)
- `VIS_MAX_ROWS` (기본: `10000`)
- `VIS_MAX_QUERY_TEXT_LENGTH` (기본: `4000`)
- `VIS_MAX_SQL_TEXT_LENGTH` (기본: `12000`)
- `ORACLE_DRIVER_MODE` (`thin`/`thick`, 기본: `thin`)
- `ORACLE_LIB_DIR` (thick 모드일 때)

## 4) 로컬 실행

```bash
cd query-visualization
python -m venv .venv
. .venv/Scripts/activate  # Windows PowerShell
pip install -r requirements.txt
uvicorn src.api.server:app --host 0.0.0.0 --port 8080 --reload
```

- API 문서: `http://localhost:8080/docs`

## 5) Docker 실행

리포 루트(`c:\final_9team\Final_9team`)에서 실행:

```bash
docker compose -f query-visualization/deploy/compose/docker-compose.yml up -d --build
```

- 컨테이너 포트: `8080`
- 호스트 포트: `${VIS_API_HOST_PORT:-8080}`
- Compose는 `query-visualization/.env`를 읽습니다.

## 6) SQL 결과 행 처리 정책(중요)

현재 구현 기준으로, 시각화 직전 행 처리 방식은 아래와 같습니다.

1. `text-to-sql` 실행기(`text-to-sql/backend/app/services/oracle/executor.py`)는 SQL 결과를 `fetchall()`로 조회합니다.
2. 실행 응답의 `row_cap`은 현재 `None`이며, 결과 rows가 그대로 `/query/run` 응답으로 전달됩니다.
3. UI(`ui/components/views/query-view.tsx`)는 `data.result.rows`를 그대로 `records`로 변환해 `/visualize`에 전달합니다.
4. `query-visualization` API는 입력 rows 전체를 사용하되, `VIS_MAX_ROWS`(기본 10000) 초과 시 `413`으로 거절합니다.
5. 의도 추출은 전달된 DataFrame 전체 스키마를 기준으로 수행합니다.
6. 인사이트 LLM 프롬프트에서만 `df.head(20)` -> 실패 시 `df.head(8)` 샘플을 사용합니다.
7. 참고로 UI 채팅 상태 저장 시에는 `MAX_PERSIST_ROWS=200`으로 잘라 저장하지만, 이는 영속화 용도이며 실시간 실행 경로의 row 제한은 아닙니다.

즉, 현재는 “항상 소량 샘플만으로 의도 추출/시각화” 구조가 아니라,
실행 결과를 최대한 유지한 상태로 시각화 API에 전달하고, API 입력 상한(`VIS_MAX_ROWS`)으로만 제한합니다.

## 7) API 스펙

### `GET /health`

서비스 상태 확인.

### `GET /db-test`

`SELECT * from sso.patients where rownum = 1` 실행 테스트.

주의:
- `sso.patients` 접근 권한/스키마가 없으면 실패합니다.

### `POST /visualize`

요청:

```json
{
  "user_query": "성별 입원 건수 보여줘",
  "sql": "SELECT p.gender, COUNT(*) AS cnt ...",
  "rows": [
    {"gender": "M", "cnt": 511},
    {"gender": "F", "cnt": 489}
  ]
}
```

응답 주요 필드:
- `sql`
- `table_preview` (상위 20행)
- `analyses[]` (`chart_spec`, `figure_json`, `reason`)
- `insight`
- `fallback_used`, `fallback_stage`, `failure_reasons`, `attempt_count`
- `request_id`, `total_latency_ms`, `stage_latency_ms`

제약/오류:
- `ROWS_LIMIT_EXCEEDED` (413)
- `INVALID_ROWS` (422)
- `ANALYSIS_IMPORT_ERROR` (501)
- `DB_TEST_FAILED` (500)

## 8) RAG 인덱싱

`data/*.jsonl` 문서를 임베딩해서 Mongo 컬렉션에 upsert합니다.

```bash
cd query-visualization
python scripts/build_rag_index.py
```

동작 요약:
- 문서 정규화 (`template/example/table_profile/...`)
- 임베딩 생성 (`OPENAI_EMBEDDING_MODEL`, `OPENAI_EMBEDDING_DIM`)
- `metadata.doc_version == RAG_DOC_VERSION` 기존 문서 삭제 후 재적재

Atlas 인덱스 전제:
- DB: `QueryLENs`
- Collection: `sql-to-plot`
- Vector index: `rag_vector_index`
- Field: `embedding`
- Dimensions: `128`

## 9) 테스트

```bash
cd query-visualization
python -m pytest -q
```

포함 테스트:
- API payload 제한 검증
- 분석 파이프라인 기본/빈 결과 fallback
- 룰 엔진 차트 추천
- 코드 생성 결과 유효성

## 10) 운영 로그

`src/utils/logging.py`에서 JSON 로그를 stdout에 남기고,
Mongo 연결 가능 시 `app_events` 컬렉션에도 저장합니다.

인덱스:
- `ts`
- `(event, ts)`
- `(service, ts)`

## 11) 트러블슈팅

- Mongo 연결 실패
  - `MONGODB_URI`, Atlas 네트워크 허용 IP, 사용자 권한 확인
  - 실패해도 RAG는 로컬 `data/*.jsonl` 기반 fallback으로 동작

- Oracle 연결 실패
  - `ORACLE_DSN` 또는 `ORACLE_HOST/PORT/SERVICE_NAME` 확인
  - thick 모드면 `ORACLE_LIB_DIR`와 라이브러리 마운트 확인

- `/db-test`만 실패
  - `sso.patients` 권한/스키마 문제일 수 있음

- `scripts/evaluate_pipeline.py` 실행 오류
  - 현재 파일에 인코딩/문자열 깨짐이 있어 즉시 실행이 어려울 수 있음
  - 필요 시 해당 스크립트를 먼저 복구 후 사용
