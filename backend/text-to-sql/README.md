# Text-to-SQL

`text-to-sql`은 자연어 질문을 Oracle SQL로 변환하고, 실행 결과를 반환하는 FastAPI 서비스입니다.
운영 경로에는 RAG 문맥 조회, SQL 정책 검사, 자동 보정, 코호트/PDF 분석 기능이 포함됩니다.

현재 파이프라인은 다음 순서로 동작합니다.

1. 사용자 질문 입력 수신 (`/query/oneshot`)
2. 스키마/예시/RAG 문서 기반 문맥 구성
3. SQL 생성 (planner/intent guard 포함)
4. 정책 검사 후 Oracle 실행 (`/query/run`)
5. 오류/무결과 상황에서 템플릿 또는 LLM 기반 SQL 보정
6. 실행 결과 + 후속 응답/추천 질문 생성 (`/query/answer`)
7. 이벤트/audit 로그 저장 (Mongo 우선, 실패 시 파일 fallback)

## 1) 폴더 구성

```text
text-to-sql/
  backend/
    app/
      main.py                      # FastAPI 앱 엔트리
      api/routes/                  # query, cohort, pdf, audit, admin 라우트
      core/                        # 환경설정/경로 유틸
      services/
        agents/                    # NL2SQL 오케스트레이션
        oracle/                    # 연결 풀, 실행기, 메타데이터 추출
        rag/                       # RAG 인덱싱/조회
        policy/                    # SQL 실행 정책 게이트
        runtime/                   # 사용자별 설정/상태 저장
        logging_store/             # 이벤트 로그 저장소
        pdf_service.py             # PDF 코호트 분석 서비스
    requirements.txt
  scripts/                         # 메타데이터/RAG 관리 스크립트
  tests/                           # 핵심 정책/컴파일러 테스트
  var/
    metadata/                      # schema_catalog, sql_examples 등
    logs/                          # events, cost_state 등
    cache/                         # demo cache
  docs/
```

## 2) 사전 준비

- Python 3.11+
- Oracle DB 접속 정보
- OpenAI API Key
- MongoDB (RAG/로그 저장 시 권장)
- (옵션) Oracle Thick 모드용 Instant Client

## 3) 환경변수

아래 값은 `backend/text-to-sql/.env` 기준으로 준비하면 됩니다.

### 필수

- `ORACLE_USER`
- `ORACLE_PASSWORD`
- `ORACLE_DSN` 또는 (`ORACLE_HOST`, `ORACLE_PORT`, `ORACLE_SERVICE_NAME`)
- `OPENAI_API_KEY`

### 주요 선택값

- 모델/생성
  - `ENGINEER_MODEL` (기본: `gpt-4o`)
  - `EXPERT_MODEL` (기본: `gpt-4o`)
  - `PLANNER_MODEL` (기본: `EXPERT_MODEL`)
  - `LLM_TIMEOUT_SEC` (기본: `30`)
- SQL 실행/보정
  - `ROW_CAP` (기본: `5000`)
  - `DB_TIMEOUT_SEC` (기본: `180`)
  - `DB_TIMEOUT_SEC_ACCURACY` (기본: `180`)
  - `API_REQUEST_TIMEOUT_SEC` (기본: `190`)
  - `SQL_AUTO_REPAIR_ENABLED` (기본: `true`)
  - `SQL_AUTO_REPAIR_MAX_ATTEMPTS` (기본: `1`)
  - `SQL_ZERO_RESULT_REPAIR_ENABLED` (기본: `false`)
- Oracle 드라이버
  - `ORACLE_DRIVER_MODE` (`thin`/`thick`/`auto`, 기본: `thin`)
  - `ORACLE_LIB_DIR` (thick 모드일 때)
  - `ORACLE_TNS_ADMIN`
  - `ORACLE_SSL_MODE` (`disable`/`require`/`verify-ca`/`verify-full`)
  - `ORACLE_DEFAULT_SCHEMA`
- RAG/Mongo
  - `RAG_TOP_K` (기본: `8`)
  - `RAG_RETRIEVAL_MODE` (기본: `bm25_then_rerank`)
  - `MONGO_URI`
  - `MONGO_DB` (기본: `text_to_sql`)
  - `MONGO_COLLECTION` (기본: `rag_docs`)
  - `MONGO_VECTOR_INDEX` (기본: `rag_vector_index`)

## 4) 로컬 실행

```bash
cd backend/text-to-sql
python -m venv .venv
. .venv/Scripts/activate  # Windows PowerShell
pip install -r backend/requirements.txt
$env:PYTHONPATH="backend"
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

- API 문서: `http://localhost:8000/docs`

## 5) Docker 실행

리포 루트(`c:\final_9team`)에서 실행:

```bash
docker build -f docker/text-sql.api.Dockerfile -t querylens-text-sql .
docker run --rm -p 8000:8000 --env-file backend/text-to-sql/.env querylens-text-sql
```

- 컨테이너 내부 포트: `8000`
- thick 모드 사용 시 Instant Client 경로를 `ORACLE_LIB_DIR`와 볼륨 마운트로 맞춰야 합니다.

## 6) SQL 실행 정책(중요)

현재 구현 기준으로, `/query/run` 실행 정책은 아래와 같습니다.

1. 읽기 전용 SQL만 허용 (`SELECT`, `WITH ... SELECT`)
2. 정책 게이트(`precheck_sql`) 통과 후 Oracle 실행
3. `ROW_CAP`이 설정된 경우 `fetchmany(row_cap + 1)`로 상한을 강제
4. 상한 도달 시 응답에 `row_cap`을 포함해 절단 여부를 명시
5. 실행 오류 시 템플릿 보정 -> LLM 보정 순서로 재시도(설정값 기반)
6. `ORA-00942` 발생 시 기본 스키마/메타데이터 owner 기준 fallback 재시도

## 7) API 스펙

### `GET /health`

서비스 상태 확인.

### Query

- `POST /query/oneshot`: 자연어 -> SQL 초안/최종안 생성
- `POST /query/run`: SQL 실행 (필수: `user_ack=true`)
- `POST /query/answer`: 결과 테이블 기반 한줄 답변/후속 질문 생성
- `POST /query/transcribe`: 음성 질문 텍스트 변환
- `GET /query/get?qid=...`: 저장된 쿼리 payload 조회
- `GET /query/demo/questions`: 데모 질문 목록 조회

### Cohort / PDF

- `POST /cohort/simulate`
- `POST /cohort/sql`
- `POST /cohort/pdf/generate-sql`
- `POST /cohort/pdf/confirm`
- `GET/POST/PATCH/DELETE /cohort/library...`
- `POST /pdf/upload`
- `GET /pdf/status/{task_id}`
- `GET /pdf/history`

### Admin / Audit / Dashboard

- `GET /admin/oracle/pool/status`
- `GET/POST /admin/settings/connection`
- `GET/POST /admin/settings/table-scope`
- `POST /admin/metadata/sync`
- `GET /admin/metadata/tables`
- `POST /admin/rag/reindex`
- `GET /admin/rag/status`
- `GET /audit/logs`, `DELETE /audit/logs/{log_id}`
- `GET/POST /dashboard/queries`, `POST /dashboard/saveQuery`, `POST /dashboard/queryBundles`

## 8) 메타데이터/RAG 정합성 관리

핵심 데이터는 `var/metadata`에 저장되며, 운영 중에는 아래 순서로 관리합니다.

1. Oracle 메타데이터 동기화
   - API: `POST /admin/metadata/sync` (`owner` 전달)
2. RAG 재인덱싱
   - API: `POST /admin/rag/reindex`
3. 오프라인 점검 스크립트
   - `python scripts/clean_rag_metadata.py`
   - `python scripts/validate_assets.py`
   - `python scripts/validate_index.py`
   - `python scripts/build_table_value_profiles.py --metadata-dir var/metadata --output var/metadata/table_value_profiles.jsonl`
