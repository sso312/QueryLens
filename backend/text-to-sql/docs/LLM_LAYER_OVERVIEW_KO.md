# LLM 레이어 개념 정리 (text-to-sql)

이 문서는 text-to-sql에서 **LLM이 쓰이는 지점**을 레이어별로 정리한 개념 문서입니다. 코드 기준 경로와 설정 위치를 함께 명시합니다. (개인용 문서)

---

**1. 전체 파이프라인 요약**

요약 흐름:

```
Input Layer(질문 입력/검증)
→ Context Layer(위험도 분류 + RAG 컨텍스트)
→ Reasoning Layer(LLM Engineer → 조건부 Expert)
→ Execution Layer(SQL 보정/정책/DB 실행)
→ Output Layer(응답/로그/비용 반영)
```

**1.1 순서(상세)**

1. Input Layer: `/query/oneshot` 요청 수신, 기본 검증, 질의 정규화 준비
2. Context Layer: 위험도/복잡도 분류 + RAG 문서 검색으로 컨텍스트 구성
3. Reasoning Layer: SQL Engineer가 초안 생성 → 위험도 기준 충족 시 Expert가 보정
4. Execution Layer: SQL 후처리 + 정책 검사 + DB 실행
5. Output Layer: 결과 응답 패키징 + 비용/이벤트 로그 반영

**1.2 전체 다이어그램(개념)**

```
[Input]
  │  query.py / orchestrator._normalize_question
  ▼
[Context]
  │  risk_classifier.py + context_builder.py + rag/*
  ▼
[Reasoning]
  │  sql_engineer.generate_sql()
  ├─ (risk >= threshold) → sql_expert.review_sql()
  ▼
[Execution]
  │  sql_postprocess.py → policy.gate.py → oracle.executor.py
  ▼
[Output]
     response payload + cost_tracker
```

**2. 레이어 상세(순서대로)**

**2.1 Input Layer**

역할: 사용자의 질문을 API에서 받아 최소 검증 후 파이프라인으로 전달합니다. 데모 모드에서는 질문을 정규화해 캐시와 매칭하고, 매칭되면 LLM 호출 없이 즉시 반환합니다.

주요 작업:
- 요청 스키마 검증
- 데모 캐시 조회(정규화된 질문 기준)
- 질의 정규화(공백/특수문자 제거 등) 준비

**oneshot 이란?**
- 의미: 질문 → SQL 생성까지를 **한 번의 호출**로 처리하는 단일 엔드포인트
- 엔드포인트: `POST /query/oneshot`
- 동작: `run_oneshot(question)` 실행 → 컨텍스트 구성 → Engineer/Expert 호출 → `qid`와 `payload` 반환
- 이후 단계: 실제 DB 실행은 `/query/run`에서 `qid` 또는 `sql`로 수행

**/query/oneshot vs /query/run (흐름 비교 다이어그램)**

```
/query/oneshot
  └─ question 입력
     └─ run_oneshot()
        ├─ Context 구성
        ├─ Engineer(필요 시 Expert)
        └─ qid + payload 반환

/query/run
  └─ qid 또는 sql 입력 + user_ack=true
     ├─ policy precheck
     ├─ execute_sql
     └─ 결과 반환
```

**/query/run에서 `user_ack`가 필요한 이유**
- 의미: 사용자가 **실제 DB 실행을 명시적으로 승인**했다는 확인 플래그입니다.
- 목적: LLM이 생성한 SQL을 자동 실행하지 않고, **사용자 확인 후 실행**하는 안전장치입니다.
- 구현: `backend/app/api/routes/query.py`의 `run_query()`에서 `user_ack`가 없으면 400 에러 반환.

**Demo 모드에서 oneshot 동작**
- 조건: `DEMO_MODE=true`
- 동작: `demo_cache.json`에 있는 질문이면 **LLM 호출 없이 즉시 응답**합니다.
- 매칭 방식: 질문 원문 매칭 → 실패 시 정규화된 질문으로 재매칭
- 구현: `backend/app/services/agents/orchestrator.py`의 `_load_demo_cache()` / `_normalize_question()` / `run_oneshot()`

**demo_cache.json 구성 방식**
- 생성 스크립트: `scripts/pregen_demo_cache.py`
- 입력:
  - `var/metadata/demo_questions.jsonl` (데모 질문 리스트)
  - `var/metadata/sql_examples.jsonl` (LLM 실패 시 fallback SQL)
- 생성 로직 요약:
  1) 각 질문에 대해 `run_oneshot(..., skip_policy=True)` 실행  
  2) LLM 결과 SQL을 실행해 preview 생성  
  3) 실패 시 예제 SQL로 fallback 후 preview 생성  
  4) `var/cache/demo_cache.json`에 `{question: {sql, preview, summary, source}}` 형태로 저장
- 파일 위치: `var/cache/demo_cache.json`

구현: `backend/app/api/routes/query.py`, `backend/app/services/agents/orchestrator.py`의 `_normalize_question`

LLM 사용: 없음

흐름도:
```
[HTTP 요청]
  ▼
[query.py: oneshot]
  │  요청 바디 검증
  ▼
[orchestrator._normalize_question]
  │  데모 캐시 매칭 준비
  ▼
[run_oneshot 호출]
```

**2.2 Context Layer**

역할: 질문을 해석할 재료를 모으는 단계입니다. 위험도/복잡도를 계산해 Expert 호출 여부를 결정하고, RAG 검색으로 스키마/예시/템플릿/용어를 모아 컨텍스트를 구성합니다.

주요 작업:
- 위험도 분류
- RAG 검색
- 컨텍스트 예산 적용(길이 초과 시 우선순위대로 잘림)

구현: `backend/app/services/runtime/risk_classifier.py`, `backend/app/services/runtime/context_builder.py`, `backend/app/services/rag/*`

LLM 사용: 없음

흐름도:
```
[question]
  ▼
[risk_classifier.classify]
  │  risk/complexity 계산
  ▼
[context_builder.build_context_payload]
  ▼
[rag.retrieval.build_candidate_context]
  │  schema/example/template/glossary 검색
  ▼
[context_budget.trim_context_to_budget]
  ▼
[context payload 반환]
```

**2.2.1 스키마 검색(Context Layer) 상세**

스키마 검색은 Context Layer에서 수행됩니다. 입력 질문을 기준으로 **RAG 인덱스에서 `type = schema` 문서만 검색**해 `context.schemas`로 전달합니다.

스키마 검색 흐름도(개념):
```
[Question]
  │  run_oneshot(question)
  ▼
[Context Builder]
  │  build_context_payload(question)
  ▼
[RAG Retrieval]
  │  build_candidate_context(question)
  ├─ store.search(type=schema, k=RAG_TOP_K)
  ├─ store.search(type=example, k=EXAMPLES_PER_QUERY)
  ├─ store.search(type=template, k=TEMPLATES_PER_QUERY)
  └─ store.search(type=glossary, k=RAG_TOP_K)
  ▼
[Context Budget Trim]
  │  context_budget.trim_context_to_budget(...)
  │  (우선순위: examples → templates → schemas → glossary)
  ▼
[LLM Input Context]
  │  context.schemas 포함 → SQL Engineer로 전달
```

스키마 검색 설명:
- 스키마 문서는 `schema_catalog.json`을 기반으로 생성됩니다.
- `indexer._schema_docs()`가 테이블/컬럼/PK 정보를 텍스트로 직렬화합니다.
- 예산 초과 시 **examples → templates → schemas → glossary** 순서로 잘립니다.

**2.3 Reasoning Layer**

역할: 실제 SQL을 “생성/검토”하는 단계입니다. Engineer가 1차 SQL을 만들고, 위험도가 높으면 Expert가 더 안전하게 다듬습니다.

주요 작업:
- LLM 호출
- JSON 응답 파싱
- 사용량(토큰) 기록

구현: `backend/app/services/agents/sql_engineer.py`, `backend/app/services/agents/sql_expert.py`

모델: `ENGINEER_MODEL`, `EXPERT_MODEL`

호출 조건: `risk >= EXPERT_SCORE_THRESHOLD`일 때 Expert 수행

흐름도:
```
[context + question]
  ▼
[sql_engineer.generate_sql]
  │  LLM 호출 (Engineer)
  ▼
[조건부 Expert]
  │  risk >= threshold
  ▼
[sql_expert.review_sql]
  │  LLM 호출 (Expert)
  ▼
[final payload]
```

**2.3.1 LLM 호출 진입점**

- `sql_engineer.generate_sql()`
  - 입력: `question`, `context`
  - 프롬프트: `ENGINEER_SYSTEM_PROMPT` (`backend/app/services/agents/prompts.py`)
  - 출력: JSON 형태(`final_sql`, `warnings` 등)

- `sql_expert.review_sql()`
  - 입력: `question`, `context`, `draft`
  - 프롬프트: `EXPERT_SYSTEM_PROMPT` (`backend/app/services/agents/prompts.py`)
  - 출력: JSON 형태(보정 SQL)

- 공통 클라이언트
  - 구현: `backend/app/services/agents/llm_client.py`
  - 방식: OpenAI `chat.completions.create()`

**2.4 Execution Layer**

역할: LLM 결과를 실제 실행 가능한 SQL로 정제하고, 위험한 쿼리를 막고, DB에서 실행합니다.

주요 작업:
- Oracle 문법/스키마 보정
- 정책 검사(SELECT 제한 등)
- DB 실행 및 결과 반환

구현: `backend/app/services/agents/sql_postprocess.py`, `backend/app/services/policy/gate.py`, `backend/app/services/oracle/executor.py`

LLM 사용: 없음

세부 단계(실행 순서):
1. `final_sql` 수신
2. `sql_postprocess`에서 Oracle 문법/스키마 보정 수행  
   - 예: `LIMIT/TOP/FETCH` → `ROWNUM` 형태로 변환, 테이블/컬럼 매핑 보정
3. `policy.gate.precheck_sql`에서 안전성 검사  
   - `SELECT`만 허용, `WHERE` 필수, `JOIN` 개수 제한, 쓰기 쿼리 차단
4. `oracle.executor.execute_sql` 실행  
   - 세미콜론 제거, `ROWNUM <= ROW_CAP` 래핑  
   - `CURRENT_SCHEMA` 설정(옵션)  
   - `DB_TIMEOUT_SEC` 적용, 결과 `row_cap` 만큼만 반환

관련 설정(환경변수):
- `ROW_CAP`, `DB_TIMEOUT_SEC`, `ORACLE_DEFAULT_SCHEMA`, `MAX_DB_JOINS`

흐름도:
```
[final_sql]
  ▼
[sql_postprocess]
  │  문법/스키마 보정
  ▼
[policy.gate.precheck_sql]
  │  안전성 검사
  ▼
[oracle.executor.execute_sql]
  ▼
[result]
```

**2.5 Output Layer**

역할: 최종 결과를 API 응답으로 패키징하고, 비용/이벤트 로그를 남깁니다.

주요 작업:
- 결과/SQL/메타를 응답 형태로 정리
- 비용 누적

구현: `backend/app/api/routes/query.py`, `backend/app/services/cost_tracker.py`

LLM 사용: 없음

흐름도:
```
[result + sql]
  ▼
[query.py 응답 패키징]
  ▼
[cost_tracker.add_cost]
  ▼
[API 응답]
```

---

**3. 횡단/운영 레이어(파이프라인 외부 성격)**

아래 레이어들은 순차 파이프라인에 포함되기보다는 **횡단 관심사** 또는 **운영 단계**로 동작합니다.

Governance Layer(예산/정책 게이트)
- 역할: 비용 한도 초과 차단, 위험한 SQL 차단 등 안전 장치 제공
- 구현: `backend/app/services/budget_gate.py`, `backend/app/services/policy/gate.py`
- 특징: `ensure_budget_ok()`는 요청 초입에서 실행되며, 정책 게이트는 실행 직전에 적용됩니다.

Observability Layer(비용/이벤트 로깅)
- 역할: LLM 사용 비용, 이벤트 로그를 저장해 운영 모니터링에 활용
- 구현: `backend/app/services/cost_tracker.py`, `backend/app/services/logging_store/store.py`
- 특징: LLM 호출 시점과 SQL 실행 시점에 비용이 누적됩니다.

Storage/Index Layer(RAG 저장소)
- 역할: 스키마/용어/예시/템플릿 문서를 벡터 스토어에 저장 및 검색
- 구현: `backend/app/services/rag/mongo_store.py`, `backend/app/services/rag/indexer.py`
- 특징: MongoDB 저장소 사용, 설정이 없으면 SimpleStore 사용

---

**4. 운영/사전 준비 레이어(메타데이터 추출 & RAG 재색인)**

이 레이어는 **요청 처리 파이프라인 밖**에서 실행됩니다. 즉, Input/Context/Reasoning/Execution/Output와는 별도의 운영 단계입니다.

메타데이터 추출
- 구현: `backend/app/services/oracle/metadata_extractor.py`
- 트리거: `backend/app/api/routes/admin_metadata.py`의 `/admin/metadata/sync`
- 산출물: `var/metadata/schema_catalog.json`, `var/metadata/join_graph.json`

RAG 재색인
- 구현: `backend/app/services/rag/indexer.py`
- 트리거: `backend/app/api/routes/admin_metadata.py`의 `/admin/rag/reindex`
- 산출물: MongoDB 컬렉션 (또는 `var/rag/simple_store.json`)

메타데이터 추출 과정 흐름도(개념):
```
[Admin Trigger]
  │  POST /admin/metadata/sync
  ▼
[Oracle Connection]
  │  oracle/connection.py (pool/DSN)
  ▼
[Metadata Extractor]
  │  oracle/metadata_extractor.py
  │  - 테이블/컬럼/PK/FK 수집
  ▼
[Artifacts]
  │  var/metadata/schema_catalog.json
  │  var/metadata/join_graph.json
  ▼
[Optional] RAG Reindex
  │  POST /admin/rag/reindex
  ▼
[Mongo Store Update]
     MongoDB collection or var/rag/simple_store.json
```

메타데이터 추출 과정 흐름 상세:
1. API 트리거 수신
   - `POST /admin/metadata/sync` 요청에서 `owner`(스키마 소유자)를 입력받습니다.
   - 구현: `backend/app/api/routes/admin_metadata.py`
2. Oracle 연결 획득
   - `oracle/connection.py`의 커넥션 풀을 통해 DB 연결을 확보합니다.
   - 환경변수: `ORACLE_DSN`, `ORACLE_USER`, `ORACLE_PASSWORD`
3. 테이블 목록 조회
   - `ALL_TABLES`에서 `owner`에 해당하는 테이블 목록을 가져옵니다.
4. 컬럼 메타데이터 수집
   - `ALL_TAB_COLUMNS`에서 테이블별 컬럼명, 타입, NULL 가능 여부를 조회합니다.
   - 결과는 `schema_catalog.json`의 `tables.{table}.columns[]`로 누적됩니다.
5. 제약 조건 수집(Primary Key / Foreign Key)
   - `ALL_CONS_COLUMNS` + `ALL_CONSTRAINTS` 조인으로 PK/FK 정보를 수집합니다.
   - PK는 `schema_catalog.json`의 `primary_keys[]`로 기록합니다.
6. 조인 그래프 생성
   - FK → PK 매핑을 통해 `join_graph.json`의 `edges[]`를 구성합니다.
   - 형식: `{from_table, from_column, to_table, to_column, type: "FK"}`
7. 산출물 저장
   - `var/metadata/schema_catalog.json`
   - `var/metadata/join_graph.json`
8. (선택) RAG 재색인
   - 스키마 변경이 있으면 `/admin/rag/reindex`로 RAG 인덱스를 갱신합니다.

---

**5. 모델/설정 위치**

환경변수: `.env`
- `ENGINEER_MODEL` (기본: `gpt-4o`)
- `EXPERT_MODEL` (기본: `gpt-4o-mini`)
- `LLM_MAX_OUTPUT_TOKENS`
- `EXPERT_SCORE_THRESHOLD`
- `DEMO_MODE`

설정 로더: `backend/app/core/config.py`

참고:
- `INTENT_MODEL`은 설정에 존재하지만 현재 코드에서는 사용되지 않습니다.
- `DEMO_MODE=true`이면 데모 캐시에 있는 질문은 LLM을 호출하지 않고 바로 응답합니다.

---

**6. 비용/로깅 관점**

- LLM 호출 토큰 사용량은 `orchestrator.run_oneshot()`에서 비용으로 누적됩니다.
- 구현: `backend/app/services/agents/orchestrator.py`, `backend/app/services/cost_tracker.py`

---

**7. 프로젝트 기준 답변 템플릿 (LLM/RAG 설계 중심)**

아래는 외부 질문에 답할 때 바로 써먹을 수 있는 Q/A 템플릿입니다. 실제 값(모델명, 파라미터)은 `.env` 기준으로 최신화하세요.

Q. 어떤 LLM 구조를 쓰나요?  
A. 두 단계로 나눕니다. 먼저 `Engineer`가 SQL 초안을 만들고(`backend/app/services/agents/sql_engineer.py`), 질문이 위험하거나 복잡하다고 판단되면 `Expert`가 다시 검토합니다(`backend/app/services/agents/sql_expert.py`). 쉽게 말해 **작성자(Engineer) + 검토자(Expert)** 구조입니다. Expert 호출 기준은 `EXPERT_SCORE_THRESHOLD`로 조정됩니다.  
예: 단순 집계 질문은 Engineer만, 위험도가 높으면 Expert가 추가 호출됩니다.

Q. Engineer/Expert 모델은 어떻게 선택하나요?  
A. `.env`의 `ENGINEER_MODEL`, `EXPERT_MODEL` 값을 바꾸면 됩니다. 코드 변경 없이 모델을 교체할 수 있게 설계되어 있고, 호출은 `llm_client.py`에서 OpenAI Chat Completions로 통일되어 있습니다. 입문자 관점에서는 “환경변수만 바꾸면 모델이 바뀐다”로 이해하면 됩니다.  
예: `ENGINEER_MODEL=gpt-4o`, `EXPERT_MODEL=gpt-4o-mini`.

Q. RAG는 어떤 문서를 쓰고, 어떻게 검색하나요?  
A. RAG는 “질문과 관련된 참고자료를 먼저 찾아서 LLM에 같이 보여주는 방식”입니다. 이 프로젝트는 스키마, 예시 SQL, 조인 템플릿, 용어집 4종을 문서로 만들어 벡터 검색합니다. 검색 결과는 `context_builder.py`에서 합쳐져 LLM 입력(`context`)으로 전달됩니다.  
예: “입원 환자 수” 질문 → admissions 스키마 + 관련 예시 SQL + 용어 설명을 함께 제공.

Q. 벡터 DB는 무엇을 쓰고, 임베딩은 어떻게 만드나요?  
A. 기본은 MongoDB(`backend/app/services/rag/mongo_store.py`)를 사용합니다. `MONGO_URI`가 없으면 `SimpleStore`를 사용해 최소 기능만 제공합니다. 임베딩은 코드 내에서 해시 기반의 간단한 bag-of-words 임베딩(128차원)을 만들어 저장합니다.  
예: Mongo 설정이 있으면 MongoDB, 없으면 SimpleStore로 자동 전환.

Q. MongoDB를 쓸 때 임베딩/검색은 어떻게 동작하나요?  
A. 문서를 upsert할 때 해시 기반 임베딩을 계산해 MongoDB에 저장합니다. `MONGO_VECTOR_INDEX`가 설정되어 있으면 `$vectorSearch`를 사용하고, 없거나 지원되지 않으면 Python에서 코사인 유사도를 계산해 상위 k개를 반환합니다.  
예: `MONGO_VECTOR_INDEX` 설정 시 Atlas Vector Search로 유사 문서 5개 반환.

Q. SimpleStore는 어떤 방식인가요?  
A. SimpleStore는 빠르고 가벼운 대체재입니다. 텍스트를 단어 단위로 쪼갠 뒤 해시(md5)로 128차원 벡터를 만들고 L2 정규화합니다. 검색은 코사인 유사도로 수행합니다. 정확도는 MongoDB 벡터 검색보다 낮을 수 있지만, 외부 의존성 없이 동작합니다.  
예: 인터넷이 막힌 환경에서도 최소한의 검색 기능을 제공합니다.

Q. 임베딩 모델을 직접 지정하나요?  
A. 현재는 코드에서 해시 기반 임베딩을 직접 생성합니다. 다른 임베딩 모델을 쓰고 싶다면 `mongo_store.py`의 `_embed_texts()` 부분을 교체하거나 외부 임베딩 호출을 추가해야 합니다.  
예: 특정 임베딩 모델을 쓰려면 `MongoStore`에 임베딩 함수 주입 필요.

Q. 임베딩/벡터 검색 품질은 어디에서 영향을 받나요?  
A. 가장 큰 요인은 입력 문서 품질입니다. `glossary_docs.jsonl`, `sql_examples.jsonl`, `join_templates.jsonl`, `sql_templates.jsonl`, `schema_catalog.json`이 좋아질수록 검색이 좋아집니다. 그 다음은 검색 범위를 결정하는 파라미터(`RAG_TOP_K`, `EXAMPLES_PER_QUERY`, `TEMPLATES_PER_QUERY`, `CONTEXT_TOKEN_BUDGET`)입니다.  
예: 용어집에 약어를 추가하면 검색 정확도가 체감 개선됩니다.

Q. 스키마 문서는 어떻게 만들어지나요?  
A. `schema_catalog.json`을 읽어 “테이블 이름 + 컬럼 + PK”를 한 문장으로 만듭니다(`rag/indexer.py`의 `_schema_docs`). 이렇게 만든 문서가 RAG 인덱스에 들어가고, 검색 결과가 `context.schemas`로 전달됩니다.  
예: `Table admissions. Columns: subject_id, hadm_id... Primary keys: hadm_id.`

Q. 컨텍스트 길이 제한은 어떻게 처리하나요?  
A. LLM 입력 길이를 넘지 않도록 `context_budget.py`에서 자릅니다. 우선순위는 `examples → templates → schemas → glossary` 순이며, 예산이 부족하면 뒤쪽부터 제거됩니다. 초보자 관점에서는 “예시가 가장 중요하게 유지된다”로 이해하면 됩니다.  
예: 예산이 부족하면 glossary가 먼저 잘립니다.

Q. LLM이 생성한 SQL의 안정성은 어떻게 보장하나요?  
A. 1) `sql_postprocess.py`에서 문법/스키마 보정을 하고, 2) `policy/gate.py`에서 SELECT만 허용, WHERE 필수, JOIN 제한을 강제합니다. 3) 실행 직전에 `oracle/executor.py`가 `ROW_CAP`과 타임아웃을 적용해 과다 조회를 막습니다. 즉, **보정 → 정책 → 실행 제한**의 3단 안전장치입니다.  
예: WHERE 없는 SQL은 정책 단계에서 차단됩니다.

Q. 메타데이터와 RAG 인덱스는 언제 갱신하나요?  
A. 스키마 변경이 있을 때 운영 단계에서 `/admin/metadata/sync`로 메타데이터를 추출하고, 이후 `/admin/rag/reindex`로 인덱스를 갱신합니다. 이 과정은 온라인 요청과 분리된 사전 준비 작업입니다.  
예: 테이블이 추가되면 sync → reindex를 수행합니다.

Q. 데모 모드에서는 LLM을 호출하나요?  
A. `DEMO_MODE=true`면 `demo_cache.json`에 있는 질문은 LLM을 호출하지 않고 바로 응답합니다. 즉, 데모용으로 속도를 보장하기 위한 캐시입니다. 캐시는 `scripts/pregen_demo_cache.py`로 생성합니다.  
예: 데모 질문 클릭 시 즉시 결과가 나오는 이유가 이 캐시입니다.

Q. API 엔드포인트는 무엇이 있나요?  
A. 사용자 흐름용은 `/query/oneshot`, `/query/run`, `/query/get`, `/query/demo/questions`입니다. 운영용은 `/admin/metadata/sync`, `/admin/rag/reindex`, `/admin/rag/status`입니다.  
예: 운영 중 스키마 변경 시 `/admin/metadata/sync` 호출.

Q. `/query/oneshot`과 `/query/run`의 역할 차이는?  
A. `/query/oneshot`은 **SQL 생성**까지 하고 `qid`를 돌려줍니다. `/query/run`은 **실제 DB 실행**을 수행합니다. 실행 전에는 `user_ack=true`로 사용자가 확인했다는 표시가 필요합니다.  
예: UI에서 “SQL 확인 → 실행” 버튼을 누를 때 `/query/run`이 호출됩니다.

Q. `/query/get`은 언제 쓰나요?  
A. `qid`로 이전에 만든 결과(`payload`)를 다시 조회할 때 사용합니다. 서버 메모리에 저장된 값을 그대로 반환합니다.  
예: 페이지 새로고침 후 동일 결과를 다시 불러올 때 사용 가능합니다.

Q. `/query/demo/questions`는 무엇을 반환하나요?  
A. 데모에서 보여줄 질문 리스트입니다. `demo_cache.json`이 있으면 캐시 키 목록을, 없으면 `var/metadata/demo_questions.jsonl`을 읽습니다.  
예: 데모 UI의 질문 드롭다운 목록.

Q. Expert 모델은 Engineer보다 더 “좋은” 모델을 써야 하나요?  
A. 꼭 그렇지는 않습니다. Engineer가 전체 SQL을 만드는 더 어려운 작업이고, Expert는 “초안 검토”에 가까워 상대적으로 가벼운 모델을 쓰는 경우가 많습니다. 다만 보정 품질이 중요하거나 오류가 잦다면 Expert 모델을 상향하는 전략도 가능합니다.  
예: 복잡한 조인 오류가 많으면 Expert를 상향해 개선할 수 있습니다.

Q. 언제 Expert 모델 업그레이드를 고려하나요?  
A. Expert가 실제로 오류를 많이 줄여주는지 측정했을 때 효과가 크다면 업그레이드를 고려합니다. 반대로 비용/지연이 부담이라면 모델을 올리기보다 `EXPERT_SCORE_THRESHOLD`를 올려 호출 빈도를 낮추는 방법이 더 효율적일 수 있습니다.  
예: 비용이 급증하면 Expert 호출 기준을 높여 호출 횟수를 줄입니다.

---

**8. 한 줄 요약**

- LLM 레이어는 **SQL 초안 생성(Engineer)**과 **조건부 리뷰(Expert)** 2단으로 나뉘며, 그 전후는 모두 규칙 기반 파이프라인입니다.
