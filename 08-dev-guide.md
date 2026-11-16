# 배구 전력 분석 플랫폼 개발 가이드

| 항목 | 내용 |
|---|---|
| 문서 버전 | v1.0 |
| 작성일 | 2026-11-16 |
| 대상 | 이 시스템을 처음부터 다시 만들거나 이어서 개발하는 개발자 |
| 목적 | 개발 환경을 빈 PC에서 끝까지 올리는 순서, 코드·문서 규칙, 기능을 바꿀 때 함께 고칠 곳, 1차 구현에서 실제로 겪은 문제와 해결법 |
| 관련 문서 | [README](reference/project-README.md)(명령 요약), [아키텍처 설계서](02-architecture.md), [상세 설계서](03-design.md), [테스트 계획서](07-test-plan.md), [운영 매뉴얼](09-operations.md) |

| 버전 | 변경 내용 |
|---|---|
| v1.0 | 최초 작성 |

## 1. 준비물

| 항목 | 버전·설정 | 비고 |
|---|---|---|
| OS | Windows 11 + WSL2, 또는 Linux | macOS도 Compose는 동작(잠금 파일 제한기는 POSIX에서만) |
| Docker Desktop | WSL2 백엔드. 메모리 24GB 이상(`%USERPROFILE%\.wslconfig`에 `[wsl2]` `memory=24GB`, 적용은 `wsl --shutdown` 후 재시작) | 전체 상한 합계 약 22GB. 일부 프로필만 쓰면 16GB로도 된다 |
| 디스크 | 여유 50GB 이상 | HDFS 원천(전 시즌 약 수 GB), ES, Docker 이미지 |
| Git | Windows는 Git Bash 포함 | |
| Python | 3.11(로컬 테스트·스크립트) | 컨테이너 안도 3.11(Flink만 3.10) |
| Node.js | 22 이상 | 프론트엔드, 문서 변환(`scripts/docs`) |
| 브라우저 | Chrome 또는 Edge | 문서의 Mermaid 그림 변환에도 쓴다 |
| MS Word(선택) | Windows | Word 문서 목차 갱신(`scripts/docs/update_fields.ps1`) |

## 2. 저장소 구조와 책임

| 폴더 | 언어 | 실행 위치 | 책임 |
|---|---|---|---|
| `collectors/` | Python | airflow 컨테이너(작업), `live-collector`·`kafka-lag` 컨테이너, 로컬 | 외부 → Kafka |
| `airflow/dags/` | Python | airflow 컨테이너 | 순서·일정·재시도·알림. 로직은 `kovo_tasks/pipeline.py` |
| `spark_jobs/` | PySpark + SQL | airflow(드라이버) + YARN(실행기) | 정제·마트·색인·ML |
| `flink_jobs/` | PyFlink | flink 컨테이너 | 원천 보관·실시간·기사 |
| `backend/` | Django | `django-api`, `django-sse` | API·관리자. **DB 스키마의 주인** |
| `frontend/` | Vue 3 | `frontend`(개발 서버) | 화면 |
| `infra/` | Dockerfile·설정 | 각 서비스 | 이미지·토픽·DB·대시보드 |
| `scripts/` | PowerShell·Python·Node | 호스트 | 재시작, PC 이전, 성능 측정, 문서 변환 |
| `docs/` | Markdown | - | 문서 원본(Word는 생성물) |

## 3. 빈 PC에서 처음부터 올리기

순서가 중요하다. 특히 9~12단계는 작업 사이에 데이터 의존이 있다(아래 "왜 이 순서인가").

| 단계 | 할 일 | 확인 |
|---|---|---|
| 1 | `git clone` 후 `cp .env.example .env`. `POSTGRES_PASSWORD`, `DJANGO_SECRET_KEY`(긴 무작위 값)를 바꾼다. 네이버 키·웹훅은 선택 | `.env`가 git에 올라가지 않는지(`git status`) |
| 2 | `docker compose up -d` | `postgres`, `kafka` healthy. `kafka-init` 로그에 토픽 8개 |
| 3 | `docker compose --profile app up -d --build` → `docker compose exec django-api python manage.py migrate` → `createsuperuser`(관리자) | <http://localhost:5173>에 "API: ok · DB: ok", <http://localhost:8000/admin/> 로그인 |
| 4 | `docker compose --profile stream up -d --build` | <http://localhost:8081>에 `raw-archiver`, `article-enricher`, `live-metrics` RUNNING(`flink-job-keeper`가 1분 안에 제출) |
| 5 | `docker compose --profile batch up -d --build` | <http://localhost:8080>(Airflow), <http://localhost:9870>(HDFS), <http://localhost:8088>(YARN), <http://localhost:5000>(MLflow) |
| 6 | Airflow에서 `dev_smoke_spark_yarn` 실행 | 성공, HDFS `/tmp/dev-smoke/pi` 생김 |
| 7 | `docker compose --profile search up -d --build` | `curl localhost:9200/_cat/plugins`에 `analysis-nori`, Kibana 대시보드 "배구 파이프라인 운영" |
| 8 | Airflow DAG는 처음에 모두 **일시 중지** 상태로 생긴다(Airflow 기본값). 일시 중지된 DAG는 실행을 눌러도 대기열에만 쌓이므로, 이번에 돌릴 DAG만 켠다. 백필이 끝나기 전에는 `kovo_daily`·`kovo_postgame`·`ml_*`을 켜지 않는다(KOVO 요청 예산과 Spark 메모리를 나눠 쓰지 않게). `news_collect`는 켜도 된다 | Airflow 화면의 켜기 스위치 |
| 9 | `kovo_backfill` 실행 `{"seasons": "001-023"}`(시즌당 약 22분, 전체 약 8~9시간). 시험이면 `{"seasons": "022", "limit": 10}`부터 | 관리자 "수집 현황", 끝에 `build_marts` 성공 |
| 10 | `kovo_honors` 실행 `{"scope": "all"}`(약 20분) | `player_honor` 행 생김 |
| 11 | `kovo_daily` 실행 → 끝나면 **한 번 더** 실행 | 첫 실행이 능력치 카드(`/marts/kovo/player_rating`)를 만들고, 두 번째 실행의 `build_marts`가 그 카드로 선수 Elo를 계산한다 |
| 12 | `ml_weekly_retrain` 실행(약 수십 분) → `ml_daily_predict` 실행 | MLflow `match-winner@champion`, `match_prediction`에 7일 안 경기 |
| 13 | 예약 DAG를 모두 켠다(`kovo_daily`, `kovo_postgame`, `kovo_honors`, `news_collect`, `ml_weekly_retrain`, `ml_daily_predict`) | 다음 날 06:00부터 자동 |
| 14 | (경기가 없으면) `live_replay` `{"match": "023-6", "speed": 10}` | `/matches/023-6/live`에서 "재생" 화면 |

**왜 이 순서인가**
- Flink `raw-archiver`(4단계)가 먼저 떠 있어야 수집 메시지가 HDFS에 쌓인다. 없으면 DAG의 적재 확인 센서가 30~60분 뒤 실패한다(Kafka에는 7일 남으므로 나중에 띄워도 따라잡는다).
- `build_marts`의 선수 Elo는 앞선 실행의 능력치 카드를 읽는다. 처음에는 카드가 없어 모두 1500 쪽으로 되돌린다(오류 아님). 그래서 `kovo_daily`를 두 번 돈다.
- `build_features`는 `/marts/kovo/player_rating`과 `/marts/kovo/match_elo`가 있어야 한다. 그래서 학습·예측은 `kovo_daily` 뒤다.
- `train_model`은 champion이 없으면 새 설정을 그대로 champion으로 등록한다(첫 실행).

## 4. 일상 개발

### 4.1 로컬 환경

```bash
py -3.11 -m venv .venv
.venv/Scripts/pip install -r backend/requirements.txt -e "collectors[dev]" pytest pyspark==3.5.5 numpy   # Spark 작업 테스트가 pyspark를 import한다
cd frontend && npm install && npm run dev        # /api는 localhost:8000으로 프록시
```

로컬에서 Django를 띄울 때는 `.env` 값을 환경 변수로 넣고 `POSTGRES_HOST=localhost`. 컨테이너 Django는 `./backend`를 바인드 마운트하므로 코드를 고치면 바로 반영된다.

### 4.2 자주 쓰는 명령

| 목적 | 명령 |
|---|---|
| 수집기 하나 시험 | `cd collectors && KAFKA_BOOTSTRAP_SERVERS=localhost:29092 python -m collectors games --season 023 --gnum 6 --dry-run` |
| 새 KOVO API 응답 보기 | `MSYS_NO_PATHCONV=1 python -m collectors probe --path /stat/real-time -p season=023 -p gnum=6 …` |
| Spark 작업 손으로 | `docker compose exec airflow sh /opt/airflow/spark_jobs/submit.sh curate_kovo.py --seasons 023` |
| Flink 작업 손으로 | `docker compose exec flink-jobmanager sh /opt/flink/usrlib/jobs/submit.sh live_metrics.py live-metrics` |
| DAG 실행 | `docker compose exec airflow airflow dags trigger kovo_reprocess --conf '{"games": "023-9"}'` |
| 마이그레이션 | `docker compose exec django-api python manage.py makemigrations` → `migrate` |
| 테스트 | [테스트 계획서 10장](07-test-plan.md#10-테스트-실행-명령) |
| 응답 시간 | `python scripts/measure_latency.py` |
| 문서 → Word | 8장 |

## 5. 코드 규칙

### 5.1 공통
- **주석과 문서는 한국어**, 처음 보는 사람이 읽을 수 있게 "왜"를 쓴다. 숫자 상수에는 근거(실험 결과, 날짜)를 붙인다(예: `ELO_K = 28.0  # 2026-11-12 재조정 …`).
- **비밀 값은 환경 변수로만.** 코드·문서에 실제 비밀번호·키를 쓰지 않는다(NFR-33).
- **시간**: 업무 날짜는 서울 시간, 저장은 `timestamptz`, 메시지의 `fetched_at`은 UTC(상세 설계서 1.3).
- **식별자**: 시즌 코드는 항상 3자리 문자열. 화면·API·메시지에 DB id를 쓰지 않는다(BR-01).
- **멱등**: 같은 입력으로 다시 돌려도 결과가 같게. 적재는 자연 키 upsert 또는 한 트랜잭션 전체 교체(상세 설계서 1.4).

### 5.2 Python(수집기·DAG·Spark·Flink)
- 외부 입출력(HTTP, Kafka, DB, HDFS)과 계산을 나눈다. 계산은 순수 함수로 두고 단위 테스트한다(예: `live_state.py`, `article_tags.py`, `standings.py`, `kovo_tasks/pipeline.py`는 Airflow·Flink를 import하지 않는다).
- 클라이언트·발행기는 생성자로 주입받는다(테스트에서 가짜로 바꿀 수 있게).
- KOVO를 부르는 코드는 반드시 `KovoClient`를 거친다(요청 간격·재시도·머리글).
- Spark: JSON은 **명시적 스키마**로 읽는다(추측 금지, 바뀐 필드를 바로 드러냄). DB 적재는 `pg_merge.load_and_merge`와 병합 SQL로만. 병합 SQL은 자식 행부터 지운다.
- Flink: 체크포인트 설정은 작업 코드 안에 둔다(제출한 쪽 설정을 따르므로). 작업 이름(`pipeline.name`)은 `submit.sh`의 이름과 같게.
- Airflow: DAG 파일에는 순서만, 로직은 `pipeline.py`. 모든 DAG에 `alerts.default_args()`와 `on_failure_callback=alerts.on_dag_failure`. 센서는 `mode="reschedule"`.

### 5.3 Django
- 모델이 스키마다. 파이프라인이 쓰는 테이블은 `PipelineModel`(생성·수정 시각 DB 기본값)을 상속한다. SQL로 쓰는 쪽이 값을 안 넣어도 되는 기본값은 `db_default`로(ERD 1장).
- 조회 API는 계산하지 않는다. 계산이 필요하면 마트를 만든다(예외: 순위는 경기 기록으로 계산, BR-05).
- 목록은 `select_related`·`prefetch_related`로 N+1을 막는다(`base_matches()` 참고). 홈·원정 양쪽 JOIN은 하위 쿼리로(행이 곱해져 느려진 사례).
- 응답에 `source`(출처) 필드를 붙인다.

### 5.4 Vue
- 색·글꼴·간격은 `tokens.css`의 토큰만 쓴다(디자인 규칙 [MASTER.md](reference/design-system-MASTER.md)).
- 데이터 구역은 `AsyncState`로 불러오는 중·비었음·오류를 처리한다.
- 필터·시즌·보기 방식은 주소 쿼리에 담는다.
- 차트는 SVG 컴포넌트로 직접 그리고, 요약 문장과 키보드 조작을 함께 둔다.
- 대비 4.5:1, 포커스 표시, `prefers-reduced-motion` 존중.

## 6. 바꿀 때 함께 고칠 곳(체크리스트)

| 변경 | 함께 고칠 곳 |
|---|---|
| **서빙 테이블·컬럼 추가** | ① `backend/*/models.py` + `makemigrations` ② 그 테이블을 쓰는 Spark `staging` DataFrame 컬럼 ③ 병합 SQL(`spark_jobs/sql/*.sql`)의 INSERT·UPDATE 목록 ④ `앱:` 기본값 컬럼이면 병합 SQL에 값 ⑤ API 응답 ⑥ [ERD](04-erd.md) 3·4장 ⑦ 보정 대상이면 `ops/corrections.py`의 `FIELDS` |
| **KOVO 필드 새로 쓰기** | ① 표본 저장(`probe`) ② `curate_kovo.py` 스키마 문자열(`GAME`, `PLAYER`, `TEAM`, `EVENT`) ③ 변환 ④ 품질 검사 규칙 필요 여부 ⑤ [인터페이스 명세서](06-interface-spec.md) 2장 ⑥ 표본 회귀 테스트 |
| **Kafka 토픽 추가** | ① `infra/kafka/create-topics.sh` ② 이름이 `kovo.`·`news.`·`ops.`로 시작해야 raw-archiver가 보관 ③ 생산자·소비자 ④ [아키텍처 7.1](02-architecture.md#71-kafka-토픽), 인터페이스 명세서 5장 |
| **Spark 작업 추가** | ① `spark_jobs/<작업>.py`(`pg_merge` 사용) ② 병합 SQL ③ `pipeline.run_*` 또는 `run_spark_job` ④ DAG에 연결(Spark 작업은 차례로: 드라이버 메모리) ⑤ 상세 설계서 4장 |
| **DAG 추가** | ① `alerts.default_args()`·`on_dag_failure` ② 수동 실행 대상이면 `ops/forms.py`와 관리자 "수동 실행" ③ 수집 현황 표시 대상(`ops/jobs.py`의 `DAGS`) ④ 상세 설계서 5장, 운영 매뉴얼 2장 |
| **REST API 추가** | ① `config/urls.py` ② 뷰(조회만) ③ `frontend/src/api/client.js` ④ 상세 설계서 6.3 ⑤ 테스트 ⑥ 응답 시간 측정 목록(`scripts/measure_latency.py`) |
| **화면 추가** | ① `router/index.js` ② 뷰·컴포넌트 ③ 머리글 메뉴(필요하면) ④ [화면 설계서](05-screen-spec.md) |
| **알고리즘 상수 변경**(Elo K, 능력치 가중치, 피처 등) | ① 과거 시즌으로 고르고 다른 시즌으로 시험(시간순, 무작위 분할 금지) ② 개선이 경기별 차이의 표준오차보다 큰지 ③ 근거를 코드 머리말과 상세 설계서 8장에 기록 ④ 채택하지 않은 시도도 남긴다(다시 시험하지 않게) |
| **포지션·코드 변환** | 정제 단계 한 곳에서만(BR-09). 다른 단계에서 다시 바꾸지 않는다 |

## 7. Git과 리뷰

- 기본 브랜치 `master`. 기능별 브랜치에서 작업하고 리뷰 후 합친다.
- 커밋 메시지는 한국어 한 줄 요약 + 필요하면 본문(무엇을, 왜).
- `.env`, `node_modules/`, `scripts/docs/.cache/`, 데이터 덤프는 올리지 않는다(`.gitignore`).
- 셸 스크립트·Python·YAML은 LF 줄바꿈(`.gitattributes`). 컨테이너 안에서 CRLF면 실행이 깨진다.
- 리뷰 확인: 단위 테스트, 멱등성, 시간대, 요청 간격, 비밀 값, 문서 갱신.

## 8. 문서 규칙

- **Markdown(`docs/*.md`)이 원본**이다. Word(`docs/배구_전력분석_*.docx`)는 생성물이므로 직접 고치지 않는다.
- 문서마다 첫 표에 `문서 버전`, `작성일`을 두고, 두 번째 표(첫 열 "버전")에 변경 이력을 쌓는다. Word 표지와 파일 이름이 이 값을 쓴다.
- 다이어그램은 Mermaid로 쓴다(그림 파일을 따로 관리하지 않음).
- 코드와 문서가 다르면 둘 중 하나를 바로 고친다. 상세 설계서의 상수·규칙은 코드에서 확인한 값만 쓴다.

Word 다시 만들기:

```bash
cd scripts/docs
npm install                     # 처음 한 번(docx, marked, mermaid, puppeteer-core)
npm run build                   # 모든 문서 → docs/배구_전력분석_<문서>_<버전>.docx
node build_docx.mjs erd.md      # 일부만
powershell -ExecutionPolicy Bypass -File update_fields.ps1   # (Windows + Word) 목차 쪽 번호 채우기
```

버전을 올려 파일 이름이 바뀌면 이전 버전 Word 파일은 지운다(이력은 git에 남는다). 같은 Word 파일을 Word로 열어 둔 채 변환하지 않는다(잠금 파일 `~$…`이 남으면 자동 갱신이 멈춘다).

## 9. 자주 겪는 문제

1차 구현에서 실제로 겪은 것들이다.

| 증상 | 원인 | 해결 |
|---|---|---|
| Git Bash에서 `--path /stat/...`가 `C:/Program Files/Git/stat/...`로 바뀜 | MSYS 경로 변환 | `MSYS_NO_PATHCONV=1`을 붙인다. 클라이언트는 `/`로 시작하지 않는 경로를 거부한다 |
| 프론트엔드 코드를 고쳐도 화면이 안 바뀜(Windows) | Docker Desktop이 바인드 마운트 변경을 알리지 않음 | `VITE_USE_POLLING=true`(Compose에 설정됨) |
| Windows에서 API 측정이 요청마다 약 2초 느림 | `localhost`가 IPv6를 먼저 시도 | `127.0.0.1`로 접속 |
| 모든 메시지가 HDFS에 두 번 저장됨 | raw-archiver를 두 번 제출 | `submit.sh`만 쓴다(같은 이름이 돌면 제출 안 함) |
| HDFS에 파일이 하나도 마감되지 않음 | 체크포인트 간격이 꺼진 채 제출 | 작업 코드 안에서 체크포인트를 설정한다. "RUNNING"만 보고 믿지 않는다 |
| 재시작 후 원천 일부가 빠짐 | 커밋된 Kafka 위치에서 새로 시작해 쓰다 만 파일 내용이 버려짐 | 항상 최신 체크포인트에서 복원(`latest_checkpoint.py`), 일부러 멈출 때는 `flink stop` |
| Spark 드라이버가 exit 137로 죽음 | LocalExecutor 기본 병렬 32개가 메모리를 차지 | `AIRFLOW__CORE__PARALLELISM=6`(Compose에 설정됨), Spark 작업은 차례로 |
| 2005~2019 리베로가 아웃사이드 히터로 묶임 | 포지션 코드를 두 번 변환 | 변환은 정제 단계 한 곳에서만 |
| Django를 ES보다 먼저 켜면 모든 검색이 503 | 고장 난 ES 클라이언트가 캐시에 남음 | 연결 오류 때 클라이언트를 버리고 다시 만듦(`articles/search.py`) |
| 팀 목록 API가 느림(p95 381ms) | 홈·원정 경기를 동시에 JOIN해 행이 곱해짐 | 하위 쿼리로(25ms) |
| 예측에 전날 경기가 빠짐 | `ml_daily_predict`가 `kovo_daily` 정제보다 먼저 돎 | 정제 DAG가 끝날 때까지 기다리는 센서 |
| 새 볼륨에 쓰기 권한 없음 | 볼륨은 root 소유, 이미지는 다른 uid | `kafka-volume-perms`, `kovo-rate-perms` 1회성 서비스 |
| MLflow 서버가 시작되지 않음 | SQLAlchemy 2.1과 MLflow 3.1 비호환 | `sqlalchemy==2.0.41` 고정 |
| Airflow 이미지 빌드에서 pip 실패(resolution-too-deep) | Airflow와 ML 라이브러리를 한 번에 설치 | `requirements-ml.txt`를 따로 설치 |
| Metricbeat에 Flink 그룹의 컨슈머 랙이 없음 | 멤버 없는 그룹은 건너뜀 | `kafka-lag` 서비스가 직접 기록 |
| 헤드리스 브라우저 캡처가 화면 전환 도중을 찍음 | 전환 애니메이션 | 개발자 도구 프로토콜로 실제 상태를 확인하고 찍는다 |
