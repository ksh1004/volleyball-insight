# 배구 전력 분석 플랫폼 테스트 계획서

| 항목 | 내용 |
|---|---|
| 문서 버전 | v1.0 |
| 작성일 | 2026-11-16 |
| 기준 | 요구사항 명세서 v1.0, 상세 설계서 v1.0, 1차 구현의 테스트 코드(약 180개)와 검증 기록 |
| 목적 | 재개발한 시스템이 요구사항을 만족하는지 무엇을, 어디서, 어떤 데이터로, 어떤 기준으로 확인할지 정한다. 1차 구현에서 실제로 잰 값을 기대 결과의 기준으로 쓴다 |
| 관련 문서 | [요구사항 명세서](01-requirements.md), [상세 설계서](03-design.md)(12장 테스트 설계), [개발 가이드](08-dev-guide.md), [운영 매뉴얼](09-operations.md) |

| 버전 | 변경 내용 |
|---|---|
| v1.0 | 최초 작성 |

## 1. 범위와 원칙

| 구분 | 내용 |
|---|---|
| 대상 | 수집기, Kafka 메시지, Flink 작업 3개, Spark 작업 10개, Airflow DAG 10개, Django API·관리자, Vue 화면, 운영 대시보드 |
| 제외 | KOVO·언론사 서버 자체, Docker·Hadoop 등 도구 자체의 결함 |
| 원칙 1 | **외부 없이 도는 단위 테스트가 먼저다.** KOVO·Kafka·HDFS·DB는 생성자 주입으로 가짜를 끼운다. 순수 계산(정제 규칙, 승리 확률, 태깅, 능력치)은 Spark·Flink 없이 시험할 수 있게 함수로 분리한다 |
| 원칙 2 | **실제 데이터로 맞춰 본다.** 공식 기록(세트 점수, 박스스코어)을 정답으로 삼아 정제·실시간 계산을 대조한다 |
| 원칙 3 | **다시 해도 같아야 한다.** 모든 적재·재처리는 두 번 실행해 결과(행 수, id, 값)가 같은지 본다(NFR-25) |
| 원칙 4 | **시간 순서를 지킨다.** 예측 관련 시험은 경기 시작 이후 정보가 들어가지 않았는지 확인한다(BR-12) |

## 2. 테스트 수준

| 수준 | 내용 | 실행 환경 | 실행 주기 |
|---|---|---|---|
| 단위 | 함수·클래스 하나. 가짜 클라이언트·시계·DB | 로컬 venv(Python 3.11), `django-api` 컨테이너 | 변경마다 |
| 회귀(표본) | 실제 KOVO 응답 표본(`collectors/samples/`)으로 파서·정제 규칙 | 로컬 | 변경마다 |
| 통합 | 구성 요소 둘 이상: 수집 → Kafka → HDFS, Spark → PostgreSQL, Flink → DB·Kafka, Django → ES | Compose 프로필 | 단계 완료 시 |
| 시스템(종단) | DAG 하나를 끝까지, 화면까지 | Compose 전체 | 단계 완료 시 |
| 성능 | NFR-11~14 | Compose(`app`, `search`, `stream`) | 단계 완료 시, 큰 변경 후 |
| 인수 | 요구사항별 시험 항목(5장) | Compose 전체 + 실제 시즌 데이터 | 단계 완료 시 |

## 3. 시험 환경

| 환경 | 구성 | 비고 |
|---|---|---|
| 로컬 | Python 3.11 venv(`backend/requirements.txt`, `collectors[dev]`, pytest), Node 22 | 외부 서비스 없음 |
| Compose 최소 | 기본 + `app` | API·화면 시험 |
| Compose 배치 | 기본 + `batch` + `stream`(raw-archiver 필요) | 수집·정제·학습 |
| Compose 전체 | 모든 프로필(WSL2 메모리 24GB 이상) | 종단·성능·인수 |
| 브라우저 | Chrome 최신, 폭 1440px·390px | 반응형·접근성 |

시험 전 상태 확인: `GET /api/health`가 `{"status":"ok","database":"ok"}`, `scripts/resume.ps1`의 상태 요약에 오류 없음.

## 4. 시험 데이터

| 데이터 | 용도 |
|---|---|
| `collectors/samples/*.json` | 시즌 목록, 2025-26 일정, 경기 `022-3` 상세, `022-3` 1·2세트 문자중계(1세트 첫 페이지 포함): 파서 회귀 |
| 2025-26 시즌(`022`) 전체 | 정제 정답 비교(1,042세트), 박스스코어 합계 |
| 2026-27 시즌(`023`) 진행분 | 화면·예측 시험(예: 끝난 경기 `023-22`, 예정 경기 `023-30`) |
| 재생용 끝난 경기 `023-6` | 실시간 경로 시험 |
| 알려진 원천 누락 경기 | `022-163`(3·4세트 문자중계 없음), `020-43`·`020-234`(문자중계 없음), 1세트 0-0 경기 6개, 2011-12 상무신협 10경기 → 품질 검사가 잡아야 함 |
| 전체 역사 `001`~`023` | Elo·능력치·예측 백테스트 |

## 5. 요구사항별 시험 항목

표기: **기대 결과**의 숫자는 1차 구현 실측값이다. 재개발 결과가 이와 다르면 원인을 기록한다(규칙을 바꾼 것인지, 결함인지).

### 5.1 수집(COL)

| ID | 요구사항 | 절차 | 기대 결과 |
|---|---|---|---|
| TC-COL-01 | COL-02 | `python -m collectors schedule --season 022 --dry-run` | KOVO 요청 9건(시즌 목록 1 + 리그 4 × 성별 2), 발행 8건(일정 응답만), 메시지 키 = `022` |
| TC-COL-02 | COL-03, 10 | `games --season 022 --gnum 3 --dry-run` | `kovo.match` 1건(키 `022-3`), `kovo.rally`는 세트 수만큼(페이지 500) |
| TC-COL-03 | COL-10 | 시즌 `019` 경기로 `games --only rally` | 요청을 보내지 않고 "skipping … before season 020" 로그 |
| TC-COL-04 | COL-10 | 가짜 클라이언트로 1세트 첫 페이지가 빈 경기 | 그 경기의 나머지 세트 요청 안 함 |
| TC-COL-05 | COL-10 | 한 세트 이벤트 600개(가짜) | 페이지 2개를 이어 받음. 10페이지 초과면 오류 |
| TC-COL-06 | NFR-01 | 두 프로세스가 같은 잠금 파일로 동시에 10번씩 요청(가짜 시계) | 모든 요청 간격 ≥ 1초 |
| TC-COL-07 | NFR-01 | 429·503 응답(가짜) | 2·4·8초 뒤 재시도, 3번 뒤 `KovoApiError`. 404는 즉시 오류 |
| TC-COL-08 | NFR-04 | 요청 머리글 확인 | User-Agent에 비상업 프로젝트 표시, `Origin` 없음 |
| TC-COL-09 | PIPE-02 | Kafka 꺼진 상태에서 수집 | KOVO 요청 전에 `PublishError` |
| TC-COL-10 | COL-08 | 수집 성공·실패 각 1회 | `ops.collection-log`에 `RUNNING` → `SUCCESS`(건수) / `FAILED`(오류 문구) |
| TC-COL-11 | COL-07, BR-16 | 기사 수집 `--dry-run` | 배구 기사만, 요약 ≤ 160자, "지배구조" 기사 제외, URL에서 `utm_*` 제거 |
| TC-COL-12 | COL-07 | 피드 하나만 실패(가짜) | 나머지 피드는 발행, 실행은 성공, `failures`에 기록. 전부 실패면 실행 실패 |
| TC-COL-13 | COL-04 | `honors --player 1000525 --dry-run`, 없는 코드 하나 | 1건 발행, 없는 코드는 `skipped` |
| TC-COL-14 | COL-06 | 녹화된 세트 피드를 늘려 가며 돌려줌(가짜) | 새 이벤트만 발행(`start` 올바름), `ET` 뒤 다음 세트, 3세트 승리 후 종료 |
| TC-COL-15 | COL-06 | 수집기 재시작(가짜 `live_match_state`) | `last_event_seq`부터 이어 받음 |
| TC-COL-16 | COL-01~03 | 실제 표본 파싱(`test_real_samples.py`) | 필드 이름이 바뀌면 실패 |

### 5.2 파이프라인(PIPE)

| ID | 요구사항 | 절차 | 기대 결과 |
|---|---|---|---|
| TC-PIPE-01 | PIPE-03 | 수집 후 HDFS `/raw/topic=kovo.match/dt=오늘/` 확인 | 메시지 수 = 줄 수, 줄 하나 = 봉투 JSON, 6분 안에 마감 파일로 보임 |
| TC-PIPE-02 | PIPE-03 | raw-archiver 취소 후 `submit.sh` 재제출(체크포인트 복원) | 빠지거나 두 번 저장된 메시지 없음 |
| TC-PIPE-03 | PIPE-03 | 같은 작업을 두 번 제출 시도 | 두 번째는 "already running" |
| TC-PIPE-04 | NFR-27 | `raw_arrived`(가짜 WebHDFS: 일부만 마감) | 모두 보일 때까지 False |
| TC-PIPE-05 | PIPE-04, BR-03 | `022` 정제 | 모든 세트(1,042개)의 문자중계 누적 점수 = 공식 세트 점수 |
| TC-PIPE-06 | PIPE-04, BR-09 | `015`·`021` 정제 후 포지션 분포 | 옛 코드 `L/C/R/Li`가 `OH/MB/OP/L`로. 리베로가 OH로 묶이지 않음 |
| TC-PIPE-07 | PIPE-05, NFR-25 | 같은 시즌을 두 번 정제 | 테이블별 행 수·id·값 동일 |
| TC-PIPE-08 | PIPE-05 | 정제와 정제를 동시에(`kovo_daily` + `kovo_postgame`) | 둘 다 성공, staging 스키마 충돌 없음, 끝난 뒤 `staging_*` 스키마 없음 |
| TC-PIPE-09 | PIPE-08 | `kovo_reprocess` `games=023-9`, `recollect=true` | 그 경기 다시 수집 → 그 시즌 정제 → 보고 로그에 최종 상태 |
| TC-PIPE-10 | PIPE-08 | `kovo_reprocess` `seasons=022`, `recollect=false` | KOVO 요청 0건, 정제 결과 TC-PIPE-07과 같음 |
| TC-PIPE-11 | PIPE-09 | 작업을 일부러 실패(재시도 소진) | `job_alert` 1행, 웹훅 1건(`SENT`), 6시간 안 같은 작업 재실패는 `REPEAT` |
| TC-PIPE-12 | PIPE-09 | 작업 프로세스 강제 종료 | DAG 콜백이 실행 전체로 1행 |
| TC-PIPE-13 | PIPE-10 | 알려진 원천 누락 경기 정제 | 해당 규칙(`MISSING_RALLY`, `SET_COUNT_MISMATCH`, `MISSING_BOX_SCORE`)이 ERROR로 기록되고 경기는 `FINISHED` |
| TC-PIPE-14 | PIPE-10 | 위 문제를 "확인함" 후 다시 정제 | 상태 `ACKNOWLEDGED` 유지, 품질 관문 통과 |
| TC-PIPE-15 | PIPE-10 | 문제가 없어진 뒤 정제 | `RESOLVED`, 다시 나타나면 `OPEN` |
| TC-PIPE-16 | BR-02 | 결과 없는 예정일 +3일 경기 | `CANCELLED`. 연기돼 날짜가 바뀌면 `SCHEDULED` |
| TC-PIPE-17 | BR-02 | `LIVE` 경기를 일정(결과 없음)으로 다시 병합 | 상태가 `SCHEDULED`로 돌아가지 않음 |
| TC-PIPE-18 | BR-10 | 옛 시즌 백필 뒤 팀 이름 | 가장 최근 이름 유지 |
| TC-PIPE-19 | PIPE-06 | `023-6` 재생(10배속) | `live_*` 행 생성, 끝나면 `status=FINISHED`, 세트 점수 = 공식 |
| TC-PIPE-20 | PIPE-06 | 재생 결과의 세트 지표 vs 박스스코어 | 공격 시도·성공, 서브 에이스·범실, 블로킹, 범실이 공식 기록과 같음 |
| TC-PIPE-21 | PIPE-06 | 같은 메시지 두 번 / 겹치는 이벤트 | 상태 변화 없음(`seq` 중복 제거) |
| TC-PIPE-22 | BR-18 | 같은 경기를 다시 재생 | 새 세션으로 처음부터(`reset: true`), 이전 행 지워짐 |
| TC-PIPE-23 | PIPE-11, BR-02 | 재생 중 `match.status` | 바뀌지 않음 |
| TC-PIPE-24 | PIPE-07, BR-14 | 같은 기사 URL 2번, 같은 제목 다른 URL | 저장 1건 |
| TC-PIPE-25 | PIPE-07, BR-15 | 태깅 표본(`test_article_tags.py`) | "IBK기업은행" 팀 1개, 두 글자 이름은 팀이 함께 나올 때만, 동명이인 미태그 |
| TC-PIPE-26 | PIPE-07 | `news_collect` 실행 | `wait_for_index` 통과(PostgreSQL·ES 모두 저장) |

### 5.3 분석·예측(ANL)

| ID | 요구사항 | 절차 | 기대 결과 |
|---|---|---|---|
| TC-ANL-01 | ANL-01 | `build_marts` 요약 로그 | 홈 승률 약 53.8%, 홈 이점 약 26점, 경기 전 Elo 적중률 약 62% |
| TC-ANL-02 | ANL-01 | 시즌 첫 경기 팀 Elo | 직전 값의 1500 쪽 절반 되돌림 |
| TC-ANL-03 | ANL-02, 11 | 팀 화면 레이더 값 | 1위 100, 꼴찌 0, 범실은 적을수록 높음 |
| TC-ANL-04 | ANL-11 | 2023-24 이전 시즌 팀 요약 | 사이드아웃·브레이크·상황별 효율 NULL |
| TC-ANL-05 | ANL-05 | 선수 백분위 | 순위 자격 선수만, 0~100, 범실은 반대 방향 |
| TC-ANL-06 | ANL-03, BR-12 | 피처 누출 검사: 경기의 피처를 그 경기 결과를 지운 데이터로 다시 계산 | 값이 같음 |
| TC-ANL-07 | ANL-03 | `train_model` 실행 | 시험 시즌 적중률·Log Loss가 같은 시즌 Elo 기준선보다 좋음(1차: 63.2% / 0.6422 vs 62.8% / 0.6593) |
| TC-ANL-08 | ANL-09 | 같은 데이터로 재학습 두 번 | champion 설정이 이기면 설정 유지, 새 버전은 최신 데이터로 등록 |
| TC-ANL-09 | ANL-10 | `prediction_backtest` | 최근 4시즌, 각 행의 `train_seasons`가 그 시즌보다 이전만 |
| TC-ANL-10 | ANL-03, BR-11 | `predict_matches` 후 | 7일 안 예정 경기만 예측, 시작한 경기의 예측은 바뀌지 않음, 끝난 경기 `is_correct` 채점 |
| TC-ANL-11 | ANL-06 | `player_types` | 성별마다 k ∈ [4, 9], 모든 k 실루엣이 MLflow에 기록, 유사 선수 5명 |
| TC-ANL-12 | ANL-07 | 승리 확률 단위 테스트 | 0:0 세트 확률이 대칭 입력에서 0.5, 24:24(듀스) 닫힌 해 = 재귀 근사, 끝난 경기 1 또는 0 |
| TC-ANL-13 | ANL-08 | 폼 판정 단위 테스트 | 최소 변화 미만이면 `flat`, z ≥ 1.64면 `up` |
| TC-ANL-14 | ANL-13 | 능력치 단위 테스트(`test_player_ratings.py`) | 점수 1~99, 기준 집단 평균 ≈ 60, 종합 = 가중 평균, 통산 = 세트 가중 평균 |
| TC-ANL-15 | ANL-13 | 팀 승률 상관 재계산 | 팀 자격 선수 종합(세트 가중)과 팀 승률의 상관 ≈ 0.74(234 팀-시즌) |
| TC-ANL-16 | ANL-14 | 선수 Elo 단위 테스트(`test_player_elo.py`) | 이적 선수의 점수가 새 팀 전력에 반영, 이번 경기 출전은 경기 전 전력에 안 쓰임 |

### 5.4 웹(WEB)·관리자(ADM)

| ID | 요구사항 | 절차 | 기대 결과 |
|---|---|---|---|
| TC-WEB-01 | WEB-01 | 홈 열기(비회원·회원) | 진행 중·다음·최근 경기, 남녀 순위, 기사. 회원은 "내 관심" |
| TC-WEB-02 | WEB-02 | 일정 화면 시즌·성별·팀·월, 달력 전환, 새로 고침 | 같은 조건 유지(NFR-43) |
| TC-WEB-03 | WEB-03, 08 | `/matches/023-22`, `/matches/023-30` | 끝난 경기: 점수·기록·흐름·문자중계·예측 적중. 예정 경기: 프리뷰만 |
| TC-WEB-04 | WEB-03 | 품질 문제가 있는 경기(`022-163`) | 점수판 아래 "3·4세트 문자중계 없음" 안내 |
| TC-WEB-05 | WEB-04 | `023-6` 재생 중 `/matches/023-6/live` | "재생" 표시, 점수·확률이 SSE로 갱신, 끊었다 다시 연결해도 빠짐없음 |
| TC-WEB-06 | WEB-05, BR-04~07 | 순위표를 손 계산과 대조 | 승점·순서 일치(승자승 미적용), 선수 순위 자격 적용 |
| TC-WEB-07 | WEB-06, 07 | 팀·선수 화면 | 문서의 구역이 모두 보임, 데이터 없으면 이유 문구 |
| TC-WEB-08 | WEB-09 | `/news?team=1001`, 더 보기 | 그 팀 태그 기사 20개씩, 제목은 원문 새 창 |
| TC-WEB-09 | WEB-10 | "대한항공", "기업은행", "레오" 검색과 자동완성 | 팀·선수·기사, 강조 표시. ES 끄면 검색만 503 안내(NFR-21) |
| TC-WEB-10 | WEB-11, NFR-31·35 | 가입(약한 비밀번호, 중복 이메일), 로그인 11번 연속 | 오류 문구, 11번째 429, 실패 문구로 가입 여부 노출 안 됨 |
| TC-WEB-11 | NFR-36 | `/login?next=//evil.example` | 홈으로 이동 |
| TC-WEB-12 | WEB-12, BR-13 | 관심 팀 11개 추가 | 11번째 거절 |
| TC-WEB-13 | NFR-03 | 탈퇴(비밀번호 확인) | 계정·관심·토큰 삭제, 같은 토큰 401 |
| TC-WEB-14 | WEB-13, NFR-41·44 | 390px 화면, 키보드만으로 조작, 동작 줄이기 | 가로 스크롤 없음(표 제외), 포커스 표시, 애니메이션 꺼짐, 대비 4.5:1 |
| TC-WEB-15 | WEB-14 | 모든 화면 바닥글, API 응답 | KOVO 출처 문구, `source` 필드 |
| TC-WEB-16 | WEB-16 | `/ratings`, `?scope=career` | 포지션 탭, 자격 선수만 순위, 미달 보기 |
| TC-WEB-17 | WEB-17 | `/predictions` | 시즌별 모델 대 Elo, 확신도 구간, 이번 시즌 결과 |
| TC-ADM-01 | ADM-01, 07 | 작업 실패 후 관리 첫 화면, 수집 현황 | "미확인 작업 실패 N건", DAG 실행 점, 수집기 결과, 확인함 처리 |
| TC-ADM-02 | ADM-02 | 수동 실행 4종(잘못된 입력 포함), 일시 중지 DAG | 잘못된 입력 거절, 실행 기록·Airflow 링크, 일시 중지면 거절 |
| TC-ADM-03 | ADM-03, BR-17 | 선수 이름 보정 → 사이트 → 다시 정제 → 끄기 | 즉시 반영, 정제 뒤에도 유지, 끄면 원래 값 |
| TC-ADM-04 | ADM-04 | Kibana 대시보드 | 모든 패널에 데이터, 컨슈머 랙에 Flink 그룹 표시 |
| TC-ADM-05 | ADM-05 | 다른 버전을 champion으로 | MLflow 별칭 이동, `model_change` 기록, 예측 재실행 |
| TC-ADM-06 | NFR-32 | 일반 회원으로 `/admin/` | 접근 불가. 모델 교체는 최고 관리자만 |

## 6. 비기능 시험

| ID | 요구사항 | 방법 | 기준 | 1차 실측 |
|---|---|---|---|---|
| TC-NFR-11 | NFR-11 | `python scripts/measure_latency.py --runs 30`(화면이 부르는 API 19개, `127.0.0.1`) | 일반 API p95 ≤ 500ms | 121ms |
| TC-NFR-12 | NFR-12 | 같은 스크립트의 검색 묶음 | p95 ≤ 1초 | 32ms |
| TC-NFR-13 | NFR-13 | 경기 종료 → `CONFIRMED`까지 시간(`kovo_postgame` 로그) | KOVO 게시 후 1시간 이내 | 수집 주기 20분 + 정제 2~3분 |
| TC-NFR-14 | NFR-14 | 재생 메시지의 `fetched_at` → `processed_at`·DB 저장 | 30초 이내 | 평균 약 3초, 최대 4.5초(120배속) |
| TC-NFR-21 | NFR-21 | batch·stream·search를 끄고 화면 사용 | 조회 화면 정상(검색만 안내) | 확인 |
| TC-NFR-24 | NFR-24 | 새 PC에서 개발 가이드 3장 순서대로 | 전체 기동, health ok | 확인 |
| TC-NFR-26 | NFR-26 | PC 재시작 후 `scripts/resume.ps1` | Flink 3개 작업 1분 안 복원, 데이터 유지 | 확인 |
| TC-NFR-34 | NFR-34 | 다른 PC에서 각 포트 접속 | 모두 거절 | 확인 |

## 7. 데이터 검증 기준(정답 대조)

| 검증 | 정답 | 허용 |
|---|---|---|
| 문자중계 누적 점수 | 공식 세트 점수(`set_score`) | 문자중계 시즌에서 알려진 누락 외 불일치 0 |
| 팀 박스스코어 | 그 팀 선수 기록의 합 | 경고만(원천 차이) |
| 선수 득점 | 공격 성공 + 블로킹 성공 + 서브 에이스 | `005` 시즌부터 경고만 |
| 세트 점수 규칙 | 25점(5세트 15점), 2점 차 | 알려진 누락 외 오류 0 |
| 실시간 세트 지표 | 같은 경기 공식 박스스코어 | 재생 기준 일치 |
| 수집 완전성 | KOVO 일정의 끝난 경기 수 | `CONFIRMED` + 확인함 처리된 `FINISHED` = 끝난 경기 수 |
| 규모 | 1차 실적: 끝난 경기 4,755개, 선수 1,098명, 수상 960 · 트리플크라운 368 · 기록 달성 156 | 그 이후 경기만큼 증가 |

## 8. 단계별 시작·완료 기준

요구사항 9장의 단계와 같다.

| 단계 | 시작 조건 | 완료 기준(모두 통과) |
|---|---|---|
| 0 환경 | Docker·WSL 메모리 준비 | `dev_smoke_spark_yarn` 성공, Flink 점검 작업 성공, nori 플러그인 확인, `migrate` 성공 |
| 1 데이터 기반 | 0단계 완료 | TC-COL-01~16, TC-PIPE-01~18, TC-NFR-26 |
| 2 서비스 MVP | 1단계 완료, 시즌 `022`·`023` 정제됨 | TC-PIPE-24~26, TC-WEB-01~03, 05~15(해당 화면), TC-ADM-01~04·06, TC-NFR-11·12·21·34 |
| 3 분석·실시간 | 전체 시즌 백필 완료 | TC-ANL-01~10·12, TC-PIPE-19~23, TC-WEB-04·05, TC-NFR-13·14 |
| 4 확장 | 3단계 완료 | TC-ANL-11·13~16, TC-WEB-16·17, TC-ADM-03·05 |

모든 단계 공통: 단위 테스트 전부 통과, 새로 생긴 경고·오류 로그가 Kibana에 없음.

## 9. 결함 관리

| 등급 | 기준 | 처리 |
|---|---|---|
| 치명 | 데이터 유실·중복 저장, 잘못된 공식 기록 노출, 보안 문제 | 단계 완료 불가, 즉시 수정 |
| 높음 | 요구사항 기능 동작 안 함, 성능 기준 미달 | 단계 완료 전 수정 |
| 보통 | 화면 일부 오류, 안내 문구 누락 | 다음 단계 시작 전 수정 |
| 낮음 | 모양·문구 | 일정에 맞춰 |

결함은 재현 절차, 기대·실제 결과, 관련 요구사항 ID, 로그(Kibana 링크)를 함께 기록한다. 원천(KOVO) 문제로 밝혀지면 품질 문제를 "확인함"으로 두고 [reference/collectors-README.md](reference/collectors-README.md)의 "알려진 원천 누락" 표에 적는다.

## 10. 테스트 실행 명령

| 대상 | 명령 |
|---|---|
| 수집기 | `cd collectors && python -m pytest` |
| DAG 보조 코드 | `python -m pytest airflow/tests` |
| Spark 순수 함수 | `python -m pytest spark_jobs/tests` |
| Flink 계산 | `python -m pytest flink_jobs/tests` |
| Django | `docker compose exec django-api python manage.py test` |
| 응답 시간 | `python scripts/measure_latency.py` |
| Spark on YARN 점검 | Airflow에서 `dev_smoke_spark_yarn` 실행 |
| 실시간 경로 | Airflow에서 `live_replay`(`{"match": "023-6", "speed": 10}`) 실행 후 `/matches/023-6/live` |
