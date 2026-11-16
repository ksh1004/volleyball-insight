# 배구 전력 분석 플랫폼 운영 매뉴얼

| 항목 | 내용 |
|---|---|
| 문서 버전 | v1.0 |
| 작성일 | 2026-11-16 |
| 대상 | 시스템을 매일 돌리고 문제를 처리하는 운영자(관리자) |
| 기준 | 1차 구현의 개발 구성(PC 한 대, Docker Compose). 운영 서버로 옮기면 [아키텍처 설계서 11장](02-architecture.md#11-운영-환경-전환-시-바꿀-것)의 차이를 반영한다 |
| 관련 문서 | [README](reference/project-README.md), [개발 가이드](08-dev-guide.md), [상세 설계서](03-design.md)(5장 DAG, 6.6 관리자 화면, 9장 모니터링), [테스트 계획서](07-test-plan.md) |

| 버전 | 변경 내용 |
|---|---|
| v1.0 | 최초 작성. README·수집기 README·설계 메모에 흩어진 운영 절차와 1차 구현 중 겪은 장애 대응을 정리 |

## 1. 운영 화면

모두 이 PC(`127.0.0.1`)에서만 열린다.

| 화면 | 주소 | 볼 것 |
|---|---|---|
| 사이트 | <http://localhost:5173> | 사용자 화면 |
| Django 관리자 | <http://localhost:8000/admin/> | 미확인 작업 실패, 수집 현황, 품질 검사 결과, 데이터 보정, 수동 실행, 모델 관리 |
| Kibana | <http://localhost:5601/app/dashboards#/view/volley-ops> | 대시보드 "배구 파이프라인 운영" |
| Airflow | <http://localhost:8080> | DAG 실행·로그 |
| Flink | <http://localhost:8081> | 작업 3개(RUNNING), 체크포인트 |
| HDFS / YARN | <http://localhost:9870> / <http://localhost:8088> | 용량, Spark 애플리케이션 |
| MLflow | <http://localhost:5000> | `match-winner` 버전·실험 |
| 상태 확인 API | <http://localhost:8000/api/health> | `{"status": "ok", "database": "ok"}` |

## 2. 자동 작업 일정(서울 시간)

| 시각 | 작업 | 하는 일 | 실패하면 |
|---|---|---|---|
| 상시 15초 | `live-collector` | 10분 안에 시작하거나 진행 중인 경기의 문자중계 | 실시간 화면만 멈춤(공식 기록은 경기 후 수집) |
| 상시 60초 | `flink-job-keeper` | Flink 작업 3개가 없으면 최신 체크포인트에서 다시 제출 | 5.1 |
| 상시 30초 / 5분 | `kafka-lag` / Metricbeat | 컨슈머 랙 / Airflow 지연·품질 문제 수 → Kibana | 대시보드만 비어 보임 |
| 매시 5분 | `news_collect` | 기사 수집 → HDFS·DB·검색 색인 확인 | 다음 시간에 다시 모음(피드는 최신 기사를 반복 제공) |
| 월 04:00 | `ml_weekly_retrain` | 예측 모델 재학습·등록·백테스트 | 기존 champion이 그대로 쓰임 |
| 월 05:00 | `kovo_honors` | 현역 선수 수상·주요 기록 | 다음 주에 다시 |
| 매일 06:00 | `kovo_daily` | 현재 시즌 일정·결과, 최근 7일 미확정 경기 보충, 정제, 마트, 검색 색인, 선수 유형·능력치, 품질 관문 | 5.4~5.6 |
| 매일 07:00 | `ml_daily_predict` | 정제가 끝나길 기다린 뒤(최대 3시간) 피처 → 7일 안 경기 예측 → 끝난 경기 채점 | 5.7 |
| 14:00~23:40, 20분마다 | `kovo_postgame` | 시작했지만 확정 안 된 경기가 있으면 결과·기록·문자중계 수집과 정제(없으면 KOVO 요청 없이 끝) | 다음 20분 뒤 다시. 계속 실패해도 알림은 6시간에 한 번 |

## 3. 일상 점검

### 3.1 매일(아침 07:30 이후)

| 확인 | 어디서 | 정상 |
|---|---|---|
| 미확인 작업 실패 | 관리자 첫 화면 | 0건(있으면 4장) |
| `kovo_daily`·`ml_daily_predict` | 관리자 "수집 현황" 또는 Airflow | 오늘 실행 성공(초록) |
| 품질 관문 | `kovo_daily`의 `quality_gate` | 성공. 실패면 5.5 |
| 예측 | 사이트 홈 "다음 경기" | 7일 안 경기에 승리 확률 막대 |
| Flink 작업 | Flink 화면 또는 Kibana 컨슈머 랙 | 3개 RUNNING, 랙이 계속 늘지 않음 |
| 오류 로그 | Kibana "서비스별 오류", "최근 오류 로그" | 새 오류 유형 없음 |

### 3.2 경기일 밤

| 확인 | 정상 |
|---|---|
| 경기 시작 10분 전 이후 홈의 "진행 중 경기" | 점수가 15~30초마다 바뀜 |
| 경기 종료 후 1시간 안 | 경기 상세가 `CONFIRMED`(기록·문자중계 표시). KOVO가 문자중계를 늦게 올리면 `FINISHED`로 남았다가 다음 실행에서 확정 |

### 3.3 매주

| 확인 | 정상 |
|---|---|
| 월요일 `ml_weekly_retrain` | MLflow에 새 버전, 관리자 "모델 관리"에서 시험 성적이 Elo 기준선보다 좋음 |
| 디스크 | HDFS 사용량(9870), Docker 디스크 여유 20% 이상 |
| 품질 문제 | 관리자 "품질 검사 결과"에 새 `OPEN` 문제가 쌓이지 않음 |

## 4. 작업 실패 알림 처리

1. 관리자 "수집 현황" 아래 작업 실패 내역에서 실패 행을 연다(DAG, 단계, 실행, 시도, 오류 첫 줄, Airflow 로그 링크).
2. Airflow 로그로 원인을 확인하고 5장의 해당 절차를 따른다.
3. 조치 후 같은 작업을 다시 실행한다(관리자 "수동 실행" → 예약 작업 지금 실행, 또는 Airflow에서 실패한 작업 지우고 재실행).
4. 실패 행을 골라 "확인함으로 표시"하고 메모를 남긴다. 확인하지 않은 실패가 있으면 같은 단계의 새 실패는 6시간 동안 메신저로 다시 오지 않는다.

## 5. 장애 대응

### 5.1 Flink 작업이 없거나 두 개다

| 증상 | 확인 | 조치 |
|---|---|---|
| Flink 화면에 작업이 없음 | `docker compose logs flink-job-keeper` | 보통 1분 안에 저절로 다시 제출. "HDFS not ready"면 `namenode`·`datanode` 상태 확인 |
| 같은 이름의 작업이 두 개 RUNNING | Flink 화면 | 나중에 뜬 쪽을 `flink cancel`. raw-archiver였다면 그동안의 메시지가 두 번 저장됐다(정제는 최신 사본만 쓰므로 결과는 같음) |
| 작업이 계속 재시작(RESTARTING) | Flink 작업 예외 탭, taskmanager 로그 | DB·ES·HDFS 접속 오류인지 확인 후 해당 서비스 기동 |
| 일부러 멈춰야 함 | - | `flink stop`(세이브포인트와 함께). `cancel`은 쓰지 않는다(쓰다 만 파일 유실 위험) |

### 5.2 `wait_for_raw`(HDFS 적재 확인) 시간 초과

1. raw-archiver가 RUNNING인지(5.1).
2. Kibana 컨슈머 랙에서 `raw-archiver` 랙이 0으로 줄어드는지. 랙이 남아 있으면 작업이 밀린 것이다(기다리면 됨).
3. HDFS `/raw/topic=…/dt=오늘/`에 `.inprogress`만 있고 마감 파일이 없으면 체크포인트가 동작하지 않는 것이다: Flink 화면의 체크포인트 탭 확인, 작업을 `flink stop` 후 `submit.sh`로 다시 제출.
4. 해결되면 실패한 DAG 실행에서 `wait_for_raw`부터 다시 실행(수집은 다시 할 필요 없음).

### 5.3 KOVO 요청 실패

| 증상 | 원인 | 조치 |
|---|---|---|
| `KovoApiError … HTTP 429` 반복 | 요청이 너무 잦음 | 잠금 파일(`kovo-rate` 볼륨)을 airflow와 live-collector가 함께 쓰는지 확인. 동시에 도는 백필·재처리를 줄인다 |
| HTTP 5xx·연결 실패 | KOVO 서버 문제 | 자동 재시도 후 실패. 몇 시간 뒤 다시 실행 |
| 파싱 오류, 필드 없음(Spark 스키마) | KOVO 응답 구조 변경 | `python -m collectors probe`로 새 응답을 받아 [인터페이스 명세서](06-interface-spec.md) 2장과 비교 → 수집기·정제 스키마 수정([개발 가이드 6장](08-dev-guide.md#6-바꿀-때-함께-고칠-곳체크리스트)) |
| 404(선수 수상) | KOVO에 없는 선수 | 정상(건너뜀) |

### 5.4 경기가 `FINISHED`에서 `CONFIRMED`로 넘어가지 않음

1. 관리자 "품질 검사 결과"에서 그 경기(시즌, 경기 번호)의 ERROR 문제를 본다. 경기 상세 화면 점수판 아래 안내도 같은 내용이다.
2. `MISSING_RALLY`: KOVO가 문자중계를 아직 안 올렸을 수 있다. 다음 `kovo_postgame`(당일)이나 `kovo_daily`(7일까지)가 다시 수집한다. 그 뒤에도 없으면 KOVO 기록실에서 확인한다.
3. KOVO 원천에도 없는 문제면 문제를 골라 "확인함으로 표시"하고 메모(예: "KOVO 기록실에도 없음, 2026-11-05 확인")를 남긴 뒤, [reference/collectors-README.md](reference/collectors-README.md)의 "알려진 원천 누락" 표에 적는다.
4. 우리 쪽 오류(값이 틀림)면 6.3 데이터 보정.
5. KOVO가 기록을 고친 경우 6.1 재처리(`recollect: true`).

### 5.5 품질 관문(`quality_gate`) 실패

`kovo_daily`의 `quality_gate`는 재시도하지 않는다. 오류 문구에 원인이 나온다.

| 원인 | 조치 |
|---|---|
| 최근 7일 경기에 `OPEN` ERROR 문제 | 5.4 |
| 정규리그 경기가 시작 6시간 뒤에도 결과 없음 | KOVO 일정 확인. 연기된 경기면 다음 일정 수집 때 새 날짜로 바뀜. 결과가 늦게 올라온 것이면 6.1 재처리 |

### 5.6 `kovo_daily` 다른 단계 실패

| 단계 | 흔한 원인 | 조치 |
|---|---|---|
| `curate`, `build_marts`, `player_*`, `index_entities` | YARN 자원 부족(대기), 메모리 부족(exit 137), DB 연결 | YARN 화면에서 다른 애플리케이션 확인, airflow 컨테이너 메모리 확인, 다시 실행 |
| `index_entities` | Elasticsearch 꺼짐 | `search` 프로필 기동 후 재실행(검색은 이전 색인으로 계속 동작) |

### 5.7 예측이 없거나 오래됨

1. `ml_daily_predict`의 `wait_for_curation`이 시간 초과(실패 알림)여도 예측은 진행된다. `predict` 단계 로그를 본다.
2. MLflow가 꺼져 있으면(`batch` 프로필) 실패한다. 기동 후 재실행.
3. champion이 없으면 `ml_weekly_retrain`을 먼저 실행한다.
4. 7일보다 먼 경기에는 일부러 예측을 만들지 않는다(BR-11).

### 5.8 실시간 화면이 멈춤

| 확인 | 조치 |
|---|---|
| `docker compose logs -f live-collector` | 경기를 찾았는지("watching"), KOVO 요청 오류 |
| Flink `live-metrics` RUNNING | 5.1 |
| `django-sse` 컨테이너 | `docker compose restart django-sse`. 브라우저는 3초 뒤 다시 연결하고 스냅숏부터 받는다 |
| 홈의 진행 중 경기 목록 | 30분 넘게 갱신이 없는 경기는 목록에서 빠진다(수집 멈춤 신호) |

경기가 끝나면 공식 기록은 `kovo_postgame`이 따로 수집하므로, 실시간이 실패해도 기록은 남는다.

### 5.9 검색이 안 됨(503)

Elasticsearch 상태(`curl localhost:9200/_cluster/health`) 확인 후 `search` 프로필 기동. Django는 연결 오류가 나면 클라이언트를 다시 만들므로 재시작이 필요 없다. `entities` 색인이 없으면 `kovo_daily`의 `index_entities`를, `articles` 색인이 비었으면 6.6을 실행한다.

### 5.10 메모리·디스크

| 증상 | 조치 |
|---|---|
| 컨테이너가 OOM으로 재시작 | `docker stats`로 확인. 필요 없는 프로필을 내리거나 WSL2 메모리를 늘린다. 서비스별 상한은 `compose.yaml`의 `mem_limit` |
| Docker 디스크 부족 | 오래된 이미지 정리(`docker image prune`). 데이터 볼륨(`hdfs-*`, `pg-data`, `es-data`)은 지우지 않는다 |
| ES 디스크 | 오래된 `logs-volley-*` 일별 인덱스를 지운다(보관 정책 미적용, 목표 30일) |

### 5.11 PC를 다시 켰을 때

```powershell
powershell -ExecutionPolicy Bypass -File scripts\resume.ps1             # 전체 기동 + 상태 요약
powershell -ExecutionPolicy Bypass -File scripts\resume.ps1 -Backfill   # 끝나지 않은 백필 다시 실행
```

데이터는 볼륨에 남고, Flink 작업은 체크포인트에서 이어진다. 중단 시점에 돌던 Airflow 작업은 자동 재시도(최대 3회)되므로 `running`이면 그대로 둔다.

## 6. 데이터 작업 절차

모든 수동 작업은 관리자 "수동 실행"에서 시작한다(누가·무엇을·언제가 기록됨). 대상 DAG가 일시 중지돼 있으면 거절되므로 먼저 Airflow에서 켠다.

### 6.1 재처리(`kovo_reprocess`)

| 상황 | 입력 |
|---|---|
| KOVO가 특정 경기 기록을 고침 | 경기 `023-9`(여러 개는 쉼표), "KOVO에서 다시 받기" 켬. 확정 경기도 다시 받는다(50경기까지) |
| 며칠치 경기를 다시 | 날짜 `2026-11-01~2026-11-05`(31일까지) |
| 정제 규칙을 고친 뒤 시즌 전체 | 시즌 `020-022`, "다시 받기" 끔(KOVO 요청 없이 HDFS 원천으로) |

결과는 그 DAG의 `report` 로그에 경기별 최종 상태로 나온다. 시즌 전체를 다시 "수집"하려면 6.2.

### 6.2 과거 시즌 수집(`kovo_backfill`)

시즌 `001-021` 형식, "확정된 경기 건너뛰기"를 켜면 빠진 경기만 받는다. 시즌당 약 22분. 시험은 `limit`으로 몇 경기만.

### 6.3 데이터 보정

1. 관리자 "데이터 보정" → 추가: 대상, 키(선수·팀 코드 / `023-5` / `023-5/3` / `023-5/{코드}`), 항목, 고친 값, 근거(KOVO 공식 화면, 기사 등).
2. 저장하면 사이트에 바로 반영되고 원래 값은 "원래 값"에 남는다. 이후 정제에도 계속 적용된다.
3. 경기 단위 보정은 다음 날 `kovo_daily`가 그 시즌을 다시 정제해 Elo·시즌 요약에 반영한다(바로 반영하려면 6.1 시즌 재처리, 다시 받기 끔).
4. 되돌릴 때는 지우지 말고 "켜짐"을 끈다(원래 값 복원, 이력 유지).

### 6.4 서비스 모델 교체·되돌리기

관리자 "모델 관리"에서 버전들의 시험 성적·Elo 기준선·실전 적중을 비교하고, 버전과 사유를 골라 "선택한 버전을 서비스 모델로"(최고 관리자만). 다가오는 경기 예측이 바로 다시 만들어진다. 되돌리기도 같은 방법으로 이전 버전을 고른다. 다음 주 재학습은 그 버전의 설정과 새 설정을 다시 겨룬다.

### 6.5 경기 재생(시연)

수동 실행 → 경기 재생: 문자중계가 있는 끝난 경기(2023-24부터, 예: `023-6`), 배속 1~600(10이면 3세트 경기가 약 9분). 실행 중 `/matches/{경기}/live`를 연다. 화면에 "재생"이 표시되고 경기 상태·공식 기록은 바뀌지 않는다.

### 6.6 기사 검색 색인 다시 만들기

기사 원본은 PostgreSQL `article`과 HDFS `/raw/topic=news.article`에 있다. 색인이 사라졌으면 매핑(`flink_jobs/es/articles.json`)으로 새 인덱스를 만들고 PostgreSQL 기사로 다시 색인한 뒤 별칭 `articles`를 옮긴다(문서 id = sha1(url)). `scripts/migrate_restore.ps1`의 5단계가 같은 일을 한다.

## 7. 백업·복구·이전

| 데이터 | 위치 | 잃으면 |
|---|---|---|
| HDFS 원천 `/raw` | 볼륨 `hdfs-nn`, `hdfs-dn` | **재처리 원천 손실.** 전 시즌을 KOVO에서 다시 받아야 함(약 9시간) |
| PostgreSQL `service` | 볼륨 `pg-data` | 회원·보정·품질 확인 기록 손실(경기 데이터는 HDFS로 재생성 가능) |
| PostgreSQL `mlflow` + 볼륨 `mlflow-artifacts` | | 모델 이력 손실(재학습 가능) |
| `.env` | 저장소 밖 | 비밀 값 재설정 필요 |
| Elasticsearch | 볼륨 `es-data` | `articles`는 6.6으로, `entities`는 `kovo_daily`로 재생성 |

백업·이전 절차:

```powershell
# 옛 PC(서비스 켠 상태): .env, PostgreSQL 3개 DB, MLflow 모델, HDFS(/raw /curated /features /marts), 기사 색인을 한 폴더로
powershell -ExecutionPolicy Bypass -File scripts\migrate_export.ps1 -Out D:\PJT_transfer
# 새 PC(git clone + Docker Desktop 뒤): 복원 후 resume
powershell -ExecutionPolicy Bypass -File scripts\migrate_restore.ps1 -From E:\PJT_transfer
powershell -ExecutionPolicy Bypass -File scripts\resume.ps1
```

내보낸 폴더에는 비밀 값과 회원 이메일이 들어 있으므로 저장소나 공개된 곳에 두지 않는다. 복원은 DB를 덤프 시점으로 통째로 바꾼다(`pg_restore --clean`).

## 8. 보안 운영

| 항목 | 절차 |
|---|---|
| 관리자 계정 | `docker compose exec django-api python manage.py createsuperuser`. 모델 교체는 최고 관리자만. 퇴사·역할 변경 시 `is_staff` 해제 |
| 비밀 값 교체 | `.env` 수정 → 해당 서비스 재기동(`docker compose up -d <서비스>`). `DJANGO_SECRET_KEY`를 바꾸면 관리자 세션이 끊긴다(회원 토큰은 DB에 있어 유지) |
| 포트 | 모든 포트가 `127.0.0.1`인지 `compose.yaml` 변경 때마다 확인(NFR-34) |
| 회원 탈퇴 요청 | 사용자가 "내 정보"에서 직접(비밀번호 확인). 관리자가 처리할 때는 관리자 화면에서 회원 삭제(관심 목록·토큰 함께 삭제) |

## 9. 시즌 전환(새 시즌 시작 전)

KOVO가 새 시즌 코드(예: `024`)를 시즌 목록에 올리면 `kovo_daily`가 "경기 리그가 있는 가장 최근 시즌"을 현재 시즌으로 자동으로 잡는다. Elo의 시즌 되돌림, 능력치의 기준 집단(앞 두 시즌 포함), 예측 피처의 시즌 초 처리도 자동이다. 사람이 확인할 것:

| 확인 | 위치 |
|---|---|
| 새 팀·바뀐 팀 이름(후원사) | 기사 태깅 별칭 `flink_jobs/article_tags.py`의 `TEAM_ALIASES`, 기사 선별 낱말 `collectors/collectors/news.py`의 `VOLLEY_WORDS`, 팀 컬러 `frontend/src/tokens.css`의 `--team-{코드}` |
| 실시간 승리 확률의 기본 비율 | `flink_jobs/live_metrics.py`의 `DEFAULT_RATES`(지난 시즌 정규리그 평균으로 갱신) |
| 경기 리그 코드 | 새 리그 코드가 생겼으면 수집 제외 목록(`SKIP_LEAGUES`)과 포스트시즌 목록(`POST_LEAGUES`) |
| 첫 경기 전 | 일정이 들어왔는지(사이트 일정 화면), `ml_daily_predict`가 7일 안 경기를 예측했는지 |
| 개막 첫 주 | 능력치 순위 화면의 "시즌 초반" 안내, 팀 화면 Elo 차트에 지난 시즌 끝 10경기가 흐리게 이어지는지 |

## 10. 운영 기록

| 기록 | 어디에 |
|---|---|
| 원천 누락(KOVO에도 없음) | [reference/collectors-README.md](reference/collectors-README.md) "알려진 원천 누락" 표 + 품질 문제 "확인함" 메모 |
| 데이터 보정 | 관리자 "데이터 보정"(근거 필수) |
| 모델 교체 | 관리자 "모델 관리"(사유 필수) |
| 수동 실행 | 관리자 "수동 실행"(자동 기록, 메모) |
| 장애와 조치 | 작업 실패 행의 메모. 반복되는 장애는 이 문서 5장에 절차로 추가 |
