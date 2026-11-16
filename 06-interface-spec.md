# 배구 전력 분석 플랫폼 인터페이스 명세서

| 항목 | 내용 |
|---|---|
| 문서 버전 | v1.0 |
| 작성일 | 2026-11-16 |
| 기준 | 1차 구현(커밋 `d1094e7`)의 코드와 실제 응답 표본(`collectors/samples/`, 2026-11-03 수집) |
| 목적 | 시스템이 주고받는 모든 데이터의 형식을 한곳에 모은다. 외부 API 필드 사전, 내부 메시지·파일·색인 형식, 관리자 화면이 부르는 Airflow·MLflow API, 사용자 REST·SSE 규약 |
| 관련 문서 | [요구사항 명세서](01-requirements.md), [아키텍처 설계서](02-architecture.md), [상세 설계서](03-design.md)(REST API 상세는 6.3절), [ERD](04-erd.md) |

| 버전 | 변경 내용 |
|---|---|
| v1.0 | 최초 작성. 상세 설계서·수집기 README·코드에 흩어져 있던 형식을 인터페이스 단위로 정리하고, KOVO 응답 필드 사전을 실제 표본으로 작성 |

## 1. 인터페이스 목록

| ID | 인터페이스 | 방향 | 프로토콜·형식 | 주기 | 상세 |
|---|---|---|---|---|---|
| IF-K01 | KOVO 시즌 목록 | KOVO → 수집기 | HTTPS GET, JSON | 매일 06:00, 백필 시 | 2.2 |
| IF-K02 | KOVO 경기 일정·결과 | KOVO → 수집기 | HTTPS GET, JSON | 매일 06:00, 경기일 20분마다 | 2.3 |
| IF-K03 | KOVO 경기 상세 기록 | KOVO → 수집기 | HTTPS GET, JSON | 끝난 경기마다 1회 | 2.4 |
| IF-K04 | KOVO 문자중계 | KOVO → 수집기 | HTTPS GET, JSON | 끝난 경기 세트마다 1회 / 경기 중 15초마다 | 2.5 |
| IF-K05 | KOVO 선수 수상·주요 기록 | KOVO → 수집기 | HTTPS GET, JSON | 매주 월 05:00 | 2.6 |
| IF-N01 | 언론사 스포츠 RSS 5곳 | 언론사 → 수집기 | HTTPS GET, RSS 2.0 XML | 매시 5분 | 3.1 |
| IF-N02 | 네이버 뉴스 검색 API | 네이버 → 수집기 | HTTPS GET, JSON | 매시 5분(키가 있을 때) | 3.2 |
| IF-W01 | 작업 실패 알림 웹훅 | Airflow → 메신저 | HTTPS POST, JSON | 최종 실패 시 | 4 |
| IF-M01~M05 | Kafka 메시지 | 수집기·Flink → Kafka → 소비자 | Kafka, JSON(UTF-8) | 상시 | 5 |
| IF-H01 | HDFS 원천·정제 파일 | Flink·Spark ↔ HDFS | HDFS, JSON 줄 / Parquet | 상시 | 6 |
| IF-H02 | WebHDFS 적재 확인 | Airflow → HDFS | HTTP GET | 수집 후 1분마다 | 6.3 |
| IF-D01 | Spark → PostgreSQL 적재 | Spark → PostgreSQL | JDBC + SQL | 정제·분석 때 | 7 |
| IF-E01 | Elasticsearch 색인 | Flink·Spark·Filebeat·Metricbeat → ES | HTTP JSON | 상시 | 8 |
| IF-A01 | Airflow REST API | Django 관리자·Airflow 작업 → Airflow | HTTP JSON(`/api/v2`) | 관리자 조작 시 | 9.1 |
| IF-A02 | MLflow REST API | Django 관리자 → MLflow | HTTP JSON(`/api/2.0/mlflow`) | 관리자 조작 시 | 9.2 |
| IF-S01 | 서비스 REST API | 브라우저 → Django | HTTP JSON | 요청 시 | 10.1, 상세 설계서 6.3 |
| IF-S02 | 실시간 SSE | Django ASGI → 브라우저 | HTTP `text/event-stream` | 경기 중 상시 | 10.2 |

---

## 2. KOVO API

### 2.1 공통 규칙

| 항목 | 내용 |
|---|---|
| 기본 주소 | `https://user-api.kovo.co.kr`(환경 변수 `KOVO_API_BASE`) |
| 인증 | 없음(공개 API, 로그인하지 않음) |
| 요청 머리글 | `User-Agent: volley-insight-collector/0.1 (non-commercial student project)`, `Accept: application/json`, `Accept-Language: ko`. 구단 사이트 식별 머리글(`Origin`, `X-Site-Origin`)은 보내지 않는다(NFR-04) |
| 요청 간격 | 모든 프로세스 합쳐 1초에 1번 이하(NFR-01, 잠금 파일 공유) |
| 시간 제한 | 10초 |
| 응답 구조 | `{"result": {"status": 200, …}, "payload": …}`. HTTP 200이어도 `result.status`가 200이 아니면 실패 |
| 재시도 | 네트워크 오류·HTTP 429·500·502·503·504는 2·4·8초 뒤 최대 3번. 그 밖의 4xx는 재시도하지 않음 |
| 404 | 선수 수상 API에서는 "그런 선수 없음"으로 보고 건너뛴다 |
| 목록 페이지 | `payload.content`(항목 배열) + `payload.page`(`size`, `number`, `totalElements`, `totalPages`). 문자중계는 페이지 정보가 없어 받은 수가 `size`보다 적으면 마지막 |
| 코드 값 | 성별 `1` 남자부 / `2` 여자부. 대회 `gcode` `001` V-리그. 리그 `201` 정규, `204` 준PO, `202` PO, `203` 챔프전(제외: `200`, `801`, `901`, `903`). 시즌 `001`(2004-05) ~ `023`(2026-27) |

### 2.2 IF-K01 시즌 목록 `GET /stat/season-list`

| 요청 파라미터 | 값 |
|---|---|
| `gcode` | `001` |
| `seasonCode` | (선택) 시즌 하나만 |

| 응답 필드(`payload[]`) | 형 | 사용 | 매핑 |
|---|---|---|---|
| `seasonCode` | 문자열 3자리 | ● | `season.code` |
| `seasonName` | 문자열 | ● | `season.name`(예: 진에어 2025~2026 V-리그) |
| `ryear` | 문자열 | ● | `season.years`(`2025-2026` 형식으로) |
| `gcode`, `remark` | | | |
| `leagues[]` | 배열 | ● | 시즌의 리그 목록. 현재 시즌 = 경기 리그가 하나라도 있는 가장 최근 시즌 |
| `leagues[].leagueCode` | 문자열 | ● | `competition.league_code`(제외 리그는 버림) |
| `leagues[].leagueName` | 문자열 | ● | `competition.name` |
| `leagues[].num`, `rounds`, `cpart` | | | |

### 2.3 IF-K02 경기 일정·결과 `GET /stat/game-schedule`

| 요청 파라미터 | 값 |
|---|---|
| `gcode` | `001` |
| `seasonCode` | 시즌 코드 |
| `leagueCode` | 리그 코드(리그마다 따로 요청) |
| `gender` | `1` / `2`(성별마다 따로) |
| `page`, `size` | `0`부터, `200` |

응답 `payload.content[]`의 경기 항목(경기 상세의 `payload.game`도 같은 모양):

| 필드 | 예 | 사용 | 매핑·규칙 |
|---|---|---|---|
| `seasonCode`, `leagueCode`, `leagueName` | `022`, `201`, 정규리그 | ● | 대회 찾기 |
| `round`, `roundName` | `1`, 1라운드 | ● | `match.round` |
| `gender` | `1` | ● | `match.gender`(`1`→`M`, `2`→`F`) |
| `gnum` | `3` | ● | `match.game_no`. 시즌 안에서 리그·성별을 통틀어 유일 |
| `hcode`, `acode` | `1006`, `1009` | ● | 홈·원정 팀 코드 |
| `hname`, `aname` | 수원 한국전력 VIXTORM | ● | `team.full_name` |
| `hsname`, `asname` | 한국전력 | ● | `team.name`(가장 최근 경기의 값) |
| `homeEmblemUrl`, `awayEmblemUrl` | 엠블럼 그림 주소 | 금지 | 구단 로고는 쓰지 않는다(NFR-02) |
| `gdate`, `gstime`, `getime` | `2025-10-20`, `19:00`, `20:36` | ● | `match.scheduled_at` = 날짜+시작 시각을 서울 시각으로 |
| `hspoint`, `aspoint` | `0`, `3` | ● | 세트 스코어(`match.home_sets`, `away_sets`). 합이 3 이상이고 `result`가 있으면 끝난 경기 |
| `hs{n}point`, `as{n}point`(n=1~5) | `20`, `25` | ● | `set_score`(치른 세트만) |
| `s{n}Ptime` | `"28"` | ● | 세트 시간(분, 문자열) → `set_score.duration_min` |
| `sptime` | `"01:28"` | ● | 경기 시간 → `match.duration_min`(분) |
| `place`, `city` | 수원체육관, 수원 | ● | `match.venue`, `match.city`, 팀 홈 경기장·연고지(그 시즌 최다) |
| `spectators` | `1348` | ● | `match.spectators` |
| `referee`, `subreferee` | | ● | `match.referee_main`, `referee_sub` |
| `gtv` | `KBSN Sports,SBS Sports,NAVER` | ● | `match.broadcasters` |
| `result` | `W` / `L` / 빈 값 | ● | 홈 기준 결과. 비어 있으면 끝나지 않은 경기 |
| `score`, `gpart`, `rank`, `weather`, `gsintendence`, `rsintendence`, `video` | | | 쓰지 않음 |

### 2.4 IF-K03 경기 상세 `GET /stat/game-schedule/{gnum}`

| 요청 파라미터 | 값 |
|---|---|
| 경로 `gnum` | 경기 번호 |
| `gcode`, `seasonCode`, `leagueCode` | 그 경기의 값 |

응답 `payload`: `game`(2.3과 같은 경기 항목), `player[]`(출전 선수별), `team[]`(두 팀 합계), `ranking`(부문별 상위 선수, 쓰지 않음).

`player[]` 필드(사용하는 것만):

| 필드 | 뜻 | 매핑 |
|---|---|---|
| `tcode`, `tsname` | 소속 팀 | `player_match_stat.team_id` |
| `pcode`, `pname` | 선수 코드·이름 | `player` |
| `position` | 포지션(옛 코드 `L`·`C`·`R`·`Li`는 시즌 `018`까지) | 현재 코드로 바꿔 저장(BR-09) |
| `height` | 키(cm) | `player.height_cm` |
| `bnum` | 등번호 | `back_number` |
| `ynCaptain` | 주장 여부 | `is_captain` |
| `ynS1`~`ynS5` | 세트별 출전. `-2`면 그 세트에 나오지 않음 | `played_sets`(예: `11100`) |
| `point` | 득점(공격+블로킹+서브) | `points` |
| `att`, `ats`, `atf`, `ate` | 공격 시도·성공·피블로킹·범실 | `attack_*` |
| `st`, `ss`, `sf` | 서브 시도·에이스·범실 | `serve_*` |
| `bt`, `bs`, `blkvb`, `blkas` | 블로킹 시도·성공·유효·어시스트 | `block_*` |
| `rt`, `rs`, `rf` | 리시브 시도·정확·실패 | `receive_*` |
| `dt`, `ds` | 디그 시도·성공 | `dig_*` |
| `sett`, `sets` | 세트(토스) 시도·성공 | `set_*` |
| `err`, `warning` | 범실 합계, 경고 | `errors`, `warnings` |
| 공격 종류 `{p}{x}` | 접두 `oa` 오픈, `ta` 시간차, `ma` 이동, `ba` 후위, `sa` 속공, `csa` 퀵오픈 × 접미 `t` 시도, `s` 성공, `f` 피블로킹, `e` 범실 | `player_attack_stat` |
| `*sp`, `*gp`, `bf`, `blke`, `df`, `de`, `setf`, `sete`, `terr`, `ba2p` 등 | 비율·기타 | 쓰지 않음(직접 계산) |

`team[]`은 같은 기록 필드에 `g_count`, `s_count`가 더 있다. `point` = 상대 범실을 포함한 팀 전체 득점 → `team_match_stat.points`, 상대 범실 득점 = `point − ats − bs − ss`.

### 2.5 IF-K04 문자중계 `GET /stat/real-time`

| 요청 파라미터 | 값 |
|---|---|
| `season` | 시즌 코드 |
| `gcode` | `001` |
| `gnum` | 경기 번호 |
| `hcode`, `acode` | 홈·원정 팀 코드 |
| `rset` | 세트 번호 |
| `page`, `size` | `0`부터, `500` |

응답 `payload`: `feed[]`(이벤트), `lineup`(선발 명단), `game`, `team`, `player`, `recentGames`(쓰지 않음). **2023-24(`020`) 이전 시즌은 `feed`가 비어 있다.**

`feed[]` 이벤트:

| 필드 | 예 | 뜻 |
|---|---|---|
| `rtcode`, `rtname` | `1009`, 우리카드 | 이벤트 팀 |
| `rpcode` | `1000241` | 선수 코드 7자리. `0000000` = 선수 없음. `ST` 이벤트에는 시각(`19:00`)이 들어 있으므로 7자리 숫자만 선수로 본다 |
| `rpname`, `rbnum` | 이름, 등번호 | |
| `raction` | `VV` | 플레이 코드(요구사항 부록 A.1) |
| `raresult` | `tot` | 결과 코드(부록 A.2) |
| `rgrade` | `1` / `-1` / `0` | 이 팀이 랠리를 땀 / 잃음 / 무관(BR-03) |
| `rpage` | `1` | 묶음 번호: 랠리·교체·작전 타임마다 하나. 세트 시작 `ST`는 0, 종료 `ET`는 200 |
| `rindex` | `2` | 묶음 안 순서 |

한 세트는 보통 250~300개(긴 듀스는 500개 초과 → 다음 페이지). 세트 피드는 뒤에 이어 붙기만 하므로, 피드 안 위치가 이벤트의 정체가 된다(실시간 `seq`).

### 2.6 IF-K05 선수 수상·주요 기록 `GET /players/{code}/honor`

| 응답 필드(`payload`) | 형 | 매핑 |
|---|---|---|
| `awards[]` | 문장 배열 | `player_honor.kind = AWARD` |
| `tripleCrowns[]` | 문장 배열 | `TRIPLE_CROWN` |
| `standardRecords[]` | 문장 배열 | `RECORD` |

문장 예 "2025~2026 V-리그 라운드 MVP (5R)" → `season_years = 2025-2026`, `title = 라운드 MVP (5R)`, `text` = 원문. 목록 순서(오래된 것부터)가 `seq`. 없는 선수는 404.

### 2.7 쓰지 않는 KOVO 인터페이스

요구사항 4.1.1의 순위·선수 목록·주요 기록 목록 API는 1차 구현에서 부르지 않았다. 코칭스태프(`/teams/coaches`)와 KOVO 뉴스는 수집 금지, `archive.kovo.co.kr`(robots 금지)와 `dbbank.kovo.co.kr`(로그인 필요)는 접근하지 않는다.

---

## 3. 뉴스

### 3.1 IF-N01 언론사 스포츠 RSS

| 피드 이름 | 언론사 | 주소 |
|---|---|---|
| `yonhap` | 연합뉴스 | `https://www.yna.co.kr/rss/sports.xml` |
| `newsis` | 뉴시스 | `https://www.newsis.com/RSS/sports.xml` |
| `donga` | 동아일보 | `https://rss.donga.com/sports.xml` |
| `hankyung` | 한국경제 | `https://www.hankyung.com/feed/sports` |
| `hani` | 한겨레 | `https://www.hani.co.kr/rss/sports/` |

- 요청: `User-Agent: volley-insight-news/0.1 (non-commercial student project)`, 출처마다 1초 1회 이하. 각 사이트 robots.txt가 허용하는 경로(2026-11-04 확인).
- 읽는 필드(RSS 2.0 `<item>`): `title`, `link`(없으면 `guid`, `http`로 시작해야 함), `description`, `pubDate`(RFC 822). 일부 피드는 `description`에 본문 전체를 넣으므로 일반 텍스트로 바꾼 뒤 160자로 자른다.

### 3.2 IF-N02 네이버 뉴스 검색 API

| 항목 | 내용 |
|---|---|
| 주소 | `GET https://openapi.naver.com/v1/search/news.json` |
| 인증 머리글 | `X-Naver-Client-Id`, `X-Naver-Client-Secret`(`.env`의 `NAVER_CLIENT_ID`, `NAVER_CLIENT_SECRET`. 없으면 이 출처를 쓰지 않음) |
| 파라미터 | `query`(`프로배구`, `V리그 배구` 두 번), `display=100`, `sort=date` |
| 읽는 필드 | `items[].originallink`(없으면 `link`), `title`, `description`(HTML 태그 제거), `pubDate` |
| 언론사 | 원문 링크 도메인으로 이름 표(40여 곳)에서 찾고, 모르면 도메인 그대로 |

### 3.3 정리한 기사 1건(수집기 → `news.article` payload)

| 필드 | 형 | 설명 |
|---|---|---|
| `url` | 문자열 | 정규화 URL(https, 도메인 소문자, `#` 제거, `utm_*`·`fbclid`·`gclid`·`ref`·`from` 제거). Kafka 키와 같다 |
| `title` | 문자열 | 일반 텍스트 |
| `summary` | 문자열 | 160자 이내, 넘으면 `…` |
| `publisher` | 문자열 | 언론사 이름 |
| `published_at` | ISO-8601 또는 null | |
| `source` | `RSS` / `NAVER_API` | |
| `feed` | 문자열 | 피드 이름 또는 `naver:{검색어}` |

---

## 4. IF-W01 작업 실패 알림 웹훅

| 항목 | 내용 |
|---|---|
| 주소 | `.env`의 `ALERT_WEBHOOK_URL`(Mattermost·Slack·Discord incoming webhook). 비면 보내지 않음(`delivery = NO_WEBHOOK`) |
| 요청 | `POST`, `Content-Type: application/json`, `User-Agent: volley-insight-alerts` |
| 본문 | Discord 주소면 `{"content": "<최대 2000자>"}`, 그 밖에는 `{"text": "<문구>"}` |
| 문구 | `:rotating_light: [배구 분석] 작업 실패: {DAG} › {작업}` / `실행: {run_id}` / `시도: N회째 (재시도를 모두 썼습니다)` / `오류: {첫 줄 300자}` / `로그: {Airflow 로그 링크}` |
| 억제 | 같은 (DAG, 작업)의 미확인 알림이 6시간 안에 있으면 보내지 않음(`REPEAT`) |
| 실패 | 전송 실패는 `delivery = FAILED`로 기록하고 예외를 던지지 않는다 |

---

## 5. Kafka 메시지

토픽 설정(파티션, 보관, 키)은 [아키텍처 설계서 7.1](02-architecture.md#71-kafka-토픽). 모든 메시지 값은 UTF-8 JSON(공백 없는 구분자), 키는 UTF-8 문자열.

### 5.1 IF-M01 봉투(envelope)

`kovo.schedule`, `kovo.match`, `kovo.rally`, `kovo.player`, `kovo.live`, `news.article`의 공통 형식.

| 필드 | 형 | 설명 |
|---|---|---|
| `source` | 문자열 | `kovo`, `news`, `replay` |
| `endpoint` | 문자열 | 호출한 API 경로(기사는 피드 주소) |
| `params` | 객체 | 요청 파라미터(재수집·추적용) |
| `collector` | 문자열 | `kovo.seasons`, `kovo.schedule`, `kovo.match`, `kovo.rally`, `kovo.honors`, `news_collect`, `kovo.live`, `kovo.replay` |
| `run_id` | 문자열(32자 hex) | 수집 실행 id. HDFS 적재 확인이 이 값으로 줄을 센다 |
| `fetched_at` | ISO-8601 UTC(초) | 수집 시각 |
| `payload` | 객체 | KOVO 응답 원본 그대로 / 기사 1건(3.3) / 실시간 이벤트 묶음(5.3) |

### 5.2 IF-M02 수집 이력 `ops.collection-log`

키 = 작업 이름(`collector`와 같은 값). 실행마다 두 건.

| 필드 | 시작 | 끝 | 설명 |
|---|---|---|---|
| `run_id` | ● | ● | |
| `job_name` | ● | ● | |
| `target` | ● | ● | 사람이 읽는 대상 요약(예: `gcode=001 games=3 [023-5, 023-6, 023-7]`) |
| `status` | `RUNNING` | `SUCCESS` / `FAILED` | |
| `started_at` | ● | ● | ISO-8601 UTC |
| `record_count` | | ● | 보낸 메시지 수 |
| `error_message` | | ● | 실패 시 `예외클래스: 메시지`, 성공이면 null |
| `finished_at` | | ● | |

### 5.3 IF-M03 실시간 이벤트 `kovo.live` payload

| 필드 | 형 | 설명 |
|---|---|---|
| `match` | 문자열 | 경기 키(`023-7`). Kafka 키와 같다 |
| `set` | 정수 | 세트 번호 |
| `start` | 정수 | `events[0]`의 세트 피드 안 위치(0부터) |
| `events` | 배열 | KOVO `feed` 이벤트(2.5) 그대로. 실시간은 새로 생긴 것만, 재생은 랠리 하나 |
| `mode` | `LIVE` / `REPLAY` | 이 밖의 값은 Flink가 버린다 |
| `session` | 문자열 | 실시간은 `live`, 재생은 실행마다 새 uuid |

봉투의 `params`에 `hcode`, `acode`가 있어 Flink가 홈·원정 팀을 안다.

### 5.4 IF-M04 실시간 지표 `live.metrics`

키 = 경기 키. 값:

| 필드 | 형 | 설명 |
|---|---|---|
| `type` | `update` | (SSE 스냅숏은 `snapshot`) |
| `reset` | 불리언 | 새 세션이라 그 경기를 처음부터 다시 계산했는가 |
| `state` | 객체 | 경기 상태(아래 표) |
| `points` | 배열 | 이번에 새로 생기거나 바뀐 득점(아래 표) |
| `fetched_at`, `processed_at` | ISO-8601 | 수집 시각, Flink 처리 시각(지연 측정) |

`state`: `match`, `mode`, `session`, `status`(`LIVE`/`FINISHED`), `home`, `away`(팀 코드), `current_set`, `set_open`, `home_points`, `away_points`, `home_sets`, `away_sets`, `set_scores`(`[[홈, 원정], …]`), `serving`(팀 코드 또는 null), `home_run`, `away_run`, `home_set_win_prob`, `home_win_prob`(0~1 또는 null), `last_play`(득점 항목), `last_event_seq`, `metrics[]`(`set`, `side`, `team`, `points`, `attack_attempts`, `attack_success`, `attack_success_rate`, `serve_aces`, `serve_errors`, `block_points`, `errors`).

득점 항목: `set`, `point_no`(홈+원정 점수), `rally_no`, `seq`, `home_score`, `away_score`, `winner`, `server`(팀 코드), `kind`(`ATTACK`/`BLOCK`/`ACE`/`OPP_ERROR`/`OTHER`), `player_code`, `player_name`, `action`, `result`.

### 5.5 IF-M05 소비자 그룹

| 그룹 | 토픽 | 시작 위치(커밋이 없을 때) | 비고 |
|---|---|---|---|
| `raw-archiver` | `kovo.*`, `news.*`, `ops.*` | earliest | Flink 체크포인트 때 커밋 |
| `article-enricher` | `news.article` | earliest | |
| `live-metrics` | `kovo.live` | latest | |
| `filebeat-ops` | `ops.collection-log` | oldest | Filebeat |
| `sse-gateway-{호스트}-{pid}` | `live.metrics` | 끝(오프셋 미사용) | 프로세스마다 새 그룹 |

---

## 6. HDFS 파일

### 6.1 IF-H01 원천 `/raw`

- 경로: `hdfs://namenode:9000/raw/topic={토픽}/dt={yyyy-MM-dd}/part-*`. `dt`는 Kafka 레코드 시각의 서울 날짜.
- 형식: 줄 하나 = Kafka 메시지 값(봉투 JSON) 하나. 줄바꿈 구분.
- 쓰는 중 파일은 `.part-*.inprogress`(숨김). 마감된 파일만 읽는다. 파티션마다 `_SUCCESS` 표시 파일.
- 읽기: Spark는 토픽 폴더 전체를 명시적 스키마로 읽고 `endpoint`로 응답 종류를 가른다.

### 6.2 정제·마트·피처

| 경로 | 형식 | 파티션 |
|---|---|---|
| `/curated/kovo/{season,competition,team,player,match,set_score,team_match_stat,player_match_stat,player_attack_stat,rally_event,quality_issue}/` | Parquet | `season_code=` |
| `/marts/kovo/{match_elo,team_rating,team_season_summary,player_season_summary,player_rating}/` | Parquet | 없음(전체 교체) |
| `/features/kovo/match/` | Parquet | 없음 |
| `/flink/checkpoints/{작업}/{job id}/chk-N/` | Flink 체크포인트 | |

정제 Parquet의 컬럼은 서비스 테이블과 같은 이름을 쓰되, 외래 키 대신 KOVO 코드(`season_code`, `game_no`, `team_code`, `player_code` 등)를 갖는다.

### 6.3 IF-H02 WebHDFS(적재 확인)

| 호출 | 용도 |
|---|---|
| `GET {WEBHDFS_URL}/raw/topic={토픽}/dt={날짜}?op=LISTSTATUS&user.name=airflow` | 파일 목록(404면 폴더 없음). `type=FILE`이고 이름이 `.`·`_`로 시작하지 않는 것만 |
| `GET {WEBHDFS_URL}{파일}?op=OPEN&user.name=airflow` | 내용 스트리밍(`"run_id":"{id}"`가 든 줄을 셈) |

`WEBHDFS_URL` 기본값 `http://namenode:9870/webhdfs/v1`.

---

## 7. IF-D01 Spark → PostgreSQL 적재

| 항목 | 규칙 |
|---|---|
| 연결 | JDBC `jdbc:postgresql://{POSTGRES_HOST}:5432/{SERVICE_DB}?rewriteBatchedInserts=true`, 5000행 배치 |
| 임시 스키마 | `staging_<YARN 애플리케이션 id를 소문자·영숫자로>`. 실행마다 만들고 끝나면 지움 |
| 병합 | `spark_jobs/sql/*.sql`의 `staging.`을 그 스키마로 바꿔 한 트랜잭션에서 실행. 시작에 `pg_advisory_xact_lock(hashtext('<잠금 이름>'))` |
| 결과 | 병합 SQL의 마지막 `SELECT`가 요약(드라이버 로그) |

| 작업 | staging 테이블 | 잠금 이름 |
|---|---|---|
| `curate_kovo.py` | `season`, `competition`, `team`, `player`, `match`, `set_score`, `team_match_stat`, `player_match_stat`, `player_attack_stat`, `rally_event`, `quality_issue` | `merge_kovo` |
| `build_marts.py` | `team_rating`, `team_season_summary`, `player_season_summary` | `merge_marts` |
| `player_types.py` | `player_type`, `player_style` | `merge_player_types` |
| `player_ratings.py` | `player_rating`, `player_rating_career` | `merge_player_ratings` |
| `curate_honors.py` | `player_honor` | `merge_honors` |
| `predict_matches.py` | `model_version`, `match_prediction` | `merge_predictions` |
| `train_model.py` | `model_version`, `prediction_backtest` | `merge_backtest` |

잠금 이름 = 병합 SQL 파일 이름. 같은 종류의 병합끼리만 줄을 선다(예: `kovo_daily`와 `kovo_postgame`의 정제가 겹치면 뒤의 것이 앞의 병합이 끝날 때까지 기다린다).

Flink `live-metrics`·`article-enricher`와 Airflow 실패 콜백은 psycopg2로 직접 upsert한다(ERD 4장의 `DB:` 기본값에 의존).

---

## 8. IF-E01 Elasticsearch 문서

### 8.1 `articles`(별칭 → `articles-v2`)

문서 id = `sha1(url)`(16진수). 매핑은 `flink_jobs/es/articles.json`(`dynamic: strict`).

| 필드 | 형 | 내용 |
|---|---|---|
| `url` | keyword(색인 안 함) | |
| `title`, `summary` | text(`ko` nori) + `.bigram` | |
| `publisher`, `source` | keyword | |
| `published_at`, `collected_at` | date | |
| `team_codes`, `team_names`, `player_codes`, `player_names` | keyword 배열 | 태그 |

### 8.2 `entities`(별칭 → `entities-<시각>`)

매핑 `spark_jobs/es/entities.json`(`dynamic: strict`).

| 필드 | 형 | 내용 |
|---|---|---|
| `type` | keyword | `team` / `player` |
| `code`, `name` | keyword | |
| `suggest` | search_as_you_type(+ `suggest_bigram` text 2-gram) | 자동완성 |
| `team_code`, `team_name`, `gender`, `position` | keyword | |
| `active` | boolean | |
| `weight` | integer | 정렬 가중치 |
| `last_seen_on` | date | |

### 8.3 운영 인덱스

| 인덱스 | 쓰는 쪽 | 주요 필드 |
|---|---|---|
| `ops-collection-YYYY.MM` | Filebeat(`ops.collection-log`) | `collection.*`(5.2의 필드), `@timestamp` = 시작·끝 시각, `log_type: collection` |
| `logs-volley-YYYY.MM.dd` | Filebeat(컨테이너 로그) | `message`, `compose_service`, `container.*`, `log.level`(`error`/`warning`/`info`) |
| `metrics-volley` | Metricbeat SQL, `kafka-lag` | Airflow 대기·지연(초), 품질 문제 수, 컨슈머 랙(Metricbeat kafka 모듈과 같은 필드: 그룹, 토픽, 파티션, 커밋 위치, 끝 위치, 지연) |

---

## 9. 관리 도구 API

### 9.1 IF-A01 Airflow REST API(v2)

기본 주소 `AIRFLOW_API_URL`(Django 기본 `http://airflow:8080`). 개발 구성은 단순 인증(모두 관리자)이라 인증 머리글 없음.

| 호출 | 쓰는 곳 | 용도 |
|---|---|---|
| `GET /api/v2/dags?limit=100` | 관리자 수집 현황 | DAG 목록: `is_paused`, `timetable_description`, `next_dagrun_run_after`, `has_import_errors` |
| `GET /api/v2/dags/{dag_id}/dagRuns?order_by=-run_after&limit=12` | 수집 현황 | 최근 실행(`dag_run_id`, `state`, `run_after`, `start_date`, `end_date`, `conf`) |
| `GET /api/v2/dags/{dag_id}/dagRuns?state=failed&run_after_gte={7일 전}&limit=1` | 수집 현황 | 7일 실패 수(`total_entries`) |
| `GET /api/v2/dags/{dag_id}` | 수동 실행 | 일시 중지면 거절 |
| `POST /api/v2/dags/{dag_id}/dagRuns` `{"logical_date": null, "conf": {…}, "note": "…(1000자까지)"}` | 수동 실행, 모델 교체(conf 없음) | 실행 요청 → `dag_run_id` |
| `GET /api/v2/dags/{dag_id}/dagRuns?state=running&state=queued&limit=1` | DAG `ml_daily_predict`의 대기 센서 | `kovo_daily`·`kovo_reprocess`가 도는 중인가(`total_entries`) |

수동 실행 `conf` 형식:

| DAG | conf |
|---|---|
| `kovo_reprocess` | `{"games": "023-9, 023-10", "dates": "2026-11-01~2026-11-05", "seasons": "020-022", "recollect": true}` |
| `kovo_backfill` | `{"seasons": "001-021", "limit": null, "skip_confirmed": true}` |
| `kovo_honors` | `{"scope": "current" \| "all"}` |
| `live_replay` | `{"match": "023-6", "speed": 10}` |
| 그 밖의 예약 DAG | `{}` |

### 9.2 IF-A02 MLflow REST API

기본 주소 `MLFLOW_TRACKING_URI`(`http://mlflow:5000`).

| 호출 | 용도 |
|---|---|
| `GET /api/2.0/mlflow/registered-models/get?name=match-winner` | 별칭(`aliases`)으로 현재 champion 찾기 |
| `GET /api/2.0/mlflow/model-versions/search?filter=name='match-winner'&max_results=200` | 등록 버전 목록(버전, 실행 id, 태그) |
| `POST /api/2.0/mlflow/registered-models/alias` `{"name": "match-winner", "alias": "champion", "version": "N"}` | 서비스 모델 교체 |

Spark 작업은 MLflow 파이썬 클라이언트(`mlflow-skinny`)로 실험 기록·모델 등록·`models:/match-winner@champion` 로드를 한다.

---

## 10. 서비스 인터페이스

### 10.1 IF-S01 REST API 공통 규약

| 항목 | 규칙 |
|---|---|
| 기본 경로 | `/api/` (개발: Vite가 `localhost:8000`으로 프록시) |
| 형식 | 요청·응답 모두 JSON(UTF-8). 날짜·시각은 ISO-8601(서울 시간대 표시) |
| 인증 | `Authorization: Token <key>`(로그인 응답의 `token`). 없으면 비회원 |
| 식별자 | 경기 `{시즌}-{경기번호}`, 팀·선수 KOVO 코드. DB id는 내보내지 않는다 |
| 페이지 | `?page=`(1부터), `?page_size=`. 응답 `{count, next, previous, results}` |
| 출처 | 조회 응답에 `source: {name, url}` |
| 오류 | 400 `{"필드": ["설명"]}` 또는 `{"detail": "…"}`, 401 인증 필요, 404 없음, 429 시도 제한, 503 외부 서비스(검색·DB) 장애 `{"detail": "…"}` |

엔드포인트별 파라미터와 응답은 [상세 설계서 6.3](03-design.md#63-rest-api-명세).

### 10.2 IF-S02 실시간 SSE `GET /api/live/{경기 키}/stream`

```text
retry: 3000

event: snapshot
data: {"type": "snapshot", "match": {...경기 목록 행...}, "state": {...5.4 state...} | null, "points": [...모든 득점...]}

event: update
data: {...5.4 live.metrics 메시지 그대로...}

: ping
```

| 항목 | 규칙 |
|---|---|
| 머리글 | `Content-Type: text/event-stream; charset=utf-8`, `Cache-Control: no-cache`, `X-Accel-Buffering: no` |
| 순서 | 연결 직후 `retry`, `snapshot` 한 번, 이후 갱신마다 `update`, 15초 동안 없으면 `: ping`(주석 줄) |
| 재연결 | 브라우저가 3초 뒤 다시 연결하고 새 `snapshot`부터 받는다(이벤트 id·`Last-Event-ID`는 쓰지 않음) |
| 처리 서버 | ASGI(`django-sse`). WSGI 서버로 오면 503 |
| 느린 연결 | 연결마다 최대 50개를 쌓고 넘치면 오래된 갱신부터 버린다 |
