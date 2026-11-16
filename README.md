# 배구 전력 분석 플랫폼 개발 문서 묶음

| 항목 | 내용 |
|---|---|
| 만든 날 | 2026-11-16 |
| 출처 | 저장소 `volley-insight`의 `docs/`(Markdown 원본, 커밋 `d1094e7` 이후 작업 사본 포함)와 참고 자료 |
| 목적 | 이 문서들만으로 시스템을 처음부터 다시 개발할 수 있게 필요한 문서를 한 폴더에 모았다 |
| 다시 만들기 | 원래 저장소에서 `python scripts/docs/export_bundle.py`(이 폴더를 지우고 새로 만든다) |

## 문서

| 파일 | 문서 | 버전 | 내용 |
|---|---|---|---|
| [01-requirements.md](01-requirements.md) | 요구사항 명세서 | v1.0 | 기능·비기능 요구사항(1차 구현 상태 포함), 업무 규칙 BR-01~18, 데이터 요구사항, 단계 계획, 미결정 사항, 문자중계 코드표, 추적표 |
| [02-architecture.md](02-architecture.md) | 아키텍처 설계서 | v1.0 | 설계 원칙, 구성 요소, 데이터 흐름, Compose 배포(서비스·포트·메모리), 저장소 배치, 기술 스택, 결정 기록 |
| [03-design.md](03-design.md) | 상세 설계서 | v1.0 | 수집·Flink·Spark·Airflow·Django·Vue 모듈 설계, REST API, 분석·예측 알고리즘, 운영 모니터링, UML, DFD |
| [04-erd.md](04-erd.md) | ERD | v1.1 | 서빙 DB 35개 테이블 관계도와 컬럼 사전(자료형·NULL·기본값·키·삭제 규칙) |
| [05-screen-spec.md](05-screen-spec.md) | 화면 설계서 | v1.1 | 사용자 화면 S01~S14(캡처), 관리자 화면 A01~A06, 공통 규칙 |
| [06-interface-spec.md](06-interface-spec.md) | 인터페이스 명세서 | v1.0 | KOVO·뉴스 API 필드 사전, Kafka·HDFS·ES 형식, Airflow·MLflow API, REST·SSE 규약 |
| [07-test-plan.md](07-test-plan.md) | 테스트 계획서 | v1.0 | 테스트 수준·환경·데이터, 요구사항별 시험 항목, 데이터 검증 기준, 단계별 완료 기준 |
| [08-dev-guide.md](08-dev-guide.md) | 개발 가이드 | v1.0 | 빈 PC에서 구축 순서, 코드 규칙, 변경 체크리스트, 문서 규칙, 자주 겪는 문제 |
| [09-operations.md](09-operations.md) | 운영 매뉴얼 | v1.0 | 작업 일정, 일상 점검, 장애 대응, 재처리·보정·백업, 시즌 전환 |
| [10-wbs.md](10-wbs.md) | 개발 계획서(WBS) | v1.0 | 마일스톤, 작업 분해, 1차 구현 실적, 위험 |

## 참고 자료

| 파일 | 자료 | 내용 |
|---|---|---|
| [reference/project-README.md](reference/project-README.md) | 1차 구현 README | 실행 명령, 프로필, 접속 주소, 화면·API 요약 |
| [reference/collectors-README.md](reference/collectors-README.md) | 수집기 README | KOVO API에서 확인한 사실, 메시지 형식, 알려진 원천 누락 표 |
| [reference/design-system-MASTER.md](reference/design-system-MASTER.md) | 디자인 규칙 | 색·글꼴·면·움직임 규칙(Hallmark Sport 테마) |
| [reference/font-licenses.md](reference/font-licenses.md) | 글꼴 라이선스 | 사용 글꼴 4종의 라이선스(SIL OFL) |
| [reference/tech-review.md](reference/tech-review.md) | 기술 정리 | 영역별 선택 이유·구현·배운 점, 실측 숫자 |
| [reference/llm-options.md](reference/llm-options.md) | LLM 선택 검토 | AI 기능(ANL-12, WEB-15)용 LLM 비교 |
| [reference/kovo-samples/](reference/kovo-samples/) | KOVO 응답 표본 | 2026-11-03 실제 응답(JSON). 인터페이스 명세서 2장의 필드 사전 근거, 파서 회귀 테스트용 |
| [images/screens/](images/screens/) | 화면 캡처 | 화면 설계서가 쓰는 1차 구현 캡처 |
| [images/diagrams/](images/diagrams/) | ERD 그림 | ERD v1.0 시점 Mermaid 그림의 PNG(원본은 04-erd.md의 Mermaid 코드) |

## 읽는 순서

| 역할 | 먼저 | 다음 |
|---|---|---|
| 처음 보는 사람 | 01 요구사항(1장, 2장, 7장 업무 규칙) → 02 아키텍처 | 10 WBS |
| 백엔드·데이터 개발 | 02 아키텍처 → 03 상세 설계(1~5장, 8장) → 04 ERD → 06 인터페이스 | 08 개발 가이드, 07 테스트 |
| 화면 개발 | 05 화면 설계 → 03 상세 설계(6.3 REST API, 7장) → reference/design-system-MASTER.md | 07 테스트(5.4) |
| 환경 구축·운영 | 08 개발 가이드(3장 구축 순서) → 09 운영 매뉴얼 | 02 아키텍처(6장 배포) |
| 계획·관리 | 10 WBS → 01 요구사항(9장 단계, 10장 미결정) | 07 테스트(8장 완료 기준) |

## 알아 둘 것

- 문서 속 `backend/league/models.py`, `spark_jobs/curate_kovo.py` 같은 경로는 **1차 구현 저장소 기준**이다. 재개발에서 구조를 바꾸면 그에 맞춰 읽는다. KOVO 표본은 `collectors/samples/` 대신 이 묶음의 `reference/kovo-samples/`에 있다.
- 다이어그램은 Mermaid 코드로 들어 있다. GitHub, VS Code("Markdown Preview Mermaid Support" 확장), Obsidian 등에서 그림으로 보인다.
- 문서 사이 링크는 이 폴더 안에서 동작하도록 고쳐 두었다.
- 인쇄·제출용 Word 판은 원래 저장소 `docs/배구_전력분석_*.docx`에 있다(`scripts/docs/build_docx.mjs`로 생성).
