---
title: "Finance RAG Lakehouse, Part 1: 프로젝트 배경과 아키텍처"
date: 2026-08-18
categories: [data-engineering, finance-rag-lakehouse]
tags: [databricks, pyspark, delta-lake, vector-search, architecture]
---

## 왜 이 프로젝트인가

데이터센터 운영 업무에서 데이터 엔지니어링으로 커리어를 전환하는 중이었고,
메가존클라우드 AIR 사업부(Data Engineer, Junior)에 지원을 준비하고 있었다 —
금융·제조·헬스케어 등 엔터프라이즈 고객을 대상으로 AI/데이터 기반을 구축하는
팀이다. 공고를 꼼꼼히 읽어보니, 이력서에 적기만 할 게 아니라 실제로
*증명*해야 할 요구사항 몇 가지가 눈에 띄었다.

- 레이크하우스로의 ETL/ELT 파이프라인
- Databricks와 Spark 실무 경험
- AI/ML을 위한 데이터 기반 — Feature Store, Vector Store
- PoC에서 프로덕션으로 가는 패턴, 데이터 품질, 거버넌스

Databricks와 AWS 자격증은 이미 갖고 있었지만, Spark와 레이크하우스 아키텍처를
end-to-end로 실제 동작시켜본 프로젝트는 없었다. 그래서 이력서에 한 줄을 더
적는 대신, 그 공고가 설명하는 시스템을 직접 만들어보기로 했다.

## 무엇을 만드는가

정형·비정형 금융 데이터를 모두 적재하고, Medallion 아키텍처로 처리한 뒤, 그
위에서 RAG(Retrieval-Augmented Generation) 유스케이스를 서빙하는 레이크하우스
파이프라인. 메가존 AIR 사업부가 고객사를 위해 구축하는 시스템과 같은 형태를,
혼자서 만들 수 있는 규모로 줄인 버전이다.

**정형 데이터:** `yfinance`를 통한 일별 주가와 FRED를 통한 거시경제 지표 —
금융팀이 상시로 조회하는 종류의 정량 데이터.

**비정형 데이터:** SEC EDGAR 공시(10-K, 10-Q) — 텍스트가 풍부하고, 청킹하기에
충분히 잘 구조화되어 있어 RAG에 자연스럽게 맞는 데이터다.

목표는 이런 질문 하나로 정량 시계열과 공시 텍스트에 대한 시맨틱 검색을 동시에
건드리는 것이다. 예를 들어 *"NVIDIA의 최근 변동성은 어느 정도이고, 최신
10-K에서 어떤 리스크 요인을 언급했는가?"*

## 아키텍처

```
[Sources]
 ├── Structured: yfinance (prices), FRED (macro indicators)
 └── Unstructured: SEC EDGAR filings (10-K, 10-Q)

        ↓ PySpark ingestion

[BRONZE]  raw, as-landed data
        ↓ schema enforcement, null/duplicate handling
[SILVER]  cleaned, structurally trustworthy data
        ↓ aggregation (structured) + chunking (unstructured)
[GOLD]
 ├── Structured: analytics tables (returns, volatility, joined indicators)
 └── Unstructured: chunked filing text, ready for embedding

        ↓

[Vector Search Index]  Delta Sync, Databricks-managed embeddings
        ↓
[Serving]  RAG query demo — structured + unstructured combined
```

표준적인 Bronze/Silver/Gold Medallion 패턴에 한 가지를 더했다: Gold 레이어가
두 갈래로 갈라져서 같은 레이크하우스로 다시 합류한다는 점이다 — 하나는
BI 스타일의 정형 쿼리용, 다른 하나는 Vector Search 인덱스용. 그동안 봤던
포트폴리오 프로젝트들은 대부분 둘 중 하나만 골랐다. 두 가지를 같은
레이크하우스 안에 함께 구축하는 것이 핵심이었다 — 실제 엔터프라이즈 데이터
기반의 모습에 더 가깝고, 공고에 명시된 형태이기도 하다.

## 왜 이 기술 스택인가

**단순 Spark/Airflow 조합 대신 Databricks를 선택한 이유.** 공고에서 Databricks를
직접 명시했고, 이미 Databricks Data Engineer Associate 자격증을 갖고 있었다.
더 중요한 건 Unity Catalog와 Delta Lake가 거버넌스와 ACID 보장을 기본으로
제공한다는 점 — 이걸 직접 손으로 얹지 않아도 됐다.

**Parquet 대신 Delta Lake를 선택한 이유.** ACID 트랜잭션, 스키마 진화, 증분
upsert를 위한 `MERGE INTO` — 전부 설명만 하고 넘어가는 게 아니라 실제로
써보고 싶었던 기능들이다. 금융 시계열 데이터는 매일 업데이트되므로, MERGE
기반 증분 적재는 장난감 예제가 아니라 현실적인 패턴이다.

**단순 pgvector 대신 Vector Search를 선택한 이유.** Databricks Vector Search는
Unity Catalog, Delta 테이블과 바로 통합된다 — Delta Sync 인덱스는 원본
테이블과 자동으로 동기화되므로 별도의 동기화 잡을 작성하고 유지보수할 필요가
없다. 나머지 파이프라인도 이미 Databricks로 통일하고 있었으므로, 외부 벡터
DB를 붙이는 대신 이 방식이 전체 스택의 일관성을 지켜줬다.

**도메인으로 금융을 선택한 이유.** 공고에는 금융·제조·헬스케어가 언급돼
있었다. 그중 금융을 고른 건 공개 데이터가 유난히 풍부하기 때문이다 —
`yfinance`로 무료 시장 데이터, FRED로 거시지표, SEC EDGAR로 전문(全文) 공시까지
전부 무료이고 문서화도 잘 되어 있다. 덕분에 데이터 확보 로직이 아니라
파이프라인 설계 자체에 노력을 집중할 수 있었다.

## 설계에 반영해야 했던 제약 하나

이 프로젝트는 유료 워크스페이스가 아니라 **Databricks Free Edition** 위에서
만들고 있고, 여기엔 실질적인 제약이 따른다: 서버리스 컴퓨트만 가능하고
GPU나 커스텀 컴퓨트는 없으며 — 여기서 가장 중요한 부분인데 — 이 티어의
Vector Search는 **Delta Sync** 인덱스만 지원하고 **Direct Access** 인덱스는
지원하지 않는다. 즉 직접 임베딩 모델을 붙여 임베딩 단계를 직접 제어하는 건
불가능하고, 대신 Databricks가 관리하는 임베딩 모델(`databricks-gte-large-en`)을
써야 한다.

이건 피해야 할 제약이 아니라 설계에 반영해야 할 제약이라고 판단했다. 애초에
Delta Sync가 실무에서 더 흔한 패턴이기도 하다 — 대부분의 팀은 별도의 임베딩
동기화 프로세스를 계속 돌보고 싶어하지 않는다. 그래서 처음부터 이 제약을
전제로 설계하면, 파이프라인이 "리소스 제한 없을 때 돌아가는 모습"이 아니라
"실제로 이렇게 운영될 모습"을 반영하게 된다.

## 다음 단계

Part 2에서는 첫 번째 구체적인 조각을 다룬다: `yfinance` 데이터를 Bronze Delta
테이블로 적재하는 정형 데이터 파이프라인, 그리고 부딪히기 전까지는 전혀
예상하지 못했던 yfinance의 함정(MultiIndex 컬럼) 이야기다. FRED 거시지표
적재는 같은 패턴으로 이어서 진행할 예정이다.
