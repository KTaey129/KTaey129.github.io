---
title: "Finance RAG Lakehouse, Part 4: 스키마 검증, 그리고 EDGAR 10-K 적재"
date: 2026-08-22
categories: [data-engineering, finance-rag-lakehouse]
tags: [databricks, pyspark, delta-lake, sec-edgar, pytest]
---

## 계기: "삽질기"로 쓰려다 관둔 이유

이번 주 Bronze 레이어에 적재 메타(`ingested_at`, `source`, `batch_id`)와 스키마
검증을 추가했다(F1). 계기는 [Part 3]({% post_url 2026-08-21-finance-rag-lakehouse-part-3-fred-macro-indicators-ingestion %})에서
다룬 `ticker_` 컬럼 버그였다. 처음엔 이걸 "yfinance 삽질기"로 한 번 더 쓰려고
했는데 관뒀다. 이 버그의 진짜 문제는 코드 한 줄이 틀렸다는 게 아니라,
**틀린 데이터를 내려보내면서도 파이프라인이 성공으로 끝났다는 것**이기
때문이다. 그래서 이번 글은 그 버그 자체가 아니라, 같은 종류의 실패를 다시는
조용히 통과시키지 않기 위해 무엇을 바꿨는지에 대한 기록이다.

## 복기: 왜 침묵 실패가 위험한가

`yfinance.download()`가 반환하는 MultiIndex 컬럼을 평탄화하는 과정에서
`('ticker', '')`가 `"ticker_"`로 잘못 저장될 뻔했던 이야기는 Part 3에서 다뤘다.
여기서 다시 짚고 싶은 건 코드 자체가 아니라 **실패 방식**이다. 컬럼명이
하나 어긋나도:

- Spark는 에러를 던지지 않는다. `spark.createDataFrame()`도, `.write.saveAsTable()`도
  전부 성공한다.
- 파이프라인 로그에는 아무 이상 신호가 없다.
- 문제는 다음 단계(Silver에서 `ticker`를 join key로 기대하는 코드 등)에
  가서야 `KeyError`나 null 조인으로 터지고, 그것도 원인이 몇 단계 전
  컬럼명이라는 걸 파악하는 데 시간이 걸린다.

에러 없이 성공하면서 틀린 결과를 다음 단계로 넘기는 것 — 이게 침묵 실패다.
버그 하나를 고치는 걸로 끝내지 않고, 같은 종류의 실패를 구조적으로 막는
두 가지를 추가했다.

## 고친 방법

**1. 스키마 검증 함수** — 적재 직전에 컬럼명과 타입을 기대값과 비교해서,
하나라도 어긋나면 예외를 던지고 파이프라인을 실패시킨다.

```python
def validate_schema(df, expected_schema, table_name):
    missing = set(expected_schema) - set(df.columns)
    extra = set(df.columns) - set(expected_schema)
    if missing or extra:
        raise SchemaValidationError(
            f"[{table_name}] column mismatch — missing: {sorted(missing)}, unexpected: {sorted(extra)}"
        )
    # 타입 체크는 생략 (전체 코드는 저장소 참고)
```

**2. 회귀 테스트** — `('ticker', '')` 같은 MultiIndex 튜플이 들어왔을 때
`"ticker_"`가 아니라 `"ticker"`로 평탄화되는지 직접 검증하는 pytest 케이스.
이제 누군가 이 평탄화 로직을 "정리"하다가 `.rstrip("_")`를 지워도, 배포 전에
테스트가 잡는다.

두 함수는 `src/ingest_utils.py`에 pandas 레벨 순수 함수로 뽑아뒀다. Databricks
노트북 밖에서, `spark`나 `dbutils` 없이도 로컬에서 pytest로 돌아간다 — 지금
6개 테스트가 전부 통과한다.

## 같은 날, 같은 원칙이 다시 증명됐다

F1 바로 다음에 SEC EDGAR 10-K 수집(F2)을 붙였는데, 여기서 예상 못 한 방식으로
같은 교훈이 다시 나왔다.

13개 종목 중 5개(AAPL, NVDA, JPM, XOM, INTC)를 골라 최신 10-K를 가져오는데,
XOM에서만 `ValueError: No 10-K filing found`가 났다. 원인을 추적해보니:
SEC의 ticker→CIK 매핑(`company_tickers.json`)이 현재 `XOM`을 "ExxonMobil
Holdings Corp"(CIK `0002115436`)이라는 **신설 지주회사**로 연결하고 있었다.
최근 지주회사 재편으로 생긴 승계 법인인데, 이 CIK 밑에는 아직 10-K가 하나도
없다 — 제출된 28건 중 대부분이 재편 발표용 `8-K12B`였다.

이건 내 코드 버그가 아니라 실제 데이터 상태였다. 그리고 이번엔 `ticker_`
때와 반대로, **설계한 대로 시스템이 반응했다**: 조용히 빈 데이터나 잘못된
데이터를 내려보내는 대신, 명확한 예외로 즉시 멈췄다. 원인을 파악하는 데
몇 분 걸렸고, XOM을 CVX로 교체하는 것으로 끝났다. 만약 이 로직이 "10-K가
없으면 그냥 건너뛰고 계속 진행" 방식이었다면, `bronze_edgar_filing`에서
XOM 데이터가 통째로 빠진 걸 몇 주 뒤 RAG 데모에서 "왜 XOM 리스크 요인
질문에 답이 안 나오지?"로 발견했을 것이다.

## 숫자

- F1 회귀 테스트 + 스키마 검증: 로컬 유닛 테스트 6개, 전부 통과
- F2 EDGAR 유틸 테스트: 4개, 전부 통과 (총 10개)
- F2 최종 결과: 10-K 5건, 원문 총 25,797,385자(~25.8MB) 적재 완료

## 왜 이게 프로젝트 전체에서 중요한가

이 프로젝트(finance-rag-lakehouse)의 진짜 목적은 포트폴리오 항목이 아니라,
세 번째 프로젝트(fab-sensor-lakehouse)에 이식할 컴포넌트를 여기서 검증하는
것이다. 스키마 검증 함수는 이미 `REUSABLE.md` 이식 목록에 올라가 있다.
반도체 제조 데이터 파이프라인에서 센서 컬럼 하나가 조용히 어긋나면, 그
대가는 트레일링 언더스코어 하나보다 훨씬 크다. 오늘 배운 건 "yfinance를
조심히 다루자"가 아니라, **성공과 정확함은 다른 말이고, 파이프라인은
후자를 검증해야 전자를 신뢰할 수 있다**는 것이다.
