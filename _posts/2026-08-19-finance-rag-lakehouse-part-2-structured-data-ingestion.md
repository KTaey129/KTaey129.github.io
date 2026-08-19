---
title: "Finance RAG Lakehouse, Part 2: yfinance와 Delta Lake로 정형 데이터 적재하기"
date: 2026-08-19
categories: [data-engineering, finance-rag-lakehouse]
tags: [databricks, pyspark, delta-lake, yfinance]
---

## 오늘 만든 것

Finance RAG Lakehouse 프로젝트에서 처음으로 동작하는 조각을 만들었다. `yfinance`로
13개 티커의 일별 OHLCV 데이터를 가져와 Databricks Free Edition 위에 Delta 테이블로
적재하는 Bronze 레이어 파이프라인이다. 아직 화려할 건 없다 — Bronze는 원본에 가까운
사본이면 충분하다 — 하지만 이 적재 경계(ingestion boundary)를 여기서 제대로 잡아두는
것이 이후 레이어에서 겪을 고통을 줄여준다.

## 왜 정형 데이터부터 시작했는가

이 프로젝트는 시리즈 후반부의 RAG 유스케이스를 위해 정형 시장 데이터와 비정형 SEC
공시 데이터를 결합한다. 정형 데이터 쪽이 상대적으로 쉬운 절반이므로, 텍스트 청킹과
벡터 인덱싱을 다루기 전에 파이프라인의 뼈대(적재 → Bronze → 스키마 검증)를 여기서
먼저 완성해두는 게 순서상 맞다고 판단했다.

## 파이프라인

```python
import yfinance as yf
import pandas as pd
from pyspark.sql import functions as F

tickers = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "TSM", "AVGO", "AMD", "INTC",
    "JPM", "GS",
    "XOM", "CVX"
]

all_data = []
for ticker in tickers:
    df = yf.download(ticker, start="2023-01-01", end="2026-08-19", progress=False)
    df = df.reset_index()
    df.columns = df.columns.get_level_values(0)  # MultiIndex를 평탄화 (아래에서 이유 설명)
    df["ticker"] = ticker
    all_data.append(df)

pdf = pd.concat(all_data, ignore_index=True)
pdf.columns = [c.lower().replace(" ", "_") for c in pdf.columns]

sdf = spark.createDataFrame(pdf)
sdf = sdf.withColumn("ingestion_timestamp", F.current_timestamp())

spark.sql("CREATE SCHEMA IF NOT EXISTS main.finance_rag")
sdf.write.format("delta").mode("overwrite") \
    .saveAsTable("main.finance_rag.bronze_stock_prices")
```

## 함정: MultiIndex 컬럼

들어가기 전엔 전혀 예상하지 못했던 부분: 최신 버전의 `yfinance`는 티커를 하나씩
다운로드할 때도 **MultiIndex 컬럼**을 반환한다 — `'Close'`가 아니라
`('Close', 'AAPL')` 형태다. 예전 튜토리얼이나 예제 코드는 대부분 평평한(flat)
컬럼명을 전제로 하기 때문에, 이걸 모르고 넘어가면 `spark.createDataFrame()`이
에러를 던지거나, 알아보기 힘든 컬럼명으로 인해 이후 단계가 전부 꼬인다.

평탄화 전 컬럼:

```
MultiIndex([( 'Date',     ''),
            ('Close', 'AAPL'),
            ( 'High', 'AAPL'),
            (  'Low', 'AAPL'),
            ( 'Open', 'AAPL'),
            ('Volume', 'AAPL')],
           )
```

## 설계 결정 정리

**배치 다운로드 대신 티커별 루프.** `yfinance`는 여러 티커를 한 번에 받아오는
기능도 지원하지만, 티커별로 루프를 도는 방식을 택했다. 각 반복에서 `df`가 예측
가능한 형태로 유지되기 때문에, 아래에서 다룰 MultiIndex 문제를 디버깅할 때도
전체 결합 테이블을 건드리기 전에 티커 하나의 결과만 떼어내서 원인을 좁힐 수
있었다.

**다른 작업보다 `reset_index()`를 먼저.** `yf.download()`는 `Date`를 컬럼이
아니라 DataFrame의 인덱스로 반환한다. pandas는 인덱스라는 개념이 있지만 Spark는
없다 — Spark DataFrame은 그냥 행과 열의 집합이다. `Date`가 인덱스로 남은 채
`spark.createDataFrame()`을 호출하면 날짜 컬럼이 조용히 사라진다. 이걸 초반에
처리해두면 나중에 "날짜 컬럼이 어디 갔지" 하며 헤매는 시간을 아낄 수 있다.

**결합 전에 `ticker`를 먼저 태깅.** 13개 티커의 행을 하나의 테이블로 쌓고 나면,
각 행에 티커를 명시적으로 라벨링해두지 않는 이상 AAPL 행과 MSFT 행을 구분할
방법이 없다.

## 트러블슈팅: 평탄화했는데도 왜 깨졌는가

처음엔 MultiIndex를 다음 코드로 평탄화하면 될 거라 생각했다.

```python
pdf.columns = ['_'.join(c).strip() if isinstance(c, tuple) else c for c in pdf.columns]
```

튜플이면 `_`로 이어 붙이고(`('Close', 'AAPL')` → `close_aapl`), 이미 평범한
문자열이면(내가 직접 추가한 `ticker` 컬럼처럼) 그대로 둔다는 논리였다. 합치기
전에 `print(pdf.columns.tolist())`로 결과를 찍어보기 전까지는 이게 맞다고
생각했다.

실제로 찍어보니 예상과 달랐다. `('Date', '')`는 `date_`처럼 뒤에 언더스코어가
붙은 채로 나왔고, 더 심각하게는 **내가 직접 추가한 `ticker` 컬럼조차 그대로
남지 않았다** — `ticker_`가 되어 있었다. 이유는 pandas 내부 동작에 있었다:
MultiIndex 컬럼을 가진 DataFrame에 `df["ticker"] = ticker`처럼 평범한 문자열
컬럼을 추가하면, pandas는 이를 조용히 `('ticker', '')`라는 튜플로 맞춰 넣는다.
즉 "이미 문자열이면 그대로 둔다"는 분기 자체가 실행될 일이 없었던 것이다.

여파는 여기서 끝나지 않는다. OHLCV 컬럼명도 `close_aapl`, `close_msft`처럼
티커별로 다르게 나온다. 이 상태로 13개 티커의 결과를 `pd.concat`하면, 의도한
long format(티커·일자별로 한 행, `close`/`high`/`low`/`open`/`volume`처럼
공통 컬럼명 사용)이 아니라 컬럼이 약 60개가 넘는 넓고 성긴(wide, sparse)
테이블이 만들어진다 — 각 행은 자기 티커의 컬럼에만 값이 있고 나머지는 전부
NULL이 된다. "결합 전에 ticker를 태깅해둔다"는 설계 의도 자체가 무색해지는
결과다.

근본 원인은 애초에 두 번째 레벨(티커 심볼)이 불필요한 정보였다는 데 있다 —
한 번에 티커 하나씩만 다운로드하고, 어차피 `ticker` 컬럼을 따로 추가할
것이기 때문이다. 그래서 튜플을 이어 붙이는 대신, 첫 번째 레벨만 남기고
버리는 게 맞는 해법이었다.

```python
df.columns = df.columns.get_level_values(0)
```

이걸 `reset_index()` 직후, `ticker` 컬럼을 추가하기 *전에* 적용하면 —
그 시점엔 DataFrame이 이미 flat index이므로 `ticker` 컬럼도 튜플로 뒤틀리지
않고 문자열 그대로 유지된다. 결과는 `date`, `close`, `high`, `low`, `open`,
`volume`, `ticker` — 처음 의도했던 정확한 long format이다. 위 "파이프라인"
섹션의 코드는 이 수정을 반영한 최종본이다.

## 다음 단계

Bronze → Silver: 스키마 강제, null 처리, 날짜 일관성 검증을 진행하고, 이어서
같은 적재 패턴으로 FRED 거시지표 수집을 붙인다. 두 파이프라인이 안정적으로
붙으면 Week 1은 증분 일별 업데이트를 위한 Delta MERGE 로직으로 마무리한다.
