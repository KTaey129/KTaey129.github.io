---
title: "Finance RAG Lakehouse, Part 3: FRED로 거시경제 지표 적재하기"
date: 2026-08-21
categories: [data-engineering, finance-rag-lakehouse]
tags: [databricks, pyspark, delta-lake, fred-api]
---

## 오늘 만든 것

프로젝트의 두 번째 Bronze 테이블. 연준(Federal Reserve) FRED API에서 CPI,
실업률, 기준금리(fed funds rate), 10년물 국채 금리를 가져와, 어제 만든 주가
데이터 옆에 Delta 테이블로 적재했다.

그 과정에서 어제 작성한 적재 코드에 있던 작지만 배울 점이 많은 버그도 하나
고쳤다.

## 어제의 `ticker_` 버그 고치기

주가 적재 코드가 `ticker`가 아니라 `ticker_`라는 컬럼명을 만들어내고
있었다. 원인은, `df["ticker"] = ticker`가 실행되는 시점에 `df`가 이미
2단계 MultiIndex 컬럼을 가지고 있었기 때문이다 (`yfinance`의 특성 — 자세한
내용은 [Part 2]({% post_url 2026-08-19-finance-rag-lakehouse-part-2-structured-data-ingestion %}) 참고).
2단계 MultiIndex 컬럼을 가진 DataFrame에 평범한 컬럼명을 추가하는 건 생각한
대로 동작하지 않는다 — pandas가 새 컬럼을 강제로 같은 2단계 형태에 맞춰
넣으면서, 두 번째 레벨을 빈 문자열로 채워버린다. 그래서 `"ticker"`는 내부적으로
조용히 `("ticker", "")`가 되어 있었다.

평탄화 코드가 실행될 때:

```python
'_'.join(c).strip() if isinstance(c, tuple) else c
```

`("ticker", "")`를 이어 붙이면 `"ticker_"`가 된다 — 뒤에 붙은 언더스코어는
그냥 `"_".join(["ticker", ""])`의 결과일 뿐이다.

수정 — 평탄화 후에 끝에 붙은 언더스코어를 제거한다:

```python
pdf.columns = ['_'.join(c).strip() if isinstance(c, tuple) else c for c in pdf.columns]
pdf.columns = [c.lower().replace(" ", "_").rstrip("_") for c in pdf.columns]
```

`rstrip("_")`는 끝에 붙은 언더스코어만 건드리기 때문에, `close_aapl`은
그대로 남고 `ticker_`만 `ticker`로 바뀐다.

## FRED 데이터 적재

FRED는 한 가지 지점에서 yfinance보다 확실히 단순했다: MultiIndex 문제 자체가
없다. `fred.get_series()`가 넓은(wide) 컬럼을 가진 DataFrame이 아니라 평범한
pandas Series를 반환하기 때문이다.

```python
%pip install fredapi
dbutils.library.restartPython()
```

```python
from fredapi import Fred
import pandas as pd
from pyspark.sql import functions as F

fred = Fred(api_key=dbutils.secrets.get(scope="finance-rag", key="fred_api_key"))

series_ids = {
    "CPI": "CPIAUCSL",
    "FED_FUNDS_RATE": "FEDFUNDS",
    "UNEMPLOYMENT": "UNRATE",
    "10Y_TREASURY": "DGS10"
}

fred_data = []
for name, sid in series_ids.items():
    s = fred.get_series(sid, observation_start="2023-01-01", observation_end="2026-08-19")
    s = s.reset_index()
    s.columns = ["date", "value"]
    s["indicator"] = name
    fred_data.append(s)

fred_pdf = pd.concat(fred_data, ignore_index=True)
fred_sdf = spark.createDataFrame(fred_pdf)
fred_sdf = fred_sdf.withColumn("ingestion_timestamp", F.current_timestamp())

fred_sdf.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("main.finance_rag.bronze_macro_indicators")
```

## 래퍼를 믿기 전에 원본 API 응답부터 확인하기

`fredapi`가 파싱해준 결과를 그대로 믿기 전에, FRED의 원본 JSON 응답을 직접
확인해봤다. `fredapi`는 API를 감싸서 깔끔한 Series로 돌려주는 래퍼이기
때문에, 별도로 확인하지 않으면 변환되기 전에 FRED가 실제로 무엇을 반환하는지
전혀 볼 일이 없다. FRED에는 알아둘 만한 진짜 함정이 하나 있다 — 결측값이
원본 API에서 `null`이나 `NaN`이 아니라 문자열 `"."`로 온다는 것이다.
라이브러리 버전에 따라 이 변환을 제대로 처리하지 못하면, 이후 숫자 파싱이
조용히 깨질 수 있다.

```python
import requests

url = "https://api.stlouisfed.org/fred/series/observations"
params = {
    "series_id": "CPIAUCSL",
    "api_key": "YOUR_FRED_KEY_HERE",
    "file_type": "json",
    "observation_start": "2023-01-01",
    "observation_end": "2026-08-19"
}
r = requests.get(url, params=params)
r.json()["observations"][:5]
```

새로운 외부 데이터 소스를 붙일 때마다 한 번씩은 해볼 만한 확인이다 —
yfinance의 MultiIndex 문제를 잡아낸 것과 같은 습관이다: 편리한 래퍼가
해석해준 결과를 믿기 전에, 소스가 실제로 무엇을 보내는지 먼저 눈으로
확인하는 것.

## 정합성 체크 결과 — 그리고 처음엔 왜 숫자가 "이상해" 보였는가

```
+--------------+--------+
|     indicator|count(*)|
+--------------+--------+
|           CPI|      43|
|  UNEMPLOYMENT|      43|
|  10Y_TREASURY|     948|
|FED_FUNDS_RATE|      43|
+--------------+--------+
```

처음 든 생각: 왜 한 지표만 행이 22배나 많지? 답: 이건 버그가 아니라 맞는
결과다. CPI, 실업률, 기준금리는 **월별(monthly)** 시계열이다 — 2023-01-01부터
2026-08-19까지 약 43~44개월이니 43행이 딱 맞는다. 10년물 국채 금리는
**일별(daily)** 시계열이다(영업일 기준) — 같은 기간 동안 약 948 거래일이니
이것도 맞아떨어진다.

이건 단순히 "카운트가 이상해 보인다"는 수준에서 끝나는 문제가 아니다. 이
네 지표는 날짜를 정확히 맞춰서 일별 주가 데이터와 그냥 조인할 수 **없다**는
뜻이다 — 별도 처리 없이 단순 조인하면, CPI·실업률·기준금리는 수백 개
날짜 중 약 43개에만 값이 있으므로 대부분의 행이 null로 남는다. Silver/Gold
조인을 만들 때의 해법은, 월별 지표를 일별 주가와 조인하기 전에 일별
그래뉼래리티(granularity)로 **forward-fill**하는 것이다 — 다음 월별 관측치가
나올 때까지 마지막으로 알려진 값을 그대로 이어 붙이는 방식이다. 나중에
놀라지 않도록 지금 미리 남겨둔다.

## 다음 단계

두 개의 Bronze 테이블(`bronze_stock_prices`, `bronze_macro_indicators`)이
모두 적재됐다. 3일차는 Bronze→Silver로 넘어간다: 스키마 강제, null 처리,
그리고 위에서 설명한 forward-fill 조인 로직을 만들어서 두 소스의 정형
데이터가 실제로 Gold에서 합쳐질 수 있도록 하는 작업이다.
