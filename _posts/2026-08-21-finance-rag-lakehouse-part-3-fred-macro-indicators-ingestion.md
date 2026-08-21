---
title: "Finance RAG Lakehouse, Part 3: 에러 없이 실패하는 파이프라인 — ticker_ 컬럼 사건"
date: 2026-08-21
categories: [data-engineering, finance-rag-lakehouse]
tags: [databricks, pyspark, delta-lake, fred-api]
---

## 사건: 파이프라인은 성공했다고 보고했다

파이프라인은 초록불이었다. 에러 로그도 없었고, Job은 성공(Success)으로
끝났고, 어제 만든 주가 Bronze 테이블(`bronze_stock_prices`)에는 매일 새
행이 정상적으로 쌓이고 있었다. 그런데 그 안의 데이터는 틀려 있었다 —
컬럼 이름 중 하나가 `ticker`가 아니라 `ticker_`로 저장되고 있었던 것이다.

발견 경위는 이랬다. 오늘 FRED 거시경제 지표를 적재하고 나서, 두 Bronze
테이블을 조인해 정합성을 확인해보려는 순간 `ticker` 컬럼이 없다는 걸
알아챘다. 코드는 한 번도 멈춘 적이 없었고, 스키마 검증도 없었고, 아무도
알림을 받지 않았다. 어제 하루치 데이터는 이미 그 상태로 쌓여 있었다.

## 원인: MultiIndex와 조용히 오염된 컬럼명

원인은, `df["ticker"] = ticker`가 실행되는 시점에 `df`가 이미
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

## 왜 이게 버그가 아니라 침묵 실패인가

에러가 나는 버그는 사실 고마운 축에 속한다. 스택 트레이스가 남고,
파이프라인이 멈추고, 누군가 그 자리에서 바로 알아챈다. 하지만 `ticker_`는
아무것도 멈추지 않았다 — 컬럼 하나가 늘었을 뿐 타입도 맞고 값도 채워져
있으니, 스키마 검증도 정합성 체크도 전부 통과한다. 이게 진짜 위험한
지점이다. 이런 침묵 실패(silent failure)는 즉시 드러나지 않는다. 그
컬럼에 의존하는 무언가가 눈에 띄게 망가질 때까지 — 이번 경우엔 다른
테이블과 조인해보다가 우연히 발견할 때까지 — 조용히 잠복한다. 만약
오늘 FRED 데이터를 적재하며 조인을 시도하지 않았다면, 이 버그는 앞으로
몇 주는 더 아무 일 없다는 듯 데이터를 계속 쌓았을 것이다.

제조 현장이라면 이 패턴은 훨씬 비싸게 먹힌다. 잘못된 센서 값 하나가
파이프라인을 멈추지 않고 그대로 흘러 들어가면, 그 값을 근거로 잘못된
수율 판단이 내려지고, 그 판단을 근거로 잘못된 공정 조정이 실행된다.
에러 없이 실패하는 파이프라인은 단순한 버그가 아니라, 그 위에 얹힌
모든 의사결정을 조용히 오염시키는 신뢰할 수 없는 데이터 체인이다.

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

그리고 `ticker_` 사건에서 얻은 교훈도 그대로 들고 간다. 스키마 검증을
통과했다는 사실만으로는 컬럼이 맞다는 걸 보장하지 못한다는 걸 이번에
직접 확인했으니, Bronze→Silver 경계에 스키마 계약(schema contract)을
넣고, 이 `ticker_` 버그를 그대로 재현하는 회귀 테스트를 붙이기로 했다 —
같은 종류의 침묵 실패가 다음에는 조용히 통과하지 못하도록.
