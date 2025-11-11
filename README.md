# Google Maps Restaurant & Review Crawler (Grid-Based)

뉴욕시 59개 커뮤니티 그리드별로 식당 정보와 리뷰를 자동 수집하는 크롤러입니다.

## 목차

- [주요 특징](#주요-특징)
- [프로젝트 구조](#프로젝트-구조)
- [설치](#설치)
- [사용법](#사용법)
  - [빠른 시작](#빠른-시작)
  - [팀원별 작업 분할](#팀원별-작업-분할)
  - [개별 스크립트 실행](#개별-스크립트-실행)
- [유틸리티](#유틸리티)
- [파일 형식](#파일-형식)
- [주의사항](#주의사항)

## 주요 특징

### 그리드 기반 자동화 파이프라인 (main.py)
- **gridInfo.txt 기반 자동 수집**: 뉴욕시 59개 커뮤니티 그리드별 자동 처리
- **Tier 기반 자동 조정**: grid_tier.csv를 사용하여 지역 중요도(HOT/MID/RES)에 따라 수집할 식당 개수 자동 조정
- **그리드별 완전 수집**: 각 그리드마다 레스토랑 정보 수집 → 즉시 리뷰 수집
- **팀원별 작업 분할**: `--start_from`, `--limit` 옵션으로 작업을 나눠서 병렬 실행 가능
- **중간 저장**: 레스토랑 수집 완료, 식당별 리뷰 수집 완료 시마다 즉시 저장 (오류 발생 시에도 데이터 보존)
- **개별 파일 저장**: 각 레스토랑의 리뷰를 개별 JSON 파일로 저장
- **자동 로그 기록**: 전체 실행 결과를 pipeline_log.json에 자동 저장

### 식당 정보 수집 (getRestaurantsInfo.py)
- Google Places API를 사용하여 식당 정보 수집
- 이름, 주소, place_id, 평점, 리뷰 수, 전화번호 수집
- 검색 쿼리 기반 식당 검색
- 그리드별 자동 쿼리 생성 및 저장
- **Grid 모드**: gridInfo.txt와 grid_tier.csv를 읽어 59개 그리드 자동 처리

### 리뷰 수집 (getReviews.py)
- Selenium을 이용한 동적 크롤링
- 리뷰 별점, 작성일자, 텍스트, 언어 정보 수집
- 자동으로 "자세히", "원문보기" 버튼 클릭하여 전체 리뷰 내용 수집
- 스크롤을 통한 동적 로딩으로 모든 리뷰 수집
- 최신순 정렬로 리뷰 수집
- **식당별 개별 파일 저장**: {그리드코드}_{식당명}_reviews.json 형식
- 오류 발생 시에도 오류 정보 저장

## 프로젝트 구조

```
Crawler/
├── main.py                           # 그리드 기반 전체 파이프라인 (메인 스크립트)
├── getRestaurantsInfo.py             # 식당 정보 수집 유틸리티
├── getReviews.py                     # 리뷰 수집 유틸리티
├── getReviews_optimized.py           # 리뷰 수집 최적화 버전 (권장)
├── config.py                         # API 키 및 Tier별 수집 개수 설정
├── requirements.txt                  # 필수 패키지 목록
├── .env                              # 환경 변수 (API 키 저장)
├── gridInfo.txt                      # 뉴욕시 59개 그리드 정보 (입력)
├── grid_tier.csv                     # 그리드별 Tier 정보 (HOT/MID/RES) (입력)
│
├── docs/                             # 문서
│   ├── README_OPTIMIZATION.md        # 크롤링 최적화 가이드
│   └── README_PARQUET.md             # Parquet 변환 가이드
│
├── scripts/                          # 데이터 처리 스크립트
│   ├── convert_reviews_to_parquet.py # JSON → Parquet 변환
│   └── analyze_parquet_reviews.py    # Parquet 데이터 분석
│
├── utils/                            # 유틸리티
│   └── check_tier_mapping.py         # Tier 매칭 확인 스크립트
│
├── restaurants/                      # 그리드별 레스토랑 정보 (출력)
│   ├── restaurants_MN1.json          # 맨해튼 1지구 레스토랑 정보
│   ├── restaurants_MN2.json          # 맨해튼 2지구 레스토랑 정보
│   ├── restaurants_BX1.json          # 브롱스 1지구 레스토랑 정보
│   └── ...                           # (총 59개 파일)
│
├── reviews/                          # 레스토랑별 리뷰 정보 (출력)
│   ├── MN1_Gramercy Tavern_reviews.json
│   ├── MN1_The Smith_reviews.json
│   ├── MN2_Balthazar_reviews.json
│   └── ...                           # (레스토랑 개수만큼 파일 생성)
│
└── parquet_data/                     # Parquet 형식 데이터 (출력)
    ├── restaurants.parquet           # 레스토랑 정보
    ├── reviews.parquet               # 모든 리뷰 데이터
    └── ...                           # (샘플 CSV 파일들)
```

## 설치

### 1. 필수 패키지 설치

```bash
pip install -r requirements.txt
```

### 2. Google Maps API 키 설정

식당 정보 수집을 위해 Google Maps API 키가 필요합니다.

1. [Google Cloud Console](https://console.cloud.google.com/)에서 프로젝트 생성
2. Places API 활성화
3. API 키 생성
4. 프로젝트 루트 디렉토리에 `.env` 파일 생성:

```env
GOOGLE_MAPS_API_KEY=your_api_key_here
```

### 2-1. Tier별 식당 수집 개수 설정 (선택 사항)

`config.py`에서 각 Tier별로 수집할 식당 개수를 조정할 수 있습니다:

```python
# config.py
TIER_RESTAURANT_COUNT = {
    "HOT": 80,   # 핫플레이스 지역 (기본값: 80개)
    "MID": 50,   # 중간 지역 (기본값: 50개)
    "RES": 25    # 주거 지역 (기본값: 25개)
}
```

이 설정은 `--use_tier_based_restaurants` 플래그를 사용할 때만 적용됩니다.

### 3. Chrome WebDriver 설치

Selenium은 Chrome 브라우저를 제어하기 위해 ChromeDriver가 필요합니다.

**방법 1: 자동 설치 (권장)**
- Selenium 4.6 이상에서는 자동으로 ChromeDriver를 관리합니다.
- 별도 설치가 필요 없습니다.

**방법 2: 수동 설치**
1. [ChromeDriver 다운로드](https://chromedriver.chromium.org/downloads)
2. Chrome 브라우저 버전에 맞는 ChromeDriver 다운로드
3. PATH에 추가하거나 프로젝트 폴더에 저장

## 사용법

### 빠른 시작

#### 테스트 실행 (첫 1개 그리드만)

```bash
python main.py --grid_file gridInfo.txt --limit 1 --max_restaurants 10 --max_reviews 20
```

이 명령은:
- 첫 번째 그리드(MN1)만 처리
- 그리드당 최대 10개 레스토랑 수집
- 레스토랑당 최대 20개 리뷰 수집
- 결과는 `restaurants/`와 `reviews/` 디렉토리에 저장

#### 전체 그리드 실행 - Tier 기반 자동 조정 (권장)

```bash
python main.py --grid_file gridInfo.txt --use_tier_based_restaurants --max_reviews 40 --headless
```

이 명령은:
- 뉴욕시 59개 그리드 전체 처리
- **Tier에 따라 자동으로 식당 개수 조정**: HOT 지역 80개, MID 지역 40개, RES 지역 25개
- 레스토랑당 최대 50개 리뷰 수집
- 백그라운드 모드로 실행 (브라우저 창 숨김)

#### 전체 그리드 실행 - 고정 개수 방식

```bash
python main.py --grid_file gridInfo.txt --max_restaurants 30 --max_reviews 50 --headless
```

이 명령은:
- 뉴욕시 59개 그리드 전체 처리
- 모든 그리드에서 동일하게 30개 레스토랑 수집
- 레스토랑당 최대 40개 리뷰 수집
- 백그라운드 모드로 실행 (브라우저 창 숨김)

### 팀원별 작업 분할

59개 그리드를 5명의 팀원이 나눠서 동시에 처리하는 예시:

#### Tier 기반 모드 (권장)

**팀원 1: 그리드 0~11 (12개)**
```bash
python main.py --grid_file gridInfo.txt --start_from 0 --limit 12 --use_tier_based_restaurants --max_reviews 40 --headless
```

**팀원 2: 그리드 12~23 (12개)**
```bash
python main.py --grid_file gridInfo.txt --start_from 12 --limit 12 --use_tier_based_restaurants --max_reviews 40 --headless
```

**팀원 3: 그리드 24~35 (12개)**
```bash
python main.py --grid_file gridInfo.txt --start_from 24 --limit 12 --use_tier_based_restaurants --max_reviews 40 --headless
```

**팀원 4: 그리드 36~47 (12개)**
```bash
python main.py --grid_file gridInfo.txt --start_from 36 --limit 12 --use_tier_based_restaurants --max_reviews 40 --headless
```

**팀원 5: 그리드 48~58 (11개)**
```bash
python main.py --grid_file gridInfo.txt --start_from 48 --limit 11 --use_tier_based_restaurants --max_reviews 40 --headless
```

#### 고정 개수 모드

**팀원 1: 그리드 0~11 (12개)**
```bash
python main.py --grid_file gridInfo.txt --start_from 0 --limit 12 --max_restaurants 30 --max_reviews 40 --headless
```

**팀원 2: 그리드 12~23 (12개)**
```bash
python main.py --grid_file gridInfo.txt --start_from 12 --limit 12 --max_restaurants 30 --max_reviews 40 --headless
```

**팀원 3: 그리드 24~35 (12개)**
```bash
python main.py --grid_file gridInfo.txt --start_from 24 --limit 12 --max_restaurants 30 --max_reviews 40 --headless
```

**팀원 4: 그리드 36~47 (12개)**
```bash
python main.py --grid_file gridInfo.txt --start_from 36 --limit 12 --max_restaurants 30 --max_reviews 40 --headless
```

**팀원 5: 그리드 48~58 (11개)**
```bash
python main.py --grid_file gridInfo.txt --start_from 48 --limit 11 --max_restaurants 30 --max_reviews 40 --headless
```

### main.py 명령어 파라미터

| 파라미터 | 설명 | 기본값 | 예시 |
|---------|------|--------|------|
| `--grid_file` | Grid 정보 파일 경로 | gridInfo.txt | `--grid_file my_grid.txt` |
| `--start_from` | 시작 그리드 인덱스 | 0 | `--start_from 20` |
| `--limit` | 처리할 그리드 수 | 전체 | `--limit 20` |
| `--max_restaurants` | 그리드당 최대 레스토랑 수 (tier 모드가 아닐 때) | 30 | `--max_restaurants 40` |
| `--use_tier_based_restaurants` | Tier 기반 자동 식당 개수 조정 활성화 | False | `--use_tier_based_restaurants` |
| `--tier_file` | Tier 정보 CSV 파일 경로 | grid_tier.csv | `--tier_file my_tier.csv` |
| `--max_reviews` | 레스토랑당 최대 리뷰 수 | 제한 없음 | `--max_reviews 100` |
| `--headless` | 백그라운드 실행 | False | `--headless` |
| `--restaurants_dir` | 레스토랑 정보 출력 디렉토리 | restaurants | `--restaurants_dir ./data/rest` |
| `--reviews_dir` | 리뷰 출력 디렉토리 | reviews | `--reviews_dir ./data/rev` |
| `--delay` | 그리드 처리 간 대기 시간(초) | 2.0 | `--delay 3.0` |

### 개별 스크립트 실행

일반적으로는 main.py를 사용하는 것을 권장하지만, 필요한 경우 개별 스크립트를 직접 실행할 수도 있습니다.

#### 2-1. 식당 정보 수집 (getRestaurantsInfo.py)

**단일 쿼리 모드:**
```bash
# 기본 사용
python getRestaurantsInfo.py --query "restaurants in Seoul"

# 최대 결과 수 지정
python getRestaurantsInfo.py --query "sushi in Gangnam" --max_results 50

# 출력 파일 지정
python getRestaurantsInfo.py --query "korean food" --output my_restaurants.json
```

**Grid 모드 (59개 그리드 자동 처리):**
```bash
# Tier 기반으로 59개 그리드의 식당 정보 수집
python getRestaurantsInfo.py --grid_mode
```

**파라미터:**
- `--query`: 검색 쿼리 (단일 쿼리 모드에서 필수)
- `--max_results`: 최대 식당 수 (기본값: 30, 단일 쿼리 모드)
- `--output`: 출력 파일 경로 (기본값: restaurants.json, 단일 쿼리 모드)
- `--grid_mode`: Grid 모드 활성화 (gridInfo.txt와 grid_tier.csv 기반 자동 처리)

#### 2-2. 리뷰 수집 (getReviews.py)

```bash
# 특정 그리드의 레스토랑 리뷰 수집
python getReviews.py --input restaurants/restaurants_MN1.json --output_dir reviews --max_reviews 40 --headless

# 백그라운드 실행
python getReviews.py --input restaurants/restaurants_BX1.json --output_dir reviews --headless
```

**파라미터:**
- `--input`: 입력 파일 경로 (기본값: restaurants.json)
- `--output_dir`: 출력 디렉토리 (기본값: reviews)
- `--max_reviews`: 레스토랑당 최대 리뷰 수 (기본값: 제한 없음)
- `--headless`: 백그라운드 실행

**참고**: getReviews.py는 각 레스토랑의 리뷰를 `{그리드코드}_{레스토랑명}_reviews.json` 형식의 개별 파일로 저장합니다.

## 유틸리티

### Tier 매칭 확인 (utils/check_tier_mapping.py)

grid_tier.csv와 gridInfo.txt의 매칭 상태를 확인하는 유틸리티:

```bash
python utils/check_tier_mapping.py
```

이 스크립트는:
- 59개 그리드 각각의 Tier 정보 표시
- Tier별 통계 (HOT/MID/RES 개수)
- 예상 총 식당 수집 개수 계산
- 누락된 Tier 정보 경고
- 매칭 오류 검사

**출력 예시:**
```
================================================================================
코드       Tier   식당 개수      지역명
================================================================================
MN1      HOT    80         트라이베카, 금융 지구 (Tribeca, Financial District)
MN2      HOT    80         그리니치 빌리지, 소호, 차이나타운 (Greenwich Village...)
BX2      RES    25         롱우드, 헌츠 포인트 (Longwood, Hunts Point)
...

================================================================================
[통계 요약]
================================================================================
전체 그리드 수: 59개

✓  HOT: 18개 (30.5%) - 그리드당 80개 식당 = 총 1440개
✓  MID: 31개 (52.5%) - 그리드당 50개 식당 = 총 1550개
✓  RES: 10개 (16.9%) - 그리드당 25개 식당 = 총 250개

예상 총 식당 수집 개수: 3240개
```

### Parquet 변환 (scripts/convert_reviews_to_parquet.py)

수집한 JSON 리뷰 데이터를 효율적인 Parquet 형식으로 변환:

```bash
cd scripts
python convert_reviews_to_parquet.py
```

- JSON 대비 70-90% 용량 절감
- 빠른 쿼리 성능
- Pandas, DuckDB, BigQuery 등과 호환
- 자세한 내용은 `docs/README_PARQUET.md` 참조

### Parquet 데이터 분석 (scripts/analyze_parquet_reviews.py)

변환된 Parquet 데이터를 분석:

```bash
cd scripts
python analyze_parquet_reviews.py
```

- 기본 통계 분석
- Grid별 분석
- 한국어 리뷰 분석
- 레스토랑 검색 기능

## 파일 형식

### 입력 파일 1: gridInfo.txt

뉴욕시 59개 커뮤니티 그리드 정보:

```
MN 1,"트라이베카, 금융 지구 (Tribeca, Financial District)"
MN 2,"그리니치 빌리지, 소호, 차이나타운 (Greenwich Village, SoHo, Chinatown)"
BX 1,"모트 헤이븐, 멜로즈 (Mott Haven, Melrose)"
...
```

형식: `지구코드,"한국어 지역명 (영문 지역명)"`

### 입력 파일 2: grid_tier.csv

각 그리드의 Tier 정보 (HOT/MID/RES):

```csv
code,tier
MN1,HOT
MN2,HOT
MN3,HOT
MN6,MID
BX2,RES
...
```

- **HOT**: 핫플레이스 지역 (기본값: 80개 식당 수집)
- **MID**: 중간 지역 (기본값: 50개 식당 수집)
- **RES**: 주거 지역 (기본값: 25개 식당 수집)

Tier별 수집 개수는 `config.py`의 `TIER_RESTAURANT_COUNT`에서 조정 가능합니다.

### 출력 파일 1: restaurants/{CODE}.json

각 그리드별 레스토랑 정보 (예: `restaurants/restaurants_MN1.json`):

```json
[
    {
        "name": "Gramercy Tavern",
        "address": "42 E 20th St, New York, NY 10003",
        "place_id": "ChIJxxxxxx",
        "rating": 4.6,
        "user_ratings_total": 3245,
        "phone_number": "(212) 477-0777"
    },
    ...
]
```

### 출력 파일 2: reviews/{CODE}_{name}_reviews.json

각 레스토랑별 리뷰 정보 (예: `reviews/MN1_Gramercy Tavern_reviews.json`):

```json
{
    "name": "Gramercy Tavern",
    "place_id": "ChIJxxxxxx",
    "grid": "MN1",
    "address": "42 E 20th St, New York, NY 10003",
    "rating": 4.6,
    "user_ratings_total": 3245,
    "phone_number": "(212) 477-0777",
    "reviews_count": 50,
    "reviews": [
        {
            "rating": 5,
            "date": "3주 전",
            "text": "Amazing food and service...",
            "language": "en"
        },
        {
            "rating": 4,
            "date": "1개월 전",
            "text": "정말 맛있었어요...",
            "language": "ko"
        }
    ]
}
```

### 출력 파일 3: restaurants/pipeline_log.json

전체 파이프라인 실행 로그:

```json
{
    "timestamp": "2025-01-15 14:30:45",
    "total_grids": 20,
    "success_count": 19,
    "total_restaurants": 570,
    "total_reviews": 28500,
    "elapsed_seconds": 7200.5,
    "results": [
        {
            "code": "MN1",
            "restaurants_success": true,
            "reviews_success": true,
            "restaurant_count": 30,
            "review_count": 1500
        }
    ]
}
```

## 작동 방식

### 그리드 기반 파이프라인 (main.py)

1. **초기화 및 설정**
   - `gridInfo.txt` 파일을 읽어 59개 그리드 정보 파싱
   - `--use_tier_based_restaurants` 옵션이 있으면 `grid_tier.csv`에서 Tier 정보 로드
   - `--start_from`, `--limit` 옵션에 따라 처리할 그리드 범위 결정

2. **각 그리드별 처리 (순차적)**
   - **Step 1: 레스토랑 정보 수집**
     - Google Places API로 그리드별 검색 쿼리 생성 (예: "restaurants in Tribeca New York")
     - Tier 기반 모드인 경우:
       - 그리드 코드로 Tier 조회 (HOT/MID/RES)
       - Tier에 따라 수집할 식당 개수 자동 결정 (config.py의 TIER_RESTAURANT_COUNT 참조)
     - 그리드별 레스토랑 정보를 `restaurants/restaurants_{CODE}.json`에 **즉시 저장**

   - **Step 2: 리뷰 수집**
     - 방금 수집한 레스토랑 파일을 읽어 각 레스토랑의 리뷰 크롤링
     - Selenium으로 브라우저 자동화:
       - 리뷰 탭 클릭 → 최신순 정렬 → 스크롤 → "자세히", "원문보기" 클릭
     - 각 레스토랑의 리뷰를 `reviews/{CODE}_{name}_reviews.json`에 **즉시 저장**

3. **오류 처리**
   - 레스토랑 수집 실패 시: 해당 그리드의 리뷰 수집 건너뜀
   - 리뷰 수집 실패 시: 오류 정보를 JSON 파일로 저장하고 다음 레스토랑 진행

4. **최종 요약**
   - 처리된 그리드 수, 성공/실패 통계
   - 총 레스토랑 수, 총 리뷰 수
   - 실행 로그를 `restaurants/pipeline_log.json`에 저장

### Tier 기반 자동 조정

Tier 시스템을 통해 지역 중요도에 따라 효율적으로 데이터를 수집합니다:

- **HOT (핫플레이스)**: 맨해튼 주요 지역, 인기 관광지 등 → 80개 식당 수집
- **MID (중간 지역)**: 일반적인 주거/상업 혼합 지역 → 50개 식당 수집
- **RES (주거 지역)**: 외곽 주거 지역 → 25개 식당 수집

이를 통해:
- API 비용 절감 (덜 중요한 지역은 적게 수집)
- 데이터 품질 향상 (중요한 지역은 더 많이 수집)
- 작업 시간 단축

## 주의사항

### 비용 및 제한
- **Google Maps API 비용**: Places API 사용량에 따라 비용이 발생합니다. [가격 정책](https://mapsplatform.google.com/pricing/) 확인 필요
- **API 요청 제한**: Google Places API는 요청 횟수 제한이 있습니다. `--delay` 옵션으로 대기 시간 조정 가능
- **수집 시간**: 59개 그리드 전체 수집 시 수 시간 소요될 수 있습니다

### 기술적 요구사항
- **안정적인 인터넷 연결**: 크롤링 중 연결이 끊기지 않도록 주의
- **Chrome 브라우저**: Chrome 브라우저가 설치되어 있어야 합니다
- **충분한 저장 공간**: 리뷰가 많을 경우 수 GB의 저장 공간 필요

### 데이터 안정성
- **중간 저장**: 레스토랑 수집 완료 시, 각 식당의 리뷰 수집 완료 시마다 즉시 저장되므로 중간에 오류가 발생해도 데이터 손실 최소화
- **오류 처리**: 오류 발생 시에도 오류 정보가 JSON 파일로 저장됩니다
- **팀원별 작업**: 여러 팀원이 동시에 다른 그리드를 처리할 수 있어 시간 절약 가능

### 크롤링 주의사항
- **크롤링 속도**: 너무 빠르면 구글에서 차단할 수 있습니다
- **헤드리스 모드**: 백그라운드 실행 시 `--headless` 옵션 사용 권장
- **대기 시간**: API 제한에 걸리면 `--delay` 값을 늘리세요 (기본 2초 → 3~5초)

## 문제 해결

### API 키 오류
```
RuntimeError: API_KEY가 설정되어 있지 않습니다.
```
- `.env` 파일이 프로젝트 루트에 있는지 확인
- `GOOGLE_MAPS_API_KEY` 값이 올바른지 확인
- API 키에 Places API가 활성화되어 있는지 확인

### ChromeDriver 오류
```
selenium.common.exceptions.WebDriverException: 'chromedriver' executable needs to be in PATH
```
- Chrome과 ChromeDriver 버전이 일치하는지 확인
- ChromeDriver가 PATH에 있는지 확인
- Selenium 4.6 이상 버전 사용 권장 (자동 ChromeDriver 관리)

### 요소를 찾을 수 없음
```
TimeoutException: Message:
```
- 인터넷 연결 확인
- 페이지 로딩 시간이 충분한지 확인
- 구글 맵 UI가 변경되었을 수 있음 (value.txt의 HTML 구조 확인)

### API 사용량 초과
```
RuntimeError: Text Search API error: OVER_QUERY_LIMIT
```
- API 사용량이 일일 한도를 초과했습니다
- Google Cloud Console에서 사용량 확인
- 필요시 결제 정보 등록 또는 한도 증가 요청

## 라이선스

MIT
