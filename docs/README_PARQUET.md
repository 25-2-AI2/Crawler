# NYC Restaurant Reviews: JSON to Parquet 변환 가이드

## 📋 개요
이 프로젝트는 NYC 레스토랑 리뷰 데이터를 JSON 형식에서 효율적인 Parquet 형식으로 변환합니다.

## 🗂️ 파일 구조

### 입력 데이터 구조
```
E:\gitrepo\Crawler\reviews\
├── BK1\
│   ├── BK1_ACRE_reviews.json
│   ├── BK1_Five Leaves_reviews.json
│   └── ...
├── BK2\
├── MN1\
├── QN1\
└── ...
```

### 출력 데이터 구조
```
E:\gitrepo\Crawler\parquet_data\
├── restaurants.parquet     # 레스토랑 정보 테이블
├── reviews.parquet         # 모든 리뷰 데이터
├── sample_restaurants.csv  # 샘플 데이터 (확인용)
├── sample_reviews.csv      # 샘플 데이터 (확인용)
└── conversion.log         # 변환 로그
```

## 🚀 사용 방법

### 1. 필요 패키지 설치
```bash
pip install pandas numpy pyarrow
```

### 2. JSON을 Parquet으로 변환
```bash
python convert_reviews_to_parquet.py
```

### 3. Parquet 데이터 분석
```bash
python analyze_parquet_reviews.py
```

## 📊 데이터 스키마

### restaurants.parquet
| 컬럼명 | 타입 | 설명 |
|--------|------|------|
| restaurant_id | string | Google Place ID |
| name | string | 레스토랑 이름 |
| grid | category | NYC Grid 코드 (BK1, MN1, etc.) |
| address | string | 주소 |
| rating | float64 | 평균 평점 (1-5) |
| user_ratings_total | int64 | 총 리뷰 수 |
| phone_number | string | 전화번호 |
| reviews_count | int64 | 수집된 리뷰 수 |
| file_path | string | 원본 JSON 파일 경로 |

### reviews.parquet
| 컬럼명 | 타입 | 설명 |
|--------|------|------|
| review_id | string | 고유 리뷰 ID |
| restaurant_id | string | 레스토랑 ID (조인 키) |
| restaurant_name | string | 레스토랑 이름 |
| grid | category | NYC Grid 코드 |
| date_original | string | 원본 날짜 문자열 |
| estimated_date | datetime64 | 추정 날짜 |
| is_modified | bool | 수정된 리뷰 여부 |
| language | category | 리뷰 언어 (en, ko, ja, etc.) |
| rating | int8 | 평점 (1-5) |
| text | string | 리뷰 텍스트 |
| text_length | int64 | 텍스트 길이 |

## 💡 분석 예제

### 기본 데이터 로드
```python
import pandas as pd

# 레스토랑 데이터 로드
df_restaurants = pd.read_parquet('parquet_data/restaurants.parquet')

# 리뷰 데이터 로드
df_reviews = pd.read_parquet('parquet_data/reviews.parquet')
```

### 특정 조건으로 필터링
```python
# 한국어 리뷰만 추출
korean_reviews = df_reviews[df_reviews['language'] == 'ko']

# 평점 4.5 이상 레스토랑
high_rated = df_restaurants[df_restaurants['rating'] >= 4.5]

# 특정 Grid의 리뷰
manhattan_reviews = df_reviews[df_reviews['grid'].str.startswith('MN')]
```

### SQL 스타일 쿼리 (DuckDB 사용)
```python
import duckdb

conn = duckdb.connect()

# Grid별 평균 평점
result = conn.execute('''
    SELECT 
        grid,
        COUNT(DISTINCT restaurant_id) as restaurant_count,
        COUNT(*) as review_count,
        AVG(rating) as avg_rating
    FROM 'parquet_data/reviews.parquet'
    GROUP BY grid
    ORDER BY review_count DESC
''').fetchdf()
```

### PyArrow를 이용한 효율적 필터링
```python
import pyarrow.parquet as pq
import pyarrow.compute as pc

# 테이블 로드
table = pq.read_table('parquet_data/reviews.parquet')

# 5점 리뷰만 필터링
five_star = table.filter(pc.equal(table['rating'], 5))

# 특정 컬럼만 선택
selected = table.select(['restaurant_name', 'rating', 'text'])
```

## 🎯 Parquet 형식의 장점

1. **압축 효율**: JSON 대비 70-90% 용량 절감
2. **빠른 쿼리**: 컬럼 기반 저장으로 필요한 컬럼만 읽기 가능
3. **타입 안정성**: 스키마 정의로 데이터 타입 보장
4. **대용량 처리**: 메모리 효율적인 처리 가능
5. **도구 호환성**: Spark, Pandas, DuckDB, BigQuery 등과 호환

## 📈 예상 성능

- **JSON 총 크기**: ~수백 MB
- **Parquet 총 크기**: ~수십 MB (약 80% 압축)
- **로딩 속도**: JSON 대비 5-10배 빠름
- **쿼리 성능**: 특정 컬럼 접근 시 100배 이상 빠름

## ⚠️ 주의사항

1. 날짜 필드는 상대적 시간("2주 전" 등)에서 추정된 값입니다
2. 원본 날짜 문자열은 `date_original` 필드에 보존됩니다
3. 파일 경로는 Windows 경로 기준입니다 (필요시 수정)

## 📞 문의

데이터 변환 관련 문의사항이 있으시면 이슈를 등록해주세요.
