import os
from pathlib import Path
from dotenv import load_dotenv

# .env 파일이 존재하면 해당 파일의 환경 변수를 로드합니다.
load_dotenv()

# 환경 변수에서 "GOOGLE_MAPS_API_KEY" 값을 가져옵니다.
API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

# 프로젝트의 기본 경로를 설정합니다.
# 이 파일(config.py)의 상위 디렉토리를 기본 경로로 사용합니다.
BASE_DIR = Path(__file__).resolve().parent

# 데이터 디렉토리 경로
RESTAURANTS_DIR = BASE_DIR / "restaurants"
REVIEWS_DIR = BASE_DIR / "reviews"
PARQUET_DATA_DIR = BASE_DIR / "parquet_data"
LOG_DIR = BASE_DIR / "log"

# 데이터 파일 경로
GRID_TIER_CSV = BASE_DIR / "grid_tier.csv"
GRID_INFO_TXT = BASE_DIR / "gridInfo.txt"

# Parquet 파일 경로
RESTAURANTS_PARQUET = PARQUET_DATA_DIR / "restaurants.parquet"
REVIEWS_PARQUET = PARQUET_DATA_DIR / "reviews.parquet"

# Tier별 레스토랑 수집 개수 설정
# grid_tier.csv의 tier 값에 따라 수집할 레스토랑 개수를 지정합니다.
TIER_RESTAURANT_COUNT = {
    "HOT": 80,   # 핫플레이스 지역
    "MID": 50,   # 중간 지역
    "RES": 25    # 주거 지역
}

# 디렉토리가 존재하지 않으면 생성
RESTAURANTS_DIR.mkdir(exist_ok=True)
REVIEWS_DIR.mkdir(exist_ok=True)
PARQUET_DATA_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)
