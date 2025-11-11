#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
리뷰 JSON 파일을 Parquet 형식으로 변환하는 스크립트
작성자: Review Data Converter
작성일: 2025-01-27
"""

import os
import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import re
from typing import Dict, List, Tuple
import logging
import warnings
warnings.filterwarnings('ignore')

import sys
from typing import Dict, List, Tuple
import logging
import warnings

# 상위 디렉토리를 sys.path에 추가하여 config 모듈을 임포트할 수 있도록 함
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import REVIEWS_DIR, PARQUET_DATA_DIR, LOG_DIR

warnings.filterwarnings('ignore')

# 로깅 설정
log_file_path = LOG_DIR / 'conversion.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ReviewsToParquetConverter:
    """리뷰 JSON 파일을 Parquet으로 변환하는 클래스"""
    
    def __init__(self, reviews_dir: str = REVIEWS_DIR,
                 output_dir: str = PARQUET_DATA_DIR):
        """
        초기화
        
        Args:
            reviews_dir: 리뷰 JSON 파일들이 있는 디렉토리
            output_dir: Parquet 파일을 저장할 디렉토리
        """
        self.reviews_dir = Path(reviews_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        self.restaurants_data = []
        self.reviews_data = []
        self.error_files = []
        
    def parse_date(self, date_str: str) -> Tuple[datetime, bool]:
        """
        한국어/영어 날짜 문자열을 파싱
        
        Args:
            date_str: 날짜 문자열 (예: "2주 전", "1달 전", "수정일: 3달 전")
            
        Returns:
            (추정 날짜, 수정 여부)
        """
        try:
            # 현재 날짜 기준
            base_date = datetime.now()
            is_modified = False
            
            # 수정일 체크
            if "수정일:" in date_str or "수정일:" in date_str:
                is_modified = True
                date_str = date_str.replace("수정일:", "").replace("수정일:", "").strip()
            
            # 상대 시간 파싱
            if "전" in date_str:
                # 숫자 추출
                numbers = re.findall(r'\d+', date_str)
                if numbers:
                    num = int(numbers[0])
                else:
                    num = 1
                
                if "일" in date_str or "day" in date_str.lower():
                    days_ago = num
                elif "주" in date_str or "week" in date_str.lower():
                    days_ago = num * 7
                elif "달" in date_str or "month" in date_str.lower():
                    days_ago = num * 30
                elif "년" in date_str or "year" in date_str.lower():
                    days_ago = num * 365
                else:
                    days_ago = 0
                    
                estimated_date = pd.Timestamp(base_date) - pd.Timedelta(days=days_ago)
            else:
                # 기본값: 현재 날짜
                estimated_date = pd.Timestamp(base_date)
                
            return estimated_date, is_modified
            
        except Exception as e:
            logger.warning(f"날짜 파싱 실패: {date_str} - {str(e)}")
            return pd.Timestamp(base_date), False
    
    def process_json_file(self, file_path: Path) -> bool:
        """
        단일 JSON 파일 처리
        
        Args:
            file_path: JSON 파일 경로
            
        Returns:
            성공 여부
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 레스토랑 정보 추출
            restaurant_info = {
                'restaurant_id': data.get('place_id', ''),
                'name': data.get('name', ''),
                'grid': data.get('grid', ''),
                'address': data.get('address', ''),
                'rating': float(data.get('rating', 0)),
                'user_ratings_total': int(data.get('user_ratings_total', 0)),
                'phone_number': data.get('phone_number', ''),
                'reviews_count': int(data.get('reviews_count', 0)),
                'file_path': str(file_path)
            }
            self.restaurants_data.append(restaurant_info)
            
            # 리뷰 정보 추출
            reviews = data.get('reviews', [])
            for review in reviews:
                # 날짜 파싱
                estimated_date, is_modified = self.parse_date(review.get('date', ''))
                
                review_info = {
                    'review_id': review.get('review_id', ''),
                    'restaurant_id': data.get('place_id', ''),
                    'restaurant_name': data.get('name', ''),
                    'grid': data.get('grid', ''),
                    'date_original': review.get('date', ''),
                    'estimated_date': estimated_date,
                    'is_modified': is_modified,
                    'language': review.get('language', ''),
                    'rating': float(review.get('rating', 0)),
                    'text': review.get('text', ''),
                    'text_length': len(review.get('text', '')),
                }
                self.reviews_data.append(review_info)
            
            return True
            
        except Exception as e:
            logger.error(f"파일 처리 실패: {file_path} - {str(e)}")
            self.error_files.append(str(file_path))
            return False
    
    def convert_all_files(self):
        """모든 JSON 파일을 변환"""
        logger.info("JSON 파일 검색 시작...")
        
        # 모든 JSON 파일 찾기
        json_files = list(self.reviews_dir.glob("**/*_reviews.json"))
        total_files = len(json_files)
        
        logger.info(f"총 {total_files}개의 JSON 파일 발견")
        
        # 각 파일 처리
        for i, file_path in enumerate(json_files, 1):
            if i % 100 == 0:
                logger.info(f"진행 상황: {i}/{total_files} 파일 처리 완료")
            self.process_json_file(file_path)
        
        logger.info(f"파일 처리 완료: 성공 {total_files - len(self.error_files)}개, 실패 {len(self.error_files)}개")
    
    def create_parquet_files(self):
        """Parquet 파일 생성"""
        if not self.restaurants_data or not self.reviews_data:
            logger.error("변환할 데이터가 없습니다.")
            return
        
        # 레스토랑 DataFrame 생성
        logger.info("레스토랑 데이터프레임 생성 중...")
        df_restaurants = pd.DataFrame(self.restaurants_data)
        
        # 데이터 타입 최적화
        df_restaurants['grid'] = df_restaurants['grid'].astype('category')
        
        # 리뷰 DataFrame 생성
        logger.info("리뷰 데이터프레임 생성 중...")
        df_reviews = pd.DataFrame(self.reviews_data)
        
        # 데이터 타입 최적화
        df_reviews['grid'] = df_reviews['grid'].astype('category')
        df_reviews['language'] = df_reviews['language'].astype('category')
        df_reviews['rating'] = df_reviews['rating'].astype('int8')
        
        # Parquet 파일 저장
        restaurants_path = self.output_dir / 'restaurants.parquet'
        reviews_path = self.output_dir / 'reviews.parquet'
        
        logger.info("Parquet 파일 저장 중...")
        df_restaurants.to_parquet(
            restaurants_path,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        
        df_reviews.to_parquet(
            reviews_path,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        
        # 통계 출력
        self.print_statistics(df_restaurants, df_reviews)
        
        # 샘플 데이터 저장 (분석 확인용)
        self.save_samples(df_restaurants, df_reviews)
        
        logger.info(f"변환 완료! 파일 저장 위치: {self.output_dir}")
        
    def print_statistics(self, df_restaurants: pd.DataFrame, df_reviews: pd.DataFrame):
        """데이터 통계 출력"""
        print("\n" + "="*60)
        print("📊 데이터 변환 결과 통계")
        print("="*60)
        
        print(f"\n📍 레스토랑 데이터:")
        print(f"  - 총 레스토랑 수: {len(df_restaurants):,}개")
        print(f"  - Grid 분포:")
        for grid, count in df_restaurants['grid'].value_counts().head(10).items():
            print(f"    • {grid}: {count}개")
        print(f"  - 평균 평점: {df_restaurants['rating'].mean():.2f}")
        print(f"  - 평균 리뷰 수: {df_restaurants['reviews_count'].mean():.1f}개")
        
        print(f"\n💬 리뷰 데이터:")
        print(f"  - 총 리뷰 수: {len(df_reviews):,}개")
        print(f"  - 언어 분포:")
        for lang, count in df_reviews['language'].value_counts().head(5).items():
            pct = count / len(df_reviews) * 100
            print(f"    • {lang}: {count:,}개 ({pct:.1f}%)")
        print(f"  - 평균 리뷰 길이: {df_reviews['text_length'].mean():.0f}자")
        print(f"  - 평점 분포:")
        for rating in sorted(df_reviews['rating'].unique()):
            count = (df_reviews['rating'] == rating).sum()
            pct = count / len(df_reviews) * 100
            print(f"    • {rating}점: {count:,}개 ({pct:.1f}%)")
        
        # 파일 크기 정보
        restaurants_size = (self.output_dir / 'restaurants.parquet').stat().st_size / (1024*1024)
        reviews_size = (self.output_dir / 'reviews.parquet').stat().st_size / (1024*1024)
        
        print(f"\n💾 파일 크기:")
        print(f"  - restaurants.parquet: {restaurants_size:.2f} MB")
        print(f"  - reviews.parquet: {reviews_size:.2f} MB")
        print("="*60)
        
    def save_samples(self, df_restaurants: pd.DataFrame, df_reviews: pd.DataFrame):
        """샘플 데이터를 CSV로 저장 (확인용)"""
        # 레스토랑 샘플
        sample_restaurants = df_restaurants.head(100)
        sample_restaurants.to_csv(
            self.output_dir / 'sample_restaurants.csv',
            index=False,
            encoding='utf-8-sig'
        )
        
        # 리뷰 샘플
        sample_reviews = df_reviews.head(100)
        sample_reviews.to_csv(
            self.output_dir / 'sample_reviews.csv',
            index=False,
            encoding='utf-8-sig'
        )
        
        logger.info("샘플 CSV 파일 생성 완료")
    
    def run(self):
        """전체 변환 프로세스 실행"""
        logger.info("리뷰 데이터 Parquet 변환 시작")
        
        # JSON 파일 처리
        self.convert_all_files()
        
        # Parquet 파일 생성
        self.create_parquet_files()
        
        # 에러 파일 로깅
        if self.error_files:
            logger.warning(f"처리 실패 파일 목록:")
            for file in self.error_files:
                logger.warning(f"  - {file}")
        
        logger.info("모든 작업 완료!")


def main():
    """메인 함수"""
    print("\n🚀 NYC Restaurant Reviews JSON to Parquet Converter")
    print("="*60)
    
    try:
        # 필요한 패키지 체크
        try:
            import pyarrow
        except ImportError:
            print("⚠️ pyarrow가 설치되지 않았습니다. 설치 중...")
            os.system("pip install pyarrow")
            import pyarrow
        
        # 변환기 실행
        converter = ReviewsToParquetConverter()
        converter.run()
        
        print("\n✅ 변환이 성공적으로 완료되었습니다!")
        print(f"📂 출력 디렉토리: {PARQUET_DATA_DIR}")
        print("\n다음 파일들이 생성되었습니다:")
        print("  • restaurants.parquet - 레스토랑 정보")
        print("  • reviews.parquet - 모든 리뷰 데이터")
        print("  • sample_restaurants.csv - 레스토랑 샘플 (확인용)")
        print("  • sample_reviews.csv - 리뷰 샘플 (확인용)")
        print(f"  • {log_file_path.name} - 변환 로그")
        
    except Exception as e:
        logger.error(f"변환 중 오류 발생: {str(e)}")
        print(f"\n❌ 오류 발생: {str(e)}")
        print(f"자세한 내용은 {log_file_path} 파일을 확인하세요.")


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
