#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Parquet 형식으로 저장된 리뷰 데이터를 분석하는 예제 코드
작성자: Review Data Analyzer
작성일: 2025-01-27
"""

import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# 한글 폰트 설정 (Windows)
import matplotlib.font_manager as fm
plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False


import sys
import os
from pathlib import Path

# 상위 디렉토리를 sys.path에 추가하여 config 모듈을 임포트할 수 있도록 함
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import PARQUET_DATA_DIR

# 한글 폰트 설정 (Windows)
import matplotlib.font_manager as fm
plt.rcParams['font.family'] = 'Malgun Gothic'
plt.rcParams['axes.unicode_minus'] = False


class ReviewAnalyzer:
    """Parquet 형식의 리뷰 데이터 분석 클래스"""
    
    def __init__(self, data_dir: str = PARQUET_DATA_DIR):
        """
        초기화
        
        Args:
            data_dir: Parquet 파일이 있는 디렉토리
        """
        self.data_dir = Path(data_dir)
        self.df_restaurants = None
        self.df_reviews = None
        
    def load_data(self):
        """Parquet 파일 로드"""
        print("📂 Parquet 파일 로딩 중...")
        
        restaurants_path = self.data_dir / 'restaurants.parquet'
        reviews_path = self.data_dir / 'reviews.parquet'
        
        self.df_restaurants = pd.read_parquet(restaurants_path)
        self.df_reviews = pd.read_parquet(reviews_path)
        
        print(f"✅ 레스토랑 {len(self.df_restaurants):,}개 로드 완료")
        print(f"✅ 리뷰 {len(self.df_reviews):,}개 로드 완료")
        
    def basic_statistics(self):
        """기본 통계 분석"""
        print("\n📊 기본 통계 분석")
        print("="*60)
        
        # 1. Grid별 레스토랑 수
        print("\n1. Grid별 레스토랑 분포 (상위 10개):")
        grid_counts = self.df_restaurants['grid'].value_counts().head(10)
        for grid, count in grid_counts.items():
            print(f"   {grid}: {count}개")
        
        # 2. 언어별 리뷰 분포
        print("\n2. 언어별 리뷰 분포:")
        lang_counts = self.df_reviews['language'].value_counts()
        for lang, count in lang_counts.head(10).items():
            pct = count / len(self.df_reviews) * 100
            print(f"   {lang}: {count:,}개 ({pct:.1f}%)")
        
        # 3. 평점 통계
        print("\n3. 평점 통계:")
        print(f"   평균 평점: {self.df_reviews['rating'].mean():.2f}")
        print(f"   중앙값: {self.df_reviews['rating'].median():.1f}")
        print(f"   표준편차: {self.df_reviews['rating'].std():.2f}")
        
        # 4. 리뷰 길이 통계
        print("\n4. 리뷰 길이 통계:")
        print(f"   평균 길이: {self.df_reviews['text_length'].mean():.0f}자")
        print(f"   최소 길이: {self.df_reviews['text_length'].min()}자")
        print(f"   최대 길이: {self.df_reviews['text_length'].max()}자")
        print(f"   중앙값: {self.df_reviews['text_length'].median():.0f}자")
        
    def analyze_top_restaurants(self, n=20):
        """상위 레스토랑 분석"""
        print(f"\n🏆 상위 {n}개 레스토랑 (리뷰 수 기준)")
        print("="*60)
        
        top_restaurants = self.df_restaurants.nlargest(n, 'reviews_count')
        
        for idx, row in top_restaurants.iterrows():
            # 해당 레스토랑의 리뷰들 가져오기
            restaurant_reviews = self.df_reviews[
                self.df_reviews['restaurant_id'] == row['restaurant_id']
            ]
            
            # 언어 분포
            lang_dist = restaurant_reviews['language'].value_counts().head(3)
            lang_str = ", ".join([f"{lang}({cnt})" for lang, cnt in lang_dist.items()])
            
            print(f"\n{row['name']} ({row['grid']})")
            print(f"  📍 평점: {row['rating']:.1f} | 리뷰: {row['reviews_count']}개")
            print(f"  📝 언어: {lang_str}")
            print(f"  📊 리뷰 평점 분포: ", end="")
            for rating in [1, 2, 3, 4, 5]:
                count = (restaurant_reviews['rating'] == rating).sum()
                print(f"{rating}★({count}) ", end="")
        
    def analyze_by_grid(self):
        """Grid별 분석"""
        print("\n🗺️ Grid별 상세 분석")
        print("="*60)
        
        # Grid별 통계 계산
        grid_stats = []
        for grid in self.df_restaurants['grid'].unique():
            grid_restaurants = self.df_restaurants[self.df_restaurants['grid'] == grid]
            grid_reviews = self.df_reviews[self.df_reviews['grid'] == grid]
            
            stats = {
                'grid': grid,
                'restaurant_count': len(grid_restaurants),
                'review_count': len(grid_reviews),
                'avg_rating': grid_restaurants['rating'].mean(),
                'avg_review_per_restaurant': len(grid_reviews) / max(len(grid_restaurants), 1),
                'korean_review_pct': (grid_reviews['language'] == 'ko').mean() * 100
            }
            grid_stats.append(stats)
        
        df_grid_stats = pd.DataFrame(grid_stats)
        df_grid_stats = df_grid_stats.sort_values('review_count', ascending=False)
        
        print("\nGrid별 통계 (리뷰 수 상위 10개):")
        for _, row in df_grid_stats.head(10).iterrows():
            print(f"\n{row['grid']}:")
            print(f"  레스토랑: {row['restaurant_count']}개")
            print(f"  리뷰: {row['review_count']}개")
            print(f"  평균 평점: {row['avg_rating']:.2f}")
            print(f"  레스토랑당 평균 리뷰: {row['avg_review_per_restaurant']:.1f}개")
            print(f"  한국어 리뷰 비율: {row['korean_review_pct']:.1f}%")
        
        return df_grid_stats
    
    def analyze_korean_reviews(self):
        """한국어 리뷰 특별 분석"""
        print("\n🇰🇷 한국어 리뷰 분석")
        print("="*60)
        
        korean_reviews = self.df_reviews[self.df_reviews['language'] == 'ko']
        print(f"총 한국어 리뷰 수: {len(korean_reviews):,}개")
        print(f"전체 리뷰 중 비율: {len(korean_reviews)/len(self.df_reviews)*100:.1f}%")
        
        # 한국어 리뷰가 많은 레스토랑
        korean_review_counts = korean_reviews.groupby('restaurant_name').size()
        top_korean = korean_review_counts.nlargest(10)
        
        print("\n한국어 리뷰가 많은 레스토랑 TOP 10:")
        for name, count in top_korean.items():
            restaurant_info = self.df_restaurants[
                self.df_restaurants['name'] == name
            ].iloc[0] if len(self.df_restaurants[self.df_restaurants['name'] == name]) > 0 else None
            
            if restaurant_info is not None:
                total_reviews = restaurant_info['reviews_count']
                korean_pct = (count / total_reviews * 100) if total_reviews > 0 else 0
                print(f"  • {name}: {count}개 ({korean_pct:.1f}% 한국어)")
        
        # 한국어 리뷰 평점 vs 전체 평점
        print(f"\n평점 비교:")
        print(f"  한국어 리뷰 평균 평점: {korean_reviews['rating'].mean():.2f}")
        print(f"  전체 리뷰 평균 평점: {self.df_reviews['rating'].mean():.2f}")
        
    def search_restaurants(self, keyword: str):
        """레스토랑 검색 기능"""
        print(f"\n🔍 '{keyword}' 검색 결과")
        print("="*60)
        
        # 레스토랑 이름에서 검색
        matches = self.df_restaurants[
            self.df_restaurants['name'].str.contains(keyword, case=False, na=False)
        ]
        
        if len(matches) == 0:
            print("검색 결과가 없습니다.")
            return
        
        print(f"총 {len(matches)}개 레스토랑 발견:")
        for _, restaurant in matches.head(10).iterrows():
            print(f"\n• {restaurant['name']} ({restaurant['grid']})")
            print(f"  평점: {restaurant['rating']:.1f} | 리뷰: {restaurant['reviews_count']}개")
            print(f"  주소: {restaurant['address'][:50]}...")
            
            # 해당 레스토랑의 샘플 리뷰
            sample_reviews = self.df_reviews[
                self.df_reviews['restaurant_id'] == restaurant['restaurant_id']
            ].head(2)
            
            if len(sample_reviews) > 0:
                print("  샘플 리뷰:")
                for _, review in sample_reviews.iterrows():
                    text = review['text'][:100] if len(review['text']) > 100 else review['text']
                    print(f"    - [{review['rating']}★] {text}...")
    
    def export_filtered_data(self, condition: str, output_name: str):
        """조건에 맞는 데이터 추출 및 저장"""
        print(f"\n💾 조건부 데이터 추출: {condition}")
        
        # 예시: 평점 4.5 이상 레스토랑의 리뷰만 추출
        if condition == "high_rating":
            high_rated = self.df_restaurants[self.df_restaurants['rating'] >= 4.5]
            restaurant_ids = high_rated['restaurant_id'].tolist()
            filtered_reviews = self.df_reviews[
                self.df_reviews['restaurant_id'].isin(restaurant_ids)
            ]
            
            output_path = self.data_dir / f"{output_name}.parquet"
            filtered_reviews.to_parquet(output_path, compression='snappy')
            print(f"✅ {len(filtered_reviews):,}개 리뷰를 {output_path}에 저장")
            
        # 추가 조건들을 여기에 구현 가능


def main():
    """메인 실행 함수"""
    print("\n🔍 NYC Restaurant Reviews Parquet Data Analyzer")
    print("="*60)
    
    analyzer = ReviewAnalyzer()
    
    # 데이터 로드
    analyzer.load_data()
    
    # 기본 통계
    analyzer.basic_statistics()
    
    # 상위 레스토랑 분석
    analyzer.analyze_top_restaurants(n=10)
    
    # Grid별 분석
    grid_stats = analyzer.analyze_by_grid()
    
    # 한국어 리뷰 분석
    analyzer.analyze_korean_reviews()
    
    # 검색 예시
    print("\n" + "="*60)
    analyzer.search_restaurants("Pizza")
    
    # 데이터 추출 예시
    # analyzer.export_filtered_data("high_rating", "high_rated_reviews")
    
    print("\n✅ 분석 완료!")
    
    # 간단한 쿼리 예시
    reviews_parquet_path = PARQUET_DATA_DIR / 'reviews.parquet'
    print("\n📝 Parquet 데이터 쿼리 예시:")
    print("="*60)
    print(f"""
# Parquet 파일을 직접 쿼리하는 방법:

import pandas as pd
import pyarrow.parquet as pq

# 1. 전체 데이터 로드
df = pd.read_parquet('{reviews_parquet_path}')

# 2. 특정 컬럼만 로드 (메모리 효율적)
df = pd.read_parquet('{reviews_parquet_path}', 
                    columns=['restaurant_name', 'rating', 'text'])

# 3. 조건부 필터링 (PyArrow 사용)
import pyarrow.parquet as pq
import pyarrow.compute as pc

table = pq.read_table('{reviews_parquet_path}')
# 평점 5점인 리뷰만
high_rated = table.filter(pc.equal(table['rating'], 5))
df = high_rated.to_pandas()

# 4. SQL 스타일 쿼리 (DuckDB 사용)
import duckdb
conn = duckdb.connect()
result = conn.execute('''
    SELECT restaurant_name, AVG(rating) as avg_rating, COUNT(*) as review_count
    FROM '{reviews_parquet_path}'
    WHERE language = 'ko'
    GROUP BY restaurant_name
    ORDER BY review_count DESC
    LIMIT 10
''').fetchdf()
    """)


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
