"""
ETF 고배당 유니버스 — 배당 관련 설정
"""

from datetime import datetime

# Open DART API 키 (없으면 None)
DART_API_KEY = None

# 액면분할 이벤트 테이블
SPLIT_EVENTS = [
    {'code': '005930', 'name': '삼성전자', 'date': '2018-05-04', 'ratio': 50},
    {'code': '051910', 'name': 'LG화학',  'date': '2023-06-01', 'ratio': 5},
]

# 배당 수집 연도 범위
DIVIDEND_START_YEAR = 2019
DIVIDEND_END_YEAR = 2025  # 현재 연도 - 1 (사업보고서 반영 기준)

# 포트폴리오 설정
PORTFOLIO_TOTAL = 10_000_000_000  # 100억
MAX_SINGLE_WEIGHT = 0.10

# KRX 배당 조회 시 trdDd 매핑 (사업연도 → 조회일)
# 대부분 사업보고서는 3~5월 제출 → KRX 반영은 보통 5~7월
def get_trdDd_for_biz_year(biz_year):
    """사업연도 → KRX 조회일(YYYYMMDD)

    원칙적으로는 다음해 7월 이후지만, 아직 7월이 안 됐으면
    현재 날짜에서 가능한 가장 최근 날짜로 cap한다.
    """
    target = f'{biz_year + 1}0701'
    today = datetime.now().strftime('%Y%m%d')
    if target <= today:
        return target
    # 아직 7월이 안 됐을 경우: 5월 기준 시도 (사업보고서 대부분 4~5월 KRX 반영)
    early = f'{biz_year + 1}0501'
    if early <= today:
        return early
    # 그것도 안 됐으면 오늘 날짜 기준
    return today
