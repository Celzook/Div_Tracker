"""
ETF 고배당 유니버스 — 배당 관련 설정
"""

# Open DART API 키 (없으면 None)
DART_API_KEY = None

# 액면분할 이벤트 테이블
SPLIT_EVENTS = [
    {'code': '005930', 'name': '삼성전자', 'date': '2018-05-04', 'ratio': 50},
    {'code': '051910', 'name': 'LG화학',  'date': '2023-06-01', 'ratio': 5},
]

# 배당 수집 연도 범위
DIVIDEND_START_YEAR = 2019
DIVIDEND_END_YEAR = 2024

# 포트폴리오 설정
PORTFOLIO_TOTAL = 10_000_000_000  # 100억
MAX_SINGLE_WEIGHT = 0.10

# KRX 배당 조회 시 trdDd 매핑 (사업연도 → 조회일)
# 매년 5~6월에 전년도 사업보고서 반영 → 7월 1일 이후 조회
def get_trdDd_for_biz_year(biz_year):
    """사업연도 → KRX 조회일(YYYYMMDD)"""
    return f'{biz_year + 1}0701'
