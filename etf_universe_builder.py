"""
==============================================================================
 한국 상장 ETF Managed Portfolio 유니버스 빌더 v6.3
==============================================================================
 [pykrx 완전 제거 → 네이버 금융 + KRX 직접 HTTP]

 v6.3 수정사항:
  - [버그수정] 설정일: '상장일' 키워드 + 'YYYY년 MM월 DD일' 한국어 날짜 형식 파싱
  - [버그수정] _parse_date_str: YYYY년 MM월 DD일 형식 추가
  - [컬럼변경] NAV(억원) 삭제, 종가 위치 이동 (시가총액 바로 뒤)
  - [컬럼변경] 거래량 → 거래량(주) 이름 변경
  - [컬럼변경] 거래대금(억) 컬럼 추가 (amonut 필드 /100)
  - [캐시버전] listing_dates_v8 (기존 빈 캐시 무효화)

 v6.2 수정사항:
  - [버그수정] naver_get_index_history: requestType=2 → 1 (KOSPI 0일 문제 해결)
  - [버그수정] _naver_etf_holdings: main.naver HTML 구조 기반 regex 파싱으로 교체
  - [버그수정] _naver_listing_dates: etfItemList API list_dt YYYYMMDD 포맷 처리
  - [캐시버전] listing_dates_v7, holdings_v7

 데이터 소스:
  - 네이버 금융 API: ETF 전종목 리스트 (티커/이름/시총/NAV/종가/거래량)
  - 네이버 차트 API: 일별 OHLCV, KOSPI 지수
  - 네이버 금융 웹: 설정일(상장일)
  - KRX 직접 HTTP: 구성종목(PDF)

 워크플로우:
  Step 1: 네이버 금융 → 전체 ETF 티커 + 이름 + 시총 + NAV + 종가 + 거래량
  Step 2: 유형 필터링 — 키워드 기반
  Step 3: 시가총액 필터 (Step 1에서 이미 수집)
  Step 4: 가격/상장일/PDF 수집
  Step 5: 수익률/BM/순위 계산 + 엑셀 저장

 pip install pandas openpyxl tqdm requests
==============================================================================
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tqdm import tqdm
import time, warnings, os, re, pickle, json
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import urlopen, Request
from urllib.error import URLError

warnings.filterwarnings("ignore")

# requests 라이브러리 (없으면 urllib fallback)
try:
    import requests as _requests_lib
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


# ============================================================================
# 설정
# ============================================================================
class Config:
    BASE_DATE = None
    MIN_MARKET_CAP_BILLIONS = 200      # 클라우드 배포 시 메모리 절약
    PRICE_HISTORY_DAYS = 365
    API_DELAY = 0.05
    MAX_WORKERS = 8                    # 클라우드 안정성
    USE_CACHE = True
    CACHE_DIR = "./etf_cache"
    OUTPUT_DIR = "./etf_universe_output"
    TOP_N_HOLDINGS = 10

    EXCLUDE_KEYWORDS = [
        '채권', '국고채', '통안채', '국채', '회사채', '하이일드', '크레딧',
        '중기채', '장기채', '단기채', '초장기', '금리', '선진국채',
        'KIS국채', '국공채', '우량채',
        '특수채', '전단채', '물가채', '공사채', '은행채', '종금채',
        '카드채', '캐피탈채', '지방채', '도시채',
        '머니마켓', 'CD금리', 'KOFR', '단기자금', '예금',
        '파킹', '단기채권', 'KCD',
        'TDF', 'TRF',
        '혼합', '자산배분',
        '커버드콜', 'COVERED CALL', 'COVERED',
        '레버리지', '인버스', '2X', '곱버스',
    ]


# ============================================================================
# 네이버 금융 / KRX 직접 HTTP 래퍼
# ============================================================================
_NAVER_ETF_CACHE = {}   # 메모리 캐시: {date_key: DataFrame}


def _http_get(url, headers=None, timeout=10, encoding=None):
    """범용 HTTP GET — requests 우선, urllib fallback
    encoding=None: 자동 감지 (네이버 금융은 EUC-KR, 차트 API는 UTF-8)
    encoding='euc-kr' 등 명시 시: 해당 인코딩 사용
    """
    hdr = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'}
    if headers:
        hdr.update(headers)
    if HAS_REQUESTS:
        resp = _requests_lib.get(url, headers=hdr, timeout=timeout)
        resp.raise_for_status()
        if encoding:
            resp.encoding = encoding
        else:
            # 응답 헤더 charset 우선 → apparent_encoding → euc-kr/utf-8 시도
            ct = resp.headers.get('Content-Type', '')
            if 'charset=' in ct.lower():
                # 헤더에 charset 명시된 경우 그대로 사용
                resp.encoding = resp.encoding  # requests가 이미 파싱함
            elif resp.apparent_encoding and resp.apparent_encoding.lower() not in ('ascii', 'windows-1252'):
                resp.encoding = resp.apparent_encoding
            else:
                # 네이버 금융 도메인은 대부분 EUC-KR
                if 'naver.com' in url and 'fchart' not in url:
                    resp.encoding = 'euc-kr'
                else:
                    resp.encoding = 'utf-8'
        return resp.text
    else:
        req = Request(url, headers=hdr)
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            if encoding:
                return raw.decode(encoding, errors='ignore')
            # 자동 감지: euc-kr 먼저 시도 (네이버), 실패 시 utf-8
            if 'naver.com' in url and 'fchart' not in url:
                try:
                    return raw.decode('euc-kr')
                except (UnicodeDecodeError, LookupError):
                    pass
            try:
                return raw.decode('utf-8')
            except (UnicodeDecodeError, LookupError):
                return raw.decode('euc-kr', errors='ignore')


def _http_post(url, data, headers=None, timeout=10):
    """범용 HTTP POST — requests 우선, urllib fallback"""
    hdr = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
           'Content-Type': 'application/x-www-form-urlencoded',
           'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiStat/standard/MDCSTAT05901.cmd'}
    if headers:
        hdr.update(headers)
    if HAS_REQUESTS:
        resp = _requests_lib.post(url, data=data, headers=hdr, timeout=timeout)
        resp.raise_for_status()
        return resp.text
    else:
        from urllib.parse import urlencode
        body = urlencode(data).encode('utf-8')
        req = Request(url, data=body, headers=hdr)
        with urlopen(req, timeout=timeout) as resp:
            return resp.read().decode('utf-8', errors='ignore')


# ──────────────────────────────────────────────────────────
# 네이버 금융: ETF 전종목 리스트
# ──────────────────────────────────────────────────────────
def naver_get_all_etfs():
    """네이버 금융 ETF 전종목 조회 → DataFrame
    index: 티커(6자리, 중복 제거)
    columns: ETF명, 시가총액(억원), 종가, 거래량(주), 거래대금(억)

    ※ 네이버 API 필드 (v2 진단 확인):
       marketSum: 시가총액(억원)
       nowVal:    종가(원)
       quant:     거래량(주)
       amonut:    거래대금(백만원) → /100 → 억원  ← API 오타 필드명
    """
    global _NAVER_ETF_CACHE
    cache_key = datetime.now().strftime("%Y%m%d")
    if cache_key in _NAVER_ETF_CACHE:
        return _NAVER_ETF_CACHE[cache_key].copy()

    all_items = []
    url = ("https://finance.naver.com/api/sise/etfItemList.nhn"
           "?etfType=0&targetColumn=market_sum&sortOrder=desc")
    try:
        text = _http_get(url)
        data = json.loads(text)
        all_items = data.get('result', {}).get('etfItemList', [])
    except Exception as e:
        print(f"    ⚠️ 네이버 ETF 리스트 실패: {e}")

    if not all_items:
        print("  ⚠️ 네이버 ETF 리스트 비어있음!")
        return pd.DataFrame(columns=['ETF명', '시가총액(억원)', '종가', '거래량(주)', '거래대금(억)'])

    rows = []
    for item in all_items:
        ticker = str(item.get('itemcode', '')).strip()
        if not ticker or len(ticker) != 6:
            continue
        name = str(item.get('itemname', 'N/A')).strip()

        # 시가총액 (억원 단위)
        raw_cap = item.get('marketSum', 0)
        try:
            cap = float(str(raw_cap).replace(',', ''))
        except (ValueError, TypeError):
            cap = 0

        # 종가
        raw_price = item.get('nowVal', 0)
        try:
            close_price = float(str(raw_price).replace(',', ''))
        except (ValueError, TypeError):
            close_price = 0

        # 거래량(주)
        raw_vol = item.get('quant', 0)
        try:
            volume = int(float(str(raw_vol).replace(',', '')))
        except (ValueError, TypeError):
            volume = 0

        # 거래대금(억) — API 필드명 'amonut'(오타), 단위: 백만원 → /100 → 억원
        raw_amt = item.get('amonut', item.get('amount', 0))
        try:
            trade_amount = round(float(str(raw_amt).replace(',', '')) / 100, 1)
        except (ValueError, TypeError):
            trade_amount = 0

        rows.append({
            '티커': ticker,
            'ETF명': name,
            '시가총액(억원)': cap,
            '종가': close_price,
            '거래량(주)': volume,
            '거래대금(억)': trade_amount,
        })

    df = pd.DataFrame(rows).set_index('티커')

    # 중복 티커 제거
    if df.index.duplicated().any():
        dup_count = df.index.duplicated().sum()
        print(f"    ⚠️ 중복 티커 {dup_count}개 제거")
        df = df[~df.index.duplicated(keep='first')]

    # 시가총액 단위 자동 보정 (혹시 '원' 단위면 → 억원 변환)
    if not df.empty and df['시가총액(억원)'].median() > 1e6:
        df['시가총액(억원)'] = (df['시가총액(억원)'] / 1e8).round(0)

    _NAVER_ETF_CACHE[cache_key] = df.copy()
    return df


# ──────────────────────────────────────────────────────────
# 네이버 차트 API: 일별 종가
# ──────────────────────────────────────────────────────────
def naver_get_price_history(ticker, start_date, end_date):
    """네이버 차트 API → 일별 종가 Series
    start_date, end_date: 'YYYYMMDD'
    """
    url = (f"https://fchart.stock.naver.com/siseJson.naver"
           f"?symbol={ticker}&requestType=1"
           f"&startTime={start_date}&endTime={end_date}&timeframe=day")
    try:
        text = _http_get(url, encoding='utf-8')
        return _parse_naver_chart(text)
    except Exception:
        return pd.Series(dtype=float)


def naver_get_index_history(symbol, start_date, end_date):
    """네이버 차트 API → 지수 일별 종가 Series (KOSPI, KOSDAQ 등)
    KOSPI는 심볼 'KOSPI' 또는 'KPI200' 등을 시도
    """
    # 심볼 매핑: 네이버 차트 API에서 사용하는 심볼
    symbol_candidates = [symbol]
    if symbol.upper() == 'KOSPI':
        symbol_candidates = ['KOSPI', 'KPI200']
    elif symbol.upper() == 'KOSDAQ':
        symbol_candidates = ['KOSDAQ', 'KQI150']

    for sym in symbol_candidates:
        url = (f"https://fchart.stock.naver.com/siseJson.naver"
               f"?symbol={sym}&requestType=1"
               f"&startTime={start_date}&endTime={end_date}&timeframe=day")
        try:
            text = _http_get(url, encoding='utf-8')
            result = _parse_naver_chart(text)
            if not result.empty:
                return result
        except Exception:
            continue
    return pd.Series(dtype=float)


def _parse_naver_chart(text):
    """네이버 차트 API 응답 파싱 → 종가 Series
    응답 형식: [["날짜",시가,고가,저가,종가,거래량], ...]
    날짜에 공백/따옴표가 포함될 수 있음
    """
    text = text.strip()
    rows = []

    # 방법 1: 전체 JSON 배열 파싱 (가장 신뢰도 높음)
    try:
        # 작은따옴표 → 큰따옴표, 후행 콤마 제거
        cleaned = text.replace("'", '"')
        # 후행 콤마 처리: ],] → ]] 패턴
        cleaned = re.sub(r',\s*\]', ']', cleaned)
        data = json.loads(cleaned)
        for row in data:
            if isinstance(row, list) and len(row) >= 5:
                date_str = str(row[0]).strip().strip('"').strip()
                # 날짜 문자열에서 숫자만 추출
                digits = re.sub(r'\D', '', date_str)
                if len(digits) == 8:
                    try:
                        close_val = float(row[4])
                        rows.append({
                            'date': pd.Timestamp(digits),
                            'close': close_val
                        })
                    except (ValueError, TypeError):
                        continue
    except (json.JSONDecodeError, ValueError):
        pass

    # 방법 2: 줄 단위 파싱 (JSON 전체 파싱 실패 시)
    if not rows:
        for line in text.split('\n'):
            line = line.strip().rstrip(',')
            if not line or line in ('[', ']', '[[', ']]'):
                continue
            # 헤더 행 스킵 (날짜/date 문자열 포함 행)
            if '날짜' in line or 'date' in line.lower():
                continue
            try:
                # 작은따옴표 → 큰따옴표
                line = line.replace("'", '"')
                row = json.loads(line)
                if isinstance(row, list) and len(row) >= 5:
                    date_str = str(row[0]).strip().strip('"').strip()
                    digits = re.sub(r'\D', '', date_str)
                    if len(digits) == 8:
                        rows.append({
                            'date': pd.Timestamp(digits),
                            'close': float(row[4])
                        })
            except (json.JSONDecodeError, ValueError):
                continue

    # 방법 3: 정규식으로 직접 추출 (최후 수단)
    if not rows:
        pattern = r'\[?\s*["\']?(\d{8})\s*["\']?\s*,\s*([\d.]+)\s*,\s*([\d.]+)\s*,\s*([\d.]+)\s*,\s*([\d.]+)'
        for m in re.finditer(pattern, text):
            try:
                rows.append({
                    'date': pd.Timestamp(m.group(1)),
                    'close': float(m.group(5))
                })
            except (ValueError, TypeError):
                continue

    if not rows:
        return pd.Series(dtype=float)

    df = pd.DataFrame(rows).set_index('date').sort_index()
    # 중복 날짜 제거
    df = df[~df.index.duplicated(keep='first')]
    return df['close']


# ──────────────────────────────────────────────────────────
# KRX 직접 HTTP: ETF 구성종목 (PDF)
# ──────────────────────────────────────────────────────────
def _krx_get_isin(ticker):
    """KRX에서 티커코드 → ISIN 코드 조회"""
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    params = {
        'bld': 'dbms/comm/finder/finder_secuprodisu',
        'mktsel': 'ETF',
        'searchText': ticker,
        'locale': 'ko_KR',
    }
    try:
        text = _http_post(url, data=params, timeout=10)
        data = json.loads(text)
        blocks = data.get('block1', [])
        for b in blocks:
            short_cd = b.get('short_code', '').strip()
            if short_cd == ticker:
                return b.get('full_code', '')
        if blocks:
            return blocks[0].get('full_code', '')
    except Exception:
        pass
    return ''


def krx_get_etf_holdings(ticker, base_date):
    """KRX 직접 HTTP → ETF 구성종목(PDF) 조회
    Returns: [(종목명, 비중%), ...] 리스트
    """
    isin = _krx_get_isin(ticker)
    if not isin:
        return []

    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    params = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT05901',
        'locale': 'ko_KR',
        'tboxisuCd_finder_secuprodisu1_3': ticker,
        'isuCd': isin,
        'isuCd2': ticker,
        'codeNmisuCd_finder_secuprodisu1_3': '',
        'param1isuCd_finder_secuprodisu1_3': '',
        'strtDd': base_date,
        'endDd': base_date,
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false',
    }

    try:
        text = _http_post(url, data=params, timeout=25)
        data = json.loads(text)
        items_raw = data.get('output', [])
        if not items_raw:
            # 1회 재시도
            time.sleep(1)
            text = _http_post(url, data=params, timeout=25)
            data = json.loads(text)
            items_raw = data.get('output', [])
        if not items_raw:
            return []

        items = []
        for row in items_raw:
            stock_name = row.get('ISU_NM', row.get('ISU_ABBRV', '')).strip()
            weight_str = row.get('COMPST_RTO', '0').replace(',', '')
            try:
                weight = float(weight_str)
            except (ValueError, TypeError):
                weight = 0
            if weight > 0 and stock_name:
                items.append((stock_name[:20], round(weight, 2)))

        items.sort(key=lambda x: x[1], reverse=True)
        return items[:Config.TOP_N_HOLDINGS]

    except Exception:
        return []


# ──────────────────────────────────────────────────────────
# 네이버: 종목명 조회 (KRX holdings 결과 보완용)
# ──────────────────────────────────────────────────────────
def naver_get_stock_name(code):
    """네이버에서 종목코드 → 종목명 조회"""
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        html = _http_get(url, encoding='euc-kr')
        m = re.search(r'<title>\s*:?\s*(.+?)\s*:', html)
        if m:
            name = m.group(1).strip()
            if name and name != code:
                return name
        m = re.search(r'class="wrap_company"[^>]*>.*?<h2[^>]*>.*?<a[^>]*>([^<]+)', html, re.DOTALL)
        if m:
            return m.group(1).strip()
    except Exception:
        pass
    return code


# ============================================================================
# 유틸리티
# ============================================================================
def find_latest_business_date(max_lookback=30):
    """최근 영업일 찾기 (네이버 차트 API로 확인)"""
    try:
        from zoneinfo import ZoneInfo
        now_kst = datetime.now(ZoneInfo("Asia/Seoul"))
    except Exception:
        now_kst = datetime.utcnow() + timedelta(hours=9)

    today_kst = now_kst.date()
    hour_kst = now_kst.hour

    print(f"  🕐 현재 KST: {now_kst.strftime('%Y-%m-%d %H:%M')}")

    start_offset = 0 if hour_kst >= 18 else 1

    for i in range(start_offset, max_lookback):
        d = today_kst - timedelta(days=i)
        if d.weekday() >= 5:
            continue
        ds = d.strftime("%Y%m%d")

        # 네이버 차트 API로 KOSPI 데이터 확인
        try:
            kospi = naver_get_index_history("KOSPI", ds, ds)
            if not kospi.empty:
                print(f"  ✅ 최근 영업일: {ds}")
                return ds
        except Exception:
            pass

        # fallback: 대표 ETF 가격 확인
        try:
            price = naver_get_price_history("069500", ds, ds)
            if not price.empty:
                print(f"  ✅ 최근 영업일: {ds}")
                return ds
        except Exception:
            pass

        time.sleep(0.1)

    # 최후 수단
    fallback = today_kst - timedelta(days=3)
    while fallback.weekday() >= 5:
        fallback -= timedelta(days=1)
    ds = fallback.strftime("%Y%m%d")
    print(f"  ⚠️ fallback 영업일: {ds}")
    return ds


def _timer(label):
    class Timer:
        def __enter__(self):
            self.t0 = time.time()
            return self
        def __exit__(self, *a):
            elapsed = time.time() - self.t0
            print(f"  ⏱️ {label}: {elapsed:.1f}초")
    return Timer()


def _load_cache(name):
    path = os.path.join(Config.CACHE_DIR, name)
    if Config.USE_CACHE and os.path.exists(path):
        try:
            with open(path, 'rb') as f: return pickle.load(f)
        except Exception: pass
    return None


def _save_cache(name, data):
    if Config.USE_CACHE:
        os.makedirs(Config.CACHE_DIR, exist_ok=True)
        with open(os.path.join(Config.CACHE_DIR, name), 'wb') as f:
            pickle.dump(data, f)


# ============================================================================
# Step 1: 전체 ETF 티커 + 이름 + 시장 데이터 (네이버 금융 1회 호출)
# ============================================================================
def step1_get_tickers_and_names(base_date):
    print("\n" + "="*60)
    print(" Step 1: 전체 ETF 티커 + 이름 수집 (네이버 금융)")
    print("="*60)

    with _timer("Step 1"):
        df_naver = naver_get_all_etfs()
        print(f"  → 네이버 금융: {len(df_naver)}개 ETF")

        if len(df_naver) == 0:
            print("  ⚠️ ETF 티커를 하나도 가져오지 못했습니다!")
            df = pd.DataFrame(columns=['ETF명'])
            df.index.name = '티커'
            return df

        # 전체 컬럼 유지 (시가총액/NAV/종가/거래량 포함 — Step 3에서 별도 join 불필요)
        df = df_naver.copy()
        df.index.name = '티커'

        # 메타데이터 캐시 저장 (하위 호환)
        _save_cache(f"naver_meta_{base_date}.pkl", df_naver)

        print(f"  → DataFrame: {df.shape}, 컬럼: {df.columns.tolist()}")
        assert 'ETF명' in df.columns, f"ETF명 컬럼 없음! 컬럼: {df.columns.tolist()}"
        print(f"  ✅ {len(df)}개 ETF 이름 수집 완료")
    return df


# ============================================================================
# Step 2: 유형 필터링 — 키워드 기반 + 카테고리 분류
# ============================================================================
def step2_type_filter_and_classify(df):
    print("\n" + "="*60)
    print(" Step 2: 유형 필터링 + 카테고리 분류")
    print("="*60)

    t0 = time.time()
    before = len(df)

    print(f"  → 입력 DataFrame: {df.shape}, 컬럼: {df.columns.tolist()}")
    if 'ETF명' not in df.columns:
        print("  ⚠️ ETF명 컬럼이 없습니다! 스킵합니다.")
        df['대카테고리'] = '기타'
        df['중카테고리'] = '기타'
        df['소카테고리'] = ''
        return df

    if len(df) == 0:
        print("  ⚠️ ETF가 0개입니다!")
        df['대카테고리'] = pd.Series(dtype=str)
        df['중카테고리'] = pd.Series(dtype=str)
        df['소카테고리'] = pd.Series(dtype=str)
        return df

    def should_exclude(name):
        if pd.isna(name): return True
        s = str(name)
        for kw in Config.EXCLUDE_KEYWORDS:
            if kw.upper() in s.upper(): return True
        if re.search(r'[가-힣]{1,3}채(?![권])', s): return True
        if re.search(r'TRF\d{4}', s.upper()): return True
        return False

    mask = df['ETF명'].apply(should_exclude)
    excluded = df[mask]
    df = df[~mask].copy()

    print(f"  → 제외: {len(excluded)}개 (채권/머니마켓/커버드콜/레버리지/인버스/TDF 등)")
    if len(excluded) > 0:
        for idx, row in excluded.head(10).iterrows():
            print(f"    - {idx} {row['ETF명']}")
        if len(excluded) > 10:
            print(f"    ... 외 {len(excluded)-10}개")

    print(f"  → 필터 후: {df.shape}, 컬럼: {df.columns.tolist()}")

    if len(df) > 0 and 'ETF명' in df.columns:
        df = _classify(df)
    else:
        df['대카테고리'] = '기타'
        df['중카테고리'] = '기타'
        df['소카테고리'] = ''

    print(f"\n  → {before}개 → {len(df)}개")
    print(f"  ⏱️ Step 2: {time.time()-t0:.1f}초")
    return df


# ============================================================================
# Step 3: 시가총액 필터 (네이버 데이터 활용 — API 호출 불필요)
# ============================================================================
def step3_market_cap_filter(df, base_date, min_cap=100):
    print("\n" + "="*60)
    print(f" Step 3: 시가총액 필터 ({min_cap}억 이상)")
    print("="*60)

    t0 = time.time()
    before = len(df)

    # Step 1에서 이미 시가총액이 포함된 경우 — 별도 join 불필요
    if '시가총액(억원)' in df.columns and df['시가총액(억원)'].notna().any():
        print(f"  → 시가총액 컬럼 이미 존재 ({df['시가총액(억원)'].notna().sum()}개)")
    else:
        # 시가총액이 없으면 캐시 또는 네이버 메타에서 join (하위 호환)
        cache_name = f"mktcap_v6_{base_date}.pkl"
        cached = _load_cache(cache_name)
        if cached is not None and '시가총액(억원)' in cached.columns:
            print(f"  → 💾 시총 캐시 로드: {len(cached)}개")
            join_cols = [c for c in ['시가총액(억원)', '종가', '거래량(주)', '거래대금(억)'] if c in cached.columns]
            df = df.join(cached[join_cols].dropna(how='all'), how='left')
        else:
            naver_meta = _load_cache(f"naver_meta_{base_date}.pkl")
            if naver_meta is None:
                print("  → 네이버 메타데이터 없음, 재수집...")
                naver_meta = naver_get_all_etfs()

            if naver_meta is not None and not naver_meta.empty:
                print(f"  → 네이버 시총 데이터: {len(naver_meta)}개")
                meta_cols = ['시가총액(억원)', '종가', '거래량(주)', '거래대금(억)']
                available_cols = [c for c in meta_cols if c in naver_meta.columns]
                df = df.join(naver_meta[available_cols], how='left')

    # 시가총액 필터 적용
    if '시가총액(억원)' in df.columns:
        valid = df['시가총액(억원)'].notna() & (df['시가총액(억원)'] >= min_cap)
        df = df[valid].copy()

        # 단위 자동 보정 (혹시 '원' 단위면 → 억원 변환)
        if not df.empty and df['시가총액(억원)'].median() > 1e6:
            df['시가총액(억원)'] = (df['시가총액(억원)'] / 1e8).round(0)

        df['시가총액(억원)'] = df['시가총액(억원)'].astype(int)

        if not df.empty:
            print(f"  → 시가총액 범위: {df['시가총액(억원)'].min():,} ~ {df['시가총액(억원)'].max():,}억원")

        # 캐시 저장 (종가/거래량도 포함)
        cache_name = f"mktcap_v6_{base_date}.pkl"
        cache_df = df[['시가총액(억원)']].copy()
        for extra_col in ['종가', '거래량(주)', '거래대금(억)']:
            if extra_col in df.columns:
                cache_df[extra_col] = df[extra_col]
        _save_cache(cache_name, cache_df)
    else:
        print("  ⚠️ 시가총액 컬럼 없음 — 필터 건너뜀")

    print(f"  → {before}개 → {len(df)}개 (시총 {min_cap}억+ 필터)")

    etc = df[df['대카테고리'] == '기타']
    if len(etc) > 0:
        print(f"\n  ⚠️ [기타: {len(etc)}개]")
        for idx, row in etc.iterrows():
            print(f"    - {idx} {row['ETF명']}")

    print(f"  ⏱️ Step 3: {time.time()-t0:.1f}초")
    return df


# ============================================================================
# Step 4: 최종 리스트 → 가격 / 상장일 / PDF 수집
# ============================================================================
def step4_collect_all_data(df, base_date):
    print("\n" + "="*60)
    print(f" Step 4: 가격 / 상장일 / 구성종목 수집 ({len(df)}개 ETF)")
    print("="*60)

    t0_total = time.time()
    tickers = df.index.tolist()

    t0 = time.time()
    df, df_close, kospi_close = _collect_prices(df, tickers, base_date)
    print(f"  ⏱️ Step 4-A (가격): {time.time()-t0:.1f}초")

    t0 = time.time()
    df = _collect_listing_dates(df, tickers, base_date)
    print(f"  ⏱️ Step 4-B (설정일): {time.time()-t0:.1f}초")

    t0 = time.time()
    df_pdf = _collect_pdf_holdings(df, tickers, base_date)
    print(f"  ⏱️ Step 4-C (PDF): {time.time()-t0:.1f}초")

    t0 = time.time()
    df = _calc_returns(df, df_close, kospi_close, base_date)
    print(f"  ⏱️ Step 4-D (수익률): {time.time()-t0:.1f}초")

    print(f"  ⏱️ Step 4 전체: {time.time()-t0_total:.1f}초")
    return df, df_close, df_pdf


# ──────────────────────────────────────────────────────────
# 4-A: 가격 (네이버 차트 API)
# ──────────────────────────────────────────────────────────
def _collect_prices(df, tickers, base_date):
    print("\n  ── 4-A: 가격 데이터 (네이버 차트 API) ──")

    base_dt = datetime.strptime(base_date, "%Y%m%d")
    start_date = (base_dt - timedelta(days=Config.PRICE_HISTORY_DAYS)).strftime("%Y%m%d")
    ytd_start = base_dt.replace(month=1, day=1).strftime("%Y%m%d")
    if ytd_start < start_date:
        start_date = ytd_start

    print(f"  → 기간: {start_date} ~ {base_date}")

    # KOSPI (네이버)
    print("  → KOSPI 수집 (네이버)...")
    try:
        kospi = naver_get_index_history("KOSPI", start_date, base_date)
        print(f"  → KOSPI: {len(kospi)}일")
    except Exception as e:
        print(f"  ⚠️ KOSPI 실패: {e}")
        kospi = pd.Series(dtype=float)

    # 캐시
    cache_file = os.path.join(Config.CACHE_DIR, f"price_v7_{base_date}.pkl")
    if Config.USE_CACHE and os.path.exists(cache_file):
        try:
            with open(cache_file, 'rb') as f:
                df_close = pickle.load(f)['close']
            common = [t for t in tickers if t in df_close.columns]
            if len(common) / max(len(tickers), 1) > 0.9:
                print(f"  → 💾 캐시: {len(common)}개 ETF")
                return df, df_close[common], kospi
        except Exception:
            pass

    # 네이버 차트 API로 개별 종가 수집
    print(f"  → 네이버 차트 API: {len(tickers)}개 ETF 가격 수집...")
    df_close = _fetch_prices_naver(tickers, start_date, base_date)

    if not df_close.empty:
        print(f"  → 가격: {df_close.shape[0]}일 × {df_close.shape[1]}개 ETF")

    if Config.USE_CACHE and not df_close.empty:
        os.makedirs(Config.CACHE_DIR, exist_ok=True)
        with open(cache_file, 'wb') as f:
            pickle.dump({'close': df_close}, f)

    return df, df_close, kospi


def _fetch_prices_naver(tickers, start_date, end_date):
    """네이버 차트 API로 개별 ETF 종가 수집 → DataFrame"""
    d = {}
    failed = []

    def fetch(ticker):
        try:
            s = naver_get_price_history(ticker, start_date, end_date)
            time.sleep(Config.API_DELAY)
            if s is not None and not s.empty:
                return ticker, s
        except Exception:
            pass
        return ticker, None

    with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as exe:
        futs = {exe.submit(fetch, t): t for t in tickers}
        with tqdm(total=len(tickers), desc="  네이버 가격") as pbar:
            for f in as_completed(futs):
                t, s = f.result()
                if s is not None:
                    d[t] = s
                else:
                    failed.append(t)
                pbar.update(1)

    print(f"  → 수집 성공: {len(d)}/{len(tickers)}개 (실패: {len(failed)}개)")
    if not d:
        return pd.DataFrame()

    out = pd.DataFrame(d).sort_index().apply(pd.to_numeric, errors='coerce')
    return out


# ──────────────────────────────────────────────────────────
# 4-B: 설정일 (네이버 금융 — pykrx fallback 제거)
# ──────────────────────────────────────────────────────────
def _collect_listing_dates(df, tickers, base_date):
    print("\n  ── 4-B: 설정일 수집 (네이버 금융) ──")

    cache_file = os.path.join(Config.CACHE_DIR, "listing_dates_v8.pkl")
    cached = {}
    if Config.USE_CACHE and os.path.exists(cache_file):
        try:
            with open(cache_file, 'rb') as f:
                cached = pickle.load(f)
        except Exception:
            cached = {}

    to_fetch = [t for t in tickers if t not in cached]
    print(f"  → 신규: {len(to_fetch)}개 / 캐시: {len(tickers)-len(to_fetch)}개")

    if to_fetch:
        print("  → 네이버 금융 설정일 수집...")
        naver = _naver_listing_dates(to_fetch)
        cached.update(naver)

        if Config.USE_CACHE:
            os.makedirs(Config.CACHE_DIR, exist_ok=True)
            with open(cache_file, 'wb') as f:
                pickle.dump(cached, f)

    ok = sum(1 for t in tickers if cached.get(t))
    print(f"  → 설정일 완료: {ok}/{len(tickers)}개")
    df['설정일'] = df.index.map(lambda t: cached.get(t, ''))
    return df


def _parse_date_str(raw):
    """날짜 문자열 → 'YYYY-MM-DD' 형식 변환. 실패 시 ''."""
    s = str(raw).strip()
    # 숫자만 8자리 (e.g. '20200115')
    if re.fullmatch(r'\d{8}', s):
        try:
            dt = datetime.strptime(s, '%Y%m%d')
            if 1990 <= dt.year <= 2030:
                return dt.strftime('%Y-%m-%d')
        except ValueError:
            pass
    # YYYY년 M월 D일 (네이버 main.naver '상장일' 형식)
    m = re.search(r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일', s)
    if m:
        try:
            dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            if 1990 <= dt.year <= 2030:
                return dt.strftime('%Y-%m-%d')
        except ValueError:
            pass
    # YYYY.MM.DD / YYYY-MM-DD / YYYY/MM/DD
    m = re.fullmatch(r'(\d{4})[.\-/](\d{2})[.\-/](\d{2})', s)
    if m:
        try:
            dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            if 1990 <= dt.year <= 2030:
                return dt.strftime('%Y-%m-%d')
        except ValueError:
            pass
    return ''


# 전체 ETF 리스트 API에서 설정일 일괄 수집 (1회 호출 → 전체)
_LISTING_DATE_API_CACHE = {}   # 메모리 캐시

def _fetch_all_listing_dates_from_api():
    """네이버 etfItemList.nhn API → {티커: 'YYYY-MM-DD'} 딕셔너리 반환.
    list_dt 필드가 'YYYYMMDD' 형식(점 없음)임에 주의."""
    global _LISTING_DATE_API_CACHE
    if _LISTING_DATE_API_CACHE:
        return _LISTING_DATE_API_CACHE
    result = {}
    try:
        url = ("https://finance.naver.com/api/sise/etfItemList.nhn"
               "?etfType=0&targetColumn=market_sum&sortOrder=desc")
        text = _http_get(url)
        data = json.loads(text)
        items = data.get('result', {}).get('etfItemList', [])
        for item in items:
            code = str(item.get('itemcode', '')).strip().zfill(6)
            if not code:
                continue
            # API 필드명 후보 (모두 시도)
            raw_date = None
            for fk in ('list_dt', 'listDt', 'listingDate', 'foundingDate', 'startDate'):
                v = item.get(fk)
                if v:
                    raw_date = v
                    break
            if raw_date:
                parsed = _parse_date_str(raw_date)
                if parsed:
                    result[code] = parsed
    except Exception:
        pass
    _LISTING_DATE_API_CACHE = result
    return result


def _naver_listing_dates(tickers):
    results = {}

    # ── 방법 0: etfItemList.nhn API 일괄 수집 (가장 효율적) ──
    try:
        api_dates = _fetch_all_listing_dates_from_api()
        for t in tickers:
            if t in api_dates:
                results[t] = api_dates[t]
    except Exception:
        pass

    # 아직 미수집 티커만 HTML 스크래핑으로 처리
    remaining = [t for t in tickers if t not in results]
    if not remaining:
        print(f"  → 네이버 성공: {len(results)}/{len(tickers)}")
        return results

    def fetch(ticker):
        # 방법 1: 네이버 ETF 상세 페이지 스크래핑
        # main.naver 확인: '상장일' 키워드 + 'YYYY년 MM월 DD일' 형식
        for page in ('main', 'etfinfo'):
            try:
                if page == 'main':
                    url = f"https://finance.naver.com/item/main.naver?code={ticker}"
                else:
                    url = f"https://finance.naver.com/item/etfinfo.naver?code={ticker}"
                req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                raw = urlopen(req, timeout=5).read()
                try:
                    html = raw.decode('utf-8')
                except (UnicodeDecodeError, LookupError):
                    try:
                        html = raw.decode('euc-kr')
                    except (UnicodeDecodeError, LookupError):
                        html = raw.decode('utf-8', errors='ignore')

                # 패턴 목록 (확인된 실제 형식 우선)
                patterns = [
                    # 네이버 main.naver: '상장일' + 'YYYY년 MM월 DD일'
                    r'상장일\s*[^<\d]*(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)',
                    r'상장일\s*</th>\s*<td[^>]*>\s*(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)',
                    r'상장일\s*</dt>\s*<dd[^>]*>\s*(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)',
                    # 설정일 (한국어 날짜 형식)
                    r'설정일\s*[^<\d]*(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)',
                    # 기존 점/슬래시/대시 형식 (하위 호환)
                    r'상장일\s*</th>\s*<td[^>]*>\s*(\d{4}[.\-/]\d{2}[.\-/]\d{2})',
                    r'상장일\s*[^<\d]*(\d{4}[.\-/]\d{2}[.\-/]\d{2})',
                    r'설정일\s*</th>\s*<td[^>]*>\s*(\d{4}[.\-/]\d{2}[.\-/]\d{2})',
                    r'설정일\s*[^<\d]*(\d{4}[.\-/]\d{2}[.\-/]\d{2})',
                    # JSON-in-HTML (YYYYMMDD)
                    r'list_dt["\s:]+(\d{8})',
                    r'listDt["\s:]+(\d{8})',
                    r'listingDate["\s:]+(\d{8})',
                ]
                for pat in patterns:
                    m = re.search(pat, html, re.DOTALL | re.IGNORECASE)
                    if m:
                        parsed = _parse_date_str(m.group(1))
                        if parsed:
                            return ticker, parsed
            except Exception:
                continue

        return ticker, ''

    if remaining:
        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as exe:
            futs = {exe.submit(fetch, t): t for t in remaining}
            with tqdm(total=len(remaining), desc="  네이버 설정일(HTML)") as pbar:
                for f in as_completed(futs):
                    t, d = f.result()
                    if d:
                        results[t] = d
                    pbar.update(1)
    print(f"  → 네이버 성공: {len(results)}/{len(tickers)}")
    return results


# ──────────────────────────────────────────────────────────
# 4-C: PDF 구성종목 → 피벗 매트릭스 (KRX 직접 HTTP)
# ──────────────────────────────────────────────────────────
def _collect_pdf_holdings(df, tickers, base_date):
    """구성종목 수집 → 피벗 매트릭스 df_pdf 반환"""
    print(f"\n  ── 4-C: 구성종목 Top {Config.TOP_N_HOLDINGS} 수집 (KRX 직접) ──")

    cache_file = os.path.join(Config.CACHE_DIR, f"holdings_v8_{base_date}.pkl")
    cached = {}
    if Config.USE_CACHE and os.path.exists(cache_file):
        try:
            with open(cache_file, 'rb') as f:
                cached = pickle.load(f)
        except Exception:
            cached = {}

    to_fetch = [t for t in tickers if t not in cached]
    print(f"  → 신규: {len(to_fetch)}개 / 캐시: {len(tickers)-len(to_fetch)}개")

    if to_fetch:
        print("  → KRX 구성종목 조회...")
        krx_results = _krx_holdings_batch(to_fetch, base_date)
        cached.update(krx_results)

        missing_count = sum(1 for t in to_fetch
                           if t not in cached or len(cached.get(t, [])) == 0)
        if missing_count:
            print(f"  → 비중 없는 ETF(해외 등): {missing_count}개 → 빈칸 처리")

        if Config.USE_CACHE:
            os.makedirs(Config.CACHE_DIR, exist_ok=True)
            with open(cache_file, 'wb') as f:
                pickle.dump(cached, f)

    ok = sum(1 for t in tickers if cached.get(t) and len(cached[t]) > 0)
    print(f"  → 구성종목 완료: {ok}/{len(tickers)}개")

    # 피벗 매트릭스 생성
    all_stocks = set()
    for t in tickers:
        for name, w in cached.get(t, []):
            all_stocks.add(name)

    sorted_stocks = sorted(all_stocks, key=lambda x: x)
    print(f"  → 전체 고유 종목 수: {len(sorted_stocks)}개")

    pdf_data = {}
    for ticker in tickers:
        row = {'ETF명': df.at[ticker, 'ETF명'] if ticker in df.index else ''}
        holdings_dict = {name: w for name, w in cached.get(ticker, [])}
        for s in sorted_stocks:
            row[s] = holdings_dict.get(s, np.nan)
        pdf_data[ticker] = row

    df_pdf = pd.DataFrame.from_dict(pdf_data, orient='index')
    df_pdf.index.name = '티커'

    stock_cols = [c for c in df_pdf.columns if c != 'ETF명']
    filled = df_pdf[stock_cols].notna().sum().sum()
    print(f"  → 매트릭스: {len(df_pdf)} ETF × {len(sorted_stocks)} 종목 ({filled:,.0f}개 셀 채움)")

    return df_pdf


def _krx_holdings_batch(tickers, base_date):
    """KRX 직접 HTTP로 구성종목 배치 수집 (실패 시 네이버 fallback)"""
    results = {}

    # ETF 티커 집합 (ETF-in-ETF 제외용)
    etf_set = set()
    try:
        naver_df = naver_get_all_etfs()
        etf_set = set(naver_df.index)
    except Exception:
        pass

    def fetch(ticker):
        # 1차: KRX 직접 HTTP
        try:
            items = krx_get_etf_holdings(ticker, base_date)
            time.sleep(Config.API_DELAY)
            if items:
                filtered = []
                for name, w in items:
                    if name.isdigit() and len(name) == 6 and name in etf_set:
                        continue
                    filtered.append((name, w))
                if filtered:
                    return ticker, filtered[:Config.TOP_N_HOLDINGS]
        except Exception:
            pass

        # 2차: 네이버 ETF 상세 페이지에서 구성종목 스크래핑 (fallback)
        try:
            items = _naver_etf_holdings(ticker)
            if items:
                filtered = []
                for name, w in items:
                    if name.isdigit() and len(name) == 6 and name in etf_set:
                        continue
                    filtered.append((name, w))
                if filtered:
                    return ticker, filtered[:Config.TOP_N_HOLDINGS]
        except Exception:
            pass

        return ticker, []

    # KRX에 너무 빠르게 요청하면 차단되므로 스레드 수 제한
    with ThreadPoolExecutor(max_workers=min(Config.MAX_WORKERS, 4)) as exe:
        futs = {exe.submit(fetch, t): t for t in tickers}
        with tqdm(total=len(tickers), desc="  KRX 구성종목") as pbar:
            for f in as_completed(futs):
                t, items = f.result()
                if items:
                    results[t] = items
                pbar.update(1)

    print(f"  → KRX+네이버 성공: {len(results)}/{len(tickers)}")
    return results


def _naver_etf_holdings(ticker):
    """네이버 ETF 구성종목 스크래핑

    진단 확인된 실제 HTML 구조:
      국내 ETF: <td class="ctg"><a href="...">삼성전자</a></td>
                <td class="per">25.30</td>
      해외 ETF: <td class="ctg"><span>NVIDIA CORP</span></td>
                <td class="per">-</td>   ← 숫자 아닌 '-'
    """

    # 방법 1: finance.naver.com/item/main.naver
    try:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        html = None
        for enc in ('utf-8', 'euc-kr'):
            try:
                html = _http_get(url, encoding=enc, timeout=15)
                if html and len(html) > 1000:
                    break
            except Exception:
                continue
        if not html:
            raise ValueError("HTML 수신 실패")

        section_m = re.search(
            r'구성종목\(구성자산\)(.*?)</tbody>',
            html, re.DOTALL
        )
        if section_m:
            section = section_m.group(1)

            # ── 1-A: 국내 ETF — <a> 태그 + 비중 숫자 ──────────────
            row_pat_a = (
                r'<td\s+class="ctg">'
                r'(?:(?!</td>).)*?'
                r'<a[^>]*>([^<]+)</a>'       # <a>종목명</a>
                r'(?:(?!</td>).)*?</td>'
                r'(?:(?!</tr>).)*?'
                r'<td\s+class="per">\s*([\d.]+)'  # 숫자 비중
            )
            items_domestic = []
            for m in re.finditer(row_pat_a, section, re.DOTALL):
                name = m.group(1).strip()
                try:
                    weight = float(m.group(2))
                    if weight > 0 and name:
                        items_domestic.append((name[:20], round(weight, 2)))
                except (ValueError, TypeError):
                    continue

            if items_domestic:
                items_domestic.sort(key=lambda x: x[1], reverse=True)
                return items_domestic[:Config.TOP_N_HOLDINGS]

            # ── 1-B: 해외 ETF — <span> 태그, 비중은 '-' ───────────
            # 진단 확인: NVIDIA CORP, APPLE INC 등이 <span>으로 감싸짐
            row_pat_b = (
                r'<td\s+class="ctg">'
                r'(?:(?!</td>).)*?'
                r'<span[^>]*>([^<]+)</span>'  # <span>종목명</span>
                r'(?:(?!</td>).)*?</td>'
            )
            items_overseas = []
            for m in re.finditer(row_pat_b, section, re.DOTALL):
                name = m.group(1).strip()
                if name and len(name) >= 2:
                    items_overseas.append((name[:20], 0.0))

            if items_overseas:
                # 비중 없으므로 등장 순서 유지 (정렬 안 함)
                return items_overseas[:Config.TOP_N_HOLDINGS]

    except Exception:
        pass

    # 방법 2: navercomp.wisereport.co.kr
    # ※ 진단 확인: JS 동적 로드라 테이블 skeleton만 있음 → 이름 수집 불가
    #   그래도 일부 국내 ETF는 HTML에 데이터 있을 수 있으므로 유지
    try:
        url2 = (f"https://navercomp.wisereport.co.kr"
                f"/v2/ETF/index.aspx?cmp_cd={ticker}")
        html2 = _http_get(url2, encoding='utf-8', timeout=15)

        section2_m = re.search(
            r'CU당 구성종목(.*?)</table>',
            html2, re.DOTALL
        )
        if section2_m:
            sec2 = section2_m.group(1)

            rows2 = re.findall(
                r'<td[^>]*>\s*(?:<a[^>]*>)?([가-힣A-Za-z][가-힣A-Za-z0-9&(). ]{1,24}?)(?:</a>)?\s*</td>'
                r'(?:.*?<td[^>]*>\s*([\d]+\.[\d]+)\s*</td>)',
                sec2, re.DOTALL
            )
            items2 = []
            for name, weight_str in rows2:
                name = name.strip()
                try:
                    w = float(weight_str)
                    if w > 0 and name:
                        items2.append((name[:20], round(w, 2)))
                except (ValueError, TypeError):
                    continue
            if items2:
                items2.sort(key=lambda x: x[1], reverse=True)
                return items2[:Config.TOP_N_HOLDINGS]

    except Exception:
        pass

    return []


# ──────────────────────────────────────────────────────────
# 4-D: 수익률 / BM / 순위
# ──────────────────────────────────────────────────────────
def _calc_returns(df, df_close, kospi_close, base_date):
    if df_close.empty or len(df_close) < 2:
        return df

    base_dt = datetime.strptime(base_date, "%Y%m%d")
    ytd_dt = base_dt.replace(month=1, day=1)
    ytd_mask = df_close.index >= pd.Timestamp(ytd_dt)
    ytd_loc = df_close.index.get_loc(df_close.index[ytd_mask][0]) if ytd_mask.any() else 0

    n = len(df_close)
    periods = {'1M': min(21, n-1), '3M': min(63, n-1),
               '6M': min(126, n-1), '1Y': n-1}

    for label, lb in periods.items():
        df[f'수익률_{label}(%)'] = ((df_close.iloc[-1] / df_close.iloc[-1-lb] - 1) * 100).round(2)
    df['수익률_YTD(%)'] = ((df_close.iloc[-1] / df_close.iloc[ytd_loc] - 1) * 100).round(2)

    dr = df_close.pct_change().dropna()
    if len(dr) > 20:
        df['연간변동성(%)'] = (dr.std() * np.sqrt(252) * 100).round(2)

    # KOSPI BM
    if not kospi_close.empty and len(kospi_close) > 1:
        kc = kospi_close.sort_index()
        kn = len(kc)
        bm = {}
        for label, lb in periods.items():
            klb = min(lb, kn-1)
            bm[label] = round((kc.iloc[-1] / kc.iloc[-1-klb] - 1) * 100, 2)

        k_ytd = kc.index >= pd.Timestamp(ytd_dt)
        bm['YTD'] = round((kc.iloc[-1] / kc[k_ytd].iloc[0] - 1) * 100, 2) if k_ytd.any() else 0.0

        print(f"\n  📈 KOSPI BM (검증):")
        print(f"    최종: {kc.iloc[-1]:,.0f} ({kc.index[-1].strftime('%Y-%m-%d')})")
        k1m = -1 - min(21, kn-1)
        print(f"    1M 기준: {kc.iloc[k1m]:,.0f} ({kc.index[k1m].strftime('%Y-%m-%d')})")
        for l, r in bm.items():
            print(f"    {l}: {r:+.2f}%")

        for label in ['1M', '3M', '6M', '1Y']:
            df[f'BM_{label}(%)'] = (df[f'수익률_{label}(%)'] - bm[label]).round(2)
        df['BM_YTD(%)'] = (df['수익률_YTD(%)'] - bm['YTD']).round(2)

    if 'BM_YTD(%)' in df.columns:
        df['순위(YTD_BM+)'] = df['BM_YTD(%)'].rank(
            ascending=False, method='min', na_option='bottom').astype(int)

    return df


# ============================================================================
# 카테고리 분류 (변경 없음)
# ============================================================================
def _classify(df):
    def classify(name):
        n = str(name); u = n.upper()

        if any(kw in u for kw in ['금현물','금선물','골드','GOLD','국제금','금액티브','금ETF']): return '원자재','금',''
        if any(kw in u for kw in ['은현물','은선물','실버','SILVER']): return '원자재','은',''
        if any(kw in u for kw in ['원유','WTI','브렌트','BRENT','오일']): return '원자재','원유',''
        if any(kw in u for kw in ['천연가스']): return '원자재','천연가스',''
        if any(kw in u for kw in ['구리','팔라듐','백금','플래티넘','비철']): return '원자재','비철금속',''
        if any(kw in u for kw in ['곡물','농산물','옥수수','대두','밀']): return '원자재','농산물',''
        if any(kw in u for kw in ['원자재','커머디티','COMMODITY']): return '원자재','원자재(종합)',''

        if any(kw in u for kw in ['달러','USD','달러선물','미국달러']): return '통화/환율','달러',''
        if any(kw in u for kw in ['엔화','엔선물','JPY']): return '통화/환율','엔화',''
        if any(kw in u for kw in ['유로','EUR']): return '통화/환율','유로',''
        if any(kw in u for kw in ['위안','CNY','CNH']): return '통화/환율','위안',''
        if any(kw in u for kw in ['환헤지','통화','외환','FX']): return '통화/환율','통화(기타)',''

        if any(kw in u for kw in ['리츠','REITS','REIT','부동산']): return '리츠/부동산','리츠',''

        if '그룹' in n:
            grp = n.split('그룹')[0]
            for pfx in ['TIGER ','KODEX ','ACE ','KBSTAR ','SOL ','HANARO ','ARIRANG ','KOSEF ','PLUS ']:
                grp = grp.replace(pfx.strip(),'').strip()
            return '그룹주', grp+'그룹', ''

        if any(kw in u for kw in ['미국','나스닥','NASDAQ','S&P','S&P500','다우','필라델피아','FANG','NYSE','미국빅테크','미국테크']):
            return '해외주식','미국',_sub(u,'미국')
        if any(kw in u for kw in ['일본','니케이','NIKKEI','TOPIX','도쿄']): return '해외주식','일본',_sub(u,'일본')
        if any(kw in u for kw in ['중국','차이나','CSI','항셍','HANG SENG','심천','상해','HSCEI','홍콩','CHINEXT','본토','CHINA']):
            return '해외주식','중국',_sub(u,'중국')
        if any(kw in u for kw in ['인도','니프티','NIFTY','INDIA']): return '해외주식','인도',_sub(u,'인도')
        if any(kw in u for kw in ['베트남','VN30','VIETNAM']): return '해외주식','베트남',_sub(u,'베트남')
        if any(kw in u for kw in ['대만','TAIWAN']): return '해외주식','대만',''
        if any(kw in u for kw in ['유럽','EURO STOXX','유로스탁스','STOXX']): return '해외주식','유럽',_sub(u,'유럽')
        if any(kw in u for kw in ['인도네시아']): return '해외주식','인도네시아',''
        if any(kw in u for kw in ['브라질']): return '해외주식','브라질',''
        if any(kw in u for kw in ['멕시코']): return '해외주식','멕시코',''
        if any(kw in u for kw in ['사우디']): return '해외주식','사우디',''
        if any(kw in u for kw in ['선진국','MSCI WORLD','ACWI','글로벌']): return '해외주식','글로벌/선진국',''
        if any(kw in u for kw in ['신흥국','EM','EMERGING']): return '해외주식','신흥국',''

        if any(kw in u for kw in ['반도체','팹리스']): return '섹터/테마','반도체',''
        if any(kw in u for kw in ['2차전지','배터리','리튬','양극재','음극재']): return '섹터/테마','2차전지/배터리',''
        if any(kw in u for kw in ['AI','인공지능','AI반도체','AI밸류체인']): return '섹터/테마','AI',''
        if any(kw in u for kw in ['소프트웨어','SW','클라우드','SAAS','데이터센터','IDC']): return '섹터/테마','소프트웨어/클라우드',''
        if any(kw in u for kw in ['게임','GAME']): return '섹터/테마','게임',''
        if any(kw in u for kw in ['엔터','K-POP','KPOP','콘텐츠','K-콘텐츠','K콘텐츠','미디어콘텐츠']): return '섹터/테마','엔터/콘텐츠',''
        if any(kw in u for kw in ['미디어','방송','광고']): return '섹터/테마','미디어',''
        if any(kw in u for kw in ['바이오','헬스케어','제약','의료기기','의료','헬스']): return '섹터/테마','바이오/헬스케어',''
        if any(kw in u for kw in ['자동차','전기차','EV','모빌리티','자율주행']): return '섹터/테마','자동차/모빌리티',''
        if any(kw in u for kw in ['로봇','자동화','로보틱스']): return '섹터/테마','로봇/자동화',''
        if any(kw in u for kw in ['은행']): return '섹터/테마','은행',''
        if any(kw in u for kw in ['증권']): return '섹터/테마','증권',''
        if any(kw in u for kw in ['보험']): return '섹터/테마','보험',''
        if any(kw in u for kw in ['금융']): return '섹터/테마','금융(기타)',''
        if any(kw in u for kw in ['건설','인프라','시멘트']): return '섹터/테마','건설/인프라',''
        if any(kw in u for kw in ['조선']): return '섹터/테마','조선',''
        if any(kw in u for kw in ['해운']): return '섹터/테마','해운',''
        if any(kw in u for kw in ['방산','방위','우주항공','항공우주','우주','스페이스']): return '섹터/테마','방산/우주항공',''
        if any(kw in u for kw in ['화학','소재','신소재']): return '섹터/테마','화학/소재',''
        if any(kw in u for kw in ['철강','금속']): return '섹터/테마','철강/금속',''
        if any(kw in u for kw in ['에너지','석유']): return '섹터/테마','에너지',''
        if any(kw in u for kw in ['유틸리티','전력','발전','전력인프라','전력설비','송전','변전']): return '섹터/테마','유틸리티/전력',''
        if any(kw in u for kw in ['필수소비재','음식료','식품','F&B']): return '섹터/테마','필수소비재/식품',''
        if any(kw in u for kw in ['경기소비재','럭셔리','화장품','뷰티']): return '섹터/테마','경기소비재/뷰티',''
        if any(kw in u for kw in ['소비재']): return '섹터/테마','소비재(기타)',''
        if any(kw in u for kw in ['통신','5G','6G','텔레콤']): return '섹터/테마','통신',''
        if any(kw in u for kw in ['운송','물류','택배']): return '섹터/테마','운송/물류',''
        if any(kw in u for kw in ['항공']) and '우주' not in u: return '섹터/테마','항공',''
        if any(kw in u for kw in ['ESG','탄소','그린','친환경']): return '섹터/테마','ESG/친환경',''
        if any(kw in u for kw in ['수소','태양광','풍력','신재생','재생에너지']): return '섹터/테마','수소/신재생에너지',''
        if any(kw in u for kw in ['원자력','원전','우라늄','SMR']): return '섹터/테마','원자력',''
        if any(kw in u for kw in ['사이버보안','보안','시큐리티']): return '섹터/테마','사이버보안',''
        if any(kw in u for kw in ['메타버스','XR','VR','AR']): return '섹터/테마','메타버스/XR',''
        if any(kw in u for kw in ['블록체인','디지털자산','비트코인','가상자산','크립토']): return '섹터/테마','블록체인/가상자산',''
        if any(kw in u for kw in ['플랫폼','인터넷','이커머스','커머스']): return '섹터/테마','플랫폼/인터넷',''
        if any(kw in u for kw in ['IT','테크','기술','ICT']): return '섹터/테마','IT/테크',''
        # 최근 테마 추가
        if any(kw in u for kw in ['액티브','ACTIVE']) and '채권' not in u and '금리' not in u: return '섹터/테마','액티브',''
        if any(kw in u for kw in ['멀티에셋','MULTI ASSET','멀티자산']): return '섹터/테마','멀티에셋',''
        if any(kw in u for kw in ['K-방산','한화방산']): return '섹터/테마','방산/우주항공',''
        if any(kw in u for kw in ['여행','레저','호텔','리조트','카지노']): return '섹터/테마','여행/레저',''

        if any(kw in u for kw in ['배당','고배당','배당성장','DIVIDEND','프리미엄','월배당','분배','인컴']):
            return '배당/인컴','배당',''

        if re.search(r'200(?:TR)?$', n.strip().upper()) or 'KOSPI200' in u or 'KOSPI 200' in u: return '시장대표','KOSPI200',''
        if any(kw in u for kw in ['코스닥','KOSDAQ']): return '시장대표','코스닥',''
        if any(kw in u for kw in ['중소형','중소']): return '시장대표','중소형주',''
        if any(kw in u for kw in ['중형','미드캡','MID']): return '시장대표','중소형주',''
        if any(kw in u for kw in ['소형','스몰캡','SMALL']): return '시장대표','소형주',''
        if any(kw in u for kw in ['코스피','KOSPI','KRX300','KRX 300','TOP10','TOP30','TOP 10','대형']):
            return '시장대표','대형주',''

        if any(kw in u for kw in ['모멘텀','MOMENTUM']): return '스마트베타','모멘텀',''
        if any(kw in u for kw in ['밸류','가치','VALUE']): return '스마트베타','밸류',''
        if any(kw in u for kw in ['퀄리티','QUALITY','우량']): return '스마트베타','퀄리티',''
        if any(kw in u for kw in ['로우볼','저변동','LOW VOL']): return '스마트베타','저변동성',''
        if any(kw in u for kw in ['동일가중','EQUAL']): return '스마트베타','동일가중',''
        if any(kw in u for kw in ['성장','GROWTH']): return '스마트베타','성장',''
        if any(kw in u for kw in ['멀티팩터']): return '스마트베타','멀티팩터',''

        return '기타','기타',''

    results = df['ETF명'].apply(classify)
    df['대카테고리'] = results.apply(lambda x: x[0])
    df['중카테고리'] = results.apply(lambda x: x[1])
    df['소카테고리'] = results.apply(lambda x: x[2])

    print("\n  [대카테고리]")
    for c, n in df['대카테고리'].value_counts().items():
        print(f"    {c}: {n}개")
    print("\n  [중카테고리 상위 20]")
    for c, n in df['중카테고리'].value_counts().head(20).items():
        print(f"    {c}: {n}개")
    return df


def _sub(u, country):
    if country == '미국':
        if any(kw in u for kw in ['S&P','S&P500']): return 'S&P500'
        if any(kw in u for kw in ['나스닥','NASDAQ']): return '나스닥'
        if any(kw in u for kw in ['다우','DOW']): return '다우'
        if any(kw in u for kw in ['반도체','필라델피아']): return '미국반도체'
        if any(kw in u for kw in ['빅테크','테크','FANG','TECH']): return '미국테크'
        if any(kw in u for kw in ['배당','DIVIDEND']): return '미국배당'
        if any(kw in u for kw in ['헬스','바이오','제약']): return '미국헬스케어'
        if any(kw in u for kw in ['금융','은행']): return '미국금융'
        if any(kw in u for kw in ['성장','GROWTH']): return '미국성장'
        if any(kw in u for kw in ['가치','VALUE']): return '미국가치'
        if any(kw in u for kw in ['AI','인공지능']): return '미국AI'
        if any(kw in u for kw in ['방산','우주항공','방위']): return '미국방산'
        if any(kw in u for kw in ['리츠','REITS']): return '미국리츠'
        if any(kw in u for kw in ['원자력','우라늄','SMR']): return '미국원자력'
        return '미국(기타)'
    if country == '일본':
        if any(kw in u for kw in ['니케이','NIKKEI']): return '니케이225'
        if 'TOPIX' in u: return 'TOPIX'
        if '반도체' in u: return '일본반도체'
        return '일본(기타)'
    if country == '중국':
        if any(kw in u for kw in ['CSI300','CSI 300']): return 'CSI300'
        if any(kw in u for kw in ['항셍','HANG SENG','HSCEI']): return '항셍'
        if any(kw in u for kw in ['심천','CHINEXT']): return '심천/차이넥스트'
        if 'CSI' in u: return 'CSI(기타)'
        return '중국(기타)'
    if country == '인도':
        if any(kw in u for kw in ['니프티','NIFTY']): return '니프티50'
        return '인도(기타)'
    if country == '베트남':
        if 'VN30' in u: return 'VN30'
        return '베트남(기타)'
    if country == '유럽':
        if any(kw in u for kw in ['STOXX','유로스탁스']): return 'EURO STOXX'
        return '유럽(기타)'
    return ''


# ============================================================================
# Step 5: 저장
# ============================================================================
def step5_save(df, df_close, df_pdf, base_date):
    print("\n" + "="*60)
    print(" Step 5: 유니버스 저장")
    print("="*60)

    os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
    if '시가총액(억원)' in df.columns:
        df = df.sort_values('시가총액(억원)', ascending=False)

    print(f"\n  📊 최종: {len(df)}개 ETF")
    if '시가총액(억원)' in df.columns:
        total = df['시가총액(억원)'].sum()
        print(f"  💰 시총: {total:,.0f}억원 ({total/10000:.1f}조원)")

    if '대카테고리' in df.columns:
        print("\n  [대카테고리]")
        for cat in df['대카테고리'].value_counts().index:
            sub = df[df['대카테고리']==cat]
            cap = sub['시가총액(억원)'].sum() if '시가총액(억원)' in sub.columns else 0
            print(f"    {cat}: {len(sub)}개 ({cap:,.0f}억)")

    cols = ['ETF명', '시가총액(억원)', '종가', '설정일',
            '대카테고리', '중카테고리', '소카테고리',
            '순위(YTD_BM+)',
            '수익률_1M(%)', '수익률_3M(%)', '수익률_6M(%)',
            '수익률_1Y(%)', '수익률_YTD(%)',
            'BM_1M(%)', 'BM_3M(%)', 'BM_6M(%)',
            'BM_1Y(%)', 'BM_YTD(%)',
            '연간변동성(%)', '거래량(주)', '거래대금(억)']

    cols = [c for c in cols if c in df.columns]
    df_export = df[cols].copy().fillna('')

    f_master = os.path.join(Config.OUTPUT_DIR, f"etf_universe_{base_date}.xlsx")
    with pd.ExcelWriter(f_master, engine='openpyxl') as writer:
        df_export.to_excel(writer, sheet_name='유니버스')
        if df_pdf is not None and not df_pdf.empty:
            pdf_order = [t for t in df.index if t in df_pdf.index]
            df_pdf_sorted = df_pdf.loc[[t for t in pdf_order if t in df_pdf.index]]
            df_pdf_sorted.to_excel(writer, sheet_name='구성종목(PDF)')
    print(f"\n  📁 유니버스 + PDF: {f_master}")

    if df_close is not None and not df_close.empty:
        f_p = os.path.join(Config.OUTPUT_DIR, f"etf_prices_{base_date}.csv")
        df_close.to_csv(f_p, encoding='utf-8-sig')
        print(f"  📁 종가: {f_p}")

    f_t = os.path.join(Config.OUTPUT_DIR, f"etf_tickers_{base_date}.csv")
    tc = [c for c in ['ETF명','설정일','대카테고리','중카테고리','소카테고리'] if c in df.columns]
    df[tc].to_csv(f_t, encoding='utf-8-sig')
    print(f"  📁 티커: {f_t}")

    print(f"\n  {'─'*110}")
    print(f"  📋 Top 15 (시가총액)")
    print(f"  {'─'*110}")
    for i, (idx, r) in enumerate(df.head(15).iterrows(), 1):
        nm = str(r.get('ETF명',''))[:24]
        cap = f"{r['시가총액(억원)']:>7,}억" if r.get('시가총액(억원)','') != '' else ""
        c1 = str(r.get('대카테고리',''))[:8]
        c2 = str(r.get('중카테고리',''))[:10]
        ytd = f"{r['수익률_YTD(%)']:+.2f}%" if r.get('수익률_YTD(%)','') != '' else "N/A"
        bm = f"{r['BM_YTD(%)']:+.2f}%" if r.get('BM_YTD(%)','') != '' else "N/A"
        rnk = str(r.get('순위(YTD_BM+)',''))
        top1 = ''
        if df_pdf is not None and idx in df_pdf.index:
            row_pdf = df_pdf.loc[idx].drop('ETF명', errors='ignore')
            num_vals = pd.to_numeric(row_pdf, errors='coerce')
            if num_vals.notna().any():
                max_stock = num_vals.idxmax()
                max_w = num_vals.max()
                top1 = f"{max_stock}({max_w:.1f}%)"
        print(f"  {i:>3}. [{idx}] {nm:<26} {cap} {c1:<8} {c2:<12} YTD:{ytd:>8} BM:{bm:>8} #{rnk:<4} {top1}")

    return df_export


# ============================================================================
# 진단 함수 — Streamlit 앱에서 호출하여 문제 파악
# ============================================================================
def diagnose():
    """네이버 금융 / KRX API 연결 및 데이터 수집 단계별 진단"""
    results = {}

    print("=== [진단 1] 영업일 찾기 ===")
    try:
        base_date = find_latest_business_date()
        results['base_date'] = base_date
        results['base_date_ok'] = True
        print(f"  ✅ 영업일: {base_date}")
    except Exception as e:
        results['base_date'] = str(e)
        results['base_date_ok'] = False
        print(f"  ❌ 실패: {e}")
        return results

    print("\n=== [진단 2] 네이버 ETF 전종목 조회 ===")
    try:
        df_all = naver_get_all_etfs()
        results['tickers_count'] = len(df_all)
        results['tickers_ok'] = len(df_all) > 0
        results['tickers_sample'] = df_all.index[:5].tolist() if not df_all.empty else []
        print(f"  ✅ {len(df_all)}개 (샘플: {df_all.index[:5].tolist()})")
        if not df_all.empty:
            print(f"  컬럼: {df_all.columns.tolist()}")
            print(f"  시가총액 범위: {df_all['시가총액(억원)'].min():.0f} ~ {df_all['시가총액(억원)'].max():.0f}억")
    except Exception as e:
        results['tickers_count'] = 0
        results['tickers_ok'] = False
        results['tickers_error'] = str(e)
        print(f"  ❌ 실패: {e}")
        return results

    print("\n=== [진단 3] 네이버 차트 API (069500 KODEX 200) ===")
    try:
        start_test = (datetime.strptime(base_date, "%Y%m%d") - timedelta(days=7)).strftime("%Y%m%d")
        price = naver_get_price_history("069500", start_test, base_date)
        results['price_ok'] = not price.empty
        if not price.empty:
            print(f"  ✅ {len(price)}일, 최근 종가: {price.iloc[-1]:,.0f}")
        else:
            print(f"  ❌ 빈 결과")
    except Exception as e:
        results['price_ok'] = False
        results['price_error'] = str(e)
        print(f"  ❌ 실패: {e}")

    print("\n=== [진단 4] 네이버 KOSPI 지수 ===")
    try:
        start_test = (datetime.strptime(base_date, "%Y%m%d") - timedelta(days=7)).strftime("%Y%m%d")
        kospi = naver_get_index_history("KOSPI", start_test, base_date)
        results['kospi_ok'] = not kospi.empty
        if not kospi.empty:
            print(f"  ✅ {len(kospi)}일, 최근: {kospi.iloc[-1]:,.2f}")
        else:
            print(f"  ❌ 빈 결과")
    except Exception as e:
        results['kospi_ok'] = False
        results['kospi_error'] = str(e)
        print(f"  ❌ 실패: {e}")

    print("\n=== [진단 5] KRX 구성종목 (069500) ===")
    try:
        isin = _krx_get_isin("069500")
        results['isin_ok'] = bool(isin)
        print(f"  ISIN: {isin or '없음'}")
        if isin:
            holdings = krx_get_etf_holdings("069500", base_date)
            results['holdings_ok'] = len(holdings) > 0
            if holdings:
                print(f"  ✅ KRX 구성종목: {len(holdings)}개")
                for name, w in holdings[:5]:
                    print(f"    - {name}: {w}%")
            else:
                print(f"  ⚠️ KRX 빈 결과 → 네이버 fallback 시도...")
                naver_holdings = _naver_etf_holdings("069500")
                if naver_holdings:
                    results['holdings_ok'] = True
                    results['holdings_source'] = 'naver_fallback'
                    print(f"  ✅ 네이버 fallback 성공: {len(naver_holdings)}개")
                    for name, w in naver_holdings[:5]:
                        print(f"    - {name}: {w}%")
                else:
                    print(f"  ❌ 네이버 fallback도 실패")
        else:
            # KRX ISIN 실패 → 네이버 fallback
            print(f"  ⚠️ ISIN 조회 실패 → 네이버 fallback 시도...")
            naver_holdings = _naver_etf_holdings("069500")
            if naver_holdings:
                results['holdings_ok'] = True
                results['holdings_source'] = 'naver_fallback'
                print(f"  ✅ 네이버 fallback 성공: {len(naver_holdings)}개")
                for name, w in naver_holdings[:5]:
                    print(f"    - {name}: {w}%")
            else:
                results['holdings_ok'] = False
                print(f"  ❌ KRX + 네이버 모두 실패")
    except Exception as e:
        results['holdings_ok'] = False
        results['holdings_error'] = str(e)
        print(f"  ❌ 실패: {e}")
        # 예외 발생 시에도 네이버 fallback 시도
        try:
            naver_holdings = _naver_etf_holdings("069500")
            if naver_holdings:
                results['holdings_ok'] = True
                results['holdings_source'] = 'naver_fallback'
                print(f"  ✅ 네이버 fallback 성공: {len(naver_holdings)}개")
        except Exception:
            pass

    print("\n=== [진단 6] 네이버 설정일 (069500) ===")
    try:
        dates = _naver_listing_dates(["069500"])
        results['listing_ok'] = bool(dates.get("069500"))
        print(f"  ✅ 069500 → {dates.get('069500', '없음')}")
    except Exception as e:
        results['listing_ok'] = False
        results['listing_error'] = str(e)
        print(f"  ❌ 실패: {e}")

    all_ok = all(results.get(k, False) for k in
                 ['base_date_ok','tickers_ok','price_ok','kospi_ok','holdings_ok','listing_ok'])
    results['all_ok'] = all_ok
    print(f"\n{'='*60}")
    print(f"  종합: {'✅ 전체 정상' if all_ok else '❌ 일부 실패 — 위 결과 확인'}")
    print(f"{'='*60}")
    return results


# ============================================================================
# 메인
# ============================================================================
def build_universe():
    print("╔" + "═"*58 + "╗")
    print("║   한국 상장 ETF 유니버스 빌더 v6.1 (네이버 금융)          ║")
    print("╚" + "═"*58 + "╝")

    t_start = time.time()

    base_date = Config.BASE_DATE or find_latest_business_date()
    Config.BASE_DATE = base_date
    print(f"\n  📅 기준일: {base_date}")
    print(f"  💰 최소 시총: {Config.MIN_MARKET_CAP_BILLIONS}억원")
    print(f"  ⚡ 스레드: {Config.MAX_WORKERS} / 캐시: {Config.USE_CACHE}")
    print(f"  📡 데이터: 네이버 금융 + KRX 직접 HTTP")

    df = step1_get_tickers_and_names(base_date)
    df = step2_type_filter_and_classify(df)
    df = step3_market_cap_filter(df, base_date, Config.MIN_MARKET_CAP_BILLIONS)
    df, df_close, df_pdf = step4_collect_all_data(df, base_date)
    step5_save(df, df_close, df_pdf, base_date)

    elapsed = time.time() - t_start
    print(f"\n{'='*60}")
    print(f" ✅ 완료! 총 소요시간: {elapsed:.0f}초 ({elapsed/60:.1f}분)")
    print(f"{'='*60}")
    return df, df_close, df_pdf


if __name__ == "__main__":
    df_universe, df_prices, df_pdf = build_universe()
