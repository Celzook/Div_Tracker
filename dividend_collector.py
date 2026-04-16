"""
ETF 고배당 유니버스 — 배당 데이터 수집기 (Phase 0, 3)
다중 소스 Fallback: KRX → pykrx → FinanceDataReader → DART
"""

import pandas as pd
import numpy as np
import json
import time
import os
import sys

# 기존 레포 유틸 재활용
from etf_universe_builder import _http_post, _http_get, _load_cache, _save_cache, Config
from config_dividend import (
    SPLIT_EVENTS, DIVIDEND_START_YEAR, DIVIDEND_END_YEAR,
    DART_API_KEY, get_trdDd_for_biz_year,
)


# ============================================================================
# pykrx / FinanceDataReader fallback 헬퍼
# ============================================================================

def _pykrx_get_dividend_data(trdDd, mktId='STK'):
    """pykrx 기반 배당 데이터 조회 (KRX 직접 HTTP 차단 시 fallback)

    pykrx는 data.krx.co.kr와 다른 내부 엔드포인트를 사용하여
    클라우드 환경에서도 접근 가능한 경우가 있다.
    """
    try:
        from pykrx import stock as pykrx_stock
    except ImportError:
        return pd.DataFrame()

    try:
        market = 'KOSPI' if mktId == 'STK' else 'KOSDAQ'
        # get_market_fundamental: index=티커, columns=[BPS, PER, PBR, EPS, DIV, DPS]
        df_fund = pykrx_stock.get_market_fundamental(trdDd, market=market)
        if df_fund is None or df_fund.empty:
            return pd.DataFrame()

        df_fund = df_fund.reset_index()
        ticker_col = '티커' if '티커' in df_fund.columns else df_fund.columns[0]

        # 종목명 일괄 조회
        ticker_list = df_fund[ticker_col].tolist()
        names = {}
        for code in ticker_list:
            try:
                names[code] = pykrx_stock.get_market_ticker_name(code) or code
            except Exception:
                names[code] = code

        # 종가: get_market_ohlcv
        df_price = pykrx_stock.get_market_ohlcv(trdDd, market=market)
        if df_price is not None and not df_price.empty:
            df_price = df_price.reset_index()
            price_col = '티커' if '티커' in df_price.columns else df_price.columns[0]
            price_map = dict(zip(df_price[price_col], df_price.get('종가', df_price.get('Close', pd.Series()))))
        else:
            price_map = {}

        rows = []
        for _, row in df_fund.iterrows():
            code = row[ticker_col]
            dps = row.get('DPS', 0)
            div = row.get('DIV', 0.0)
            try:
                dps = int(float(str(dps).replace(',', '')))
            except (ValueError, TypeError):
                dps = 0
            try:
                div = float(str(div).replace(',', ''))
            except (ValueError, TypeError):
                div = 0.0
            rows.append({
                '종목코드': code,
                '종목명': names.get(code, code),
                '종가': price_map.get(code, 0),
                '주당배당금': dps,
                '배당수익률': div,
            })

        df = pd.DataFrame(rows)
        print(f"  → pykrx fallback: {len(df)}개 종목 ({market})")
        return df

    except Exception as e:
        print(f"  ⚠️ pykrx 배당 조회 실패: {e}")
        return pd.DataFrame()


def _naver_get_dividend_history(code, start_year=None, end_year=None):
    """네이버 금융 종목 메인 페이지에서 연간 주당배당금 스크래핑 (KRX 차단 시 fallback)

    네이버 금융 기업실적분석 테이블에서 최근 연간 실적 3개년 주당배당금 추출.
    - URL: https://finance.naver.com/item/main.naver?code={code} (UTF-8)
    - 테이블: summary="기업실적분석" — 최근 연간 실적 4열 (마지막은 추정치(E), 제외)
    - sise_dividend_total.naver URL은 404로 제거됨 (2024년 이후 폐지)

    Returns: {사업연도(int): 주당배당금(int)} dict, 예: {2023: 1444, 2024: 1446, 2025: 1668}
    """
    import re
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    try:
        # main.naver는 UTF-8 응답 (euc-kr 아님)
        html = _http_get(url, encoding='utf-8', timeout=15)
        if not html or len(html) < 1000:
            return {}

        # 기업실적분석 테이블 추출
        table_match = re.search(
            r'(최근 연간 실적.*?</table>)',
            html, re.DOTALL
        )
        if not table_match:
            return {}
        table_html = table_match.group(1)

        # 연간 컬럼 헤더에서 연도 추출 (YYYY.MM 형식, 앞 4개가 연간)
        all_years = re.findall(r'(\d{4})\.\d{2}', table_html)
        if len(all_years) < 4:
            return {}
        # 마지막 연간 컬럼(4번째)은 추정치(E) → 제외, 앞 3개만 사용
        annual_year_strs = all_years[:3]

        # 주당배당금 행 추출
        dps_row_match = re.search(r'주당배당금.{0,3000}?</tr>', table_html, re.DOTALL)
        if not dps_row_match:
            return {}
        row_html = dps_row_match.group()

        # td 셀에서 숫자 값 추출 (콤마 포함 숫자, 첫 9개 중 앞 3개가 연간 확정치)
        td_values = re.findall(
            r'<td[^>]*>(?:\s*(?:<[^>]+>)*\s*)*([\d,]+)(?:\s*(?:<[^>]+>)*\s*)*</td>',
            row_html
        )
        if not td_values:
            return {}

        # 연도↔배당금 매핑 (확정 연간 3개년)
        result = {}
        for year_str, val_str in zip(annual_year_strs, td_values[:3]):
            try:
                year = int(year_str)
                if start_year and year < start_year:
                    continue
                if end_year and year > end_year:
                    continue
                dps = int(val_str.replace(',', ''))
                if dps > 0:
                    result[year] = dps
            except (ValueError, TypeError):
                continue
        return result

    except Exception:
        return {}


def _fdr_build_name_code_map():
    """FinanceDataReader 기반 종목명↔코드 매핑 (KRX 차단 시 fallback)"""
    try:
        import FinanceDataReader as fdr
    except ImportError:
        print("  ⚠️ FinanceDataReader 미설치 (pip install finance-datareader)")
        return {}

    name_to_code = {}
    for market in ['KOSPI', 'KOSDAQ']:
        try:
            df = fdr.StockListing(market)
            if df.empty:
                continue
            # 컬럼명 버전별 대응
            code_col = next((c for c in ['Code', 'Symbol'] if c in df.columns), None)
            name_col = next((c for c in ['Name', 'ISU_ABBRV'] if c in df.columns), None)
            if not code_col or not name_col:
                print(f"  ⚠️ FDR {market} 컬럼 불명확: {df.columns.tolist()}")
                continue
            for _, row in df[[code_col, name_col]].dropna().iterrows():
                name_to_code[str(row[name_col]).strip()] = str(row[code_col]).strip()
        except Exception as e:
            print(f"  ⚠️ FDR {market} 실패: {e}")

    print(f"  → FDR 매핑: {len(name_to_code)}개 종목")
    return name_to_code


# ============================================================================
# KRX 배당 데이터 조회 (1순위)
# ============================================================================

def krx_get_dividend_data(trdDd, mktId='STK'):
    """KRX 전종목 PER/PBR/배당수익률 조회

    Args:
        trdDd: 'YYYYMMDD' — 해당일 기준 최근 사업보고서의 주당배당금
        mktId: 'STK'(코스피) 또는 'KSQ'(코스닥)

    Returns:
        DataFrame [종목코드, 종목명, 종가, EPS, PER, BPS, PBR, 주당배당금, 배당수익률]
    """
    url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    params = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT03901',
        'locale': 'ko_KR',
        'mktId': mktId,
        'trdDd': trdDd,
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false',
    }
    try:
        # MDCSTAT03901 전용 Referer (기본 _http_post의 Referer는 05901)
        headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020502'}
        text = _http_post(url, data=params, headers=headers, timeout=30)
        data = json.loads(text)
    except Exception as e:
        print(f"  ⚠️ KRX 배당 조회 실패 (trdDd={trdDd}, mktId={mktId}): {e}")
        return pd.DataFrame()

    # 응답 키 탐색 (KRX는 'output' 또는 'OutBlock_1' 등 다양)
    items = None
    for key in ['output', 'OutBlock_1', 'block1']:
        items = data.get(key)
        if items:
            break
    if not items:
        print(f"  ⚠️ KRX 응답 비어있음 (trdDd={trdDd}, mktId={mktId})")
        print(f"     응답 키: {list(data.keys())}")
        if data:
            print(f"     응답 샘플: {str(data)[:300]}")
        return pd.DataFrame()

    # 0-B: 필드명 확인 (첫 호출 시 출력)
    if items:
        field_names = list(items[0].keys())

    rows = []
    for item in items:
        code = item.get('ISU_SRT_CD', '').strip()
        name = item.get('ISU_ABBRV', '').strip()

        # 종가
        clsprc_str = item.get('TDD_CLSPRC', '0').replace(',', '')
        try:
            clsprc = int(float(clsprc_str))
        except (ValueError, TypeError):
            clsprc = 0

        # 주당배당금
        dps_str = item.get('DPS', '0').replace(',', '')
        try:
            dps = int(float(dps_str))
        except (ValueError, TypeError):
            dps = 0

        # 배당수익률
        dy_str = item.get('DVD_YLD', '0').replace(',', '')
        try:
            dy = float(dy_str)
        except (ValueError, TypeError):
            dy = 0.0

        if code and name:
            rows.append({
                '종목코드': code,
                '종목명': name,
                '종가': clsprc,
                '주당배당금': dps,
                '배당수익률': dy,
            })

    df = pd.DataFrame(rows)
    return df


# ============================================================================
# DividendCollector 클래스 (v3 인터페이스)
# ============================================================================

class DividendCollector:
    def __init__(self):
        self.name_to_code = {}  # {종목명: 종목코드}
        self._field_names_printed = False

    # ── Phase 0: 데이터 소스 검증 ──────────────────────────

    def verify_source(self):
        """Phase 0: 데이터 소스 검증
        Returns: {'krx': bool, 'dart': bool, 'field_names': list}
        """
        print("\n" + "=" * 60)
        print(" Phase 0-A: KRX 배당 데이터 소스 검증")
        print("=" * 60)

        result = {'krx': False, 'dart': False, 'field_names': []}

        # 1) KRX 최근 데이터 테스트
        trdDd = get_trdDd_for_biz_year(DIVIDEND_END_YEAR)
        print(f"\n  → KRX 조회 테스트: trdDd={trdDd}, mktId=STK")

        df_test = krx_get_dividend_data(trdDd, 'STK')

        if df_test.empty:
            print("  ❌ KRX 응답 비어있음")
            # trdDd를 좀 더 뒤로 시도
            for alt_month in ['0801', '0901', '1001', '1101']:
                alt_trdDd = f'{DIVIDEND_END_YEAR + 1}{alt_month}'
                print(f"  → 재시도: trdDd={alt_trdDd}")
                df_test = krx_get_dividend_data(alt_trdDd, 'STK')
                if not df_test.empty:
                    print(f"  ✅ trdDd={alt_trdDd} 에서 성공!")
                    break
            # 또한 올해가 아니라 작년 시도
            if df_test.empty:
                for y in range(DIVIDEND_END_YEAR, DIVIDEND_START_YEAR - 1, -1):
                    alt_trdDd = f'{y}1001'
                    print(f"  → 재시도: trdDd={alt_trdDd}")
                    df_test = krx_get_dividend_data(alt_trdDd, 'STK')
                    if not df_test.empty:
                        print(f"  ✅ trdDd={alt_trdDd} 에서 성공!")
                        break

        if not df_test.empty:
            print(f"  → 전종목 수: {len(df_test)}")
            print(f"  → 컬럼: {df_test.columns.tolist()}")

            # 삼성전자 검증
            sec = df_test[df_test['종목코드'] == '005930']
            if not sec.empty:
                dps = sec.iloc[0]['주당배당금']
                dy = sec.iloc[0]['배당수익률']
                print(f"  → 삼성전자: 주당배당금={dps:,}원, 배당수익률={dy}%")
                if dps > 0:
                    result['krx'] = True
                    print("  ✅ KRX 배당 데이터 소스 검증 통과!")
                else:
                    print("  ⚠️ 삼성전자 주당배당금 = 0 (사업보고서 미반영 시점일 수 있음)")
                    # pykrx fallback 시도
                    df_pykrx = _pykrx_get_dividend_data(trdDd, 'STK')
                    if not df_pykrx.empty:
                        result['krx'] = True
                        print("  ✅ pykrx fallback 검증 통과!")
            else:
                print("  ⚠️ 삼성전자(005930) 미발견")
        else:
            print("  ❌ KRX에서 데이터를 가져오지 못함 — pykrx fallback 시도...")
            df_pykrx = _pykrx_get_dividend_data(trdDd, 'STK')
            if not df_pykrx.empty:
                result['krx'] = True
                print(f"  ✅ pykrx fallback 성공 ({len(df_pykrx)}개 종목)")
            else:
                print("  ❌ pykrx fallback도 실패. 네트워크 환경을 확인하세요.")

        # 2) 연도별 변화 확인 (최소 3개 연도)
        if result['krx']:
            print(f"\n  → 연도별 삼성전자 배당금 변화 확인:")
            yearly_dps = {}
            for year in range(DIVIDEND_END_YEAR - 2, DIVIDEND_END_YEAR + 1):
                td = get_trdDd_for_biz_year(year)
                df_y = krx_get_dividend_data(td, 'STK')
                if not df_y.empty:
                    s = df_y[df_y['종목코드'] == '005930']
                    if not s.empty:
                        d = s.iloc[0]['주당배당금']
                        yearly_dps[year] = d
                        print(f"     사업연도 {year}: 주당배당금 = {d:,}원")
                time.sleep(0.5)

            if len(yearly_dps) >= 3:
                print(f"  ✅ {len(yearly_dps)}개 연도 데이터 확보")
            else:
                print(f"  ⚠️ {len(yearly_dps)}개 연도만 확보 (3개 이상 필요)")
                if len(yearly_dps) < 2:
                    result['krx'] = False

        # 3) DART 검증 (API 키가 있을 때만)
        if DART_API_KEY:
            print(f"\n  → Open DART 검증:")
            try:
                import OpenDartReader
                dart = OpenDartReader(DART_API_KEY)
                test = dart.report('005930', '배당', DIVIDEND_END_YEAR)
                if test is not None and len(test) > 0:
                    result['dart'] = True
                    print("  ✅ DART 배당 조회 성공")
                else:
                    print("  ⚠️ DART 응답 비어있음")
            except Exception as e:
                print(f"  ⚠️ DART 실패: {e}")
        else:
            print("\n  → DART API 키 미설정 (config_dividend.py에서 설정 가능)")

        return result

    # ── Phase 0: 종목명→코드 매핑 테이블 ───────────────────

    def build_name_code_map(self, base_date):
        """KRX 전종목 조회 → {종목명: 종목코드} 매핑 테이블 구축
        KRX 차단 시 FinanceDataReader로 자동 fallback.
        """
        print("\n  → 종목명↔코드 매핑 테이블 구축...")

        cache_name = f"name_code_map_{base_date}.pkl"
        cached = _load_cache(cache_name)
        if cached:
            self.name_to_code = cached
            print(f"  → 💾 캐시 로드: {len(self.name_to_code)}개 종목")
            return self.name_to_code

        name_to_code = {}
        for mktId in ['STK', 'KSQ']:
            df = krx_get_dividend_data(base_date, mktId)
            if not df.empty:
                for _, row in df.iterrows():
                    name_to_code[row['종목명']] = row['종목코드']
            time.sleep(0.3)

        # KRX 차단 시 FDR fallback
        if not name_to_code:
            print("  → KRX 차단 감지, FDR fallback 시도...")
            name_to_code = _fdr_build_name_code_map()

        self.name_to_code = name_to_code
        if name_to_code:
            _save_cache(cache_name, name_to_code)
        print(f"  → 매핑 테이블: {len(name_to_code)}개 종목")
        return name_to_code

    # ── Phase 3: 연도별 전종목 배당금 수집 ─────────────────

    def collect_all_years(self, start_year=None, end_year=None):
        """연도별 KRX 전종목 배당금 일괄 수집

        Returns: DataFrame [종목코드, 종목명, 사업연도, 주당배당금_원본, 주당배당금_수정, 소스]
        """
        start_year = start_year or DIVIDEND_START_YEAR
        end_year = end_year or DIVIDEND_END_YEAR

        print("\n" + "=" * 60)
        print(f" Phase 3: 배당금 이력 수집 ({start_year}~{end_year})")
        print("=" * 60)

        cache_name = f"dividend_history_{start_year}_{end_year}.pkl"
        cached = _load_cache(cache_name)
        if cached is not None and isinstance(cached, pd.DataFrame) and not cached.empty:
            print(f"  → 💾 캐시 로드: {len(cached)}행")
            return cached

        all_frames = []
        for year in range(start_year, end_year + 1):
            trdDd = get_trdDd_for_biz_year(year)
            print(f"\n  ── 사업연도 {year} (trdDd={trdDd}) ──")

            frames_for_year = []
            for mktId, mkt_name in [('STK', '코스피'), ('KSQ', '코스닥')]:
                df = krx_get_dividend_data(trdDd, mktId)
                if not df.empty:
                    df['사업연도'] = year
                    df['소스'] = 'KRX'
                    frames_for_year.append(df)
                    print(f"     {mkt_name}: {len(df)}개 종목")
                else:
                    print(f"     {mkt_name}: ⚠️ 0개 종목")

                    # Fallback 1: 다른 월 시도
                    for alt_month in ['0801', '0901', '1001', '1101']:
                        alt_trdDd = f'{year + 1}{alt_month}'
                        df = krx_get_dividend_data(alt_trdDd, mktId)
                        if not df.empty:
                            df['사업연도'] = year
                            df['소스'] = 'KRX'
                            frames_for_year.append(df)
                            print(f"     {mkt_name} (trdDd={alt_trdDd}): {len(df)}개 종목")
                            break

                    # Fallback 2: pykrx (KRX 완전 차단 시)
                    if df.empty:
                        print(f"     {mkt_name}: pykrx fallback 시도...")
                        df = _pykrx_get_dividend_data(trdDd, mktId)
                        if not df.empty:
                            df['사업연도'] = year
                            df['소스'] = 'pykrx'
                            frames_for_year.append(df)
                            print(f"     {mkt_name} (pykrx): {len(df)}개 종목")
                time.sleep(0.5)

            if frames_for_year:
                df_year = pd.concat(frames_for_year, ignore_index=True)
                all_frames.append(df_year)
                print(f"     → 합계: {len(df_year)}개")

        if not all_frames:
            print("  ❌ 배당 데이터 수집 실패")
            return pd.DataFrame()

        df_all = pd.concat(all_frames, ignore_index=True)

        # 원본 배당금 보존 후 수정배당금 계산
        df_all['주당배당금_원본'] = df_all['주당배당금']
        df_all['주당배당금_수정'] = df_all.apply(
            lambda r: adjust_dividend(r['종목코드'], r['사업연도'], r['주당배당금']),
            axis=1
        )

        # 컬럼 정리
        df_all = df_all[['종목코드', '종목명', '사업연도', '주당배당금_원본', '주당배당금_수정', '소스']].copy()

        _save_cache(cache_name, df_all)
        print(f"\n  → 총 수집: {len(df_all)}행 ({df_all['종목코드'].nunique()}개 종목 × {df_all['사업연도'].nunique()}개 연도)")
        return df_all

    def adjust_for_splits(self, df):
        """액면분할 보정 (이미 collect_all_years에서 적용되지만 외부 호출용)"""
        df = df.copy()
        df['주당배당금_수정'] = df.apply(
            lambda r: adjust_dividend(r['종목코드'], r['사업연도'], r['주당배당금_원본']),
            axis=1
        )
        return df


# ============================================================================
# 액면분할 보정 함수
# ============================================================================

def adjust_dividend(code, year, raw_dps):
    """액면분할 보정: 분할 이전 연도의 배당금을 분할 후 기준으로 조정"""
    for ev in SPLIT_EVENTS:
        if ev['code'] == code:
            split_year = int(ev['date'][:4])
            if year < split_year:
                return round(raw_dps / ev['ratio'], 1)
    return raw_dps


# ============================================================================
# Gate 검증 함수
# ============================================================================

def validate_gate(phase_name, checks):
    """Gate 검증 — 모든 조건 통과 시 True"""
    print(f"\n{'=' * 50}")
    print(f" Gate {phase_name} 검증")
    print(f"{'=' * 50}")
    all_pass = True
    for name, passed, fail_msg in checks:
        status = "✅" if passed else "❌"
        print(f"  {status} {name}")
        if not passed:
            print(f"     → {fail_msg}")
            all_pass = False
    result = "PASS ✅" if all_pass else "FAIL ❌"
    print(f"\n  결과: {result}")
    return all_pass


def validate_phase3(df_div):
    """Phase 3 Gate: 배당 이력 품질 검증"""
    if df_div.empty:
        return validate_gate("Phase 3", [("데이터 존재", False, "DataFrame 비어있음")])

    stocks_with_5y = df_div.groupby('종목코드')['사업연도'].nunique()
    pct_5y = (stocks_with_5y >= 5).mean() if len(stocks_with_5y) > 0 else 0

    # 삼성전자 수정배당금 연속성
    sec = df_div[df_div['종목코드'] == '005930'].sort_values('사업연도')
    if len(sec) > 1:
        sec_jumps = sec['주당배당금_수정'].pct_change().abs()
        no_discontinuity = (sec_jumps.dropna() < 5.0).all()
    else:
        no_discontinuity = len(sec) > 0

    zero_pct = (df_div['주당배당금_수정'] == 0).mean()
    n_stocks = df_div['종목코드'].nunique()

    checks = [
        ("5년 데이터 확보율 >= 80%", pct_5y >= 0.8,
         f"현재 {pct_5y:.1%}"),
        ("삼성전자 수정배당금 연속성", no_discontinuity,
         f"불연속 감지: {sec[['사업연도', '주당배당금_수정']].to_dict('records')}"),
        ("0원 비율 < 30%", zero_pct < 0.3,
         f"현재 {zero_pct:.1%}"),
        ("종목 수 >= 15", n_stocks >= 15,
         f"현재 {n_stocks}개"),
    ]
    return validate_gate("Phase 3", checks)


# ============================================================================
# 단독 실행: Phase 0 검증
# ============================================================================

if __name__ == "__main__":
    print("╔" + "═" * 58 + "╗")
    print("║   Phase 0: 데이터 소스 검증 + 기반 코드 준비             ║")
    print("╚" + "═" * 58 + "╝")

    collector = DividendCollector()

    # 0-A: 데이터 소스 검증
    gate_result = collector.verify_source()

    # 0-B: 주가 데이터 검증
    print("\n" + "=" * 60)
    print(" Phase 0-B: 주가 데이터 소스 검증")
    print("=" * 60)

    from etf_universe_builder import naver_get_price_history
    from datetime import datetime, timedelta

    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=365 * 5 + 30)).strftime("%Y%m%d")
    print(f"  → 삼성전자 주가: {start_date} ~ {end_date}")

    prices = naver_get_price_history('005930', start_date, end_date)
    price_ok = len(prices) > 1000
    print(f"  → 데이터 행수: {len(prices)}")
    if price_ok:
        print(f"  → 최초: {prices.index[0].strftime('%Y-%m-%d')} = {prices.iloc[0]:,.0f}원")
        print(f"  → 최근: {prices.index[-1].strftime('%Y-%m-%d')} = {prices.iloc[-1]:,.0f}원")
        print("  ✅ 주가 데이터 검증 통과!")
    else:
        print(f"  ❌ 주가 데이터 부족 (1000행 이상 필요, 현재 {len(prices)}행)")

    # Gate 0 종합
    gate0_checks = [
        ("KRX 배당 데이터 소스", gate_result['krx'],
         "KRX 조회 실패 — trdDd 변경 또는 DART API 키 필요"),
        ("주가 데이터 (5년치)", price_ok,
         f"데이터 {len(prices)}행 (1000행 이상 필요)"),
    ]
    gate0_pass = validate_gate("Phase 0", gate0_checks)

    if gate0_pass:
        print("\n🎉 Phase 0 완료! Phase 1로 진행 가능합니다.")
    else:
        print("\n⚠️ Phase 0 실패 — 위 에러를 해결한 후 재실행하세요.")
        print("   DART API 키가 있다면 config_dividend.py에 설정해주세요.")
