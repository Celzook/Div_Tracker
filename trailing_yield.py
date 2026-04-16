"""
ETF 고배당 유니버스 — Trailing 배당수익률 계산 (Phase 4)
"""

import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from etf_universe_builder import naver_get_price_history, Config, _load_cache, _save_cache
from dividend_collector import validate_gate


class TrailingYieldCalculator:
    def __init__(self, div_history, price_days=2500):
        """
        div_history: Phase 3 결과 DataFrame
            [종목코드, 종목명, 사업연도, 주당배당금_수정, ...]
        price_days: 주가 수집 일수 (2500 ≈ 7년)
        """
        self.div_history = div_history
        self.price_days = price_days
        self.prices = {}  # {종목코드: Series(date→price)}

        # 배당금 빠른 조회용 딕셔너리: (종목코드, 사업연도) → 수정배당금
        self._div_map = {}
        for _, row in div_history.iterrows():
            key = (row['종목코드'], row['사업연도'])
            self._div_map[key] = row['주당배당금_수정']

    def fetch_prices(self, codes, base_date=None):
        """naver_get_price_history 병렬 호출 → {종목코드: Series}"""
        if base_date is None:
            base_date = datetime.now().strftime("%Y%m%d")

        end_date = base_date
        start_dt = datetime.strptime(base_date, "%Y%m%d") - timedelta(days=self.price_days)
        start_date = start_dt.strftime("%Y%m%d")

        print(f"\n  → 주가 수집: {len(codes)}개 종목 ({start_date}~{end_date})")

        cache_name = f"stock_prices_{start_date}_{end_date}.pkl"
        cached = _load_cache(cache_name)
        if cached and isinstance(cached, dict):
            hit = [c for c in codes if c in cached]
            if len(hit) / max(len(codes), 1) > 0.8:
                print(f"  → 💾 캐시: {len(hit)}/{len(codes)}개")
                self.prices = cached
                return cached

        results = {}
        failed = []

        def _fetch_one(code):
            try:
                s = naver_get_price_history(code, start_date, end_date)
                if not s.empty:
                    return code, s
            except Exception:
                pass
            return code, pd.Series(dtype=float)

        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            futures = {executor.submit(_fetch_one, c): c for c in codes}
            with tqdm(total=len(codes), desc="  주가 수집") as pbar:
                for future in as_completed(futures):
                    code, series = future.result()
                    if not series.empty:
                        results[code] = series
                    else:
                        failed.append(code)
                    pbar.update(1)

        print(f"  → 성공: {len(results)}개, 실패: {len(failed)}개")
        if failed[:5]:
            print(f"     실패 샘플: {failed[:5]}")

        self.prices = results
        _save_cache(cache_name, results)
        return results

    def calc_monthly_yield(self, code):
        """단일 종목 월별 Trailing 배당수익률 시계열"""
        if code not in self.prices or self.prices[code].empty:
            return pd.DataFrame()

        price_series = self.prices[code]

        # 월말 리샘플링
        df_p = price_series.to_frame('종가')
        df_monthly = df_p.resample('ME').last().dropna()

        results = []
        for date, row in df_monthly.iterrows():
            price = row['종가']
            if price <= 0:
                continue

            # 해당 시점에서 알 수 있는 최신 배당금 결정
            # 사업보고서는 보통 다음해 3~5월 공시 → KRX 반영은 5~7월
            # 6월 이후: 전년도(year-1) 확정
            # 1~5월: 원칙상 전전년도(year-2)지만, year-1 데이터가 이미 수집된 경우 사용
            if date.month >= 6:
                biz_year = date.year - 1
            else:
                # year-1 데이터가 있으면 사용(일찍 반영된 경우), 없으면 year-2
                biz_year = date.year - 1
                if self._div_map.get((code, biz_year)) is None:
                    biz_year = date.year - 2

            div = self._div_map.get((code, biz_year), 0)
            yld = (div / price * 100) if div > 0 else 0

            results.append({
                '기준월': date,
                '종목코드': code,
                'T12M배당': div,
                '수정종가': price,
                'Trailing수익률': round(yld, 4),
                '적용사업연도': biz_year,
            })

        return pd.DataFrame(results)

    def calc_all(self, codes):
        """전 종목 시계열 통합"""
        print(f"\n  → Trailing 수익률 계산: {len(codes)}개 종목")

        # 주가 미수집 시 자동 수집
        missing = [c for c in codes if c not in self.prices]
        if missing:
            self.fetch_prices(missing)

        all_frames = []
        for code in tqdm(codes, desc="  수익률 계산"):
            df = self.calc_monthly_yield(code)
            if not df.empty:
                # 종목명 추가
                name_rows = self.div_history[self.div_history['종목코드'] == code]
                if not name_rows.empty:
                    df['종목명'] = name_rows.iloc[0]['종목명']
                all_frames.append(df)

        if not all_frames:
            print("  ❌ 수익률 계산 결과 없음")
            return pd.DataFrame()

        df_all = pd.concat(all_frames, ignore_index=True)
        print(f"  → 결과: {len(df_all)}행 ({df_all['종목코드'].nunique()}개 종목)")
        return df_all


def validate_phase4(df_yield):
    """Phase 4 Gate 검증"""
    if df_yield.empty:
        return validate_gate("Phase 4", [("데이터 존재", False, "DataFrame 비어있음")])

    normal = ((df_yield['Trailing수익률'] >= 0) & (df_yield['Trailing수익률'] <= 20))
    normal_pct = normal.mean()

    sec = df_yield[df_yield['종목코드'] == '005930']
    if not sec.empty:
        sec_latest = sec.iloc[-1]['Trailing수익률']
        sec_ok = 1.0 <= sec_latest <= 5.0
    else:
        sec_latest = None
        sec_ok = True  # 삼성전자 미포함이면 스킵

    stocks_60m = df_yield.groupby('종목코드')['기준월'].nunique()
    enough = (stocks_60m >= 48).sum()  # 4년 이상이면 OK

    checks = [
        ("수익률 0~20% 비율 >= 95%", normal_pct >= 0.95,
         f"현재 {normal_pct:.1%}"),
        ("삼성전자 수익률 합리적", sec_ok,
         f"현재 {sec_latest}%"),
        ("48개월+ 시계열 종목 >= 10", enough >= 10,
         f"현재 {enough}개"),
    ]
    return validate_gate("Phase 4", checks)


def calc_etf_trailing_yield(holdings_dict, df_yield):
    """ETF별 Trailing 배당수익률 시계열 계산

    구성종목 수익률을 ETF 포트폴리오 비중으로 가중평균하여 ETF 레벨 수익률을 산출한다.

    Args:
        holdings_dict: {etf_ticker: [(종목명, 종목코드, 비중%), ...]}
                       Phase 2에서 생성된 ETF 구성종목 + 비중 딕셔너리
        df_yield: Phase 4 calc_all() 결과 — stock-level trailing yield DataFrame
                  컬럼: [기준월, 종목코드, Trailing수익률, ...]

    Returns:
        DataFrame [기준월, ETF티커, ETF_Trailing수익률, 커버리지]
        커버리지: 가중평균에 사용된 비중 합계 (0~1, 1이면 전 종목 커버)
    """
    if df_yield.empty or not holdings_dict:
        return pd.DataFrame()

    print(f"\n  → ETF Trailing 수익률 집계: {len(holdings_dict)}개 ETF")

    # 종목코드 × 기준월 피벗 (마지막 값 사용)
    pivot = df_yield.pivot_table(
        index='기준월', columns='종목코드', values='Trailing수익률', aggfunc='last'
    )

    results = []
    for etf_ticker, holdings in holdings_dict.items():
        # holdings: [(종목명, 종목코드, 비중%), ...]
        total_w = sum(w for _, _, w in holdings)
        if total_w <= 0:
            continue

        for date in pivot.index:
            weighted = 0.0
            covered_w = 0.0
            for _, stock_code, weight in holdings:
                if stock_code not in pivot.columns:
                    continue
                y = pivot.at[date, stock_code]
                if pd.isna(y):
                    continue
                norm_w = weight / total_w
                weighted += y * norm_w
                covered_w += norm_w

            # 커버리지 50% 미만이면 신뢰도 낮아 제외
            if covered_w < 0.5:
                continue

            # 누락 종목 비중만큼 재정규화
            etf_yield = weighted / covered_w
            results.append({
                '기준월': date,
                'ETF티커': etf_ticker,
                'ETF_Trailing수익률': round(etf_yield, 4),
                '커버리지': round(covered_w, 3),
            })

    if not results:
        print("  ⚠️ ETF Trailing 수익률 계산 결과 없음 (구성종목 매핑 부족)")
        return pd.DataFrame()

    df_etf = pd.DataFrame(results)
    n_etf = df_etf['ETF티커'].nunique()
    n_months = df_etf['기준월'].nunique()
    print(f"  → {n_etf}개 ETF × {n_months}개월 시계열")

    # 최신 ETF 수익률 상위 출력
    latest = df_etf.loc[df_etf.groupby('ETF티커')['기준월'].idxmax()]
    top = latest.sort_values('ETF_Trailing수익률', ascending=False).head(5)
    for _, row in top.iterrows():
        print(f"     {row['ETF티커']}: {row['ETF_Trailing수익률']:.2f}% (커버리지 {row['커버리지']:.0%})")

    return df_etf


if __name__ == "__main__":
    print("trailing_yield.py — Phase 4에서 사용. main_pipeline.py에서 실행하세요.")
