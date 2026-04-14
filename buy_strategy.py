"""
ETF 고배당 유니버스 — 배당수익률 밴드 매수 전략 (Phase 6)
"""

import pandas as pd
import numpy as np

from dividend_collector import validate_gate


class BuyStrategy:
    def __init__(self, yield_timeseries):
        """
        yield_timeseries: Phase 4 결과 (df_trailing_yield)
            [기준월, 종목코드, 종목명, T12M배당, 수정종가, Trailing수익률]
        """
        self.data = yield_timeseries
        self.bands = None

    def calc_bands(self, min_months=48):
        """종목별 밴드 통계 + Z-Score + 신호

        Args:
            min_months: 최소 데이터 포인트 수

        Returns: DataFrame [종목코드, 종목명, 현재수익률, 평균, σ, Z, 5Y최고, 5Y최저, 신호]
        """
        print("\n" + "=" * 60)
        print(" Phase 6: 배당수익률 밴드 + 매수 전략")
        print("=" * 60)

        df = self.data.copy()
        if df.empty:
            print("  ❌ 수익률 데이터 비어있음")
            return pd.DataFrame()

        results = []
        for code, grp in df.groupby('종목코드'):
            grp = grp.sort_values('기준월')
            series = grp['Trailing수익률']

            if len(series) < min_months:
                continue
            if series.max() == 0:  # 전부 0이면 스킵
                continue

            mu = series.mean()
            sigma = series.std()
            current = series.iloc[-1]
            hi = series.max()
            lo = series.min()

            # σ < 0.3% → 밴드 무의미
            if sigma > 0.3:
                z = (current - mu) / sigma
                if z > 2.0:
                    signal = "강력매수"
                elif z > 1.0:
                    signal = "매수"
                elif z < -1.0:
                    signal = "매도/회피"
                else:
                    signal = "중립"
            else:
                z = None
                signal = "밴드불가(σ작음)"

            name = grp.iloc[-1].get('종목명', code)
            price = grp.iloc[-1].get('수정종가', 0)

            results.append({
                '종목코드': code,
                '종목명': name,
                '현재가': price,
                '현재수익률': round(current, 2),
                '5Y평균': round(mu, 2),
                'σ': round(sigma, 2),
                'Z-Score': round(z, 2) if z is not None else None,
                '5Y최고': round(hi, 2),
                '5Y최저': round(lo, 2),
                '신호': signal,
            })

        self.bands = pd.DataFrame(results)
        print(f"  → 밴드 분석: {len(self.bands)}개 종목")

        # 신호 분포
        if not self.bands.empty:
            dist = self.bands['신호'].value_counts()
            for sig, cnt in dist.items():
                print(f"     {sig}: {cnt}개")

        return self.bands

    def backtest(self, signal_threshold=1.0, horizons=[3, 6, 12]):
        """과거 매수 신호 발생 후 수익률 백테스트

        Args:
            signal_threshold: Z-Score 매수 신호 기준 (기본 1.0)
            horizons: 평가 기간 (개월)

        Returns: DataFrame [종목코드, 신호월, Z, 3M수익률, 6M수익률, 12M수익률]
        """
        df = self.data.copy()
        if df.empty:
            return pd.DataFrame()

        print(f"\n  → 백테스트: Z > {signal_threshold} 신호 후 {horizons}개월 수익률")

        all_results = []
        for code, grp in df.groupby('종목코드'):
            grp = grp.sort_values('기준월').reset_index(drop=True)
            series = grp['Trailing수익률']
            prices = grp['수정종가']

            if len(series) < 48:
                continue

            mu = series.mean()
            sigma = series.std()
            if sigma <= 0.3:
                continue

            z_scores = (series - mu) / sigma

            # 신호 발생 시점 탐색
            for i, z in enumerate(z_scores):
                if z > signal_threshold:
                    signal_date = grp.iloc[i]['기준월']
                    signal_price = prices.iloc[i]
                    if signal_price <= 0:
                        continue

                    result = {
                        '종목코드': code,
                        '종목명': grp.iloc[i].get('종목명', code),
                        '신호월': signal_date,
                        'Z-Score': round(z, 2),
                        '신호시주가': signal_price,
                    }

                    for h in horizons:
                        target_idx = i + h
                        if target_idx < len(prices):
                            future_price = prices.iloc[target_idx]
                            ret = (future_price - signal_price) / signal_price * 100
                            result[f'{h}M수익률'] = round(ret, 2)
                        else:
                            result[f'{h}M수익률'] = None

                    all_results.append(result)

        df_bt = pd.DataFrame(all_results)

        if not df_bt.empty:
            print(f"  → 신호 발생: {len(df_bt)}건")
            for h in horizons:
                col = f'{h}M수익률'
                valid = df_bt[col].dropna()
                if len(valid) > 0:
                    avg = valid.mean()
                    winrate = (valid > 0).mean() * 100
                    print(f"     {h}M: 평균 {avg:+.1f}%, 승률 {winrate:.0f}% ({len(valid)}건)")
        else:
            print("  → 신호 발생 건수 0건")

        return df_bt

    def current_diagnosis(self):
        """현재 시점 매수/매도 진단표"""
        if self.bands is None:
            self.calc_bands()

        if self.bands.empty:
            return pd.DataFrame()

        # 배당성장률 추가
        div_data = self.data.copy()
        growth_map = {}
        for code, grp in div_data.groupby('종목코드'):
            years = grp.sort_values('기준월')
            if len(years) >= 48:  # 최소 4년
                first_div = years.iloc[0]['T12M배당']
                last_div = years.iloc[-1]['T12M배당']
                if first_div > 0:
                    growth = (last_div - first_div) / first_div * 100
                    growth_map[code] = round(growth, 1)

        diagnosis = self.bands.copy()
        diagnosis['배당성장률%'] = diagnosis['종목코드'].map(growth_map)

        # 매수 추천 순위 (Z-Score 높을수록 매수 매력)
        buy_candidates = diagnosis[diagnosis['신호'].isin(['매수', '강력매수'])]
        if not buy_candidates.empty:
            buy_candidates = buy_candidates.sort_values('Z-Score', ascending=False)
            print(f"\n  → 매수 신호 종목 ({len(buy_candidates)}개):")
            for _, row in buy_candidates.head(10).iterrows():
                print(f"     {row['종목명']}: 수익률 {row['현재수익률']}%, "
                      f"Z={row['Z-Score']}, 신호={row['신호']}")

        return diagnosis


def validate_phase6(bands, backtest_df):
    """Phase 6 Gate 검증"""
    if bands.empty:
        return validate_gate("Phase 6", [("밴드 데이터 존재", False, "비어있음")])

    valid_bands = bands[bands['Z-Score'].notna()]
    n_valid = len(valid_bands)

    buy_signals = bands[bands['신호'].isin(['매수', '강력매수'])]
    n_buy = len(buy_signals)

    # 백테스트 승률
    if not backtest_df.empty and '6M수익률' in backtest_df.columns:
        valid_6m = backtest_df['6M수익률'].dropna()
        winrate_6m = (valid_6m > 0).mean() * 100 if len(valid_6m) > 0 else 0
    else:
        winrate_6m = 0

    checks = [
        ("밴드 적용 종목 >= 10", n_valid >= 10,
         f"현재 {n_valid}개"),
        ("매수 신호 종목 >= 3", n_buy >= 3,
         f"현재 {n_buy}개 (Z 임계값 완화 고려)"),
        ("백테스트 6M 승률 > 50%", winrate_6m > 50,
         f"현재 {winrate_6m:.0f}%"),
    ]
    return validate_gate("Phase 6", checks)


if __name__ == "__main__":
    print("buy_strategy.py — Phase 6에서 사용. main_pipeline.py에서 실행하세요.")
